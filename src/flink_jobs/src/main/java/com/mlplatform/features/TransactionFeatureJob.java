package com.mlplatform.features;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * Flink streaming job: real-time feature engineering for transaction anomaly detection.
 *
 * <h2>Pipeline topology:</h2>
 * <pre>
 *   Kafka("raw-transactions")
 *       │
 *       ├──► [KeyBy userId] ──► SlidingWindow(5m, slide=1m)  ──► Aggregate ──► Kafka("features-5m")
 *       │                   ──► SlidingWindow(15m, slide=5m)  ──► Aggregate ──► Kafka("features-15m")
 *       │                   ──► SlidingWindow(1h, slide=15m)  ──► Aggregate ──► Kafka("features-1h")
 *       │
 *       └──► [KeyBy userId] ──► VelocityEnricher (stateful) ──► Kafka("enriched-transactions")
 *                                                             ──► Filesystem (Parquet-ready)
 * </pre>
 *
 * <h2>Feature groups computed:</h2>
 * <ul>
 *   <li><b>Volume:</b> txn count, sum, avg, max, min per window</li>
 *   <li><b>Diversity:</b> unique categories, merchants, devices per window</li>
 *   <li><b>Velocity (in-window):</b> avg/min inter-transaction time</li>
 *   <li><b>Velocity (cross-window):</b> time since user's last txn (stateful)</li>
 *   <li><b>Risk signals:</b> anomaly count, max/avg ratio, high-amount count</li>
 * </ul>
 *
 * <h2>Usage:</h2>
 * <pre>
 *   flink run flink-feature-jobs-0.1.0.jar \
 *       --kafka.bootstrap-servers localhost:9092 \
 *       --kafka.input-topic raw-transactions \
 *       --output.path s3a://ml-feature-platform/streaming/enriched/
 * </pre>
 */
public class TransactionFeatureJob {

    // ── Default configuration ──
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_INPUT_TOPIC = "raw-transactions";
    private static final String DEFAULT_GROUP_ID = "flink-feature-pipeline";
    private static final String DEFAULT_OUTPUT_PATH = "/tmp/ml-platform/streaming/enriched";

    // Kafka output topics for windowed features
    private static final String TOPIC_FEATURES_5M = "features-5m";
    private static final String TOPIC_FEATURES_15M = "features-15m";
    private static final String TOPIC_FEATURES_1H = "features-1h";
    private static final String TOPIC_ENRICHED = "enriched-transactions";

    public static void main(String[] args) throws Exception {
        // ── 1. Parse CLI parameters ──
        ParameterTool params = ParameterTool.fromArgs(args);
        String bootstrapServers = params.get("kafka.bootstrap-servers", DEFAULT_BOOTSTRAP_SERVERS);
        String inputTopic = params.get("kafka.input-topic", DEFAULT_INPUT_TOPIC);
        String groupId = params.get("kafka.group-id", DEFAULT_GROUP_ID);
        String outputPath = params.get("output.path", DEFAULT_OUTPUT_PATH);

        // ── 2. Set up execution environment ──
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpointing: exactly-once, every 30s, with 10s min pause
        env.enableCheckpointing(30_000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10_000);
        env.getCheckpointConfig().setCheckpointTimeout(60_000);

        // Make parameters available to all operators
        env.getConfig().setGlobalJobParameters(params);

        // ── 3. Kafka source ──
        KafkaSource<Transaction> kafkaSource = KafkaSource.<Transaction>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new TransactionDeserializer())
                .build();

        // Event-time with bounded out-of-orderness (5s tolerance for late events)
        WatermarkStrategy<Transaction> watermarkStrategy = WatermarkStrategy
                .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((txn, recordTimestamp) -> txn.getEventTimeMillis())
                .withIdleness(Duration.ofMinutes(1));  // handle idle partitions

        DataStream<Transaction> transactions = env
                .fromSource(kafkaSource, watermarkStrategy, "Kafka: raw-transactions")
                .filter(txn -> txn != null)  // drop deserialization failures
                .name("Filter nulls");

        // ── 4. Key by user ──
        KeyedStream<Transaction, String> keyedByUser = transactions
                .keyBy(Transaction::getUserId);

        // ── 5. Windowed feature aggregation ──
        // Each window size produces an independent feature stream.
        // Sliding windows ensure feature freshness (windows overlap).

        // 5m window, sliding every 1m
        SingleOutputStreamOperator<UserFeatureWindow> features5m = keyedByUser
                .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                .aggregate(new TransactionAggregator(), new FeatureWindowEnricher("5m"))
                .name("Window: 5m features")
                .uid("window-5m-features");

        // 15m window, sliding every 5m
        SingleOutputStreamOperator<UserFeatureWindow> features15m = keyedByUser
                .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.minutes(5)))
                .aggregate(new TransactionAggregator(), new FeatureWindowEnricher("15m"))
                .name("Window: 15m features")
                .uid("window-15m-features");

        // 1h window, sliding every 15m
        SingleOutputStreamOperator<UserFeatureWindow> features1h = keyedByUser
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(15)))
                .aggregate(new TransactionAggregator(), new FeatureWindowEnricher("1h"))
                .name("Window: 1h features")
                .uid("window-1h-features");

        // ── 6. Stateful velocity enrichment ──
        SingleOutputStreamOperator<VelocityEnricher.EnrichedTransaction> enrichedStream = keyedByUser
                .process(new VelocityEnricher())
                .name("Velocity enrichment")
                .uid("velocity-enricher");

        // ── 7. Sinks ──

        // 7a. Windowed features → Kafka topics (consumed by Feast / downstream)
        features5m.sinkTo(buildKafkaFeatureSink(bootstrapServers, TOPIC_FEATURES_5M))
                .name("Sink: features-5m → Kafka");

        features15m.sinkTo(buildKafkaFeatureSink(bootstrapServers, TOPIC_FEATURES_15M))
                .name("Sink: features-15m → Kafka");

        features1h.sinkTo(buildKafkaFeatureSink(bootstrapServers, TOPIC_FEATURES_1H))
                .name("Sink: features-1h → Kafka");

        // 7b. Enriched transactions → Kafka (for real-time consumers)
        enrichedStream.sinkTo(buildKafkaEnrichedSink(bootstrapServers))
                .name("Sink: enriched-transactions → Kafka");

        // 7c. Enriched transactions → Filesystem (for Delta Lake / batch processing)
        // Files roll every 5 min or 128 MB, whichever comes first.
        StreamingFileSink<String> fileSink = StreamingFileSink
                .forRowFormat(
                        new Path(outputPath),
                        new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(5))
                                .withInactivityInterval(Duration.ofMinutes(2))
                                .withMaxPartSize(MemorySize.ofMebiBytes(128))
                                .build())
                .build();

        enrichedStream
                .map(VelocityEnricher.EnrichedTransaction::toString)
                .addSink(fileSink)
                .name("Sink: enriched → Filesystem");

        // ── 8. Execute ──
        env.execute("Transaction Feature Pipeline");
    }

    /**
     * Build a Kafka sink for windowed UserFeatureWindow records.
     */
    private static KafkaSink<UserFeatureWindow> buildKafkaFeatureSink(
            String bootstrapServers, String topic) {

        return KafkaSink.<UserFeatureWindow>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new UserFeatureSerializer())
                                .build())
                .build();
    }

    /**
     * Build a Kafka sink for velocity-enriched transactions.
     */
    private static KafkaSink<VelocityEnricher.EnrichedTransaction> buildKafkaEnrichedSink(
            String bootstrapServers) {

        return KafkaSink.<VelocityEnricher.EnrichedTransaction>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(TOPIC_ENRICHED)
                                .setValueSerializationSchema(new EnrichedTransactionSerializer())
                                .build())
                .build();
    }
}

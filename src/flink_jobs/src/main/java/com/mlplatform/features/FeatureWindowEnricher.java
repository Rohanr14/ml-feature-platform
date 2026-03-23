package com.mlplatform.features;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Enriches the pre-aggregated {@link FeatureAccumulator} with window metadata
 * (start/end times, user key) to produce the final {@link UserFeatureWindow}.
 *
 * Used in combination with {@link TransactionAggregator} via:
 *   .aggregate(aggregator, processWindowFunction)
 *
 * This two-phase pattern gives us both incremental aggregation (O(1) memory)
 * and access to window metadata (start, end, key).
 */
public class FeatureWindowEnricher
        extends ProcessWindowFunction<FeatureAccumulator, UserFeatureWindow, String, TimeWindow> {

    private final String windowLabel;

    /**
     * @param windowLabel human-readable window size label ("5m", "15m", "1h")
     */
    public FeatureWindowEnricher(String windowLabel) {
        this.windowLabel = windowLabel;
    }

    @Override
    public void process(String userId,
                        ProcessWindowFunction<FeatureAccumulator, UserFeatureWindow, String, TimeWindow>.Context ctx,
                        Iterable<FeatureAccumulator> accumulators,
                        Collector<UserFeatureWindow> out) {

        // Exactly one accumulator per window (from aggregate step)
        FeatureAccumulator acc = accumulators.iterator().next();

        TimeWindow window = ctx.window();

        UserFeatureWindow features = new UserFeatureWindow();
        features.setUserId(userId);
        features.setWindowStartMs(window.getStart());
        features.setWindowEndMs(window.getEnd());
        features.setWindowLabel(windowLabel);

        // Volume
        features.setTxnCount(acc.count);
        features.setTxnSum(acc.sum);
        features.setTxnAvg(acc.getAvg());
        features.setTxnMax(acc.count > 0 ? acc.max : 0.0);
        features.setTxnMin(acc.count > 0 ? acc.min : 0.0);

        // Diversity
        features.setUniqueCategories(acc.categories.size());
        features.setUniqueMerchants(acc.merchants.size());
        features.setUniqueDevices(acc.devices.size());

        // Velocity
        features.setAvgInterTxnMs(acc.getAvgInterTxnMs());
        features.setMinInterTxnMs(acc.getMinInterTxnMs());

        // Risk
        features.setAnomalyCount(acc.anomalyCount);
        features.setMaxAmountRatio(acc.getMaxAmountRatio());
        features.setHighAmountCount(acc.highAmountCount);

        // Metadata
        features.setComputedAtMs(System.currentTimeMillis());

        out.collect(features);
    }
}

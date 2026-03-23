package com.mlplatform.features;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * Stateful per-user velocity tracker using Flink keyed state.
 *
 * For each incoming transaction, emits an {@link EnrichedTransaction}
 * that includes the time elapsed since that user's previous transaction.
 * This captures cross-window velocity — a critical fraud signal
 * (e.g., user transacted 3 seconds after their last txn from an hour ago).
 *
 * State is keyed by user_id and checkpointed automatically by Flink.
 */
public class VelocityEnricher
        extends KeyedProcessFunction<String, Transaction, VelocityEnricher.EnrichedTransaction> {

    private transient ValueState<Long> lastTxnTimestamp;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                "lastTxnTimestamp",
                Types.LONG
        );
        lastTxnTimestamp = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(Transaction txn,
                               KeyedProcessFunction<String, Transaction, EnrichedTransaction>.Context ctx,
                               Collector<EnrichedTransaction> out) throws Exception {

        long currentEventTime = txn.getEventTimeMillis();
        Long previousTimestamp = lastTxnTimestamp.value();

        long msSinceLastTxn = -1;  // -1 indicates first transaction for this user
        if (previousTimestamp != null) {
            msSinceLastTxn = currentEventTime - previousTimestamp;
        }

        lastTxnTimestamp.update(currentEventTime);

        out.collect(new EnrichedTransaction(txn, msSinceLastTxn));
    }

    /**
     * Transaction enriched with cross-window velocity information.
     */
    public static class EnrichedTransaction implements Serializable {

        private static final long serialVersionUID = 1L;

        @JsonProperty("transaction")
        private final Transaction transaction;

        @JsonProperty("ms_since_last_txn")
        private final long msSinceLastTxn;

        @JsonProperty("is_first_txn")
        private final boolean isFirstTxn;

        public EnrichedTransaction(Transaction transaction, long msSinceLastTxn) {
            this.transaction = transaction;
            this.msSinceLastTxn = msSinceLastTxn;
            this.isFirstTxn = msSinceLastTxn < 0;
        }

        public Transaction getTransaction() { return transaction; }
        public long getMsSinceLastTxn() { return msSinceLastTxn; }
        public boolean isFirstTxn() { return isFirstTxn; }

        @Override
        public String toString() {
            return "EnrichedTransaction{" +
                    "userId='" + transaction.getUserId() + '\'' +
                    ", amount=" + transaction.getAmount() +
                    ", msSinceLast=" + msSinceLastTxn +
                    ", first=" + isFirstTxn +
                    '}';
        }
    }
}

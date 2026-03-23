package com.mlplatform.features;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Incremental aggregation of transactions within a window.
 *
 * This is the core feature computation logic. Flink calls:
 *   1. createAccumulator() — once per window instance
 *   2. add() — for each incoming Transaction
 *   3. getResult() — when the window fires
 *   4. merge() — when merging partial window results (e.g. session windows)
 *
 * The accumulator tracks running stats, distinct sets, and timestamps.
 * The result is a fully-populated {@link UserFeatureWindow}.
 */
public class TransactionAggregator
        implements AggregateFunction<Transaction, FeatureAccumulator, FeatureAccumulator> {

    @Override
    public FeatureAccumulator createAccumulator() {
        return new FeatureAccumulator();
    }

    @Override
    public FeatureAccumulator add(Transaction txn, FeatureAccumulator acc) {
        acc.add(txn);
        return acc;
    }

    @Override
    public FeatureAccumulator getResult(FeatureAccumulator acc) {
        return acc;
    }

    @Override
    public FeatureAccumulator merge(FeatureAccumulator a, FeatureAccumulator b) {
        return a.merge(b);
    }
}

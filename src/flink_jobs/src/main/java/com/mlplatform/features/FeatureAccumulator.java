package com.mlplatform.features;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Mutable accumulator used by {@link TransactionAggregator}.
 *
 * Tracks running statistics, unique sets, and ordered timestamps
 * within a single window for a single user. Designed to be
 * mergeable for session window use cases.
 */
public class FeatureAccumulator implements Serializable {

    private static final long serialVersionUID = 1L;

    // Volume
    long count = 0;
    double sum = 0.0;
    double max = Double.MIN_VALUE;
    double min = Double.MAX_VALUE;

    // Diversity (sets for exact distinct counts within window)
    Set<String> categories = new HashSet<>();
    Set<String> merchants = new HashSet<>();
    Set<String> devices = new HashSet<>();

    // Velocity (ordered timestamps to compute inter-arrival times)
    List<Long> eventTimestamps = new ArrayList<>();

    // Risk
    long anomalyCount = 0;
    long highAmountCount = 0;  // txns > $300

    static final double HIGH_AMOUNT_THRESHOLD = 300.0;

    /**
     * Add a single transaction to this accumulator.
     */
    public void add(Transaction txn) {
        count++;
        sum += txn.getAmount();
        max = Math.max(max, txn.getAmount());
        min = Math.min(min, txn.getAmount());

        categories.add(txn.getCategory());
        merchants.add(txn.getMerchantId());
        devices.add(txn.getDeviceType());

        eventTimestamps.add(txn.getEventTimeMillis());

        if (txn.isAnomaly()) {
            anomalyCount++;
        }
        if (txn.getAmount() > HIGH_AMOUNT_THRESHOLD) {
            highAmountCount++;
        }
    }

    /**
     * Merge another accumulator into this one (for merging partial windows).
     */
    public FeatureAccumulator merge(FeatureAccumulator other) {
        this.count += other.count;
        this.sum += other.sum;
        this.max = Math.max(this.max, other.max);
        this.min = Math.min(this.min, other.min);
        this.categories.addAll(other.categories);
        this.merchants.addAll(other.merchants);
        this.devices.addAll(other.devices);
        this.eventTimestamps.addAll(other.eventTimestamps);
        this.anomalyCount += other.anomalyCount;
        this.highAmountCount += other.highAmountCount;
        return this;
    }

    public double getAvg() {
        return count == 0 ? 0.0 : sum / count;
    }

    /**
     * Compute average inter-transaction time in milliseconds.
     * Returns 0 if fewer than 2 events.
     */
    public double getAvgInterTxnMs() {
        if (eventTimestamps.size() < 2) return 0.0;
        List<Long> sorted = eventTimestamps.stream().sorted().toList();
        long totalGap = 0;
        for (int i = 1; i < sorted.size(); i++) {
            totalGap += sorted.get(i) - sorted.get(i - 1);
        }
        return (double) totalGap / (sorted.size() - 1);
    }

    /**
     * Compute minimum inter-transaction time (fastest burst).
     * Returns 0 if fewer than 2 events.
     */
    public double getMinInterTxnMs() {
        if (eventTimestamps.size() < 2) return 0.0;
        List<Long> sorted = eventTimestamps.stream().sorted().toList();
        long minGap = Long.MAX_VALUE;
        for (int i = 1; i < sorted.size(); i++) {
            minGap = Math.min(minGap, sorted.get(i) - sorted.get(i - 1));
        }
        return (double) minGap;
    }

    /**
     * Max amount / avg amount — detects single-txn spikes.
     */
    public double getMaxAmountRatio() {
        double avg = getAvg();
        return avg == 0.0 ? 0.0 : max / avg;
    }
}

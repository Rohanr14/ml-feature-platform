package com.mlplatform.features;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * Output of windowed feature aggregation for a single user.
 *
 * Each instance represents the aggregated features for one user
 * across a specific time window (5m, 15m, or 1h).
 */
public class UserFeatureWindow implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("window_start")
    private long windowStartMs;

    @JsonProperty("window_end")
    private long windowEndMs;

    @JsonProperty("window_label")
    private String windowLabel;           // "5m", "15m", "1h"

    // ── Volume features ──

    @JsonProperty("txn_count")
    private long txnCount;

    @JsonProperty("txn_sum")
    private double txnSum;

    @JsonProperty("txn_avg")
    private double txnAvg;

    @JsonProperty("txn_max")
    private double txnMax;

    @JsonProperty("txn_min")
    private double txnMin;

    // ── Diversity features ──

    @JsonProperty("unique_categories")
    private int uniqueCategories;

    @JsonProperty("unique_merchants")
    private int uniqueMerchants;

    @JsonProperty("unique_devices")
    private int uniqueDevices;

    // ── Velocity features ──

    @JsonProperty("avg_inter_txn_ms")
    private double avgInterTxnMs;         // avg gap between txns in window

    @JsonProperty("min_inter_txn_ms")
    private double minInterTxnMs;         // fastest burst

    // ── Risk signals ──

    @JsonProperty("anomaly_count")
    private long anomalyCount;

    @JsonProperty("max_amount_ratio")
    private double maxAmountRatio;        // max / avg — spike detection

    @JsonProperty("high_amount_count")
    private long highAmountCount;         // txns > $300

    // ── Metadata ──

    @JsonProperty("computed_at")
    private long computedAtMs;

    // ── Constructors ──

    public UserFeatureWindow() {
    }

    // ── Getters / Setters ──

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public long getWindowStartMs() { return windowStartMs; }
    public void setWindowStartMs(long windowStartMs) { this.windowStartMs = windowStartMs; }

    public long getWindowEndMs() { return windowEndMs; }
    public void setWindowEndMs(long windowEndMs) { this.windowEndMs = windowEndMs; }

    public String getWindowLabel() { return windowLabel; }
    public void setWindowLabel(String windowLabel) { this.windowLabel = windowLabel; }

    public long getTxnCount() { return txnCount; }
    public void setTxnCount(long txnCount) { this.txnCount = txnCount; }

    public double getTxnSum() { return txnSum; }
    public void setTxnSum(double txnSum) { this.txnSum = txnSum; }

    public double getTxnAvg() { return txnAvg; }
    public void setTxnAvg(double txnAvg) { this.txnAvg = txnAvg; }

    public double getTxnMax() { return txnMax; }
    public void setTxnMax(double txnMax) { this.txnMax = txnMax; }

    public double getTxnMin() { return txnMin; }
    public void setTxnMin(double txnMin) { this.txnMin = txnMin; }

    public int getUniqueCategories() { return uniqueCategories; }
    public void setUniqueCategories(int uniqueCategories) { this.uniqueCategories = uniqueCategories; }

    public int getUniqueMerchants() { return uniqueMerchants; }
    public void setUniqueMerchants(int uniqueMerchants) { this.uniqueMerchants = uniqueMerchants; }

    public int getUniqueDevices() { return uniqueDevices; }
    public void setUniqueDevices(int uniqueDevices) { this.uniqueDevices = uniqueDevices; }

    public double getAvgInterTxnMs() { return avgInterTxnMs; }
    public void setAvgInterTxnMs(double avgInterTxnMs) { this.avgInterTxnMs = avgInterTxnMs; }

    public double getMinInterTxnMs() { return minInterTxnMs; }
    public void setMinInterTxnMs(double minInterTxnMs) { this.minInterTxnMs = minInterTxnMs; }

    public long getAnomalyCount() { return anomalyCount; }
    public void setAnomalyCount(long anomalyCount) { this.anomalyCount = anomalyCount; }

    public double getMaxAmountRatio() { return maxAmountRatio; }
    public void setMaxAmountRatio(double maxAmountRatio) { this.maxAmountRatio = maxAmountRatio; }

    public long getHighAmountCount() { return highAmountCount; }
    public void setHighAmountCount(long highAmountCount) { this.highAmountCount = highAmountCount; }

    public long getComputedAtMs() { return computedAtMs; }
    public void setComputedAtMs(long computedAtMs) { this.computedAtMs = computedAtMs; }

    @Override
    public String toString() {
        return "UserFeatureWindow{" +
                "userId='" + userId + '\'' +
                ", window=" + windowLabel +
                ", count=" + txnCount +
                ", sum=" + String.format("%.2f", txnSum) +
                ", avg=" + String.format("%.2f", txnAvg) +
                ", max=" + String.format("%.2f", txnMax) +
                ", anomalies=" + anomalyCount +
                '}';
    }
}

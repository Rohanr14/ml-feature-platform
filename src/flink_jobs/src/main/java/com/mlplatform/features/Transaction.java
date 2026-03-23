package com.mlplatform.features;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * POJO representing a raw transaction event from Kafka.
 *
 * Mirrors the Python-side schema in src/data_generator/schemas.py.
 * All fields use snake_case JSON mapping to match the producer format.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Transaction implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("transaction_id")
    private String transactionId;

    @JsonProperty("user_id")
    private String userId;

    private double amount;

    private String currency;

    private String category;

    @JsonProperty("merchant_id")
    private String merchantId;

    private String timestamp;

    @JsonProperty("is_anomaly")
    private boolean isAnomaly;

    @JsonProperty("session_id")
    private String sessionId;

    @JsonProperty("device_type")
    private String deviceType;

    @JsonProperty("location_country")
    private String locationCountry;

    // ── Constructors ──

    public Transaction() {
    }

    public Transaction(String transactionId, String userId, double amount,
                       String currency, String category, String merchantId,
                       String timestamp, boolean isAnomaly, String sessionId,
                       String deviceType, String locationCountry) {
        this.transactionId = transactionId;
        this.userId = userId;
        this.amount = amount;
        this.currency = currency;
        this.category = category;
        this.merchantId = merchantId;
        this.timestamp = timestamp;
        this.isAnomaly = isAnomaly;
        this.sessionId = sessionId;
        this.deviceType = deviceType;
        this.locationCountry = locationCountry;
    }

    // ── Derived ──

    /**
     * Parse the ISO-8601 timestamp into epoch millis for Flink event time.
     */
    public long getEventTimeMillis() {
        return Instant.parse(timestamp).toEpochMilli();
    }

    // ── Getters ──

    public String getTransactionId() { return transactionId; }
    public String getUserId() { return userId; }
    public double getAmount() { return amount; }
    public String getCurrency() { return currency; }
    public String getCategory() { return category; }
    public String getMerchantId() { return merchantId; }
    public String getTimestamp() { return timestamp; }
    public boolean isAnomaly() { return isAnomaly; }
    public String getSessionId() { return sessionId; }
    public String getDeviceType() { return deviceType; }
    public String getLocationCountry() { return locationCountry; }

    // ── Setters ──

    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
    public void setUserId(String userId) { this.userId = userId; }
    public void setAmount(double amount) { this.amount = amount; }
    public void setCurrency(String currency) { this.currency = currency; }
    public void setCategory(String category) { this.category = category; }
    public void setMerchantId(String merchantId) { this.merchantId = merchantId; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    public void setAnomaly(boolean anomaly) { isAnomaly = anomaly; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }
    public void setDeviceType(String deviceType) { this.deviceType = deviceType; }
    public void setLocationCountry(String locationCountry) { this.locationCountry = locationCountry; }

    // ── equals / hashCode / toString ──

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transaction that = (Transaction) o;
        return Objects.equals(transactionId, that.transactionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId);
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "txnId='" + transactionId + '\'' +
                ", userId='" + userId + '\'' +
                ", amount=" + amount +
                ", category='" + category + '\'' +
                ", ts='" + timestamp + '\'' +
                ", anomaly=" + isAnomaly +
                '}';
    }
}

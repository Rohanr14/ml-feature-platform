package com.mlplatform.features;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link FeatureAccumulator}.
 * Validates aggregation correctness without needing Flink runtime.
 */
class FeatureAccumulatorTest {

    private Transaction makeTxn(String userId, double amount, String category,
                                String merchantId, boolean anomaly, String timestamp) {
        return new Transaction(
                "txn-" + System.nanoTime(), userId, amount, "USD",
                category, merchantId, timestamp, anomaly,
                "session-1", "mobile", "US"
        );
    }

    @Test
    void testSingleTransaction() {
        FeatureAccumulator acc = new FeatureAccumulator();
        acc.add(makeTxn("user_1", 100.0, "electronics", "m_1", false,
                "2025-01-15T10:00:00Z"));

        assertEquals(1, acc.count);
        assertEquals(100.0, acc.sum, 0.001);
        assertEquals(100.0, acc.max, 0.001);
        assertEquals(100.0, acc.min, 0.001);
        assertEquals(100.0, acc.getAvg(), 0.001);
        assertEquals(1, acc.categories.size());
        assertEquals(1, acc.merchants.size());
        assertEquals(0, acc.anomalyCount);
        assertEquals(0, acc.highAmountCount);
    }

    @Test
    void testMultipleTransactions() {
        FeatureAccumulator acc = new FeatureAccumulator();
        acc.add(makeTxn("user_1", 50.0, "electronics", "m_1", false, "2025-01-15T10:00:00Z"));
        acc.add(makeTxn("user_1", 150.0, "clothing", "m_2", false, "2025-01-15T10:01:00Z"));
        acc.add(makeTxn("user_1", 75.0, "electronics", "m_1", true, "2025-01-15T10:02:00Z"));

        assertEquals(3, acc.count);
        assertEquals(275.0, acc.sum, 0.001);
        assertEquals(150.0, acc.max, 0.001);
        assertEquals(50.0, acc.min, 0.001);
        assertEquals(91.667, acc.getAvg(), 0.01);
        assertEquals(2, acc.categories.size());  // electronics, clothing
        assertEquals(2, acc.merchants.size());    // m_1, m_2
        assertEquals(1, acc.anomalyCount);
        assertEquals(0, acc.highAmountCount);     // none > $300
    }

    @Test
    void testHighAmountCounting() {
        FeatureAccumulator acc = new FeatureAccumulator();
        acc.add(makeTxn("user_1", 100.0, "books", "m_1", false, "2025-01-15T10:00:00Z"));
        acc.add(makeTxn("user_1", 500.0, "jewelry", "m_2", true, "2025-01-15T10:01:00Z"));
        acc.add(makeTxn("user_1", 1000.0, "electronics", "m_3", true, "2025-01-15T10:02:00Z"));

        assertEquals(2, acc.highAmountCount);   // 500 and 1000
        assertEquals(2, acc.anomalyCount);
    }

    @Test
    void testInterTransactionTime() {
        FeatureAccumulator acc = new FeatureAccumulator();

        // 3 transactions, 60s apart
        acc.add(makeTxn("user_1", 50.0, "books", "m_1", false, "2025-01-15T10:00:00Z"));
        acc.add(makeTxn("user_1", 60.0, "books", "m_1", false, "2025-01-15T10:01:00Z"));
        acc.add(makeTxn("user_1", 70.0, "books", "m_1", false, "2025-01-15T10:02:00Z"));

        assertEquals(60_000.0, acc.getAvgInterTxnMs(), 0.001);  // avg = 60s
        assertEquals(60_000.0, acc.getMinInterTxnMs(), 0.001);  // min = 60s (all equal)
    }

    @Test
    void testInterTransactionTimeUneven() {
        FeatureAccumulator acc = new FeatureAccumulator();

        acc.add(makeTxn("user_1", 50.0, "books", "m_1", false, "2025-01-15T10:00:00Z"));
        acc.add(makeTxn("user_1", 60.0, "books", "m_1", false, "2025-01-15T10:00:10Z")); // +10s
        acc.add(makeTxn("user_1", 70.0, "books", "m_1", false, "2025-01-15T10:01:00Z")); // +50s

        // Gaps: 10s, 50s → avg = 30s, min = 10s
        assertEquals(30_000.0, acc.getAvgInterTxnMs(), 0.001);
        assertEquals(10_000.0, acc.getMinInterTxnMs(), 0.001);
    }

    @Test
    void testSingleEventVelocity() {
        FeatureAccumulator acc = new FeatureAccumulator();
        acc.add(makeTxn("user_1", 50.0, "books", "m_1", false, "2025-01-15T10:00:00Z"));

        // Can't compute velocity with only 1 event
        assertEquals(0.0, acc.getAvgInterTxnMs());
        assertEquals(0.0, acc.getMinInterTxnMs());
    }

    @Test
    void testMaxAmountRatio() {
        FeatureAccumulator acc = new FeatureAccumulator();
        acc.add(makeTxn("user_1", 10.0, "books", "m_1", false, "2025-01-15T10:00:00Z"));
        acc.add(makeTxn("user_1", 10.0, "books", "m_1", false, "2025-01-15T10:01:00Z"));
        acc.add(makeTxn("user_1", 100.0, "books", "m_1", false, "2025-01-15T10:02:00Z"));

        // avg = 40, max = 100, ratio = 2.5
        assertEquals(2.5, acc.getMaxAmountRatio(), 0.001);
    }

    @Test
    void testMerge() {
        FeatureAccumulator acc1 = new FeatureAccumulator();
        acc1.add(makeTxn("user_1", 50.0, "electronics", "m_1", false, "2025-01-15T10:00:00Z"));
        acc1.add(makeTxn("user_1", 100.0, "clothing", "m_2", true, "2025-01-15T10:01:00Z"));

        FeatureAccumulator acc2 = new FeatureAccumulator();
        acc2.add(makeTxn("user_1", 200.0, "books", "m_3", false, "2025-01-15T10:02:00Z"));

        FeatureAccumulator merged = acc1.merge(acc2);

        assertEquals(3, merged.count);
        assertEquals(350.0, merged.sum, 0.001);
        assertEquals(200.0, merged.max, 0.001);
        assertEquals(50.0, merged.min, 0.001);
        assertEquals(3, merged.categories.size());
        assertEquals(3, merged.merchants.size());
        assertEquals(1, merged.anomalyCount);
        assertEquals(3, merged.eventTimestamps.size());
    }

    @Test
    void testEmptyAccumulator() {
        FeatureAccumulator acc = new FeatureAccumulator();

        assertEquals(0, acc.count);
        assertEquals(0.0, acc.sum);
        assertEquals(0.0, acc.getAvg());
        assertEquals(0.0, acc.getAvgInterTxnMs());
        assertEquals(0.0, acc.getMinInterTxnMs());
        assertEquals(0.0, acc.getMaxAmountRatio());
    }
}

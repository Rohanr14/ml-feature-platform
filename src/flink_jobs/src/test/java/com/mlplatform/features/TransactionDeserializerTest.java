package com.mlplatform.features;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests JSON deserialization of Transaction objects.
 * Validates compatibility with the Python data generator's output format.
 */
class TransactionDeserializerTest {

    private ObjectMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
    }

    @Test
    void testDeserializePythonOutput() throws Exception {
        // This JSON mirrors exactly what txn_producer.py emits
        String json = """
                {
                    "transaction_id": "abc-123-def",
                    "user_id": "user_42",
                    "amount": 149.99,
                    "currency": "USD",
                    "category": "electronics",
                    "merchant_id": "merchant_7",
                    "timestamp": "2025-01-15T10:30:00.123456+00:00",
                    "is_anomaly": false,
                    "session_id": "sess-789",
                    "device_type": "mobile",
                    "location_country": "US"
                }
                """;

        Transaction txn = mapper.readValue(json, Transaction.class);

        assertEquals("abc-123-def", txn.getTransactionId());
        assertEquals("user_42", txn.getUserId());
        assertEquals(149.99, txn.getAmount(), 0.001);
        assertEquals("USD", txn.getCurrency());
        assertEquals("electronics", txn.getCategory());
        assertEquals("merchant_7", txn.getMerchantId());
        assertFalse(txn.isAnomaly());
        assertEquals("mobile", txn.getDeviceType());
        assertEquals("US", txn.getLocationCountry());
    }

    @Test
    void testDeserializeAnomalyTransaction() throws Exception {
        String json = """
                {
                    "transaction_id": "anomaly-001",
                    "user_id": "user_1",
                    "amount": 2500.00,
                    "currency": "USD",
                    "category": "jewelry",
                    "merchant_id": "merchant_99",
                    "timestamp": "2025-01-15T03:15:00+00:00",
                    "is_anomaly": true,
                    "session_id": "sess-suspicious",
                    "device_type": "desktop",
                    "location_country": "DE"
                }
                """;

        Transaction txn = mapper.readValue(json, Transaction.class);

        assertTrue(txn.isAnomaly());
        assertEquals(2500.00, txn.getAmount(), 0.001);
        assertEquals("jewelry", txn.getCategory());
    }

    @Test
    void testEventTimeExtraction() throws Exception {
        String json = """
                {
                    "transaction_id": "time-test",
                    "user_id": "user_1",
                    "amount": 10.0,
                    "currency": "USD",
                    "category": "books",
                    "merchant_id": "merchant_1",
                    "timestamp": "2025-01-15T12:00:00Z",
                    "is_anomaly": false,
                    "session_id": "sess-1",
                    "device_type": "mobile",
                    "location_country": "US"
                }
                """;

        Transaction txn = mapper.readValue(json, Transaction.class);
        long epochMs = txn.getEventTimeMillis();

        // 2025-01-15T12:00:00Z = 1736942400000 ms
        assertEquals(1736942400000L, epochMs);
    }

    @Test
    void testIgnoresUnknownFields() throws Exception {
        // Python might add fields in the future; Java should not break
        String json = """
                {
                    "transaction_id": "future-proof",
                    "user_id": "user_1",
                    "amount": 10.0,
                    "currency": "USD",
                    "category": "books",
                    "merchant_id": "merchant_1",
                    "timestamp": "2025-01-15T12:00:00Z",
                    "is_anomaly": false,
                    "session_id": "sess-1",
                    "device_type": "mobile",
                    "location_country": "US",
                    "brand_new_field": "should be ignored",
                    "another_field": 42
                }
                """;

        Transaction txn = mapper.readValue(json, Transaction.class);
        assertNotNull(txn);
        assertEquals("future-proof", txn.getTransactionId());
    }

    @Test
    void testFlinkDeserializerWrapper() throws Exception {
        String json = """
                {
                    "transaction_id": "flink-test",
                    "user_id": "user_1",
                    "amount": 75.50,
                    "currency": "USD",
                    "category": "clothing",
                    "merchant_id": "merchant_5",
                    "timestamp": "2025-06-01T08:30:00Z",
                    "is_anomaly": false,
                    "session_id": "sess-flink",
                    "device_type": "tablet",
                    "location_country": "CA"
                }
                """;

        TransactionDeserializer deserializer = new TransactionDeserializer();
        Transaction txn = deserializer.deserialize(json.getBytes());

        assertNotNull(txn);
        assertEquals("flink-test", txn.getTransactionId());
        assertEquals(75.50, txn.getAmount(), 0.001);
    }

    @Test
    void testFlinkDeserializerHandlesMalformed() throws Exception {
        TransactionDeserializer deserializer = new TransactionDeserializer();

        // Malformed JSON should return null, not throw
        Transaction result = deserializer.deserialize("not valid json".getBytes());
        assertNull(result);
    }
}

package com.mlplatform.features;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * Deserializes Kafka message values (JSON bytes) into {@link Transaction} objects.
 *
 * Uses Jackson with the JavaTime module for ISO-8601 timestamp parsing.
 * Malformed messages are logged and skipped (returns null → filtered downstream).
 */
public class TransactionDeserializer implements DeserializationSchema<Transaction> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper mapper;

    private ObjectMapper getMapper() {
        if (mapper == null) {
            mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
        }
        return mapper;
    }

    @Override
    public Transaction deserialize(byte[] message) throws IOException {
        try {
            return getMapper().readValue(message, Transaction.class);
        } catch (Exception e) {
            // Log and skip malformed records rather than crashing the job.
            // In production, route these to a dead-letter topic.
            System.err.println("Failed to deserialize transaction: " + e.getMessage());
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(Transaction nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Transaction> getProducedType() {
        return TypeInformation.of(Transaction.class);
    }
}

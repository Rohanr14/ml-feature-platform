package com.mlplatform.features;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * Serializes {@link VelocityEnricher.EnrichedTransaction} to JSON for Kafka sink.
 */
public class EnrichedTransactionSerializer
        implements SerializationSchema<VelocityEnricher.EnrichedTransaction> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper mapper;

    private ObjectMapper getMapper() {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        return mapper;
    }

    @Override
    public byte[] serialize(VelocityEnricher.EnrichedTransaction element) {
        try {
            return getMapper().writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize EnrichedTransaction", e);
        }
    }
}

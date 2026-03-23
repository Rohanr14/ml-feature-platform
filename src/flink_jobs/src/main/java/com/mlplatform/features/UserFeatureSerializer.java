package com.mlplatform.features;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * Serializes {@link UserFeatureWindow} to JSON bytes for Kafka sink.
 */
public class UserFeatureSerializer implements SerializationSchema<UserFeatureWindow> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper mapper;

    private ObjectMapper getMapper() {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        return mapper;
    }

    @Override
    public byte[] serialize(UserFeatureWindow element) {
        try {
            return getMapper().writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize UserFeatureWindow", e);
        }
    }
}

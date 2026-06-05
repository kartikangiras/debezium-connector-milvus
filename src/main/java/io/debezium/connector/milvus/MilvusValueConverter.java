/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import io.debezium.relational.ValueConverter;

/**
 * Converts Milvus native types to Kafka Connect schema types.
 *
 * <p>Mapping includes:</p>
 * <ul>
 *   <li>INT8 / INT16 / INT32 / INT64 → integer types</li>
 *   <li>FLOAT / DOUBLE → floating point</li>
 *   <li>BOOL → boolean</li>
 *   <li>STRING / VARCHAR → string</li>
 *   <li>JSON → string (JSON payload)</li>
 *   <li>VECTOR_FLOAT / VECTOR_BINARY → bytes (serialized vector)</li>
 *   <li>ARRAY → array of the element type</li>
 * </ul>
 */
public class MilvusValueConverter implements ValueConverter {

    private final MilvusConnectorConfig config;

    public MilvusValueConverter(MilvusConnectorConfig config) {
        this.config = config;
    }

    @Override
    public Object convert(Object value) {
        // TODO: map Milvus types to Connect types
        return value;
    }
}

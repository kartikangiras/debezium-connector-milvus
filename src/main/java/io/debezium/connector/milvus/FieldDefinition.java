/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.util.Objects;

import io.milvus.grpc.DataType;

/**
 * Immutable description of a single Milvus field/column used when registering a
 * collection schema.
 *
 * <p>
 * Bundles the field name, a representative sample value, and the Milvus
 * {@link DataType} into one explicit object. This avoids passing three loosely
 * tied parallel arrays through the schema registration API and makes the
 * ordering contract self-documenting.
 * </p>
 *
 * @see MilvusDatabaseSchema#registerCollection(String, String, java.util.List)
 */
public record FieldDefinition(String fieldName, Object sampleValue, DataType dataType) {

    public FieldDefinition {
        Objects.requireNonNull(fieldName, "fieldName must not be null");
        Objects.requireNonNull(dataType, "dataType must not be null");
    }
}

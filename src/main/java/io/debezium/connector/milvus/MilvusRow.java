/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.util.Objects;

import io.milvus.grpc.DataType;

/**
 * A single row of Milvus data, represented as three parallel arrays whose
 * ordering is always determined structurally — not by any {@code Map}
 * implementation's insertion-order contract.
 *
 * <ul>
 *   <li>{@link #fieldNames} — field names in column order</li>
 *   <li>{@link #fieldValues} — converted field values in the same column order</li>
 *   <li>{@link #fieldTypes} — Milvus {@link DataType} for each field in the same order</li>
 * </ul>
 *
 * <p>
 * All three arrays are guaranteed to have the same length. The arrays
 * themselves are shared across all rows produced from the same columnar
 * payload: {@link #fieldNames} and {@link #fieldTypes} are allocated once
 * per batch by {@link MilvusColumnarPivot}, while {@link #fieldValues} is
 * unique to each row instance.
 * </p>
 *
 * <p>
 * Callers must treat all arrays as read-only; no defensive copying is
 * performed for performance reasons.
 * </p>
 */
public final class MilvusRow {

    private final String[] fieldNames;
    private final Object[] fieldValues;
    private final DataType[] fieldTypes;

    /**
     * Constructs a {@code MilvusRow}.
     *
     * @param fieldNames  field names in column order; must not be null
     * @param fieldValues converted values in the same column order; must not be null
     *                    and must have the same length as {@code fieldNames}
     * @param fieldTypes  Milvus {@link DataType} for each field; must not be null
     *                    and must have the same length as {@code fieldNames}
     */
    public MilvusRow(String[] fieldNames, Object[] fieldValues, DataType[] fieldTypes) {
        this.fieldNames = Objects.requireNonNull(fieldNames, "fieldNames must not be null");
        this.fieldValues = Objects.requireNonNull(fieldValues, "fieldValues must not be null");
        this.fieldTypes = Objects.requireNonNull(fieldTypes, "fieldTypes must not be null");
    }

    /**
     * Returns the field names in column order. Shared across all rows in the same batch.
     */
    public String[] getFieldNames() {
        return fieldNames;
    }

    /**
     * Returns the converted field values in column order. Unique to this row.
     */
    public Object[] getFieldValues() {
        return fieldValues;
    }

    /**
     * Returns the Milvus {@link DataType} for each field in column order.
     * Shared across all rows in the same batch.
     */
    public DataType[] getFieldTypes() {
        return fieldTypes;
    }

    /**
     * Returns the number of fields (columns) in this row.
     */
    public int size() {
        return fieldNames.length;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("MilvusRow{");
        for (int i = 0; i < fieldNames.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(fieldNames[i]).append('=').append(fieldValues[i]);
        }
        return sb.append('}').toString();
    }
}

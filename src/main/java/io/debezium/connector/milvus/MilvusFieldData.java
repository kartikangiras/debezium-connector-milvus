/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.util.List;
import java.util.Objects;

import io.milvus.grpc.DataType;

/**
 * Transport-neutral, columnar view of a single Milvus field extracted from a
 * CDC insert payload.
 *
 * <p>
 * This decouples the wire-format parsing (protobuf {@code msgpack_batch} /
 * {@code proto_single}) from the row-oriented pivot logic. Both wire formats
 * populate {@code MilvusFieldData} instances and hand them to
 * {@link MilvusColumnarPivot}, so the pivot never touches protobuf or MsgPack
 * types directly.
 * </p>
 *
 * <p>
 * The {@code values} list holds one entry per row, in column order. The
 * concrete element type depends on {@link #dataType} (e.g. {@code List<Long>}
 * for {@link DataType#Int64}, {@code List<float[]>} for
 * {@link DataType#FloatVector}). Nulls are preserved as null.
 * </p>
 */
public final class MilvusFieldData {

    private final String fieldName;
    private final DataType dataType;
    private final List<Object> values;
    private final long dimension;

    public MilvusFieldData(String fieldName, DataType dataType, List<Object> values, long dimension) {
        this.fieldName = Objects.requireNonNull(fieldName, "fieldName must not be null");
        this.dataType = Objects.requireNonNull(dataType, "dataType must not be null");
        this.values = Objects.requireNonNull(values, "values must not be null");
        this.dimension = dimension;
    }

    public String getFieldName() {
        return fieldName;
    }

    public DataType getDataType() {
        return dataType;
    }

    public List<Object> getValues() {
        return values;
    }

    /**
     * @return vector dimension for vector fields; {@code 0} for scalar fields
     */
    public long getDimension() {
        return dimension;
    }

    @Override
    public String toString() {
        return "MilvusFieldData{fieldName='" + fieldName + '\'' +
                ", dataType=" + dataType +
                ", values.size=" + values.size() +
                ", dimension=" + dimension +
                '}';
    }
}

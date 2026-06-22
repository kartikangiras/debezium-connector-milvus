/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.ProtocolMessageEnum;

import io.debezium.relational.ValueConverter;
import io.debezium.util.Strings;
import io.milvus.grpc.DataType;

/**
 * Converts Milvus native values to plain Java types suitable for a Kafka
 * Connect {@code Struct}/{@code Map} payload.
 *
 * <p>
 * Two entry points are provided:
 * </p>
 * <ul>
 * <li>{@link #convert(Object)} — type-erased passthrough/normalization, used
 * when the Milvus {@link DataType} is unknown (e.g. free-form values).
 * It unwraps protobuf-specific wrappers ({@link ByteString},
 * {@link ProtocolMessageEnum}) into {@code byte[]} and {@code int}, and
 * otherwise returns already-correct Java primitives as-is.</li>
 * <li>{@link #convertWithType(Object, DataType)} — type-aware conversion that
 * narrows a value to the Java type mapped to the given Milvus
 * {@link DataType}. This is the path used by {@link MilvusColumnarPivot}
 * for every cell, since the field's DataType is always known from the
 * columnar {@code FieldData}.</li>
 * </ul>
 *
 * <p>
 * <b>DataType → Java type mapping</b> (verified against
 * {@code io.milvus.grpc.DataType} in milvus-sdk-java 2.6.0):
 * </p>
 * <ul>
 * <li>{@link DataType#None} — skipped (never appears in data)</li>
 * <li>{@link DataType#Bool} → {@code Boolean}</li>
 * <li>{@link DataType#Int8} / {@link DataType#Int16} → {@code Short}</li>
 * <li>{@link DataType#Int32} → {@code Integer}</li>
 * <li>{@link DataType#Int64} → {@code Long}</li>
 * <li>{@link DataType#Float} → {@code Float}</li>
 * <li>{@link DataType#Double} → {@code Double}</li>
 * <li>{@link DataType#String} / {@link DataType#VarChar} /
 * {@link DataType#Text}
 * / {@link DataType#JSON} / {@link DataType#Geometry} → {@code String}</li>
 * <li>{@link DataType#Array} → {@code List<?>}</li>
 * <li>{@link DataType#BinaryVector} / {@link DataType#Int8Vector} →
 * {@code byte[]}</li>
 * <li>{@link DataType#FloatVector} / {@link DataType#Float16Vector}
 * / {@link DataType#BFloat16Vector} / {@link DataType#SparseFloatVector}
 * → {@code float[]}</li>
 * </ul>
 */
public class MilvusValueConverter implements ValueConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(MilvusValueConverter.class);

    private final MilvusConnectorConfig config;

    public MilvusValueConverter(MilvusConnectorConfig config) {
        this.config = config;
    }

    /**
     * Type-erased normalization. Returns {@code null} for null input, unwraps
     * protobuf wrappers, and passes already-correct Java types through. Used as
     * a fallback in {@link #convertWithType(Object, DataType)} for unrecognized
     * DataTypes.
     */
    @Override
    public Object convert(Object value) {
        if (value == null) {
            return null;
        }
        // Passthrough for already-correct Java types
        if (value instanceof String
                || value instanceof Integer
                || value instanceof Long
                || value instanceof Float
                || value instanceof Double
                || value instanceof Boolean
                || value instanceof Short
                || value instanceof Byte
                || value instanceof byte[]
                || value instanceof float[]
                || value instanceof List<?>) {
            return value;
        }

        // Protobuf ByteString → byte[]
        if (value instanceof ByteString bs) {
            return bs.toByteArray();
        }

        // Proto enums → Integer (their wire number)
        if (value instanceof ProtocolMessageEnum e) {
            return e.getNumber();
        }

        LOGGER.warn("Unrecognized value type {}; falling back to toString()", value.getClass().getName());
        return value.toString();
    }

    /**
     * Type-aware conversion using the known Milvus {@link DataType}. This is the
     * primary path for cell-level conversion during the columnar pivot.
     *
     * @param value    the raw value (may be null)
     * @param dataType the Milvus DataType of the owning field; never null
     * @return the value narrowed to the Java type for {@code dataType}, or null
     */
    public Object convertWithType(Object value, DataType dataType) {
        if (value == null) {
            return null;
        }
        if (dataType == null) {
            return convert(value);
        }
        switch (dataType) {
            case Bool:
                return toBoolean(value);
            case Int8:
            case Int16:
                return toShort(value);
            case Int32:
                return toInteger(value);
            case Int64:
                return toLong(value);
            case Float:
                return toFloat(value);
            case Double:
                return toDouble(value);
            case String:
            case VarChar:
            case Text:
            case JSON:
            case Geometry:
                return toString(value);
            case Array:
                return toList(value);
            case BinaryVector:
            case Int8Vector:
                return toByteArray(value);
            case FloatVector:
                return toFloatArray(value);
            case Float16Vector:
            case BFloat16Vector:
                return toByteArray(value);
            case SparseFloatVector:
                return toString(value);
            case None:
            default:
                return convert(value);
        }
    }

    private Boolean toBoolean(Object v) {
        if (v instanceof Boolean b) {
            return b;
        }
        if (v instanceof String s) {
            return Strings.asBoolean(s, false);
        }
        if (v instanceof Number n) {
            return n.intValue() != 0;
        }
        return Strings.asBoolean(v.toString(), false);
    }

    private Short toShort(Object v) {
        if (v instanceof Short s) {
            return s;
        }
        if (v instanceof Number n) {
            return n.shortValue();
        }
        return Short.valueOf(v.toString());
    }

    private Integer toInteger(Object v) {
        if (v instanceof Integer i) {
            return i;
        }
        if (v instanceof Number n) {
            return n.intValue();
        }
        return Integer.valueOf(v.toString());
    }

    private Long toLong(Object v) {
        if (v instanceof Long l) {
            return l;
        }
        if (v instanceof Number n) {
            return n.longValue();
        }
        return Long.valueOf(v.toString());
    }

    private Float toFloat(Object v) {
        if (v instanceof Float f) {
            return f;
        }
        if (v instanceof Number n) {
            return n.floatValue();
        }
        return Float.valueOf(v.toString());
    }

    private Double toDouble(Object v) {
        if (v instanceof Double d) {
            return d;
        }
        if (v instanceof Number n) {
            return n.doubleValue();
        }
        return Double.valueOf(v.toString());
    }

    private String toString(Object v) {
        return v instanceof String s ? s : v.toString();
    }

    @SuppressWarnings("unchecked")
    private List<?> toList(Object v) {
        if (v instanceof List<?>) {
            return (List<?>) v;
        }
        return List.of(v);
    }

    private byte[] toByteArray(Object v) {
        if (v instanceof byte[] bytes) {
            return bytes;
        }
        if (v instanceof ByteString bs) {
            return bs.toByteArray();
        }
        if (v instanceof String s) {
            return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
        throw new IllegalArgumentException(
                "Cannot convert " + v.getClass().getName() + " to byte[]");
    }

    private float[] toFloatArray(Object v) {
        if (v instanceof float[] floats) {
            return floats;
        }
        if (v instanceof ByteString bs) {
            byte[] bytes = bs.toByteArray();
            float[] out = new float[bytes.length / Float.BYTES];
            java.nio.ByteBuffer.wrap(bytes).asFloatBuffer().get(out);
            return out;
        }
        if (v instanceof byte[] bytes) {
            float[] out = new float[bytes.length / Float.BYTES];
            java.nio.ByteBuffer.wrap(bytes).asFloatBuffer().get(out);
            return out;
        }
        if (v instanceof List<?> list) {
            float[] out = new float[list.size()];
            for (int i = 0; i < list.size(); i++) {
                Object e = list.get(i);
                out[i] = e instanceof Number n ? n.floatValue() : Float.parseFloat(e.toString());
            }
            return out;
        }
        throw new IllegalArgumentException(
                "Cannot convert " + v.getClass().getName() + " to float[]");
    }
}

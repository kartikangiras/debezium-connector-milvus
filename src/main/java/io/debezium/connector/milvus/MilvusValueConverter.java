/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.List;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.ProtocolMessageEnum;

import io.debezium.data.Json;
import io.debezium.data.vector.FloatVector;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.relational.ValueConverterProvider;
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
 * <b>Schema/logical type mapping</b> (see {@link #schemaBuilder(Column)}):
 * </p>
 * <ul>
 * <li>{@link DataType#Bool} → {@code Schema.Type.BOOLEAN}</li>
 * <li>{@link DataType#Int8} / {@link DataType#Int16} → {@code Schema.Type.INT16}</li>
 * <li>{@link DataType#Int32} → {@code Schema.Type.INT32}</li>
 * <li>{@link DataType#Int64} → {@code Schema.Type.INT64}</li>
 * <li>{@link DataType#Float} / {@link DataType#Double} → controlled by
 *     {@code decimal.handling.mode}:
 *     <ul>
 *     <li>{@code double} (default) → {@code Schema.Type.FLOAT32} /
 *         {@code Schema.Type.FLOAT64}</li>
 *     <li>{@code precise} → {@code org.apache.kafka.connect.data.Decimal}
 *         logical type (underlying type {@code BYTES})</li>
 *     <li>{@code string} → {@code Schema.Type.STRING}</li>
 *     </ul>
 * </li>
 * <li>{@link DataType#String} / {@link DataType#VarChar} / {@link DataType#Text}
 *     → {@code Schema.Type.STRING}</li>
 * <li>{@link DataType#JSON} / {@link DataType#Geometry}
 *     → {@code io.debezium.data.Json} logical type (underlying type STRING)</li>
 * <li>{@link DataType#FloatVector} → {@code io.debezium.data.vector.FloatVector}
 *     logical type (underlying type ARRAY of FLOAT32)</li>
 * <li>{@link DataType#BinaryVector} / {@link DataType#Int8Vector}
 *     / {@link DataType#Float16Vector} / {@link DataType#BFloat16Vector}
 *     → {@code bytes} ({@link Types#BLOB})</li>
 * <li>{@link DataType#SparseFloatVector} → {@code Schema.Type.STRING} (JSON)</li>
 * </ul>
 *
 * <p>
 * <b>Note on wide integers</b>: Milvus {@code Int64} maps to Java {@code Long}
 * (64-bit signed). Should Milvus ever introduce a wider integer type,
 * {@code io.debezium.data.VariableScaleDecimal} would be the appropriate
 * encoding; a comment is left in {@link #schemaBuilder(Column)} as a reminder.
 * </p>
 */
public class MilvusValueConverter implements ValueConverter, ValueConverterProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(MilvusValueConverter.class);

    private final MilvusConnectorConfig config;
    private final DecimalMode decimalMode;

    public MilvusValueConverter(MilvusConnectorConfig config) {
        this.config = config;
        this.decimalMode = config != null
                ? config.getDecimalMode()
                : DecimalMode.DOUBLE;
    }

    // ------------------------------------------------------------------ //
    // Type-erased convert() //
    // ------------------------------------------------------------------ //

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

        if (value instanceof float[] floats) {
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(floats.length * Float.BYTES)
                    .order(java.nio.ByteOrder.LITTLE_ENDIAN);
            for (float f : floats) {
                buf.putFloat(f);
            }
            return buf.array();
        }

        if (value instanceof String
                || value instanceof Integer
                || value instanceof Long
                || value instanceof Float
                || value instanceof Double
                || value instanceof Boolean
                || value instanceof Short
                || value instanceof Byte
                || value instanceof byte[]
                || value instanceof List<?>) {
            return value;
        }

        if (value instanceof ByteString bs) {
            return bs.toByteArray();
        }

        if (value instanceof ProtocolMessageEnum e) {
            return e.getNumber();
        }

        LOGGER.warn("Unrecognized value type {}; falling back to toString()", value.getClass().getName());
        return value.toString();
    }

    // ------------------------------------------------------------------ //
    // Type-aware convertWithType() //
    // ------------------------------------------------------------------ //

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
            case SparseFloatVector:
                return toString(value);
            case Array:
                return toList(value);
            case BinaryVector:
            case Int8Vector:
                return toByteArray(value);
            case FloatVector:
                return toFloatList(value);
            case Float16Vector:
            case BFloat16Vector:
                return toByteArray(value);
            case None:
            default:
                return convert(value);
        }
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        switch (column.jdbcType()) {
            case Types.BIGINT:
                // Note: if Milvus ever introduces integers wider than 64-bit,
                // VariableScaleDecimal would be the appropriate encoding here.
                return SchemaBuilder.int64();
            case Types.INTEGER:
                return SchemaBuilder.int32();
            case Types.SMALLINT:
                return SchemaBuilder.int16();
            case Types.FLOAT:
            case Types.DOUBLE:
                return floatingPointSchema(decimalMode, column.jdbcType());
            case Types.BOOLEAN:
                return SchemaBuilder.bool();
            case Types.BLOB:
                return SchemaBuilder.bytes().optional();
            case Types.ARRAY:
                // Generic array (Milvus DataType.Array) — encode as a JSON string
                // to avoid requiring knowledge of the element type at schema-build time.
                return SchemaBuilder.string().optional();
            case Types.OTHER:
                // JSON / Geometry — annotate with the io.debezium.data.Json logical type
                return Json.builder().optional();
            case Types.JAVA_OBJECT:
                // FloatVector — annotate with io.debezium.data.vector.FloatVector logical type
                return FloatVector.builder().optional();
            case Types.VARCHAR:
            default:
                return SchemaBuilder.string();
        }
    }

    /**
     * Returns the Kafka Connect schema for Milvus floating-point columns
     * according to the configured {@code decimal.handling.mode}.
     *
     * <ul>
     * <li>{@code double} — native IEEE-754 types ({@code FLOAT32} /
     * {@code FLOAT64}).</li>
     * <li>{@code precise} — {@link Decimal} logical type.</li>
     * <li>{@code string} — plain {@code STRING}.</li>
     * </ul>
     */
    private SchemaBuilder floatingPointSchema(DecimalMode mode, int jdbcType) {
        switch (mode) {
            case PRECISE:
                int scale = jdbcType == Types.FLOAT ? 7 : 15;
                return Decimal.builder(scale).optional();
            case STRING:
                return SchemaBuilder.string().optional();
            case DOUBLE:
            default:
                return jdbcType == Types.FLOAT
                        ? SchemaBuilder.float32().optional()
                        : SchemaBuilder.float64().optional();
        }
    }

    @Override
    public ValueConverter converter(Column column, Field field) {
        switch (column.jdbcType()) {
            case Types.FLOAT:
                if (decimalMode == DecimalMode.STRING) {
                    return (value) -> value == null ? null : toString(value);
                }
                if (decimalMode == DecimalMode.PRECISE) {
                    return (value) -> {
                        if (value == null) {
                            return null;
                        }
                        return Decimal.fromLogical(field.schema(), toBigDecimal(value));
                    };
                }
                return (value) -> value == null ? null : toFloat(value);
            case Types.DOUBLE:
                if (decimalMode == DecimalMode.STRING) {
                    return (value) -> value == null ? null : toString(value);
                }
                if (decimalMode == DecimalMode.PRECISE) {
                    return (value) -> {
                        if (value == null) {
                            return null;
                        }
                        return Decimal.fromLogical(field.schema(), toBigDecimal(value));
                    };
                }
                return (value) -> value == null ? null : toDouble(value);
            case Types.JAVA_OBJECT:
                // FloatVector: convert float[] / List<Float> to List<Float>
                return (value) -> {
                    if (value == null) {
                        return null;
                    }
                    return toFloatList(value);
                };
            default:
                return this;
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

    /**
     * Converts a float vector value to a {@code List<Float>} for the
     * {@link FloatVector} logical type.
     *
     * <p>Accepts {@code float[]}, {@code List<?>}, {@link ByteString}, or
     * {@code byte[]} (little-endian IEEE-754 floats).</p>
     */
    private List<Float> toFloatList(Object v) {
        if (v instanceof float[] floats) {
            return FloatVector.fromLogical(null, floats);
        }
        if (v instanceof List<?> list) {
            // Already a List<Float> from the columnar pivot path
            @SuppressWarnings("unchecked")
            List<Float> floatList = (List<Float>) list;
            return floatList;
        }
        if (v instanceof ByteString bs) {
            return FloatVector.fromLogical(null, bs.toByteArray());
        }
        if (v instanceof byte[] bytes) {
            return FloatVector.fromLogical(null, bytes);
        }
        throw new IllegalArgumentException(
                "Cannot convert " + v.getClass().getName() + " to List<Float> (FloatVector)");
    }

    private BigDecimal toBigDecimal(Object v) {
        if (v instanceof BigDecimal bd) {
            return bd;
        }
        if (v instanceof Number n) {
            return BigDecimal.valueOf(n.doubleValue());
        }
        return new BigDecimal(v.toString());
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

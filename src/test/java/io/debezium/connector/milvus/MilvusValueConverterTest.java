/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;

import io.debezium.config.Configuration;
import io.debezium.data.Json;
import io.debezium.data.vector.FloatVector;
import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.milvus.grpc.DataType;

public class MilvusValueConverterTest {

    /** Converter without config — uses DOUBLE mode defaults. */
    private final MilvusValueConverter converter = new MilvusValueConverter(null);

    /** Build a converter with the specified decimal.handling.mode. */
    private static MilvusValueConverter converterWithMode(String mode) {
        Configuration cfg = Configuration.create()
                .with("milvus.uri", "http://localhost:19530")
                .with("topic.prefix", "test")
                .with("decimal.handling.mode", mode)
                .build();
        return new MilvusValueConverter(new MilvusConnectorConfig(cfg));
    }

    /** Helper: build a Column with the given jdbcType. */
    private static Column columnOf(int jdbcType) {
        return Column.editor()
                .name("col")
                .type("T")
                .jdbcType(jdbcType)
                .optional(true)
                .create();
    }

    // ---- type-erased convert() passthrough ----

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldPassThroughNull() {
        assertThat(converter.convert(null)).isNull();
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldPassThroughString() {
        assertThat(converter.convert("hello")).isEqualTo("hello");
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldPassThroughInteger() {
        assertThat(converter.convert(42)).isEqualTo(42);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldPassThroughLong() {
        assertThat(converter.convert(42L)).isEqualTo(42L);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldPassThroughFloat() {
        assertThat(converter.convert(1.5f)).isEqualTo(1.5f);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldPassThroughDouble() {
        assertThat(converter.convert(2.5d)).isEqualTo(2.5d);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldPassThroughBoolean() {
        assertThat(converter.convert(true)).isEqualTo(true);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldPassThroughByteArray() {
        byte[] bytes = { 1, 2, 3 };
        assertThat(converter.convert(bytes)).isSameAs(bytes);
    }

    // ---- type-erased convert() normalization ----

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldConvertProtoByteStringToByteArray() {
        ByteString bs = ByteString.copyFrom(new byte[]{ 7, 8, 9 });
        byte[] result = (byte[]) converter.convert(bs);
        assertThat(result).containsExactly(7, 8, 9);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldConvertProtoEnumToInteger() {
        assertThat(converter.convert(DataType.FloatVector)).isEqualTo(DataType.FloatVector.getNumber());
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldFallbackToStringOnUnknownObject() {
        assertThat(converter.convert(new Object() {
            @Override
            public String toString() {
                return "stringified";
            }
        })).isEqualTo("stringified");
    }

    // ---- type-aware convertWithType() with precision narrowing ----

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldConvertInt8ToShort() {
        assertThat(converter.convertWithType(127, DataType.Int8)).isEqualTo((short) 127);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldConvertInt16ToShort() {
        assertThat(converter.convertWithType(32000, DataType.Int16)).isEqualTo((short) 32000);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldConvertInt32ToInteger() {
        assertThat(converter.convertWithType(100000, DataType.Int32)).isEqualTo(100000);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldConvertInt64ToLong() {
        assertThat(converter.convertWithType(100, DataType.Int64)).isEqualTo(100L)
                .isInstanceOf(Long.class);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldConvertFloatTypeToFloat() {
        assertThat(converter.convertWithType(3.0f, DataType.Float)).isEqualTo(3.0f);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldConvertDoubleTypeToDouble() {
        assertThat(converter.convertWithType(3.0d, DataType.Double)).isEqualTo(3.0d);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldConvertStringLikeToBoolean() {
        assertThat(converter.convertWithType("true", DataType.Bool)).isEqualTo(true);
        assertThat(converter.convertWithType("false", DataType.Bool)).isEqualTo(false);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldConvertVarChartoString() {
        assertThat(converter.convertWithType("abc", DataType.VarChar)).isEqualTo("abc");
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldConvertFloatVectorByteStringToListOfFloat() {
        // FloatVector.fromLogical reads little-endian IEEE-754 floats
        byte[] raw = java.nio.ByteBuffer.allocate(8)
                .order(java.nio.ByteOrder.LITTLE_ENDIAN)
                .putFloat(1.0f).putFloat(2.0f).array();
        Object result = converter.convertWithType(ByteString.copyFrom(raw), DataType.FloatVector);
        assertThat(result).isInstanceOf(List.class);
        @SuppressWarnings("unchecked")
        List<Float> floatList = (List<Float>) result;
        assertThat(floatList).containsExactly(1.0f, 2.0f);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldConvertBinaryVectorByteStringToByteArray() {
        byte[] raw = { 0, 1, 0, 1 };
        byte[] result = (byte[]) converter.convertWithType(ByteString.copyFrom(raw), DataType.BinaryVector);
        assertThat(result).containsExactly(0, 1, 0, 1);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldConvertArrayToList() {
        assertThat(converter.convertWithType(List.of(1, 2, 3), DataType.Array))
                .isEqualTo(List.of(1, 2, 3));
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldReturnNullOnNullWithType() {
        assertThat(converter.convertWithType(null, DataType.Int64)).isNull();
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldFallbackOnUnknownDataType() {
        assertThat(converter.convertWithType(99, DataType.None)).isEqualTo(99);
    }

    // ---- schemaBuilder: JSON → io.debezium.data.Json logical type ----

    @Test
    @FixFor("debezium/dbz#2089")
    void schemaBuilderShouldReturnJsonLogicalTypeForOtherJdbcType() {
        Schema schema = converter.schemaBuilder(columnOf(Types.OTHER)).optional().build();
        assertThat(schema.name()).isEqualTo(Json.LOGICAL_NAME);
        assertThat(schema.type()).isEqualTo(Schema.Type.STRING);
    }

    // ---- schemaBuilder: FloatVector → io.debezium.data.vector.FloatVector logical type ----

    @Test
    @FixFor("debezium/dbz#2089")
    void schemaBuilderShouldReturnFloatVectorLogicalTypeForJavaObjectJdbcType() {
        Schema schema = converter.schemaBuilder(columnOf(Types.JAVA_OBJECT)).optional().build();
        assertThat(schema.name()).isEqualTo(FloatVector.LOGICAL_NAME);
    }

    // ---- schemaBuilder: decimal.handling.mode for float/double ----

    @Test
    @FixFor("debezium/dbz#2089")
    void schemaBuilderShouldReturnFloat32ForFloatInDoubleMode() {
        MilvusValueConverter c = converterWithMode("double");
        Schema schema = c.schemaBuilder(columnOf(Types.FLOAT)).build();
        assertThat(schema.type()).isEqualTo(Schema.Type.FLOAT32);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void schemaBuilderShouldReturnFloat64ForDoubleInDoubleMode() {
        MilvusValueConverter c = converterWithMode("double");
        Schema schema = c.schemaBuilder(columnOf(Types.DOUBLE)).build();
        assertThat(schema.type()).isEqualTo(Schema.Type.FLOAT64);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void schemaBuilderShouldReturnFloat32ForFloatInStringMode() {
        MilvusValueConverter c = converterWithMode("string");
        Schema schema = c.schemaBuilder(columnOf(Types.FLOAT)).build();
        assertThat(schema.type()).isEqualTo(Schema.Type.FLOAT32);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void schemaBuilderShouldReturnFloat32ForFloatInPreciseMode() {
        MilvusValueConverter c = converterWithMode("precise");
        Schema schema = c.schemaBuilder(columnOf(Types.FLOAT)).build();
        assertThat(schema.type()).isEqualTo(Schema.Type.FLOAT32);
    }

    // ---- convertWithType: FloatVector → List<Float> ----

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldConvertFloatListToListOfFloat() {
        List<Float> input = Arrays.asList(1.0f, 2.0f, 3.0f);
        Object result = converter.convertWithType(input, DataType.FloatVector);
        assertThat(result).isInstanceOf(List.class);
        @SuppressWarnings("unchecked")
        List<Float> floatList = (List<Float>) result;
        assertThat(floatList).containsExactly(1.0f, 2.0f, 3.0f);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldConvertFloatArrayToListOfFloat() {
        float[] raw = { 1.5f, 2.5f };
        Object result = converter.convertWithType(raw, DataType.FloatVector);
        assertThat(result).isInstanceOf(List.class);
        @SuppressWarnings("unchecked")
        List<Float> floatList = (List<Float>) result;
        assertThat(floatList).containsExactly(1.5f, 2.5f);
    }
}

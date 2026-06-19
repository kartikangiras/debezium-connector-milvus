/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;

import io.debezium.doc.FixFor;
import io.milvus.grpc.DataType;

public class MilvusValueConverterTest {

    private final MilvusValueConverter converter = new MilvusValueConverter(null);

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
    void shouldConvertFloatVectorByteStringToFloatArray() {
        byte[] raw = java.nio.ByteBuffer.allocate(8).putFloat(1.0f).putFloat(2.0f).array();
        float[] result = (float[]) converter.convertWithType(ByteString.copyFrom(raw), DataType.FloatVector);
        assertThat(result).containsExactly(1.0f, 2.0f);
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
}

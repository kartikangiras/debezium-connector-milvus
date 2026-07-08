/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.milvus.grpc.DataType;

public class MilvusColumnarPivotTest {

    private final MilvusColumnarPivot pivot = new MilvusColumnarPivot(new MilvusValueConverter(null));

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldPivotSingleColumnThreeRows() {
        MilvusFieldData id = new MilvusFieldData("id", DataType.Int64, List.of(1L, 2L, 3L), 0);

        List<Map<String, Object>> rows = pivot.pivot(List.of(id), 3);

        assertThat(rows).hasSize(3);
        assertThat(rows.get(0)).containsEntry("id", 1L);
        assertThat(rows.get(1)).containsEntry("id", 2L);
        assertThat(rows.get(2)).containsEntry("id", 3L);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldPivotMultipleColumnsTwoRows() {
        MilvusFieldData id = new MilvusFieldData("id", DataType.Int64, List.of(1L, 2L), 0);
        MilvusFieldData name = new MilvusFieldData("name", DataType.VarChar, List.of("a", "b"), 0);

        List<Map<String, Object>> rows = pivot.pivot(List.of(id, name), 2);

        assertThat(rows).hasSize(2);
        assertThat(rows.get(0)).hasSize(2).containsEntry("id", 1L).containsEntry("name", "a");
        assertThat(rows.get(1)).hasSize(2).containsEntry("id", 2L).containsEntry("name", "b");
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldPivotWithScalarTypes() {
        MilvusFieldData f = new MilvusFieldData("row", DataType.Int64,
                List.of(0L, 1L), 0);
        MilvusFieldData b = new MilvusFieldData("b", DataType.Bool, List.of(true, false), 0);
        MilvusFieldData i = new MilvusFieldData("i", DataType.Int32, List.of(10, 20), 0);
        MilvusFieldData s = new MilvusFieldData("s", DataType.VarChar, List.of("x", "y"), 0);
        MilvusFieldData d = new MilvusFieldData("d", DataType.Double, List.of(1.5d, 2.5d), 0);

        List<Map<String, Object>> rows = pivot.pivot(List.of(f, b, i, s, d), 2);

        assertThat(rows).hasSize(2);
        Map<String, Object> row0 = rows.get(0);
        assertThat(row0.get("b")).isEqualTo(true);
        assertThat(row0.get("i")).isEqualTo(10);
        assertThat(row0.get("s")).isEqualTo("x");
        assertThat(row0.get("d")).isEqualTo(1.5d);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldPivotWithVectorField() {
        float[] v0 = { 0.1f, 0.2f };
        float[] v1 = { 0.3f, 0.4f };
        MilvusFieldData vec = new MilvusFieldData("vector", DataType.FloatVector, List.of(v0, v1), 2);

        List<Map<String, Object>> rows = pivot.pivot(List.of(vec), 2);

        assertThat(rows).hasSize(2);
        // FloatVector is now encoded as List<Float> for io.debezium.data.vector.FloatVector
        assertThat(rows.get(0).get("vector")).isInstanceOf(List.class);
        assertThat(rows.get(1).get("vector")).isInstanceOf(List.class);
        @SuppressWarnings("unchecked")
        List<Float> vec0 = (List<Float>) rows.get(0).get("vector");
        @SuppressWarnings("unchecked")
        List<Float> vec1 = (List<Float>) rows.get(1).get("vector");
        assertThat(vec0).containsExactly(0.1f, 0.2f);
        assertThat(vec1).containsExactly(0.3f, 0.4f);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldPivotWithBinaryVectorField() {
        byte[] b0 = { 0, 1 };
        byte[] b1 = { 1, 0 };
        MilvusFieldData vec = new MilvusFieldData("vector", DataType.BinaryVector, List.of(b0, b1), 16);

        List<Map<String, Object>> rows = pivot.pivot(List.of(vec), 2);

        assertThat(rows).hasSize(2);
        assertThat((byte[]) rows.get(0).get("vector")).containsExactly(0, 1);
        assertThat((byte[]) rows.get(1).get("vector")).containsExactly(1, 0);
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldPivotWithJsonField() {
        MilvusFieldData json = new MilvusFieldData("meta", DataType.JSON,
                List.of("{\"k\":1}", "{\"k\":2}"), 0);

        List<Map<String, Object>> rows = pivot.pivot(List.of(json), 2);

        assertThat(rows.get(0).get("meta")).isEqualTo("{\"k\":1}");
        assertThat(rows.get(1).get("meta")).isEqualTo("{\"k\":2}");
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldPivotWithNullValues() {
        MilvusFieldData id = new MilvusFieldData("id", DataType.Int64, List.of(1L, 2L), 0);
        MilvusFieldData name = new MilvusFieldData("name", DataType.VarChar,
                Arrays.asList("a", null), 0);

        List<Map<String, Object>> rows = pivot.pivot(List.of(id, name), 2);

        assertThat(rows.get(0).get("name")).isEqualTo("a");
        assertThat(rows.get(1).get("name")).isNull();
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldThrowOnColumnLengthMismatch() {
        MilvusFieldData id = new MilvusFieldData("id", DataType.Int64, List.of(1L, 2L), 0);
        MilvusFieldData bad = new MilvusFieldData("bad", DataType.VarChar, List.of("only-one"), 0);

        assertThatThrownBy(() -> pivot.pivot(List.of(id, bad), 2))
                .isInstanceOf(MilvusWireFormatMismatchException.class)
                .hasMessageContaining("Column length mismatch")
                .hasMessageContaining("field 'bad'")
                .hasMessageContaining("1 values, expected 2");
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldThrowOnColumnLengthExceedsNumRows() {
        MilvusFieldData id = new MilvusFieldData("id", DataType.Int64, List.of(1L, 2L, 3L), 0);

        assertThatThrownBy(() -> pivot.pivot(List.of(id), 2))
                .isInstanceOf(MilvusWireFormatMismatchException.class)
                .hasMessageContaining("3 values, expected 2");
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldHandleEmptyFieldList() {
        assertThat(pivot.pivot(List.of(), 3)).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldHandleZeroRows() {
        MilvusFieldData id = new MilvusFieldData("id", DataType.Int64, List.of(), 0);
        assertThat(pivot.pivot(List.of(id), 0)).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldHandleNullFieldList() {
        assertThat(pivot.pivot(null, 3)).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#2089")
    void shouldPreserveFieldOrderInOutputMap() {
        MilvusFieldData a = new MilvusFieldData("a", DataType.Int64, List.of(1L), 0);
        MilvusFieldData b = new MilvusFieldData("b", DataType.VarChar, List.of("x"), 0);
        MilvusFieldData c = new MilvusFieldData("c", DataType.Bool, List.of(true), 0);

        List<Map<String, Object>> rows = pivot.pivot(List.of(a, b, c), 1);

        assertThat(rows.get(0).keySet()).containsExactly("a", "b", "c");
    }
}
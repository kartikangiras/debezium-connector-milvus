/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Pivots Milvus columnar insert data into row-oriented {@code Map} records.
 *
 * <p>
 * Milvus stores insert payloads as a list of columnar fields (one
 * {@link MilvusFieldData} per field, each holding a value list of length
 * {@code numRows}). This class transposes that into a list of row maps — one
 * {@link LinkedHashMap} per row, with field insertion order matching the input
 * column order.
 * </p>
 *
 * <p>
 * Example: a 3-row, 2-field payload
 * {@code [id=[1,2,3], name=["a","b","c"]]} produces
 * {@code [{id=1,name=a}, {id=2,name=b}, {id=3,name=c}]}.
 * </p>
 *
 * <p>
 * Each cell is run through {@link MilvusValueConverter#convertWithType} so that
 * values are narrowed to the field's Java type before being placed in the row
 * map. Nulls are preserved as null.
 * </p>
 */
public class MilvusColumnarPivot {

    private final MilvusValueConverter valueConverter;

    public MilvusColumnarPivot(MilvusValueConverter valueConverter) {
        this.valueConverter = valueConverter;
    }

    /**
     * Pivot columnar field data into a list of row maps.
     *
     * <p>Convenience overload — calls {@link #pivot(List, int, String, String, int, long)} with
     * placeholder MQ coordinates. Prefer the 6-argument form when the raw message
     * coordinates are known so that column-length mismatch exceptions contain useful
     * diagnostics.</p>
     *
     * @param fieldDataList the columnar fields; must not be null
     * @param numRows       expected row count
     * @return one {@link LinkedHashMap} per row; empty if no fields or {@code numRows == 0}
     * @throws MilvusWireFormatMismatchException if any column's value count does not equal
     *         {@code numRows}
     */
    public List<Map<String, Object>> pivot(List<MilvusFieldData> fieldDataList, int numRows)
            throws MilvusWireFormatMismatchException {
        return pivot(fieldDataList, numRows, "unknown", "<unknown>", -1, -1L);
    }

    /**
     * Pivot columnar field data into a list of row maps, attaching MQ coordinates to
     * any thrown exception so operators can locate the offending message.
     *
     * @param fieldDataList the columnar fields; must not be null
     * @param numRows       expected row count
     * @param wireFormat    wire-format label used in the exception ({@code proto_single} /
     *                      {@code msgpack_batch})
     * @param topic         Kafka topic of the source message
     * @param partition     Kafka partition of the source message
     * @param offset        Kafka offset of the source message
     * @return one {@link LinkedHashMap} per row; empty if no fields or {@code numRows == 0}
     * @throws MilvusWireFormatMismatchException if any column's value count does not equal
     *         {@code numRows}
     */
    public List<Map<String, Object>> pivot(List<MilvusFieldData> fieldDataList, int numRows,
                                           String wireFormat, String topic, int partition, long offset)
            throws MilvusWireFormatMismatchException {
        List<Map<String, Object>> rows = new ArrayList<>();
        if (fieldDataList == null || fieldDataList.isEmpty() || numRows <= 0) {
            return rows;
        }

        // Validate every column's length up-front so we fail fast with a clear
        // message before producing partial output.
        for (MilvusFieldData field : fieldDataList) {
            if (field.getValues().size() != numRows) {
                throw new MilvusWireFormatMismatchException(
                        wireFormat, wireFormat, topic, partition, offset,
                        String.format("Column length mismatch: field '%s' has %d values, expected %d",
                                field.getFieldName(), field.getValues().size(), numRows));
            }
        }

        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
            Map<String, Object> row = new LinkedHashMap<>(fieldDataList.size());
            for (MilvusFieldData field : fieldDataList) {
                Object raw = field.getValues().get(rowIdx);
                Object converted = valueConverter.convertWithType(raw, field.getDataType());
                row.put(field.getFieldName(), converted);
            }
            rows.add(row);
        }
        return rows;
    }
}

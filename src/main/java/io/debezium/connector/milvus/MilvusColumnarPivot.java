/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.util.ArrayList;
import java.util.List;

import io.debezium.util.Collect;
import io.milvus.grpc.DataType;

/**
 * Pivots Milvus columnar insert data into row-oriented {@link MilvusRow} records.
 *
 * <p>
 * Milvus stores insert payloads as a list of columnar fields (one
 * {@link MilvusFieldData} per field, each holding a value list of length
 * {@code numRows}). This class transposes that into a list of rows — one
 * {@link MilvusRow} per row — using three parallel arrays so that field order
 * is always determined structurally, not by an implicit {@code LinkedHashMap}
 * contract.
 * </p>
 *
 * <p>
 * Example: a 3-row, 2-field payload
 * {@code [id=[1,2,3], name=["a","b","c"]]} produces
 * {@code [MilvusRow(["id","name"],[1,"a"],[Int64,VarChar]),
 *          MilvusRow(["id","name"],[2,"b"],[Int64,VarChar]),
 *          MilvusRow(["id","name"],[3,"c"],[Int64,VarChar])]}.
 * </p>
 *
 * <p>
 * Each cell is run through {@link MilvusValueConverter#convertWithType} so that
 * values are narrowed to the field's Java type. Nulls are preserved as null.
 * </p>
 *
 * <p>
 * The column ordering of each {@link MilvusRow} mirrors the order of
 * {@code fieldDataList} as received from the wire — no insertion-order
 * guarantee from any {@code Map} implementation is relied upon.
 * </p>
 *
 * @see MilvusRow
 */

public class MilvusColumnarPivot {

    private final MilvusValueConverter valueConverter;

    public MilvusColumnarPivot(MilvusValueConverter valueConverter) {
        this.valueConverter = valueConverter;
    }

    /**
     * Pivot columnar field data into a list of rows.
     *
     * <p>Convenience overload — calls {@link #pivot(List, int, String, String, int, long)} with
     * placeholder MQ coordinates. Prefer the 6-argument form when the raw message
     * coordinates are known so that column-length mismatch exceptions contain useful
     * diagnostics.</p>
     *
     * @param fieldDataList the columnar fields; must not be null
     * @param numRows       expected row count
     * @return one {@link MilvusRow} per row; empty if no fields or {@code numRows == 0}
     * @throws MilvusWireFormatMismatchException if any column's value count does not equal
     *         {@code numRows}
     */
    public List<MilvusRow> pivot(List<MilvusFieldData> fieldDataList, int numRows)
            throws MilvusWireFormatMismatchException {
        return pivot(fieldDataList, numRows, "unknown", "<unknown>", -1, -1L);
    }

    /**
     * Pivot columnar field data into a list of rows, attaching MQ coordinates to
     * any thrown exception so operators can locate the offending message.
     *
     * @param fieldDataList the columnar fields; must not be null
     * @param numRows       expected row count
     * @param wireFormat    wire-format label used in the exception ({@code proto_single} /
     *                      {@code msgpack_batch})
     * @param topic         Kafka topic of the source message
     * @param partition     Kafka partition of the source message
     * @param offset        Kafka offset of the source message
     * @return one {@link MilvusRow} per row; empty if no fields or {@code numRows == 0}
     * @throws MilvusWireFormatMismatchException if any column's value count does not equal
     *         {@code numRows}
     */
    public List<MilvusRow> pivot(List<MilvusFieldData> fieldDataList, int numRows,
                                 String wireFormat, String topic, int partition, long offset)
            throws MilvusWireFormatMismatchException {
        List<MilvusRow> rows = new ArrayList<>();
        if (Collect.isNullOrEmpty(fieldDataList) || numRows <= 0) {
            return rows;
        }

        int numFields = fieldDataList.size();

        String[] fieldNames = new String[numFields];
        DataType[] fieldTypes = new DataType[numFields];
        for (int col = 0; col < numFields; col++) {
            fieldNames[col] = fieldDataList.get(col).getFieldName();
            fieldTypes[col] = fieldDataList.get(col).getDataType();
        }

        for (int col = 0; col < numFields; col++) {
            MilvusFieldData field = fieldDataList.get(col);
            if (field.getValues().size() != numRows) {
                throw new MilvusWireFormatMismatchException(
                        wireFormat, wireFormat, topic, partition, offset,
                        String.format("Column length mismatch: field '%s' has %d values, expected %d",
                                field.getFieldName(), field.getValues().size(), numRows));
            }
        }

        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
            Object[] fieldValues = new Object[numFields];
            for (int col = 0; col < numFields; col++) {
                Object raw = fieldDataList.get(col).getValues().get(rowIdx);
                fieldValues[col] = valueConverter.convertWithType(raw, fieldTypes[col]);
            }
            rows.add(new MilvusRow(fieldNames, fieldValues, fieldTypes));
        }
        return rows;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.debezium.data.Envelope;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Clock;

/**
 * Emits Debezium change records from a single Milvus change event.
 *
 * <p>
 * Translates {@link MilvusChangeEvent} subtypes (Insert, Delete, DDL)
 * into the corresponding Debezium envelope operations and delegates to the
 * {@link ChangeRecordEmitter.Receiver}.
 * </p>
 *
 * <ul>
 * <li><b>Insert</b>: {@code op=c}, {@code before=null},
 * {@code after=full row data}</li>
 * <li><b>Delete</b>: {@code op=d}, {@code before=PK-only struct},
 * {@code after=null}</li>
 * <li><b>Snapshot read</b>: {@code op=r}, {@code before=null},
 * {@code after=full row data}</li>
 * </ul>
 */
public class MilvusChangeRecordEmitter extends RelationalChangeRecordEmitter<MilvusPartition> {

    private final MilvusChangeEvent changeEvent;
    private final Envelope.Operation operation;
    private final String[] columnNames;
    private final String pkFieldName;

    public MilvusChangeRecordEmitter(MilvusPartition partition, OffsetContext offsetContext,
                                     Clock clock, RelationalDatabaseConnectorConfig connectorConfig,
                                     MilvusChangeEvent changeEvent, Envelope.Operation operation,
                                     String[] columnNames, String pkFieldName) {
        super(partition, offsetContext, clock, connectorConfig);
        this.changeEvent = changeEvent;
        this.operation = operation;
        this.columnNames = columnNames;
        this.pkFieldName = pkFieldName;
    }

    /**
     * Returns the underlying Milvus change event carried by this emitter.
     *
     * <p>Package-private: only used by tests in this package to inspect the
     * event that produced a given emitter.</p>
     *
     * @return the change event; never {@code null}
     */
    MilvusChangeEvent getChangeEvent() {
        return changeEvent;
    }

    /**
     * Returns the "before" column values for the event.
     *
     * <p>Delete events carry a "before" state containing only the primary key
     * (Milvus {@code DeleteRequest} does not carry the prior state of the
     * deleted row). Non-PK columns are set to {@code null}.</p>
     *
     * @return column values for the "before" image, or {@code null} for Insert
     *         and snapshot-read operations
     */
    @Override
    protected Object[] getOldColumnValues() {
        if (operation == Envelope.Operation.DELETE && changeEvent instanceof MilvusChangeEvent.Delete delete) {
            Object[] values = new Object[columnNames.length];
            for (int i = 0; i < columnNames.length; i++) {
                if (columnNames[i].equals(pkFieldName)) {
                    Object pks = delete.getPrimaryKeys();
                    if (pks instanceof List<?> list && !list.isEmpty()) {
                        values[i] = list.get(0);
                    }
                    else {
                        values[i] = pks;
                    }
                }
            }
            return values;
        }
        return null;
    }

    /**
     * Returns the "after" column values for the event.
     *
     * <p>
     * For Insert and snapshot-read events, this is the full row data
     * from the deserialized {@link MilvusChangeEvent.Insert#getRow()},
     * ordered by {@code columnNames}. For Delete events, the after-image
     * is {@code null} (tombstone semantics).
     * </p>
     *
     * @return column values for the "after" image, or {@code null} for Delete
     *         operations
     */
    @Override
    protected Object[] getNewColumnValues() {
        if (changeEvent instanceof MilvusChangeEvent.Insert insert) {
            MilvusRow row = insert.getRow();
            if (row == null || row.size() == 0 || columnNames == null) {
                return new Object[0];
            }
            String[] rowNames = row.getFieldNames();
            Object[] rowValues = row.getFieldValues();
            Map<String, Object> byName = new HashMap<>(rowNames.length * 2);
            for (int i = 0; i < rowNames.length; i++) {
                byName.put(rowNames[i], rowValues[i]);
            }
            Object[] values = new Object[columnNames.length];
            for (int i = 0; i < columnNames.length; i++) {
                values[i] = byName.get(columnNames[i]);
            }
            return values;
        }
        return null;
    }

    @Override
    public Envelope.Operation getOperation() {
        return operation;
    }
}

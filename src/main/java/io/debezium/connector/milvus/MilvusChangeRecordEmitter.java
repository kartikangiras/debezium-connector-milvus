/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

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
                    if (pks instanceof java.util.List<?> list && !list.isEmpty()) {
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
     * extracted from the deserialized {@link MilvusChangeEvent.Insert#getData()}
     * map, ordered by {@code columnNames}. For Delete events, the after-image
     * is {@code null} (tombstone semantics).
     * </p>
     *
     * @return column values for the "after" image, or {@code null} for Delete
     *         operations
     */
    @Override
    protected Object[] getNewColumnValues() {
        if (changeEvent instanceof MilvusChangeEvent.Insert insert) {
            Map<String, Object> data = insert.getData();
            if (data == null || columnNames == null) {
                return new Object[0];
            }
            Object[] values = new Object[columnNames.length];
            for (int i = 0; i < columnNames.length; i++) {
                values[i] = data.get(columnNames[i]);
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
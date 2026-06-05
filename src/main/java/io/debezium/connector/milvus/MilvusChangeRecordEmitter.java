/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import io.debezium.data.Envelope;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
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
 */
public class MilvusChangeRecordEmitter extends RelationalChangeRecordEmitter<MilvusPartition> {

    private final MilvusChangeEvent changeEvent;
    private final Envelope.Operation operation;

    public MilvusChangeRecordEmitter(MilvusPartition partition, OffsetContext offsetContext,
            Clock clock, RelationalDatabaseConnectorConfig connectorConfig,
            MilvusChangeEvent changeEvent, Envelope.Operation operation) {
        super(partition, offsetContext, clock, connectorConfig);
        this.changeEvent = changeEvent;
        this.operation = operation;
    }

    @Override
    protected Object[] getOldColumnValues() {
        return null;
    }

    @Override
    protected Object[] getNewColumnValues() {
        return null;
    }

    @Override
    public Envelope.Operation getOperation() {
        return operation;
    }
}
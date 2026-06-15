/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.ErrorHandler;

/**
 * Source task for the Milvus connector
 *
 * <p>
 * Reads raw messages from Kafka via the MQ consumer layer.
 * No deserialization or event processing is performed at this stage.
 * </p>
 */
public class MilvusConnectorTask extends BaseSourceTask<MilvusPartition, MilvusOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MilvusConnectorTask.class);

    private volatile MilvusConnectorConfig connectorConfig;
    private volatile boolean running = false;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    protected String connectorName() {
        return Module.name();
    }

    @Override
    public CdcSourceTaskContext<MilvusConnectorConfig> preStart(Configuration config) {
        this.connectorConfig = new MilvusConnectorConfig(config);
        return new CdcSourceTaskContext<>(config, connectorConfig, Collections.emptyMap());
    }

    @Override
    protected ChangeEventSourceCoordinator<MilvusPartition, MilvusOffsetContext> start(Configuration config) {
        LOGGER.info("Starting Milvus connector task — MQ read layer");
        this.running = true;
        // MQ read layer only: coordinator wiring deferred
        LOGGER.info("Milvus connector task started successfully (MQ read layer)");
        return null;
    }

    @Override
    protected List<SourceRecord> doPoll() throws InterruptedException {
        if (!running) {
            return null;
        }
        Thread.sleep(connectorConfig != null
                ? connectorConfig.getPollIntervalMs()
                : 500);
        return Collections.emptyList();
    }

    @Override
    protected void doStop() {
        LOGGER.info("Stopping Milvus connector task");
        this.running = false;
        LOGGER.info("Milvus connector task stopped");
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return MilvusConnectorConfig.ALL_FIELDS;
    }

    @Override
    protected Optional<ErrorHandler> getErrorHandler() {
        return Optional.empty();
    }
}

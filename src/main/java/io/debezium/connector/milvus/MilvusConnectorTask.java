/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.time.Duration;
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
 * Source task for the Milvus connector.
 *
 * <p>
 * Startup sequence:
 *
 * <pre>
 * load config
 *   → construct metadata client
 *   → construct checkpoint reader
 *   → construct partition provider
 *   → discover collections/channels
 *   → initialize source partitions
 *   → initialize offset context
 *   → wire ChangeEventSourceCoordinator
 *   → ready
 * </pre>
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
    protected ChangeEventSourceCoordinator<MilvusPartition, MilvusOffsetContext> start(Configuration config) {
        LOGGER.info("Starting Milvus connector task");
        this.connectorConfig = new MilvusConnectorConfig(config);
        this.running = true;

        // TODO: wire the full coordinator infrastructure:
        // 1. Create MilvusConnection (gRPC + etcd)
        // 2. Create partition provider via metadata client
        // 3. Load previous offsets via MilvusOffsetContext.Loader
        // 4. Create MilvusDatabaseSchema
        // 5. Create ChangeEventQueue
        // 6. Create EventDispatcher
        // 7. Build snapshot & streaming sources directly
        // 8. Instantiate ChangeEventSourceCoordinator and return it

        LOGGER.info("Milvus connector task started successfully (coordinator wiring TODO)");
        return null;
    }

    @Override
    public CdcSourceTaskContext<MilvusConnectorConfig> preStart(Configuration config) {
        return null;
    }

    @Override
    protected List<SourceRecord> doPoll() throws InterruptedException {
        if (!running) {
            return null;
        }
        // TODO: drain from ChangeEventQueue instead of sleeping
        Thread.sleep(Duration.ofMillis(connectorConfig != null
                ? connectorConfig.getPollIntervalMs()
                : 500).toMillis());
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
        return Collections.emptyList();
    }

    @Override
    protected Optional<ErrorHandler> getErrorHandler() {
        return Optional.empty();
    }
}

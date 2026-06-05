/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.io.Closeable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the lifecycle of the Milvus gRPC client and the etcd client.
 *
 * <p>Both connections are established during {@link #start()} and closed during
 * {@link #close()}. The gRPC client is used for metadata and data queries;
 * the etcd client is used for reading channel checkpoints.</p>
 */
public class MilvusConnection implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MilvusConnection.class);

    private final MilvusConnectorConfig config;
    private volatile boolean started = false;

    public MilvusConnection(MilvusConnectorConfig config) {
        this.config = config;
    }

    /**
     * Establish gRPC and etcd connections.
     */
    public void start() {
        LOGGER.info("Starting MilvusConnection to {} with etcd {}",
                config.getMilvusUri(), config.getEtcdEndpoints());
        // TODO: instantiate gRPC client and etcd client
        this.started = true;
    }

    /**
     * @return true if both connections are active
     */
    public boolean isStarted() {
        return started;
    }

    /**
     * @return the connector configuration
     */
    public MilvusConnectorConfig getConfig() {
        return config;
    }

    @Override
    public void close() {
        LOGGER.info("Closing MilvusConnection");
        this.started = false;
        // TODO: close gRPC and etcd clients
    }
}

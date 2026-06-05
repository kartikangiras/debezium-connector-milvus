/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TSO-based ordering engine for Milvus change events.
 *
 * <p>
 * TODO— will be implemented when deserialization is added.
 * </p>
 */
public class TimetickOrderingEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimetickOrderingEngine.class);

    private final MilvusConnectorConfig config;

    public TimetickOrderingEngine(MilvusConnectorConfig config) {
        this.config = config;
    }

    public boolean buffer(RawMilvusMessage event) throws MilvusBufferFullException {
        return true;
    }

    public long computeWatermark() {
        return 0L;
    }

    public List<RawMilvusMessage> flush() {
        return List.of();
    }

    public void updateWatermark(String vchannel, long tso) {
    }

    public boolean isStalled() {
        return false;
    }
}

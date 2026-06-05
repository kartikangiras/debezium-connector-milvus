/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

/**
 * Error handler for the Milvus connector.
 */
@ThreadSafe
public class MilvusErrorHandler extends ErrorHandler {

    public MilvusErrorHandler(MilvusConnectorConfig connectorConfig,
                              ChangeEventQueue<?> queue,
                              ErrorHandler replacedErrorHandler) {
        super(MilvusConnector.class, connectorConfig, queue, replacedErrorHandler);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

/**
 * Seek strategies supported by the message queue consumer.
 *
 * <p>Promoted to the transport-agnostic interface so that the streaming source
 * can express its intent without casting to a specific implementation.</p>
 */
public enum SeekPosition {
    EARLIEST,
    LATEST,
    STORED_OFFSET_PLUS_ONE,
    DEFAULT
}

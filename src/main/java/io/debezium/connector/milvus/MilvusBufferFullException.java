/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

/**
 * Exception thrown when the {@link TimetickOrderingEngine} buffer reaches its
 * configured capacity.
 *
 * <p>This signals backpressure to the streaming change event source, which
 * should pause polling until the buffer has been drained.</p>
 */
public class MilvusBufferFullException extends Exception {

    private final int currentSize;
    private final int maxSize;

    public MilvusBufferFullException(int currentSize, int maxSize) {
        super(String.format("Buffer full: %d / %d events", currentSize, maxSize));
        this.currentSize = currentSize;
        this.maxSize = maxSize;
    }

    public int getCurrentSize() {
        return currentSize;
    }

    public int getMaxSize() {
        return maxSize;
    }
}

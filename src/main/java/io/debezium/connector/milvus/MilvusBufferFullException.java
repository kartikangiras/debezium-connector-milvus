/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

/**
 * Exception thrown when the {@link TimetickOrderingEngine} buffer reaches its
 * configured capacity, either by event count or by approximate byte size.
 *
 * <p>This signals backpressure to the streaming change event source, which
 * should pause polling and wait for the watermark to advance or trigger a
 * force-flush after the stall timeout.</p>
 */
public class MilvusBufferFullException extends Exception {

    private static final long serialVersionUID = 1L;

    private final long currentSize;
    private final long maxSize;
    private final boolean bytesBased;

    /**
     * Construct an event-count based buffer-full exception.
     *
     * @param currentSize the current number of buffered events
     * @param maxSize     the configured maximum number of buffered events
     */
    public MilvusBufferFullException(int currentSize, int maxSize) {
        super(String.format("Buffer full: %d / %d events", currentSize, maxSize));
        this.currentSize = currentSize;
        this.maxSize = maxSize;
        this.bytesBased = false;
    }

    /**
     * Construct a byte-budget based buffer-full exception.
     *
     * @param currentBytes the current approximate bytes buffered
     * @param maxBytes     the configured maximum bytes allowed
     */
    public MilvusBufferFullException(long currentBytes, long maxBytes) {
        super(String.format("Buffer full: %d / %d bytes", currentBytes, maxBytes));
        this.currentSize = currentBytes;
        this.maxSize = maxBytes;
        this.bytesBased = true;
    }

    /**
     * @return the current size (event count or bytes, depending on
     *         {@link #isBytesBased()})
     */
    public long getCurrentSize() {
        return currentSize;
    }

    /**
     * @return the configured maximum (event count or bytes, depending on
     *         {@link #isBytesBased()})
     */
    public long getMaxSize() {
        return maxSize;
    }

    /**
     * @return {@code true} if this exception was triggered by the byte budget;
     *         {@code false} if by event count
     */
    public boolean isBytesBased() {
        return bytesBased;
    }
}

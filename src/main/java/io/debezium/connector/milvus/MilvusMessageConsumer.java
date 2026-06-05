/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Abstraction over the message queue consumer (typically Kafka) for a Milvus
 * pchannel.
 *
 * <p>Implementations are responsible for:</p>
 * <ul>
 *   <li>{@link #assign(String)} — manual topic assignment (no group rebalance)</li>
 *   <li>{@link #seek(String, long)} — seek to a specific offset or checkpoint</li>
 *   <li>{@link #poll(long)} — fetch the next batch of raw records</li>
 *   <li>{@link #close()} — clean shutdown</li>
 * </ul>
 */
public interface MilvusMessageConsumer {

    /**
     * Manually assign the given pchannel (topic) to this consumer.
     *
     * @param pchannel the physical channel name (Kafka topic)
     */
    void assign(String pchannel);

    /**
     * Seek to the given offset for the assigned pchannel.
     *
     * @param pchannel the physical channel name
     * @param offset   the Kafka offset to seek to
     */
    void seek(String pchannel, long offset);

    /**
     * Poll for the next batch of records.
     *
     * @param timeoutMs maximum time to wait in milliseconds
     * @return list of raw consumer records; empty if none available
     */
    List<ConsumerRecord<byte[], byte[]>> poll(long timeoutMs);

    /**
     * Close the consumer and release all resources.
     */
    void close();
}

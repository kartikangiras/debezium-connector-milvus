/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;

/**
 * Abstraction over the message queue consumer (typically Kafka) for a Milvus
 * pchannel.
 *
 * <p>Implementations are responsible for:</p>
 * <ul>
 *   <li>{@link #assignAndSeek(Map)} — manual topic assignment and explicit seek
 *       (no group rebalance)</li>
 *   <li>{@link #assignAndSeek(Set, SeekPosition, Map)} — strategy-aware assignment
 *       and seek with validation</li>
 *   <li>{@link #poll(Duration)} — fetch the next batch of raw records</li>
 *   <li>{@link #close()} — clean shutdown</li>
 * </ul>
 *
 * <p>This interface is intentionally transport-agnostic so that a Woodpecker
 * WAL implementation for Milvus 2.6 can be plugged in without changing the
 * streaming engine.</p>
 */
public interface MilvusMessageConsumer extends AutoCloseable {

    /**
     * Manually assign the given partitions and seek to the specified offsets.
     *
     * <p>Each entry in the map is a {@link TopicPartition} → absolute offset
     * mapping. The caller decides the offset value (e.g. checkpoint offset,
     * stored offset+1, etc.).</p>
     *
     * @param offsets topic-partition to offset mapping
     */
    void assignAndSeek(Map<TopicPartition, Long> offsets);

    /**
     * Assign partitions and seek using one of the supported strategies.
     *
     * @param pchannels     set of physical channel names (Kafka topics)
     * @param position      seek strategy to apply
     * @param storedOffsets stored offsets for
     *                      {@link SeekPosition#STORED_OFFSET_PLUS_ONE} or
     *                      checkpoint offsets for {@link SeekPosition#DEFAULT}
     */
    void assignAndSeek(Set<String> pchannels, SeekPosition position,
                       Map<TopicPartition, Long> storedOffsets);

    /**
     * Poll for the next batch of records.
     *
     * @param timeout maximum time to wait
     * @return list of raw messages; empty if none available
     */
    List<RawMilvusMessage> poll(Duration timeout);

    /**
     * Close the consumer and release all resources.
     */
    @Override
    void close();
}

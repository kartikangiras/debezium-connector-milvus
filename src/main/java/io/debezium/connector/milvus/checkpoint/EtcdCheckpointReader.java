/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus.checkpoint;

import java.util.Optional;

/**
 * Reader interface for fetching Milvus channel checkpoints stored in etcd.
 *
 * <p>Implementations use the jetcd client to read {@link ChannelCheckpoint}
 * entries from the Milvus metadata store.</p>
 */
public interface EtcdCheckpointReader extends AutoCloseable {

    /**
     * Read the checkpoint for the given pchannel.
     *
     * @param pchannel the physical channel name
     * @return the channel checkpoint if present, empty if no checkpoint exists
     */
    Optional<ChannelCheckpoint> read(String pchannel);

    /**
     * Check whether the etcd cluster is accessible.
     *
     * @return true if the etcd endpoints are reachable
     */
    boolean isAccessible();

    /**
     * Close the checkpoint reader and release any open connections.
     */
    @Override
    void close();
}
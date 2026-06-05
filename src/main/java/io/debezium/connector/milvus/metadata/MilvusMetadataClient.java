/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus.metadata;

import java.util.List;
import java.util.Optional;

/**
 * Client interface for fetching Milvus metadata (collections, schemas, channels).
 *
 * <p>Implementations communicate with the Milvus gRPC endpoint to discover
 * collections and their physical/virtual channel mappings.</p>
 */
public interface MilvusMetadataClient extends AutoCloseable {

    /**
     * List all collections in the configured database.
     *
     * @return list of collection metadata; never null
     */
    List<CollectionMetadata> collections();

    /**
     * Get the schema for a named collection.
     *
     * @param collection the collection name
     * @return the collection schema
     * @throws CollectionNotFoundException if the collection does not exist
     */
    MilvusCollectionSchema schema(String collection);

    /**
     * Discover the vchannels for a collection, including their pchannel mapping.
     *
     * @param collection the collection name
     * @return list of vchannel metadata for the collection
     * @throws CollectionNotFoundException if the collection does not exist
     */
    List<VChannelMetadata> channels(String collection);

    /**
     * Check whether the Milvus endpoint is reachable.
     *
     * @return true if the endpoint is reachable and responding
     */
    boolean isReachable();

    /**
     * Check whether the configured database exists.
     *
     * @return true if the database exists
     */
    boolean databaseExists();

    /**
     * Find a collection by name.
     *
     * @param collectionName the collection name
     * @return the collection metadata if found
     */
    Optional<CollectionMetadata> findCollection(String collectionName);

    /**
     * Close the metadata client and release any open connections.
     */
    @Override
    void close();
}
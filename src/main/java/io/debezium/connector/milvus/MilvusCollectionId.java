/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.util.List;

import io.debezium.spi.schema.DataCollectionId;

/**
 * Identifier for a Milvus collection used as a {@link DataCollectionId}.
 */
public class MilvusCollectionId implements DataCollectionId {

    private final String databaseName;
    private final String collectionName;

    public MilvusCollectionId(String databaseName, String collectionName) {
        this.databaseName = databaseName;
        this.collectionName = collectionName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    @Override
    public String identifier() {
        return databaseName + "." + collectionName;
    }

    @Override
    public List<String> parts() {
        return List.of(databaseName, collectionName);
    }

    @Override
    public List<String> databaseParts() {
        return List.of(databaseName);
    }

    @Override
    public List<String> schemaParts() {
        return List.of();
    }

    @Override
    public String toString() {
        return identifier();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus.metadata;

/**
 * Metadata about a Milvus collection.
 */
public class CollectionMetadata {

    private final String name;
    private final long id;
    private final int rowCount;
    private final long createdTimestamp;

    public CollectionMetadata(String name, long id, int rowCount, long createdTimestamp) {
        this.name = name;
        this.id = id;
        this.rowCount = rowCount;
        this.createdTimestamp = createdTimestamp;
    }

    public String getName() {
        return name;
    }

    public long getId() {
        return id;
    }

    public int getRowCount() {
        return rowCount;
    }

    public long getCreatedTimestamp() {
        return createdTimestamp;
    }

    @Override
    public String toString() {
        return "CollectionMetadata{name='" + name + "', id=" + id + ", rowCount=" + rowCount + "}";
    }
}
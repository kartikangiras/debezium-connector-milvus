/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus.metadata;

/**
 * Exception thrown when a collection is not found during metadata lookup.
 */
public class CollectionNotFoundException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String collectionName;

    public CollectionNotFoundException(String collectionName) {
        super("Collection not found: " + collectionName);
        this.collectionName = collectionName;
    }

    public CollectionNotFoundException(String collectionName, Throwable cause) {
        super("Collection not found: " + collectionName, cause);
        this.collectionName = collectionName;
    }

    public String getCollectionName() {
        return collectionName;
    }
}
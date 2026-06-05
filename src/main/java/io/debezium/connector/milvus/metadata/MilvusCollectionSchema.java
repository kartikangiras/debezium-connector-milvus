/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus.metadata;

import java.util.List;

/**
 * Schema representation for a Milvus collection.
 */
public class MilvusCollectionSchema {

    private final String collectionName;
    private final String databaseName;
    private final String description;
    private final List<FieldSchema> fields;
    private final String primaryKeyField;

    public MilvusCollectionSchema(String collectionName, String databaseName,
                                  String description, List<FieldSchema> fields,
                                  String primaryKeyField) {
        this.collectionName = collectionName;
        this.databaseName = databaseName;
        this.description = description;
        this.fields = fields;
        this.primaryKeyField = primaryKeyField;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getDescription() {
        return description;
    }

    public List<FieldSchema> getFields() {
        return fields;
    }

    public String getPrimaryKeyField() {
        return primaryKeyField;
    }

    public static class FieldSchema {
        private final String name;
        private final int dataType;
        private final String description;
        private final boolean isPrimaryKey;
        private final boolean isNullable;
        private final int dimension;

        public FieldSchema(String name, int dataType, String description,
                           boolean isPrimaryKey, boolean isNullable, int dimension) {
            this.name = name;
            this.dataType = dataType;
            this.description = description;
            this.isPrimaryKey = isPrimaryKey;
            this.isNullable = isNullable;
            this.dimension = dimension;
        }

        public String getName() {
            return name;
        }

        public int getDataType() {
            return dataType;
        }

        public String getDescription() {
            return description;
        }

        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

        public boolean isNullable() {
            return isNullable;
        }

        public int getDimension() {
            return dimension;
        }
    }

    @Override
    public String toString() {
        return "MilvusCollectionSchema{collection='" + collectionName +
                "', fields=" + fields.size() +
                ", primaryKey='" + primaryKeyField + "'}";
    }
}
/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.Key;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.schema.FieldNameSelector;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Database schema for Milvus collections.
 *
 * <p>
 * Extends {@link RelationalDatabaseSchema} and supports dynamic table
 * registration from streaming events via {@link #registerCollection}.
 * </p>
 */
public class MilvusDatabaseSchema extends RelationalDatabaseSchema {

    private static final Logger LOGGER = LoggerFactory.getLogger(MilvusDatabaseSchema.class);

    private final Set<TableId> registeredTableIds = new HashSet<>();
    private final Map<TableId, String[]> registeredColumnNames = new HashMap<>();
    private final Map<TableId, String> registeredPkFields = new HashMap<>();
    private final io.debezium.connector.milvus.metadata.MilvusMetadataClient metadataClient;

    public MilvusDatabaseSchema(RelationalDatabaseConnectorConfig config,
                                TopicNamingStrategy<TableId> topicNamingStrategy,
                                Tables.TableFilter tableFilter,
                                Tables.ColumnNameFilter columnFilter,
                                TableSchemaBuilder tableSchemaBuilder,
                                boolean tableIdCaseInsensitive,
                                Key.KeyMapper keyMapper,
                                CdcSourceTaskContext<?> context) {
        this(config, topicNamingStrategy, tableFilter, columnFilter,
                tableSchemaBuilder, tableIdCaseInsensitive, keyMapper, context, null);
    }

    public MilvusDatabaseSchema(RelationalDatabaseConnectorConfig config,
                                TopicNamingStrategy<TableId> topicNamingStrategy,
                                Tables.TableFilter tableFilter,
                                Tables.ColumnNameFilter columnFilter,
                                TableSchemaBuilder tableSchemaBuilder,
                                boolean tableIdCaseInsensitive,
                                Key.KeyMapper keyMapper,
                                CdcSourceTaskContext<?> context,
                                io.debezium.connector.milvus.metadata.MilvusMetadataClient metadataClient) {
        super(config, topicNamingStrategy, tableFilter, columnFilter,
                tableSchemaBuilder, tableIdCaseInsensitive, keyMapper, context);
        this.metadataClient = metadataClient;
    }

    public static MilvusDatabaseSchema create(MilvusConnectorConfig connectorConfig,
                                              CdcSourceTaskContext<?> taskContext) {
        return create(connectorConfig, taskContext, null);
    }

    public static MilvusDatabaseSchema create(MilvusConnectorConfig connectorConfig,
                                              CdcSourceTaskContext<?> taskContext,
                                              io.debezium.connector.milvus.metadata.MilvusMetadataClient metadataClient) {
        TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig
                .getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY);
        Tables.TableFilter tableFilter = Tables.TableFilter.includeAll();
        Tables.ColumnNameFilter columnFilter = (catalog, schema, table, column) -> true;
        SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();

        MilvusValueConverter valueConverter = new MilvusValueConverter(connectorConfig);
        TableSchemaBuilder tableSchemaBuilder = new TableSchemaBuilder(
                valueConverter,
                schemaNameAdjuster,
                new CustomConverterRegistry(Collections.emptyList()),
                connectorConfig.getSourceInfoStructMaker(CommonConnectorConfig.Version.V1).schema(),
                SchemaFactory.get().transactionBlockSchema(),
                FieldNameSelector.defaultSelector(schemaNameAdjuster),
                false,
                connectorConfig.getEventConvertingFailureHandlingMode());

        return new MilvusDatabaseSchema(
                connectorConfig,
                topicNamingStrategy,
                tableFilter,
                columnFilter,
                tableSchemaBuilder,
                false,
                table -> table.primaryKeyColumns(),
                taskContext,
                metadataClient);
    }

    /**
     * Dynamically register a Milvus collection as a relational table.
     *
     * <p>Uses the first Insert event's data map to infer column names
     * and JDBC types. The first field in the data map is treated as
     * the primary key (Milvus convention).</p>
     *
     * @param dbName         Milvus database name
     * @param collectionName Milvus collection name
     * @param sampleData     data map from the first Insert event (used to infer types)
     * @return {@code true} if newly registered, {@code false} if already registered
     */
    public synchronized boolean registerCollection(String dbName, String collectionName,
                                                   Map<String, Object> sampleData) {
        TableId tableId = new TableId(null, dbName, collectionName);

        if (registeredTableIds.contains(tableId)) {
            return false;
        }

        List<String> fieldNames = new ArrayList<>(sampleData.keySet());
        if (fieldNames.isEmpty()) {
            LOGGER.warn("Cannot register collection {} — empty data map", collectionName);
            return false;
        }

        String pkFieldName = resolvePrimaryKeyField(collectionName, fieldNames);
        int position = 1;

        TableEditor tableEditor = Table.editor().tableId(tableId);

        for (String fieldName : fieldNames) {
            Object value = sampleData.get(fieldName);
            int jdbcType = inferJdbcType(value);
            String typeName = inferTypeName(value);
            boolean isPk = fieldName.equals(pkFieldName);

            ColumnEditor columnEditor = Column.editor()
                    .name(fieldName)
                    .type(typeName)
                    .jdbcType(jdbcType)
                    .optional(!isPk)
                    .position(position++);

            if (jdbcType == Types.VARCHAR) {
                columnEditor.length(65535);
            }

            tableEditor.addColumn(columnEditor.create());
        }

        tableEditor.setPrimaryKeyNames(pkFieldName);
        Table table = tableEditor.create();

        refresh(table);

        registeredTableIds.add(tableId);
        registeredColumnNames.put(tableId, fieldNames.toArray(new String[0]));
        registeredPkFields.put(tableId, pkFieldName);

        LOGGER.info("Registered collection schema: tableId={}, columns={}, pk={}",
                tableId, fieldNames, pkFieldName);
        return true;
    }

    private String resolvePrimaryKeyField(String collectionName, List<String> fieldNames) {
        if (metadataClient != null) {
            try {
                io.debezium.connector.milvus.metadata.MilvusCollectionSchema schema = metadataClient.schema(collectionName);
                String pk = schema.getPrimaryKeyField();
                if (pk != null && !pk.isBlank() && fieldNames.contains(pk)) {
                    return pk;
                }
            }
            catch (Exception e) {
                LOGGER.warn("Failed to resolve primary key for collection {} from Milvus metadata; "
                        + "falling back to first field. Reason: {}", collectionName, e.getMessage());
            }
        }
        return fieldNames.get(0);
    }

    /**
     * Check whether a collection has been registered.
     */
    public boolean isCollectionRegistered(TableId tableId) {
        return registeredTableIds.contains(tableId);
    }

    /**
     * Get the column names for a registered collection.
     */
    public String[] getColumnNames(TableId tableId) {
        return registeredColumnNames.get(tableId);
    }

    /**
     * Get the primary key field name for a registered collection.
     */
    public String getPkFieldName(TableId tableId) {
        return registeredPkFields.get(tableId);
    }

    /**
     * Dynamically register a Milvus collection as a relational table using only field names.
     *
     * <p>When no sample data is available to infer types, all columns default to VARCHAR.
     * The first field in the list is treated as the primary key.</p>
     *
     * @param dbName         Milvus database name
     * @param collectionName Milvus collection name
     * @param fieldNames     ordered list of field names
     */
    public void registerCollection(String dbName, String collectionName, List<String> fieldNames) {
        TableId tableId = new TableId(null, dbName, collectionName);

        if (registeredTableIds.contains(tableId)) {
            return;
        }

        if (fieldNames == null || fieldNames.isEmpty()) {
            LOGGER.warn("Cannot register collection {} — empty field name list", collectionName);
            return;
        }

        String pkFieldName = fieldNames.get(0);
        int position = 1;

        TableEditor tableEditor = Table.editor().tableId(tableId);

        for (String fieldName : fieldNames) {
            boolean isPk = fieldName.equals(pkFieldName);

            ColumnEditor columnEditor = Column.editor()
                    .name(fieldName)
                    .type("VARCHAR")
                    .jdbcType(Types.VARCHAR)
                    .optional(!isPk)
                    .length(65535)
                    .position(position++);

            tableEditor.addColumn(columnEditor.create());
        }

        tableEditor.setPrimaryKeyNames(pkFieldName);
        Table table = tableEditor.create();

        refresh(table);

        registeredTableIds.add(tableId);
        registeredColumnNames.put(tableId, fieldNames.toArray(new String[0]));
        registeredPkFields.put(tableId, pkFieldName);

        LOGGER.info("Registered collection schema: tableId={}, columns={}, pk={}",
                tableId, fieldNames, pkFieldName);
    }

    private static int inferJdbcType(Object value) {
        if (value instanceof Long) {
            return Types.BIGINT;
        }
        if (value instanceof Integer) {
            return Types.INTEGER;
        }
        if (value instanceof Double) {
            return Types.DOUBLE;
        }
        if (value instanceof Float) {
            return Types.FLOAT;
        }
        if (value instanceof Boolean) {
            return Types.BOOLEAN;
        }
        if (value instanceof byte[] || value instanceof float[]) {
            return Types.BLOB;
        }
        return Types.VARCHAR;
    }

    private static String inferTypeName(Object value) {
        if (value instanceof Long) {
            return "INT64";
        }
        if (value instanceof Integer) {
            return "INT32";
        }
        if (value instanceof Double) {
            return "DOUBLE";
        }
        if (value instanceof Float) {
            return "FLOAT";
        }
        if (value instanceof Boolean) {
            return "BOOLEAN";
        }
        if (value instanceof byte[] || value instanceof float[]) {
            return "BLOB";
        }
        return "VARCHAR";
    }
}

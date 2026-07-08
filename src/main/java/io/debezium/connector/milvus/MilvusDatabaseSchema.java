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
import io.milvus.grpc.DataType;

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
     * and JDBC types. When {@code fieldTypes} is provided (non-null, non-empty),
     * the Milvus {@link DataType} is used for accurate JDBC type assignment;
     * otherwise the method falls back to inferring the JDBC type from the Java
     * runtime type of the sample value.</p>
     *
     * @param dbName         Milvus database name
     * @param collectionName Milvus collection name
     * @param sampleData     data map from the first Insert event (used to infer types)
     * @param fieldTypes     per-field Milvus DataType; may be null or empty
     * @return {@code true} if newly registered, {@code false} if already registered
     */
    public synchronized boolean registerCollection(String dbName, String collectionName,
                                                   Map<String, Object> sampleData,
                                                   Map<String, DataType> fieldTypes) {
        TableId tableId = new TableId(null, dbName, collectionName);

        if (registeredTableIds.contains(tableId)) {
            return false;
        }

        // Preserve insertion order so column positions are stable
        List<String> fieldNames = new ArrayList<>(sampleData.keySet());
        if (fieldNames.isEmpty()) {
            LOGGER.warn("Cannot register collection {} — empty data map", collectionName);
            return false;
        }

        String pkFieldName = resolvePrimaryKeyField(collectionName, fieldNames, fieldTypes);
        int position = 1;

        TableEditor tableEditor = Table.editor().tableId(tableId);

        for (String fieldName : fieldNames) {
            Object value = sampleData.get(fieldName);
            DataType milvusType = (fieldTypes != null) ? fieldTypes.get(fieldName) : null;
            int jdbcType;
            String typeName;
            if (milvusType != null) {
                jdbcType = inferJdbcTypeFromMilvus(milvusType);
                typeName = milvusType.name();
            }
            else {
                jdbcType = inferJdbcType(value);
                typeName = inferTypeName(value);
            }
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

    /**
     * Convenience overload — delegates to
     * {@link #registerCollection(String, String, Map, Map)} without
     * field-type information (falls back to Java runtime-type inference).
     *
     * @param dbName         Milvus database name
     * @param collectionName Milvus collection name
     * @param sampleData     data map from the first Insert event
     * @return {@code true} if newly registered, {@code false} if already registered
     */
    public synchronized boolean registerCollection(String dbName, String collectionName,
                                                   Map<String, Object> sampleData) {
        return registerCollection(dbName, collectionName, sampleData, null);
    }

    private String resolvePrimaryKeyField(String collectionName, List<String> fieldNames,
                                          Map<String, DataType> fieldTypes) {
        // 1. Prefer authoritative metadata from Milvus.
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
                        + "falling back to heuristics. Reason: {}", collectionName, e.getMessage());
            }
        }

        // 2. Heuristic: look for common primary-key names among scalar fields.
        for (String candidate : fieldNames) {
            String lower = candidate.toLowerCase();
            if (("id".equals(lower) || lower.endsWith("_id") || lower.startsWith("id_"))
                    && isScalar(fieldTypes.get(candidate))) {
                return candidate;
            }
        }

        // 3. Fall back to the first scalar field; vector fields make poor primary keys.
        for (String candidate : fieldNames) {
            if (isScalar(fieldTypes.get(candidate))) {
                return candidate;
            }
        }

        // 4. Last resort: first field.
        return fieldNames.get(0);
    }

    private static boolean isScalar(DataType type) {
        if (type == null) {
            // Unknown type is treated as scalar to avoid defaulting to a vector.
            return true;
        }
        return switch (type) {
            case Bool, Int8, Int16, Int32, Int64, Float, Double, String, VarChar, Text, JSON -> true;
            default -> false;
        };
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

        String pkFieldName = resolvePrimaryKeyField(collectionName, fieldNames, Map.of());
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

    /**
     * Map a Milvus {@link DataType} to the most appropriate JDBC type constant.
     *
     * <ul>
     *   <li>{@link DataType#JSON} → {@link Types#OTHER} (signals
     *       {@code io.debezium.data.Json} logical type in the value converter)</li>
     *   <li>{@link DataType#FloatVector} → {@link Types#JAVA_OBJECT} (signals
     *       {@code io.debezium.data.vector.FloatVector} logical type)</li>
     *   <li>{@link DataType#Float16Vector} / {@link DataType#BFloat16Vector} /
     *       {@link DataType#BinaryVector} / {@link DataType#Int8Vector} →
     *       {@link Types#BLOB} (raw byte array)</li>
     *   <li>{@link DataType#SparseFloatVector} → {@link Types#VARCHAR} (JSON string)</li>
     * </ul>
     */
    static int inferJdbcTypeFromMilvus(DataType type) {
        return switch (type) {
            case Bool -> Types.BOOLEAN;
            case Int8, Int16 -> Types.SMALLINT;
            case Int32 -> Types.INTEGER;
            case Int64 -> Types.BIGINT;
            case Float -> Types.FLOAT;
            case Double -> Types.DOUBLE;
            case String, VarChar, Text -> Types.VARCHAR;
            case JSON, Geometry -> Types.OTHER;
            case FloatVector -> Types.JAVA_OBJECT;
            case Float16Vector, BFloat16Vector, BinaryVector, Int8Vector -> Types.BLOB;
            case SparseFloatVector -> Types.VARCHAR;
            case Array -> Types.ARRAY;
            default -> Types.VARCHAR;
        };
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

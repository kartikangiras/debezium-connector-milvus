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
import io.debezium.connector.milvus.metadata.CollectionNotFoundException;
import io.debezium.connector.milvus.metadata.MilvusCollectionSchema;
import io.debezium.connector.milvus.metadata.MilvusMetadataClient;
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
    private final MilvusMetadataClient metadataClient;

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
                                MilvusMetadataClient metadataClient) {
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
                                              MilvusMetadataClient metadataClient) {
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
     * <p>Uses the first Insert event's {@link MilvusRow} to infer column names
     * and JDBC types. The Milvus {@link DataType} from each field is used for
     * accurate JDBC type assignment; when a field's type is
     * {@link DataType#None}, the method falls back to inferring the JDBC type
     * from the Java runtime type of the sample value.</p>
     *
     * <p>Field ordering is determined by the list order — not by any
     * {@code Map} implementation — so column positions are stable regardless
     * of how the row was built.</p>
     *
     * @param dbName         Milvus database name
     * @param collectionName Milvus collection name
     * @param fields         ordered list of field definitions from the first Insert event
     * @return {@code true} if newly registered, {@code false} if already registered
     */
    public synchronized boolean registerCollection(String dbName, String collectionName,
                                                   List<FieldDefinition> fields) {
        TableId tableId = new TableId(null, dbName, collectionName);

        if (registeredTableIds.contains(tableId)) {
            return false;
        }

        if (fields == null || fields.isEmpty()) {
            LOGGER.warn("Cannot register collection {} — empty field list", collectionName);
            return false;
        }

        fields = List.copyOf(fields);

        List<String> fieldNameList = new ArrayList<>(fields.size());
        Map<String, DataType> fieldTypeMap = new HashMap<>(fields.size() * 2);
        for (FieldDefinition field : fields) {
            fieldNameList.add(field.fieldName());
            if (field.dataType() != null) {
                fieldTypeMap.put(field.fieldName(), field.dataType());
            }
        }

        String pkFieldName = resolvePrimaryKeyField(collectionName, fieldNameList, fieldTypeMap);
        int position = 1;

        TableEditor tableEditor = Table.editor().tableId(tableId);

        for (FieldDefinition field : fields) {
            String fieldName = field.fieldName();
            DataType milvusType = field.dataType();
            Object sampleValue = field.sampleValue();

            int jdbcType;
            String typeName;
            if (milvusType != null && milvusType != DataType.None) {
                jdbcType = inferJdbcTypeFromMilvus(milvusType);
                typeName = milvusType.name();
            }
            else {
                jdbcType = inferJdbcType(sampleValue);
                typeName = inferTypeName(sampleValue);
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
        registeredColumnNames.put(tableId, fieldNameList.toArray(new String[0]));
        registeredPkFields.put(tableId, pkFieldName);

        LOGGER.info("Registered collection schema: tableId={}, columns={}, pk={}",
                tableId, fieldNameList, pkFieldName);
        return true;
    }

    private String resolvePrimaryKeyField(String collectionName, List<String> fieldNames,
                                          Map<String, DataType> fieldTypes) {
        if (metadataClient != null) {
            try {
                MilvusCollectionSchema schema = metadataClient.schema(collectionName);
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

        for (String candidate : fieldNames) {
            String lower = candidate.toLowerCase();
            if (("id".equals(lower) || lower.endsWith("_id") || lower.startsWith("id_"))
                    && isPrimaryKeyColumnType(fieldTypes.get(candidate))) {
                return candidate;
            }
        }

        for (String candidate : fieldNames) {
            if (isPrimaryKeyColumnType(fieldTypes.get(candidate))) {
                return candidate;
            }
        }

        return fieldNames.get(0);
    }

    private static boolean isScalar(DataType type) {
        if (type == null) {
            return true;
        }
        return switch (type) {
            case Bool, Int8, Int16, Int32, Int64, Float, Double, String, VarChar, Text, JSON,
                    Geometry, Array ->
                true;
            default -> false;
        };
    }

    /**
     * Returns {@code true} only for Milvus data types that are valid primary key
     * column types. Per the Milvus specification, primary keys must be either
     * {@link DataType#Int64} or {@link DataType#VarChar}.
     *
     * @param type the Milvus DataType to test; {@code null} returns {@code false}
     * @return {@code true} if {@code type} is a valid primary key type
     */
    private static boolean isPrimaryKeyColumnType(DataType type) {
        if (type == null) {
            return false;
        }
        return switch (type) {
            case Int64, VarChar -> true;
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
     * Attempt to register a collection by querying the Milvus metadata API.
     *
     * <p>
     * This is needed when a {@link MilvusChangeEvent.Delete} (or other non-Insert
     * event) arrives for a collection that has not yet been seen in an Insert event.
     * Without schema registration the event would be silently dropped. By fetching
     * the schema from Milvus we can register the collection on-demand and correctly
     * dispatch the event.
     * </p>
     *
     * @param dbName         Milvus database name
     * @param collectionName collection to look up
     * @return {@code true} if the collection was newly registered, {@code false} if
     *         already registered or metadata client unavailable / collection not found
     */
    public boolean registerCollectionFromMetadata(String dbName, String collectionName) {
        if (metadataClient == null) {
            return false;
        }
        TableId tableId = new TableId(null, dbName, collectionName);
        if (registeredTableIds.contains(tableId)) {
            return false;
        }
        try {
            MilvusCollectionSchema schema = metadataClient.schema(collectionName);
            List<FieldDefinition> fields = new ArrayList<>();
            for (MilvusCollectionSchema.FieldSchema f : schema.getFields()) {
                io.milvus.grpc.DataType dt = io.milvus.grpc.DataType.forNumber(f.getDataType());
                if (dt == null) {
                    dt = io.milvus.grpc.DataType.None;
                }
                fields.add(new FieldDefinition(f.getName(), null, dt));
            }
            if (fields.isEmpty()) {
                LOGGER.warn("Metadata schema for collection {} has no fields; cannot register", collectionName);
                return false;
            }
            LOGGER.info("Registering collection {} from Milvus metadata (triggered by non-Insert event)",
                    collectionName);
            return registerCollection(dbName, collectionName, fields);
        }
        catch (CollectionNotFoundException e) {
            LOGGER.warn("Collection {} not found in Milvus metadata; cannot register schema", collectionName);
            return false;
        }
        catch (Exception e) {
            LOGGER.warn("Failed to fetch schema for collection {} from Milvus metadata: {}",
                    collectionName, e.getMessage());
            return false;
        }
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
     * Map a Milvus {@link DataType} to the most appropriate JDBC type constant.
     *
     * <ul>
     *   <li>{@link DataType#JSON} / {@link DataType#Geometry} → {@link Types#OTHER} (signals
     *       {@code io.debezium.data.Json} logical type in the value converter)</li>
     *   <li>{@link DataType#FloatVector} → {@link Types#JAVA_OBJECT} (signals
     *       {@code io.debezium.data.vector.FloatVector} logical type)</li>
     *   <li>{@link DataType#Float16Vector} / {@link DataType#BFloat16Vector} /
     *       {@link DataType#BinaryVector} / {@link DataType#Int8Vector} →
     *       {@link Types#BLOB} (raw byte array)</li>
     *   <li>{@link DataType#SparseFloatVector} → {@link Types#VARCHAR} (JSON string
     *       representation of the sparse vector map)</li>
     *   <li>{@link DataType#ArrayOfVector} → {@link Types#BLOB} (opaque multi-vector
     *       structure; raw bytes is the only safe representation)</li>
     *   <li>{@link DataType#ArrayOfStruct} → {@link Types#BLOB} (opaque struct array;
     *       serialised as raw bytes)</li>
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
            case ArrayOfVector, ArrayOfStruct -> Types.BLOB;
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

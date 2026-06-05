/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.relational.Key;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Database schema for Milvus collections.
 *
 * <p>Extends {@link RelationalDatabaseSchema} for future use when
 * deserialization and table registration are added.</p>
 */
public class MilvusDatabaseSchema extends RelationalDatabaseSchema {

    public MilvusDatabaseSchema(RelationalDatabaseConnectorConfig config,
                                TopicNamingStrategy<TableId> topicNamingStrategy,
                                Tables.TableFilter tableFilter,
                                Tables.ColumnNameFilter columnFilter,
                                TableSchemaBuilder tableSchemaBuilder,
                                boolean tableIdCaseInsensitive,
                                Key.KeyMapper keyMapper,
                                CdcSourceTaskContext<?> context) {
        super(config, topicNamingStrategy, tableFilter, columnFilter,
                tableSchemaBuilder, tableIdCaseInsensitive, keyMapper, context);
    }

    public static MilvusDatabaseSchema create(MilvusConnectorConfig connectorConfig,
                                              CdcSourceTaskContext<?> taskContext) {
        TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(io.debezium.config.CommonConnectorConfig.TOPIC_NAMING_STRATEGY);
        Tables.TableFilter tableFilter = Tables.TableFilter.includeAll();
        Tables.ColumnNameFilter columnFilter = (catalog, schema, table, column) -> true;
        SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();

        TableSchemaBuilder tableSchemaBuilder = new TableSchemaBuilder(
                null,
                schemaNameAdjuster,
                new io.debezium.relational.CustomConverterRegistry(java.util.Collections.emptyList()),
                connectorConfig.getSourceInfoStructMaker(io.debezium.config.CommonConnectorConfig.Version.V1).schema(),
                io.debezium.schema.SchemaFactory.get().transactionBlockSchema(),
                io.debezium.schema.FieldNameSelector.defaultSelector(schemaNameAdjuster),
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
                taskContext);
    }

    public void registerCollection(String dbName, String collectionName, java.util.List<String> fieldNames) {
        // TODO: implement dynamic table registration when deserialization is added
    }
}

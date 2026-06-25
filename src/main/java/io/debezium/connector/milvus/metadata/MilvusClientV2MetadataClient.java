/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.milvus.MilvusConnectorConfig;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.collection.response.ListCollectionsResp;

/**
 * {@link MilvusMetadataClient} implementation backed by the Milvus SDK v2.
 *
 * <p>Communicates with the Milvus gRPC endpoint described by the connector
 * configuration to discover collections, their schemas, and primary-key fields.</p>
 */
public class MilvusClientV2MetadataClient implements MilvusMetadataClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(MilvusClientV2MetadataClient.class);

    private final MilvusClientV2 client;
    private final String databaseName;

    public MilvusClientV2MetadataClient(MilvusConnectorConfig config) {
        this.databaseName = config.getMilvusDatabase();
        ConnectConfig.ConnectConfigBuilder builder = ConnectConfig.builder()
                .uri(config.getMilvusUri())
                .connectTimeoutMs(config.getMetadataTimeoutMs());
        if (config.getMilvusToken() != null && !config.getMilvusToken().isBlank()) {
            builder.token(config.getMilvusToken());
        }
        if (databaseName != null && !databaseName.isBlank()) {
            builder.dbName(databaseName);
        }
        this.client = new MilvusClientV2(builder.build());
    }

    @Override
    public List<CollectionMetadata> collections() {
        try {
            ListCollectionsResp resp = client.listCollections();
            List<String> names = resp.getCollectionNames();
            if (names == null) {
                return Collections.emptyList();
            }
            return names.stream()
                    .map(name -> new CollectionMetadata(name, 0L, 0, 0L))
                    .collect(Collectors.toList());
        }
        catch (Exception e) {
            LOGGER.warn("Failed to list collections", e);
            return Collections.emptyList();
        }
    }

    @Override
    public MilvusCollectionSchema schema(String collection) {
        DescribeCollectionReq req = DescribeCollectionReq.builder()
                .collectionName(collection)
                .build();
        DescribeCollectionResp resp = client.describeCollection(req);
        if (resp == null) {
            throw new CollectionNotFoundException(collection);
        }

        String pkField = resp.getPrimaryFieldName();
        List<MilvusCollectionSchema.FieldSchema> fields = new ArrayList<>();
        io.milvus.v2.service.collection.request.CreateCollectionReq.CollectionSchema sdkSchema = resp.getCollectionSchema();
        if (sdkSchema != null && sdkSchema.getFieldSchemaList() != null) {
            for (io.milvus.v2.service.collection.request.CreateCollectionReq.FieldSchema f : sdkSchema.getFieldSchemaList()) {
                String name = f.getName();
                Integer dimension = f.getDimension();
                fields.add(new MilvusCollectionSchema.FieldSchema(
                        name,
                        f.getDataType() == null ? 0 : f.getDataType().getCode(),
                        f.getDescription(),
                        Boolean.TRUE.equals(f.getIsPrimaryKey()),
                        Boolean.TRUE.equals(f.getIsNullable()),
                        dimension == null ? 0 : dimension));
                if (Boolean.TRUE.equals(f.getIsPrimaryKey())) {
                    pkField = name;
                }
            }
        }

        return new MilvusCollectionSchema(
                collection,
                resp.getDatabaseName() != null ? resp.getDatabaseName() : databaseName,
                resp.getDescription(),
                fields,
                pkField);
    }

    @Override
    public List<VChannelMetadata> channels(String collection) {
        // Channel discovery is not yet implemented through the SDK v2 public API.
        return Collections.emptyList();
    }

    @Override
    public boolean isReachable() {
        try {
            client.listCollections();
            return true;
        }
        catch (Exception e) {
            LOGGER.warn("Milvus endpoint is not reachable", e);
            return false;
        }
    }

    @Override
    public boolean databaseExists() {
        try {
            client.listCollections();
            return true;
        }
        catch (Exception e) {
            LOGGER.warn("Failed to verify database existence", e);
            return false;
        }
    }

    @Override
    public Optional<CollectionMetadata> findCollection(String collectionName) {
        return collections().stream()
                .filter(c -> c.getName().equals(collectionName))
                .findFirst();
    }

    @Override
    public void close() {
        try {
            client.close(5);
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing Milvus metadata client", e);
        }
    }
}

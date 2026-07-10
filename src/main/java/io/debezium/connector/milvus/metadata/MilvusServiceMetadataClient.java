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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.milvus.MilvusConnectorConfig;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.CollectionSchema;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.ErrorCode;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.KeyValuePair;
import io.milvus.grpc.ListDatabasesResponse;
import io.milvus.grpc.ShowCollectionsResponse;
import io.milvus.grpc.Status;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.collection.ShowCollectionsParam;

/**
 * {@link MilvusMetadataClient} implementation backed by the Milvus v1 gRPC service client.
 *
 * <p>
 * The v1 service client exposes the physical/virtual channel names that the v2
 * high-level client hides. All metadata (collections, schemas, channels) is read
 * through the versioned Milvus gRPC API.
 * </p>
 */
public class MilvusServiceMetadataClient implements MilvusMetadataClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(MilvusServiceMetadataClient.class);
    private static final String DIM_PARAM = "dim";

    private final MilvusServiceClient client;
    private final String databaseName;

    public MilvusServiceMetadataClient(MilvusConnectorConfig config) {
        this.databaseName = config.getMilvusDatabase();

        ConnectParam.Builder builder = ConnectParam.newBuilder()
                .withUri(config.getMilvusUri())
                .withDatabaseName(databaseName)
                .withConnectTimeout(config.getMetadataTimeoutMs(), TimeUnit.MILLISECONDS);

        if (config.getMilvusToken() != null && !config.getMilvusToken().isBlank()) {
            builder.withToken(config.getMilvusToken());
        }

        try {
            this.client = new MilvusServiceClient(builder.build());
        }
        catch (Exception e) {
            throw new DebeziumException("Failed to create Milvus metadata client for URI " + config.getMilvusUri(), e);
        }
    }

    /**
     * Package-private constructor for unit testing that allows injecting a
     * pre-configured Milvus service client.
     */
    MilvusServiceMetadataClient(MilvusServiceClient client, String databaseName) {
        this.client = client;
        this.databaseName = databaseName;
    }

    @Override
    public List<CollectionMetadata> collections() {
        ShowCollectionsParam param = ShowCollectionsParam.newBuilder()
                .withDatabaseName(databaseName)
                .build();

        R<ShowCollectionsResponse> response = client.showCollections(param);
        throwOnFailure("list collections", response);

        ShowCollectionsResponse data = response.getData();
        if (data == null) {
            return Collections.emptyList();
        }

        List<String> names = data.getCollectionNamesList();
        List<Long> ids = data.getCollectionIdsList();
        List<Long> createdTimestamps = data.getCreatedTimestampsList();

        List<CollectionMetadata> result = new ArrayList<>(names.size());
        for (int i = 0; i < names.size(); i++) {
            long id = i < ids.size() ? ids.get(i) : 0L;
            long created = i < createdTimestamps.size() ? createdTimestamps.get(i) : 0L;
            result.add(new CollectionMetadata(names.get(i), id, 0, created));
        }
        return result;
    }

    @Override
    public MilvusCollectionSchema schema(String collection) {
        DescribeCollectionResponse response = describe(collection);
        if (!response.hasSchema()) {
            throw new CollectionNotFoundException(collection,
                    new IllegalStateException("DescribeCollection response contained no schema"));
        }
        CollectionSchema schema = response.getSchema();

        String pkField = null;
        List<MilvusCollectionSchema.FieldSchema> fields = new ArrayList<>();
        for (FieldSchema f : schema.getFieldsList()) {
            String name = f.getName();
            boolean isPrimary = f.getIsPrimaryKey();
            int dimension = extractDimension(f);
            fields.add(new MilvusCollectionSchema.FieldSchema(
                    name,
                    f.getDataTypeValue(),
                    f.getDescription(),
                    isPrimary,
                    f.getNullable(),
                    dimension));
            if (isPrimary) {
                pkField = name;
            }
        }

        return new MilvusCollectionSchema(
                response.getCollectionName(),
                response.getDbName().isBlank() ? databaseName : response.getDbName(),
                schema.getDescription(),
                fields,
                pkField);
    }

    @Override
    public List<VChannelMetadata> channels(String collection) {
        DescribeCollectionResponse response = describe(collection);

        List<String> virtualChannels = response.getVirtualChannelNamesList();
        List<String> physicalChannels = response.getPhysicalChannelNamesList();
        if (virtualChannels.isEmpty()) {
            return Collections.emptyList();
        }

        long collectionId = response.getCollectionID();
        String collectionName = response.getCollectionName();

        return IntStream.range(0, virtualChannels.size())
                .mapToObj(i -> {
                    String vchannel = virtualChannels.get(i);
                    String pchannel = i < physicalChannels.size()
                            ? physicalChannels.get(i)
                            : physicalChannels.get(physicalChannels.size() - 1);
                    return new VChannelMetadata(vchannel, pchannel, collectionName, collectionId);
                })
                .collect(Collectors.toList());
    }

    @Override
    public boolean isReachable() {
        try {
            R<ListDatabasesResponse> response = client.listDatabases();
            return response.getException() == null;
        }
        catch (Exception e) {
            LOGGER.warn("Milvus endpoint is not reachable", e);
            return false;
        }
    }

    @Override
    public boolean databaseExists() {
        R<ListDatabasesResponse> response = client.listDatabases();
        throwOnFailure("list databases", response);

        ListDatabasesResponse data = response.getData();
        return data != null && data.getDbNamesList().contains(databaseName);
    }

    @Override
    public Optional<CollectionMetadata> findCollection(String collectionName) {
        return collections().stream()
                .filter(c -> c.getName().equals(collectionName))
                .findFirst();
    }

    @Override
    public void close() {
        if (client != null) {
            try {
                client.close(5);
            }
            catch (Exception e) {
                LOGGER.warn("Exception while closing Milvus metadata client", e);
            }
        }
    }

    private DescribeCollectionResponse describe(String collection) {
        DescribeCollectionParam param = DescribeCollectionParam.newBuilder()
                .withDatabaseName(databaseName)
                .withCollectionName(collection)
                .build();

        R<DescribeCollectionResponse> response = client.describeCollection(param);
        throwOnFailure("describe collection " + collection, response);

        DescribeCollectionResponse data = response.getData();
        if (data == null) {
            throw new CollectionNotFoundException(collection);
        }
        return data;
    }

    private static void throwOnFailure(String operation, R<?> response) {
        if (response.getStatus() != null && response.getStatus() == ErrorCode.CollectionNotExists.getNumber()) {
            throw new CollectionNotFoundException(collectionNameFrom(operation),
                    new IllegalStateException(response.getMessage()));
        }

        Object data = response.getData();
        Status status = extractStatus(data);

        if (status != null && status.getErrorCode() == ErrorCode.CollectionNotExists) {
            throw new CollectionNotFoundException(collectionNameFrom(operation),
                    new IllegalStateException(status.getReason()));
        }

        if (response.getException() != null) {
            Exception exception = response.getException();
            if (exception instanceof CollectionNotFoundException collectionNotFoundException) {
                throw collectionNotFoundException;
            }
            throw new DebeziumException("Milvus API failed during " + operation, exception);
        }

        if (status != null && status.getErrorCode() != ErrorCode.Success) {
            throw new DebeziumException("Milvus API returned error during " + operation +
                    ": " + status.getErrorCode() + " — " + status.getReason());
        }
    }

    private static Status extractStatus(Object data) {
        if (data instanceof DescribeCollectionResponse r) {
            return r.getStatus();
        }
        if (data instanceof ShowCollectionsResponse r) {
            return r.getStatus();
        }
        if (data instanceof ListDatabasesResponse r) {
            return r.getStatus();
        }
        return null;
    }

    private static String collectionNameFrom(String operation) {
        String prefix = "describe collection ";
        if (operation.startsWith(prefix)) {
            return operation.substring(prefix.length());
        }
        return operation;
    }

    private static int extractDimension(FieldSchema field) {
        for (KeyValuePair param : field.getTypeParamsList()) {
            if (DIM_PARAM.equals(param.getKey())) {
                try {
                    return Integer.parseInt(param.getValue());
                }
                catch (NumberFormatException e) {
                    LOGGER.warn("Unable to parse dimension '{}' for field '{}'", param.getValue(), field.getName());
                    return 0;
                }
            }
        }
        return 0;
    }
}

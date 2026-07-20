/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.debezium.DebeziumException;
import io.debezium.doc.FixFor;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.CollectionSchema;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.ErrorCode;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.KeyValuePair;
import io.milvus.grpc.ListDatabasesResponse;
import io.milvus.grpc.ShowCollectionsResponse;
import io.milvus.grpc.Status;
import io.milvus.param.R;

@ExtendWith(MockitoExtension.class)
public class MilvusServiceMetadataClientTest {

    private static final String DATABASE = "default";

    @Mock
    private MilvusServiceClient serviceClient;

    @Test
    @FixFor("debezium/dbz#2130")
    void shouldListCollections() {
        ShowCollectionsResponse response = ShowCollectionsResponse.newBuilder()
                .setStatus(successStatus())
                .addCollectionNames("articles")
                .addCollectionNames("products")
                .addCollectionIds(100L)
                .addCollectionIds(200L)
                .addCreatedTimestamps(1000L)
                .addCreatedTimestamps(2000L)
                .build();

        when(serviceClient.showCollections(any())).thenReturn(R.success(response));

        MilvusServiceMetadataClient client = new MilvusServiceMetadataClient(serviceClient, DATABASE);
        List<CollectionMetadata> collections = client.collections();

        assertThat(collections).hasSize(2);
        assertThat(collections.get(0).getName()).isEqualTo("articles");
        assertThat(collections.get(0).getId()).isEqualTo(100L);
        assertThat(collections.get(0).getCreatedTimestamp()).isEqualTo(1000L);
        assertThat(collections.get(1).getName()).isEqualTo("products");
        assertThat(collections.get(1).getId()).isEqualTo(200L);
    }

    @Test
    @FixFor("debezium/dbz#2130")
    void shouldReturnEmptyCollectionsWhenResponseDataIsNull() {
        when(serviceClient.showCollections(any())).thenReturn(R.success(null));

        MilvusServiceMetadataClient client = new MilvusServiceMetadataClient(serviceClient, DATABASE);

        assertThat(client.collections()).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#2130")
    void shouldReturnSchemaForCollection() {
        DescribeCollectionResponse response = DescribeCollectionResponse.newBuilder()
                .setStatus(successStatus())
                .setCollectionName("articles")
                .setDbName(DATABASE)
                .setSchema(CollectionSchema.newBuilder()
                        .setName("articles")
                        .setDescription("article collection")
                        .addFields(FieldSchema.newBuilder()
                                .setName("id")
                                .setDataType(DataType.Int64)
                                .setIsPrimaryKey(true)
                                .setNullable(false)
                                .build())
                        .addFields(FieldSchema.newBuilder()
                                .setName("title")
                                .setDataType(DataType.VarChar)
                                .setNullable(true)
                                .build())
                        .build())
                .build();

        when(serviceClient.describeCollection(any())).thenReturn(R.success(response));

        MilvusServiceMetadataClient client = new MilvusServiceMetadataClient(serviceClient, DATABASE);
        MilvusCollectionSchema schema = client.schema("articles");

        assertThat(schema.getCollectionName()).isEqualTo("articles");
        assertThat(schema.getDatabaseName()).isEqualTo(DATABASE);
        assertThat(schema.getDescription()).isEqualTo("article collection");
        assertThat(schema.getPrimaryKeyField()).isEqualTo("id");
        assertThat(schema.getFields()).hasSize(2);
        assertThat(schema.getFields().get(0).getName()).isEqualTo("id");
        assertThat(schema.getFields().get(0).getDataType()).isEqualTo(DataType.Int64.getNumber());
        assertThat(schema.getFields().get(0).isPrimaryKey()).isTrue();
        assertThat(schema.getFields().get(1).isNullable()).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2130")
    void shouldExtractVectorDimensionFromTypeParams() {
        DescribeCollectionResponse response = DescribeCollectionResponse.newBuilder()
                .setStatus(successStatus())
                .setCollectionName("articles")
                .setSchema(CollectionSchema.newBuilder()
                        .addFields(FieldSchema.newBuilder()
                                .setName("embedding")
                                .setDataType(DataType.FloatVector)
                                .addTypeParams(KeyValuePair.newBuilder()
                                        .setKey("dim")
                                        .setValue("128")
                                        .build())
                                .build())
                        .build())
                .build();

        when(serviceClient.describeCollection(any())).thenReturn(R.success(response));

        MilvusServiceMetadataClient client = new MilvusServiceMetadataClient(serviceClient, DATABASE);
        MilvusCollectionSchema schema = client.schema("articles");

        assertThat(schema.getFields().get(0).getDimension()).isEqualTo(128);
    }

    @Test
    @FixFor("debezium/dbz#2130")
    void shouldReturnChannelsForCollection() {
        DescribeCollectionResponse response = DescribeCollectionResponse.newBuilder()
                .setStatus(successStatus())
                .setCollectionName("articles")
                .setCollectionID(42L)
                .addVirtualChannelNames("by-dev-rootcoord-dml_0_v0")
                .addVirtualChannelNames("by-dev-rootcoord-dml_1_v0")
                .addPhysicalChannelNames("by-dev-rootcoord-dml_0")
                .addPhysicalChannelNames("by-dev-rootcoord-dml_1")
                .build();

        when(serviceClient.describeCollection(any())).thenReturn(R.success(response));

        MilvusServiceMetadataClient client = new MilvusServiceMetadataClient(serviceClient, DATABASE);
        List<VChannelMetadata> channels = client.channels("articles");

        assertThat(channels).hasSize(2);
        assertThat(channels.get(0).getVchannel()).isEqualTo("by-dev-rootcoord-dml_0_v0");
        assertThat(channels.get(0).getPchannel()).isEqualTo("by-dev-rootcoord-dml_0");
        assertThat(channels.get(0).getCollectionName()).isEqualTo("articles");
        assertThat(channels.get(0).getCollectionId()).isEqualTo(42L);
        assertThat(channels.get(1).getVchannel()).isEqualTo("by-dev-rootcoord-dml_1_v0");
        assertThat(channels.get(1).getPchannel()).isEqualTo("by-dev-rootcoord-dml_1");
    }

    @Test
    @FixFor("debezium/dbz#2130")
    void shouldFallBackToLastPhysicalChannelWhenSizesDiffer() {
        DescribeCollectionResponse response = DescribeCollectionResponse.newBuilder()
                .setStatus(successStatus())
                .setCollectionName("articles")
                .setCollectionID(42L)
                .addVirtualChannelNames("by-dev-rootcoord-dml_0_v0")
                .addVirtualChannelNames("by-dev-rootcoord-dml_1_v0")
                .addPhysicalChannelNames("by-dev-rootcoord-dml_0")
                .build();

        when(serviceClient.describeCollection(any())).thenReturn(R.success(response));

        MilvusServiceMetadataClient client = new MilvusServiceMetadataClient(serviceClient, DATABASE);
        List<VChannelMetadata> channels = client.channels("articles");

        assertThat(channels).hasSize(2);
        assertThat(channels.get(0).getPchannel()).isEqualTo("by-dev-rootcoord-dml_0");
        assertThat(channels.get(1).getPchannel()).isEqualTo("by-dev-rootcoord-dml_0");
    }

    @Test
    @FixFor("debezium/dbz#2130")
    void shouldCheckReachable() {
        ListDatabasesResponse response = ListDatabasesResponse.newBuilder()
                .setStatus(successStatus())
                .build();
        when(serviceClient.listDatabases()).thenReturn(R.success(response));

        MilvusServiceMetadataClient client = new MilvusServiceMetadataClient(serviceClient, DATABASE);

        assertThat(client.isReachable()).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2130")
    void shouldReportUnreachableOnException() {
        when(serviceClient.listDatabases()).thenThrow(new RuntimeException("connection refused"));

        MilvusServiceMetadataClient client = new MilvusServiceMetadataClient(serviceClient, DATABASE);

        assertThat(client.isReachable()).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2130")
    void shouldCheckDatabaseExists() {
        ListDatabasesResponse response = ListDatabasesResponse.newBuilder()
                .setStatus(successStatus())
                .addDbNames("default")
                .addDbNames("custom")
                .build();
        when(serviceClient.listDatabases()).thenReturn(R.success(response));

        MilvusServiceMetadataClient client = new MilvusServiceMetadataClient(serviceClient, DATABASE);

        assertThat(client.databaseExists()).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2130")
    void shouldReportDatabaseDoesNotExist() {
        ListDatabasesResponse response = ListDatabasesResponse.newBuilder()
                .setStatus(successStatus())
                .addDbNames("other")
                .build();
        when(serviceClient.listDatabases()).thenReturn(R.success(response));

        MilvusServiceMetadataClient client = new MilvusServiceMetadataClient(serviceClient, DATABASE);

        assertThat(client.databaseExists()).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2130")
    void shouldFindCollectionByName() {
        ShowCollectionsResponse response = ShowCollectionsResponse.newBuilder()
                .setStatus(successStatus())
                .addCollectionNames("articles")
                .addCollectionNames("products")
                .build();
        when(serviceClient.showCollections(any())).thenReturn(R.success(response));

        MilvusServiceMetadataClient client = new MilvusServiceMetadataClient(serviceClient, DATABASE);

        Optional<CollectionMetadata> found = client.findCollection("products");
        assertThat(found).isPresent();
        assertThat(found.get().getName()).isEqualTo("products");

        assertThat(client.findCollection("missing")).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#2130")
    void shouldThrowCollectionNotFoundWhenDescribeFailsWithCollectionNotExists() {
        when(serviceClient.describeCollection(any())).thenReturn(R.failed(ErrorCode.CollectionNotExists, "collection not found"));

        MilvusServiceMetadataClient client = new MilvusServiceMetadataClient(serviceClient, DATABASE);

        assertThatThrownBy(() -> client.schema("missing"))
                .isInstanceOf(CollectionNotFoundException.class)
                .hasMessageContaining("missing");
    }

    @Test
    @FixFor("debezium/dbz#2130")
    void shouldThrowDebeziumExceptionWhenApiReturnsErrorStatus() {
        DescribeCollectionResponse response = DescribeCollectionResponse.newBuilder()
                .setStatus(Status.newBuilder()
                        .setErrorCode(ErrorCode.UnexpectedError)
                        .setReason("something went wrong")
                        .build())
                .build();
        when(serviceClient.describeCollection(any())).thenReturn(R.success(response));

        MilvusServiceMetadataClient client = new MilvusServiceMetadataClient(serviceClient, DATABASE);

        assertThatThrownBy(() -> client.schema("articles"))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Milvus API returned error");
    }

    @Test
    @FixFor("debezium/dbz#2130")
    void shouldThrowWhenSchemaResponseHasNoSchema() {
        DescribeCollectionResponse response = DescribeCollectionResponse.newBuilder()
                .setStatus(successStatus())
                .setCollectionName("articles")
                .build();
        when(serviceClient.describeCollection(any())).thenReturn(R.success(response));

        MilvusServiceMetadataClient client = new MilvusServiceMetadataClient(serviceClient, DATABASE);

        assertThatThrownBy(() -> client.schema("articles"))
                .isInstanceOf(CollectionNotFoundException.class)
                .hasMessageContaining("Collection not found: articles")
                .cause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no schema");
    }

    @Test
    @FixFor("debezium/dbz#2130")
    void shouldCloseServiceClient() throws InterruptedException {
        MilvusServiceMetadataClient client = new MilvusServiceMetadataClient(serviceClient, DATABASE);
        client.close();
        verify(serviceClient).close(5);
    }

    private static Status successStatus() {
        return Status.newBuilder()
                .setErrorCode(ErrorCode.Success)
                .build();
    }
}

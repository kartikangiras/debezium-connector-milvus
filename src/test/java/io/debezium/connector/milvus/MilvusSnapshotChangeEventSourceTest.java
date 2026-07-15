/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.debezium.config.Configuration;
import io.debezium.connector.milvus.checkpoint.ChannelCheckpoint;
import io.debezium.connector.milvus.checkpoint.EtcdCheckpointReader;
import io.debezium.connector.milvus.metadata.CollectionMetadata;
import io.debezium.connector.milvus.metadata.MilvusCollectionSchema;
import io.debezium.connector.milvus.metadata.MilvusMetadataClient;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.TableId;

@ExtendWith(MockitoExtension.class)
public class MilvusSnapshotChangeEventSourceTest {

    private static final String PCHANNEL = "by-dev-rootcoord-dml_0";
    private static final long GUARANTEE_TS = 440000000000L;
    private static final long KAFKA_OFFSET = 42L;

    private MilvusConnectorConfig connectorConfig;

    @Mock
    private EtcdCheckpointReader checkpointReader;

    @Mock
    private MilvusSnapshotQueryClient queryClient;

    @Mock
    private MilvusMetadataClient metadataClient;

    @SuppressWarnings("unchecked")
    @Mock
    private EventDispatcher<MilvusPartition, TableId> dispatcher;

    @Mock
    private MilvusDatabaseSchema databaseSchema;

    @SuppressWarnings("unchecked")
    @Mock
    private SnapshotProgressListener<MilvusPartition> progressListener;

    @SuppressWarnings("unchecked")
    @Mock
    private NotificationService<MilvusPartition, MilvusOffsetContext> notificationService;

    @Mock
    private ChangeEventSource.ChangeEventSourceContext context;

    private MilvusSnapshotChangeEventSource source;
    private MilvusPartition partition;
    private MilvusOffsetContext offsetContext;

    @BeforeEach
    void setUp() {
        Configuration configuration = Configuration.from(Map.of(
                "milvus.uri", "http://localhost:19530",
                "topic.prefix", "milvus-test",
                "milvus.kafka.bootstrap.servers", "localhost:9092",
                "milvus.pchannel.name", PCHANNEL));
        connectorConfig = new MilvusConnectorConfig(configuration);
        partition = MilvusPartition.create("milvus-test", PCHANNEL);
        offsetContext = new MilvusOffsetContext(new MilvusSourceInfo(connectorConfig));

        source = new MilvusSnapshotChangeEventSource(
                connectorConfig,
                progressListener,
                notificationService,
                checkpointReader,
                queryClient,
                metadataClient,
                dispatcher,
                databaseSchema);
    }

    @Test
    @FixFor("debezium/dbz#2230")
    void shouldCompleteImmediatelyWhenNoCheckpointExists() throws Exception {
        when(checkpointReader.read(PCHANNEL)).thenReturn(Optional.empty());

        SnapshotResult<MilvusOffsetContext> result = invokeDoExecute();

        assertThat(result.getStatus()).isEqualTo(SnapshotResult.SnapshotResultStatus.COMPLETED);
        assertThat(result.getOffset().isSnapshotCompleted()).isTrue();
        verify(dispatcher, never()).dispatchDataChangeEvent(any(), any(), any());
    }

    @Test
    @FixFor("debezium/dbz#2230")
    void shouldStoreCheckpointOffsetAndTimestampInOffsetContext() throws Exception {
        ChannelCheckpoint checkpoint = buildCheckpoint(GUARANTEE_TS, KAFKA_OFFSET);
        when(checkpointReader.read(PCHANNEL)).thenReturn(Optional.of(checkpoint));
        when(metadataClient.collections()).thenReturn(List.of());

        SnapshotResult<MilvusOffsetContext> result = invokeDoExecute();

        MilvusOffsetContext ctx = result.getOffset();
        assertThat(ctx.getMqOffset(PCHANNEL)).isEqualTo(KAFKA_OFFSET);
        assertThat(ctx.getCheckpointTimestamp()).isEqualTo(GUARANTEE_TS);
    }

    @Test
    @FixFor("debezium/dbz#2230")
    void shouldMarkSnapshotCompletedAfterAllCollections() throws Exception {
        ChannelCheckpoint checkpoint = buildCheckpoint(GUARANTEE_TS, KAFKA_OFFSET);
        when(checkpointReader.read(PCHANNEL)).thenReturn(Optional.of(checkpoint));
        when(metadataClient.collections()).thenReturn(List.of());

        SnapshotResult<MilvusOffsetContext> result = invokeDoExecute();

        assertThat(result.getOffset().isSnapshotCompleted()).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2230")
    void shouldRegisterCollectionSchemaBeforeEmittingRows() throws Exception {
        ChannelCheckpoint checkpoint = buildCheckpoint(GUARANTEE_TS, KAFKA_OFFSET);
        when(checkpointReader.read(PCHANNEL)).thenReturn(Optional.of(checkpoint));

        CollectionMetadata collMeta = new CollectionMetadata("test_coll", 1L, 0, 0L);
        when(metadataClient.collections()).thenReturn(List.of(collMeta));
        when(context.isRunning()).thenReturn(true);

        MilvusCollectionSchema schema = buildSchema("test_coll", "id");
        when(metadataClient.schema("test_coll")).thenReturn(schema);

        when(queryClient.queryPage(anyString(), any(), anyString(), anyLong(), anyInt(), anyLong()))
                .thenReturn(List.of());
        when(databaseSchema.registerCollection(anyString(), anyString(), any())).thenReturn(true);
        when(databaseSchema.getColumnNames(any(TableId.class))).thenReturn(new String[]{ "id", "vec" });

        invokeDoExecute();

        verify(databaseSchema).registerCollection(eq("default"), eq("test_coll"), any());
    }

    @Test
    @FixFor("debezium/dbz#2230")
    void shouldEmitSnapshotRowsAsReadOperation() throws Exception {
        ChannelCheckpoint checkpoint = buildCheckpoint(GUARANTEE_TS, KAFKA_OFFSET);
        when(checkpointReader.read(PCHANNEL)).thenReturn(Optional.of(checkpoint));

        CollectionMetadata collMeta = new CollectionMetadata("my_coll", 1L, 0, 0L);
        when(metadataClient.collections()).thenReturn(List.of(collMeta));

        MilvusCollectionSchema schema = buildSchema("my_coll", "id");
        when(metadataClient.schema("my_coll")).thenReturn(schema);

        Map<String, Object> row1 = Map.of("id", 1L, "vec", new float[]{ 0.1f, 0.2f });
        Map<String, Object> row2 = Map.of("id", 2L, "vec", new float[]{ 0.3f, 0.4f });

        when(databaseSchema.registerCollection(anyString(), anyString(), any())).thenReturn(true);
        when(databaseSchema.getColumnNames(any(TableId.class))).thenReturn(new String[]{ "id", "vec" });
        when(context.isRunning()).thenReturn(true);
        when(queryClient.queryPage(anyString(), any(), anyString(), anyLong(), anyInt(), eq(0L)))
                .thenReturn(List.of(row1, row2));

        when(dispatcher.dispatchDataChangeEvent(any(), any(), any())).thenReturn(true);

        invokeDoExecute();

        ArgumentCaptor<MilvusChangeRecordEmitter> emitterCaptor = ArgumentCaptor.forClass(MilvusChangeRecordEmitter.class);
        verify(dispatcher, times(2)).dispatchDataChangeEvent(
                eq(partition), any(TableId.class), emitterCaptor.capture());

        for (MilvusChangeRecordEmitter emitter : emitterCaptor.getAllValues()) {
            assertThat(emitter.getOperation()).isEqualTo(Envelope.Operation.READ);
        }
    }

    @Test
    @FixFor("debezium/dbz#2230")
    void shouldSkipCollectionOnExcludeList() throws Exception {
        Configuration configuration = Configuration.from(Map.of(
                "milvus.uri", "http://localhost:19530",
                "topic.prefix", "milvus-test",
                "milvus.kafka.bootstrap.servers", "localhost:9092",
                "milvus.pchannel.name", PCHANNEL,
                "milvus.collection.exclude.list", "excluded_coll"));
        connectorConfig = new MilvusConnectorConfig(configuration);
        source = new MilvusSnapshotChangeEventSource(
                connectorConfig, progressListener, notificationService,
                checkpointReader, queryClient, metadataClient, dispatcher, databaseSchema);

        ChannelCheckpoint checkpoint = buildCheckpoint(GUARANTEE_TS, KAFKA_OFFSET);
        when(checkpointReader.read(PCHANNEL)).thenReturn(Optional.of(checkpoint));
        when(metadataClient.collections())
                .thenReturn(List.of(new CollectionMetadata("excluded_coll", 1L, 0, 0L)));
        when(context.isRunning()).thenReturn(true);

        invokeDoExecute();

        verify(metadataClient, never()).schema(anyString());
        verify(dispatcher, never()).dispatchDataChangeEvent(any(), any(), any());
    }

    @Test
    @FixFor("debezium/dbz#2230")
    void shouldSnapshotNeededWhenOffsetContextNull() {
        SnapshottingTask task = source.getSnapshottingTask(partition, null);
        assertThat(task.snapshotData()).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2230")
    void shouldSkipSnapshotWhenAlreadyCompleted() {
        MilvusOffsetContext completedCtx = new MilvusOffsetContext(
                new MilvusSourceInfo(connectorConfig), true);
        SnapshottingTask task = source.getSnapshottingTask(partition, completedCtx);
        assertThat(task.snapshotData()).isFalse();
    }

    @SuppressWarnings("unchecked")
    private SnapshotResult<MilvusOffsetContext> invokeDoExecute() throws Exception {
        var snapshotContext = source.prepare(partition, false);
        return source.doExecute(context, offsetContext, snapshotContext,
                source.getSnapshottingTask(partition, offsetContext));
    }

    private static ChannelCheckpoint buildCheckpoint(long guaranteeTs, long kafkaOffset) {
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(8)
                .order(java.nio.ByteOrder.LITTLE_ENDIAN);
        buf.putLong(kafkaOffset);
        return new ChannelCheckpoint(PCHANNEL, buf.array(), guaranteeTs);
    }

    private static MilvusCollectionSchema buildSchema(String collectionName, String pkField) {
        MilvusCollectionSchema.FieldSchema idField = new MilvusCollectionSchema.FieldSchema(
                pkField, io.milvus.grpc.DataType.Int64.getNumber(), "", true, false, 0);
        MilvusCollectionSchema.FieldSchema vecField = new MilvusCollectionSchema.FieldSchema(
                "vec", io.milvus.grpc.DataType.FloatVector.getNumber(), "", false, false, 128);
        return new MilvusCollectionSchema(collectionName, "default", "",
                List.of(idField, vecField), pkField);
    }
}

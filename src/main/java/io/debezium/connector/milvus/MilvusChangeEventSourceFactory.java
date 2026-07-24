/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import io.debezium.connector.milvus.checkpoint.EtcdCheckpointReader;
import io.debezium.connector.milvus.metadata.MilvusMetadataClient;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;

/**
 * Factory for creating Milvus change event sources.
 *
 * <p>Builds the streaming source with its full dependency chain:
 * {@link KafkaMilvusMessageConsumer} → {@link MilvusProtoDeserializer} →
 * {@link TimetickOrderingEngine} → {@link MilvusStreamingChangeEventSource},
 * wired with the {@link EventDispatcher} and {@link MilvusDatabaseSchema}.</p>
 *
 * <p>Passes the shared {@link EtcdCheckpointReader} to both the snapshot
 * source (for reading {@code guarantee_ts}) and the streaming source (for
 * resolving the Kafka offset for snapshot-to-streaming handoff).</p>
 */
public class MilvusChangeEventSourceFactory implements ChangeEventSourceFactory<MilvusPartition, MilvusOffsetContext> {

    private final MilvusConnectorConfig connectorConfig;
    private final EventDispatcher<MilvusPartition, TableId> dispatcher;
    private final MilvusDatabaseSchema schema;
    private final EtcdCheckpointReader checkpointReader;
    private final MilvusSnapshotQueryClient snapshotQueryClient;
    private final MilvusMetadataClient metadataClient;
    private final MilvusStreamingChangeEventSourceMetrics streamingMetrics;

    public MilvusChangeEventSourceFactory(MilvusConnectorConfig connectorConfig,
                                          EventDispatcher<MilvusPartition, TableId> dispatcher,
                                          MilvusDatabaseSchema schema,
                                          EtcdCheckpointReader checkpointReader,
                                          MilvusSnapshotQueryClient snapshotQueryClient,
                                          MilvusMetadataClient metadataClient,
                                          MilvusStreamingChangeEventSourceMetrics streamingMetrics) {
        this.connectorConfig = connectorConfig;
        this.dispatcher = dispatcher;
        this.schema = schema;
        this.checkpointReader = checkpointReader;
        this.snapshotQueryClient = snapshotQueryClient;
        this.metadataClient = metadataClient;
        this.streamingMetrics = streamingMetrics;
    }

    @Override
    public MilvusSnapshotChangeEventSource getSnapshotChangeEventSource(
                                                                        SnapshotProgressListener<MilvusPartition> snapshotProgressListener,
                                                                        NotificationService<MilvusPartition, MilvusOffsetContext> notificationService) {
        return new MilvusSnapshotChangeEventSource(
                connectorConfig,
                snapshotProgressListener,
                notificationService,
                checkpointReader,
                snapshotQueryClient,
                metadataClient,
                dispatcher,
                schema);
    }

    @Override
    public StreamingChangeEventSource<MilvusPartition, MilvusOffsetContext> getStreamingChangeEventSource() {
        MilvusMessageConsumer messageConsumer = new KafkaMilvusMessageConsumer(connectorConfig);
        MilvusValueConverter valueConverter = new MilvusValueConverter(connectorConfig);
        MilvusColumnarPivot pivot = new MilvusColumnarPivot(valueConverter);
        MilvusProtoDeserializer deserializer = new MilvusProtoDeserializer(connectorConfig.getWireFormat(), pivot);
        TimetickOrderingEngine orderingEngine = new TimetickOrderingEngine(connectorConfig);
        return new MilvusStreamingChangeEventSource(
                connectorConfig, messageConsumer, deserializer, orderingEngine,
                dispatcher, schema, checkpointReader, streamingMetrics);
    }
}

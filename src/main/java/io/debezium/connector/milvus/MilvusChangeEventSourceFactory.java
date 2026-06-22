/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;

/**
 * Factory for creating Milvus change event sources.
 *
 * <p>Builds the streaming source with its full dependency chain:
 * {@link KafkaMilvusMessageConsumer} → {@link MilvusProtoDeserializer} →
 * {@link TimetickOrderingEngine} → {@link MilvusStreamingChangeEventSource}.</p>
 */
public class MilvusChangeEventSourceFactory implements ChangeEventSourceFactory<MilvusPartition, MilvusOffsetContext> {

    private final MilvusConnectorConfig connectorConfig;

    public MilvusChangeEventSourceFactory(MilvusConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    @Override
    public MilvusSnapshotChangeEventSource getSnapshotChangeEventSource(
                                                                        SnapshotProgressListener<MilvusPartition> snapshotProgressListener,
                                                                        NotificationService<MilvusPartition, MilvusOffsetContext> notificationService) {
        return new MilvusSnapshotChangeEventSource(connectorConfig, snapshotProgressListener, notificationService);
    }

    @Override
    public StreamingChangeEventSource<MilvusPartition, MilvusOffsetContext> getStreamingChangeEventSource() {
        MilvusMessageConsumer messageConsumer = new KafkaMilvusMessageConsumer(connectorConfig);
        MilvusValueConverter valueConverter = new MilvusValueConverter(connectorConfig);
        MilvusColumnarPivot pivot = new MilvusColumnarPivot(valueConverter);
        MilvusProtoDeserializer deserializer = new MilvusProtoDeserializer(connectorConfig.getWireFormat(), pivot);
        TimetickOrderingEngine orderingEngine = new TimetickOrderingEngine(connectorConfig);
        return new MilvusStreamingChangeEventSource(connectorConfig, messageConsumer, deserializer, orderingEngine);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * Milvus-specific metrics factory that produces custom streaming metrics
 * with TSO-based watermark lag calculation and snapshot metrics with
 * {@code GuaranteeTso} exposure.
 *
 * <p>The streaming metrics object is created externally (in
 * {@link MilvusConnectorTask}) and passed to this factory so that both the
 * {@link io.debezium.pipeline.ChangeEventSourceCoordinator}
 * (for JMX registration) and
 * {@link MilvusStreamingChangeEventSource} (for watermark updates) share
 * the same metrics instance.</p>
 *
 * <p>Snapshot metrics are created on demand by the factory and surface the
 * etcd channel checkpoint TSO via
 * {@link MilvusSnapshotChangeEventSourceMetrics#getGuaranteeTso()}.</p>
 *
 * @see MilvusStreamingChangeEventSourceMetrics
 * @see MilvusSnapshotChangeEventSourceMetrics
 */
public class MilvusChangeEventSourceMetricsFactory
        extends DefaultChangeEventSourceMetricsFactory<MilvusPartition> {

    private final MilvusStreamingChangeEventSourceMetrics streamingMetrics;

    public MilvusChangeEventSourceMetricsFactory(
                                                 MilvusStreamingChangeEventSourceMetrics streamingMetrics) {
        this.streamingMetrics = streamingMetrics;
    }

    @Override
    public <T extends CdcSourceTaskContext> SnapshotChangeEventSourceMetrics<MilvusPartition> getSnapshotMetrics(
                                                                                                                 T taskContext,
                                                                                                                 ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                                 EventMetadataProvider eventMetadataProvider) {
        return new MilvusSnapshotChangeEventSourceMetrics(
                taskContext, changeEventQueueMetrics, eventMetadataProvider);
    }

    @Override
    public <T extends CdcSourceTaskContext> StreamingChangeEventSourceMetrics<MilvusPartition> getStreamingMetrics(
                                                                                                                   T taskContext,
                                                                                                                   ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                                   EventMetadataProvider eventMetadataProvider,
                                                                                                                   CapturedTablesSupplier capturedTablesSupplier) {
        return streamingMetrics;
    }
}

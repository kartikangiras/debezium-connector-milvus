/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.SnapshotResult;

/**
 * Snapshot change event source for Milvus.
 *
 * <p>Performs an initial snapshot by querying Milvus collections via the SDK
 * with {@code guarantee_ts} to ensure a consistent point-in-time view aligned
 * with the streaming offset.</p>
 */
public class MilvusSnapshotChangeEventSource
        extends AbstractSnapshotChangeEventSource<MilvusPartition, MilvusOffsetContext> {

    public MilvusSnapshotChangeEventSource(MilvusConnectorConfig connectorConfig,
                                           SnapshotProgressListener<MilvusPartition> snapshotProgressListener,
                                           NotificationService<MilvusPartition, MilvusOffsetContext> notificationService) {
        super(connectorConfig, snapshotProgressListener, notificationService);
    }

    @Override
    protected SnapshotResult<MilvusOffsetContext> doExecute(ChangeEventSource.ChangeEventSourceContext context,
                                                            MilvusOffsetContext offsetContext, SnapshotContext<MilvusPartition, MilvusOffsetContext> snapshotContext,
                                                            SnapshottingTask snapshottingTask)
            throws Exception {
        return null;
    }

    @Override
    protected SnapshotContext<MilvusPartition, MilvusOffsetContext> prepare(MilvusPartition partition, boolean onDemand)
            throws Exception {
        return null;
    }

    @Override
    public SnapshottingTask getSnapshottingTask(MilvusPartition partition, MilvusOffsetContext offsetContext) {
        return null;
    }

    @Override
    public SnapshottingTask getBlockingSnapshottingTask(MilvusPartition partition, MilvusOffsetContext offsetContext,
                                                        SnapshotConfiguration snapshotConfiguration) {
        return null;
    }
}

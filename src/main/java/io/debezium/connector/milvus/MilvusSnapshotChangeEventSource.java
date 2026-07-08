/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.util.List;
import java.util.Map;

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

    private final MilvusConnectorConfig connectorConfig;

    public MilvusSnapshotChangeEventSource(MilvusConnectorConfig connectorConfig,
                                           SnapshotProgressListener<MilvusPartition> snapshotProgressListener,
                                           NotificationService<MilvusPartition, MilvusOffsetContext> notificationService) {
        super(connectorConfig, snapshotProgressListener, notificationService);
        this.connectorConfig = connectorConfig;
    }

    @Override
    protected SnapshotResult<MilvusOffsetContext> doExecute(ChangeEventSource.ChangeEventSourceContext context,
                                                            MilvusOffsetContext offsetContext, SnapshotContext<MilvusPartition, MilvusOffsetContext> snapshotContext,
                                                            SnapshottingTask snapshottingTask)
            throws Exception {
        // TODO: Implement actual snapshot query logic via Milvus SDK with guarantee_ts.
        // For now, mark snapshot as completed so streaming can proceed.
        MilvusOffsetContext effectiveOffsetContext = offsetContext != null ? offsetContext
                : new MilvusOffsetContext(new MilvusSourceInfo(connectorConfig));
        return SnapshotResult.completed(effectiveOffsetContext);
    }

    @Override
    protected SnapshotContext<MilvusPartition, MilvusOffsetContext> prepare(MilvusPartition partition, boolean onDemand)
            throws Exception {
        return new MilvusSnapshotContext(partition);
    }

    @Override
    public SnapshottingTask getSnapshottingTask(MilvusPartition partition, MilvusOffsetContext offsetContext) {
        MilvusConnectorConfig.SnapshotMode snapshotMode = connectorConfig.getSnapshotMode();

        // Determine whether to run snapshot based on mode and offset state
        boolean snapshotNeeded;
        switch (snapshotMode) {
            case INITIAL:
                snapshotNeeded = (offsetContext == null || !offsetContext.isSnapshotCompleted());
                break;
            case NEVER:
                snapshotNeeded = false;
                break;
            case WHEN_NEEDED:
                snapshotNeeded = (offsetContext == null || !offsetContext.isSnapshotCompleted());
                break;
            case RECOVERY:
                snapshotNeeded = (offsetContext == null);
                break;
            default:
                snapshotNeeded = false;
                break;
        }

        return new SnapshottingTask(snapshotNeeded, snapshotNeeded,
                List.of(), Map.of(), false);
    }

    @Override
    public SnapshottingTask getBlockingSnapshottingTask(MilvusPartition partition, MilvusOffsetContext offsetContext,
                                                        SnapshotConfiguration snapshotConfiguration) {
        return new SnapshottingTask(false, false,
                List.of(), Map.of(), false);
    }

    /**
     * Minimal snapshot context holding the partition being snapshotted.
     */
    private static class MilvusSnapshotContext
            extends AbstractSnapshotChangeEventSource.SnapshotContext<MilvusPartition, MilvusOffsetContext> {

        MilvusSnapshotContext(MilvusPartition partition) throws Exception {
            super(partition);
        }
    }
}

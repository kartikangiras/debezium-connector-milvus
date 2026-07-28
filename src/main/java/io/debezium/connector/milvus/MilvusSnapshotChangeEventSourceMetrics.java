/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.util.concurrent.atomic.AtomicLong;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.DefaultSnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * Milvus-specific snapshot change event source metrics.
 *
 * <p>Extends the default Debezium snapshot metrics with Milvus-specific
 * attributes:</p>
 * <ul>
 *   <li>{@code GuaranteeTso} — the HLC TSO obtained from the etcd channel
 *       checkpoint, used as the snapshot anchor point for diagnosing
 *       snapshot-to-streaming handoff issues.</li>
 *   <li>{@code SnapshotStartTs} — the system clock timestamp (ms since epoch)
 *       when the snapshot phase began. Not exposed by the standard Debezium
 *       {@link io.debezium.pipeline.meters.SnapshotMeter}.</li>
 * </ul>
 *
 * <p>Exposed via JMX at:
 * {@code debezium.milvus:type=connector-metrics,context=snapshot,server=<server>}</p>
 *
 * @see MilvusOffsetContext#getCheckpointTimestamp()
 */
@ThreadSafe
public class MilvusSnapshotChangeEventSourceMetrics
        extends DefaultSnapshotChangeEventSourceMetrics<MilvusPartition>
        implements MilvusSnapshotChangeEventSourceMetricsMXBean {

    /**
     * The etcd checkpoint TSO used as {@code guarantee_ts} for the snapshot.
     * Set to 0 when no checkpoint is available (snapshot-from-latest scenario).
     */
    private final AtomicLong guaranteeTso = new AtomicLong(0);

    /**
     * System clock timestamp (ms since epoch) recorded when the snapshot
     * phase started. Set by {@link #snapshotStarted(MilvusPartition)}.
     * Returns 0 if the snapshot has not yet started.
     */
    private final AtomicLong snapshotStartTimeMs = new AtomicLong(0);

    public <T extends CdcSourceTaskContext> MilvusSnapshotChangeEventSourceMetrics(
                                                                                   T taskContext,
                                                                                   ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                   EventMetadataProvider metadataProvider) {
        super(taskContext, changeEventQueueMetrics, metadataProvider);
    }

    @Override
    public void snapshotStarted(MilvusPartition partition) {
        super.snapshotStarted(partition);
        snapshotStartTimeMs.set(System.currentTimeMillis());
    }

    /**
     * Returns the HLC TSO from the etcd channel checkpoint, used as the
     * snapshot's {@code guarantee_ts} anchor point. This TSO corresponds to
     * the Kafka offset that the streaming source will resume from after
     * snapshot completion.
     *
     * <p>Returns 0 if no checkpoint was found (snapshot runs with no
     * guarantee_ts, meaning streaming starts from LATEST).</p>
     *
     * @return the guarantee TSO, or 0 if no checkpoint was available
     */
    @Override
    public long getGuaranteeTso() {
        return guaranteeTso.get();
    }

    /**
     * Set the guarantee TSO from the etcd channel checkpoint.
     *
     * <p>Called by {@link MilvusSnapshotChangeEventSource} once it has
     * successfully read the checkpoint for the pchannel being snapshotted.</p>
     *
     * @param tso the checkpoint {@code guarantee_ts} TSO
     */
    public void setGuaranteeTso(long tso) {
        guaranteeTso.set(tso);
    }

    /**
     * Returns the system clock timestamp (ms since epoch) when the snapshot
     * phase started. Returns 0 if the snapshot has not yet started or has
     * been reset.
     *
     * <p>This metric is listed in DDD-42 Section 12.1 but is not exposed by
     * the standard Debezium {@code SnapshotMeter}. Exposed here as a
     * Milvus-specific JMX attribute.</p>
     *
     * @return snapshot start time in milliseconds since epoch, or 0
     */
    @Override
    public long getSnapshotStartTs() {
        return snapshotStartTimeMs.get();
    }

    @Override
    public void reset() {
        super.reset();
        guaranteeTso.set(0);
        snapshotStartTimeMs.set(0);
    }
}

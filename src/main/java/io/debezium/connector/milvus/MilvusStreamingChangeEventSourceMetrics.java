/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.metrics.DefaultStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * Milvus-specific streaming change event source metrics.
 *
 * <p>Extends the default Debezium streaming metrics with TSO-based
 * watermark lag calculation. Milvus uses a Hybrid Logical Clock (HLC)
 * where the high 46 bits encode physical milliseconds since the Unix
 * epoch and the low 18 bits encode a logical counter. The
 * {@code MilliSecondsBehindSource} metric reflects the wall-clock
 * difference between now and the global watermark's physical time,
 * providing a direct measure of CDC replication lag.</p>
 *
 * <p>Exposed via JMX at:
 * {@code debezium.milvus:type=connector-metrics,context=streaming,server=<server>}</p>
 *
 * @see TimetickOrderingEngine#getGlobalWatermark()
 */
@ThreadSafe
public class MilvusStreamingChangeEventSourceMetrics
        extends DefaultStreamingChangeEventSourceMetrics<MilvusPartition> {

    /**
     * Bit shift to convert Milvus HLC TSO to physical milliseconds.
     * Milvus TSO layout: high 46 bits = physical ms, low 18 bits = logical counter.
     */
    private static final int TSO_PHYSICAL_SHIFT = 18;

    /**
     * Current global watermark TSO. Updated by the streaming change event source
     * after each flush from the {@link TimetickOrderingEngine}.
     */
    private final AtomicLong globalWatermark = new AtomicLong(0);

    /**
     * Last emitted event position, updated after each batch of dispatched events.
     */
    private final AtomicReference<Map<String, String>> sourceEventPosition = new AtomicReference<>(Map.of());

    public <T extends CdcSourceTaskContext> MilvusStreamingChangeEventSourceMetrics(
                                                                                    T taskContext,
                                                                                    ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                    EventMetadataProvider metadataProvider,
                                                                                    CapturedTablesSupplier capturedTablesSupplier) {
        super(taskContext, changeEventQueueMetrics, metadataProvider, capturedTablesSupplier);
    }

    /**
     * Update the global watermark TSO from the ordering engine.
     *
     * <p>Called by {@link MilvusStreamingChangeEventSource} after each
     * flush from the {@link TimetickOrderingEngine}.</p>
     *
     * @param watermark the current global watermark TSO
     */
    public void updateGlobalWatermark(long watermark) {
        globalWatermark.set(watermark);
    }

    /**
     * Compute CDC replication lag based on the TSO global watermark.
     *
     * <p>Derives physical time from the watermark by shifting the logical
     * counter bits ({@code watermark >> 18}) and subtracting from the
     * current system time. Returns 0 when no watermark has been received
     * yet (e.g. immediately after connector start).</p>
     *
     * @return milliseconds behind the source, or 0 if no watermark available
     */
    @Override
    public long getMilliSecondsBehindSource() {
        long watermark = globalWatermark.get();
        if (watermark <= 0) {
            return 0;
        }
        long watermarkPhysicalMs = watermark >> TSO_PHYSICAL_SHIFT;
        long lag = System.currentTimeMillis() - watermarkPhysicalMs;
        return Math.max(0, lag);
    }

    /**
     * Update the source event position from the offset context.
     *
     * <p>Called by {@link MilvusStreamingChangeEventSource} after each
     * batch of dispatched events to surface the current MQ position
     * and vchannel timeticks via JMX.</p>
     *
     * @param position a map representing the current offset position
     */
    public void updateSourceEventPosition(Map<String, String> position) {
        if (position != null) {
            sourceEventPosition.set(Map.copyOf(position));
        }
    }

    @Override
    public Map<String, String> getSourceEventPosition() {
        return sourceEventPosition.get();
    }

    @Override
    public void reset() {
        super.reset();
        globalWatermark.set(0);
        sourceEventPosition.set(Map.of());
    }
}

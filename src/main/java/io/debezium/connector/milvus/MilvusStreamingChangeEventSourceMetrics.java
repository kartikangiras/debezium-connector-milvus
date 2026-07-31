/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.metrics.DefaultStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.util.Clock;

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
 * <p>Also exposes {@code PositionResolved}, a boolean similar in spirit to the
 * standard {@code Connected} attribute, but which only flips to {@code true}
 * once the Kafka consumer's starting position has actually been resolved and
 * assigned. This lets tests and operators wait on a concrete, observable
 * signal instead of a fixed sleep when they need to know the consumer will
 * see events from "now" onward (e.g. after seeking to LATEST).</p>
 *
 * <p>Exposed via JMX at:
 * {@code debezium.milvus:type=connector-metrics,context=streaming,server=<server>}</p>
 *
 * @see TimetickOrderingEngine#getGlobalWatermark()
 */
@ThreadSafe
public class MilvusStreamingChangeEventSourceMetrics
        extends DefaultStreamingChangeEventSourceMetrics<MilvusPartition>
        implements MilvusStreamingChangeEventSourceMetricsMXBean {

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
     * pchannel of the last emitted event position, updated after each batch of
     * dispatched events. {@code null} until the first batch is dispatched.
     */
    private final AtomicReference<String> sourceEventPchannel = new AtomicReference<>();

    /**
     * Global watermark TSO at the time of the last emitted event position.
     */
    private final AtomicLong sourceEventWatermark = new AtomicLong(0);

    /**
     * Whether the Kafka consumer's starting position has been resolved (i.e.
     * {@code assignAndSeek} has completed) for the current streaming run.
     *
     * <p>Unlike {@code Connected}, which flips true as soon as the streaming
     * loop starts, this stays {@code false} until the consumer's start offset
     * (stored offset, snapshot checkpoint offset, or LATEST) has actually been
     * resolved and assigned, so tests and operators can distinguish "streaming
     * started" from "consumer position is known and events will be seen from
     * here on".</p>
     */
    private final AtomicBoolean positionResolved = new AtomicBoolean(false);

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
        long lag = Clock.SYSTEM.currentTimeInMillis() - watermarkPhysicalMs;
        return Math.max(0, lag);
    }

    /**
     * Update the source event position from the offset context.
     *
     * <p>Called by {@link MilvusStreamingChangeEventSource} after each
     * batch of dispatched events to surface the current MQ position
     * and vchannel timeticks via JMX.</p>
     *
     * @param pchannel the physical channel the dispatched batch was read from
     * @param watermark the global watermark TSO at the time of dispatch
     */
    public void updateSourceEventPosition(String pchannel, long watermark) {
        sourceEventPchannel.set(pchannel);
        sourceEventWatermark.set(watermark);
    }

    /**
     * Whether the Kafka consumer's starting position has been resolved for
     * the current streaming run.
     *
     * @return {@code true} once {@code assignAndSeek} has completed for the
     *         current streaming run, {@code false} before that or after the
     *         streaming source has stopped
     */
    @Override
    public boolean isPositionResolved() {
        return positionResolved.get();
    }

    /**
     * Update whether the consumer's starting position has been resolved.
     *
     * <p>Called by {@link MilvusStreamingChangeEventSource} with {@code true}
     * immediately after {@code assignAndSeek} completes, and with
     * {@code false} when streaming stops, mirroring how {@code connected}
     * tracks the streaming loop's running state.</p>
     *
     * @param resolved whether the consumer's starting position is resolved
     */
    public void positionResolved(boolean resolved) {
        positionResolved.set(resolved);
    }

    @Override
    public Map<String, String> getSourceEventPosition() {
        String pchannel = sourceEventPchannel.get();
        if (pchannel == null) {
            return Map.of();
        }
        return Map.of("pchannel", pchannel, "watermark", String.valueOf(sourceEventWatermark.get()));
    }

    @Override
    public void reset() {
        super.reset();
        globalWatermark.set(0);
        sourceEventPchannel.set(null);
        sourceEventWatermark.set(0);
        positionResolved.set(false);
    }
}

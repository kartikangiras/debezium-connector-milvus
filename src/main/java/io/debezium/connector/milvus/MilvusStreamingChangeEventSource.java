/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.util.Strings;

/**
 * Streaming change event source for Milvus.
 *
 * <ol>
 * <li>{@code poll()} raw messages from Kafka via
 * {@link MilvusMessageConsumer}</li>
 * <li>Deserialize via {@link MilvusProtoDeserializer}</li>
 * <li>Buffer DML/DDL events and update watermark via
 * {@link TimetickOrderingEngine}</li>
 * <li>Flush ready events in strict TSO order</li>
 * <li>Update offset context with MQ position and vchannel timeticks</li>
 * </ol>
 *
 */
public class MilvusStreamingChangeEventSource
        implements StreamingChangeEventSource<MilvusPartition, MilvusOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MilvusStreamingChangeEventSource.class);

    private final MilvusConnectorConfig connectorConfig;
    private final MilvusMessageConsumer messageConsumer;
    private final MilvusProtoDeserializer deserializer;
    private final TimetickOrderingEngine orderingEngine;
    private final Duration pollTimeout;

    public MilvusStreamingChangeEventSource(MilvusConnectorConfig connectorConfig,
            MilvusMessageConsumer messageConsumer,
            MilvusProtoDeserializer deserializer,
            TimetickOrderingEngine orderingEngine) {
        this.connectorConfig = connectorConfig;
        this.messageConsumer = messageConsumer;
        this.deserializer = deserializer;
        this.orderingEngine = orderingEngine;
        this.pollTimeout = Duration.ofMillis(connectorConfig.getPollIntervalMs());
    }

    @Override
    public void execute(ChangeEventSource.ChangeEventSourceContext context,
            MilvusPartition partition,
            MilvusOffsetContext offsetContext)
            throws InterruptedException {

        LOGGER.info("Starting Milvus streaming change event source for partition {}", partition);

        String pchannel = partition.getPchannel();
        int partitionIndex = connectorConfig.getKafkaPartitionIndex();
        TopicPartition tp = new TopicPartition(pchannel, partitionIndex);

        preWarmEngine(offsetContext);

        seekConsumer(pchannel, tp, offsetContext);

        boolean bufferFull = false;

        while (context.isRunning()) {
            if (context.isPaused()) {
                context.waitStreamingPaused();
                continue;
            }

            try {
                if (!bufferFull) {
                    List<RawMilvusMessage> messages = messageConsumer.poll(pollTimeout);
                    if (!messages.isEmpty()) {
                        processMessages(messages, pchannel, offsetContext);
                    }
                }

                List<MilvusChangeEvent> flushed = orderingEngine.flush();
                if (!flushed.isEmpty()) {
                    dispatchFlushedEvents(flushed, offsetContext);
                }

                if (orderingEngine.isStalled()) {
                    LOGGER.warn("Timetick stall detected. stalledVchannels={}, bufferedEvents={}, "
                            + "bufferedBytes={}, watermark={}",
                            orderingEngine.getStalledVchannels(),
                            orderingEngine.getBufferedEventCount(),
                            orderingEngine.getBufferedBytes(),
                            orderingEngine.getGlobalWatermark());

                    List<MilvusChangeEvent> forceFlushed = orderingEngine.forceFlush();
                    if (!forceFlushed.isEmpty()) {
                        dispatchFlushedEvents(forceFlushed, offsetContext);
                    }
                }

                bufferFull = false;

                offsetContext.setVchannelTimeticks(orderingEngine.getVchannelTimeticks());
            } catch (MilvusBufferFullException e) {
                LOGGER.warn("Buffer full: {}. Pausing poll, waiting for watermark to advance.", e.getMessage());
                bufferFull = true;

                List<MilvusChangeEvent> flushed = orderingEngine.flush();
                if (!flushed.isEmpty()) {
                    dispatchFlushedEvents(flushed, offsetContext);
                    bufferFull = false;
                } else if (orderingEngine.isStalled()) {
                    List<MilvusChangeEvent> forceFlushed = orderingEngine.forceFlush();
                    if (!forceFlushed.isEmpty()) {
                        dispatchFlushedEvents(forceFlushed, offsetContext);
                    }
                    bufferFull = false;
                } else {
                    Thread.sleep(Math.min(pollTimeout.toMillis(), 1000));
                }
            } catch (MilvusWireFormatMismatchException e) {
                LOGGER.error("Fatal wire format error during streaming: {}", e.getMessage(), e);
                throw new io.debezium.DebeziumException("Wire format mismatch during streaming", e);
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    LOGGER.info("Streaming interrupted");
                } else {
                    LOGGER.error("Error during streaming execution", e);
                }
                throw e;
            }
        }

        LOGGER.info("Milvus streaming change event source stopped. "
                + "forceFlushCount={}, lateMessagesDropped={}, finalWatermark={}",
                orderingEngine.getForceFlushCount(),
                orderingEngine.getLateMessagesDropped(),
                orderingEngine.getGlobalWatermark());
    }

    /**
     * Process raw messages: deserialize and route to the ordering engine.
     */
    private void processMessages(List<RawMilvusMessage> messages, String pchannel,
            MilvusOffsetContext offsetContext)
            throws MilvusWireFormatMismatchException, MilvusBufferFullException {

        for (RawMilvusMessage message : messages) {
            offsetContext.setMqPosition(message.getTopic(), message.getPartition(), message.getOffset());

            List<MilvusChangeEvent> events = deserializer.deserialize(message);

            for (MilvusChangeEvent event : events) {
                if (event instanceof MilvusChangeEvent.TimeTick timeTick) {
                    String vchannel = Strings.defaultIfEmpty(timeTick.getVchannel(), pchannel);
                    orderingEngine.updateWatermark(vchannel, timeTick.getTso());
                } else {
                    orderingEngine.buffer(event);
                }
            }
        }
    }

    /**
     * Dispatch flushed events. Currently logs; full dispatch to Debezium
     * ChangeRecordEmitter will be wired when the schema layer is integrated.
     */
    private void dispatchFlushedEvents(List<MilvusChangeEvent> events,
            MilvusOffsetContext offsetContext) {
        for (MilvusChangeEvent event : events) {
            LOGGER.debug("Dispatching event: type={}, collection={}, vchannel={}, tso={}",
                    event.getClass().getSimpleName(),
                    event.getCollectionName(),
                    event.getVchannel(),
                    event.getTso());

            // TODO: Wire to MilvusChangeRecordEmitter + ChangeEventDispatcher
            // when schema layer is integrated
        }
        LOGGER.info("Dispatched {} events (watermark={})",
                events.size(), orderingEngine.getGlobalWatermark());
    }

    /**
     * Pre-warm the ordering engine from stored vchannel timeticks in the
     * offset context. This avoids a zero-watermark stall on restart.
     */
    private void preWarmEngine(MilvusOffsetContext offsetContext) {
        Map<String, Long> storedTimeticks = extractVchannelTimeticks(offsetContext);
        orderingEngine.preWarm(storedTimeticks);
    }

    /**
     * Extract stored per-vchannel timeticks from the offset context.
     */
    private Map<String, Long> extractVchannelTimeticks(MilvusOffsetContext offsetContext) {
        Map<String, Long> timeticks = new HashMap<>();
        Map<String, ?> offsetMap = offsetContext.getOffset();
        for (Map.Entry<String, ?> entry : offsetMap.entrySet()) {
            String key = entry.getKey();
            if (key != null && key.startsWith("vchannel_timetick_")) {
                String vchannel = key.substring("vchannel_timetick_".length());
                if (!Strings.isNullOrBlank(vchannel) && entry.getValue() != null) {
                    timeticks.put(vchannel, Strings.asLong(entry.getValue().toString(), 0L));
                }
            }
        }
        return timeticks;
    }

    /**
     * Seek the consumer to the appropriate starting position.
     */
    private void seekConsumer(String pchannel, TopicPartition tp,
            MilvusOffsetContext offsetContext) {
        Long storedOffset = offsetContext.getMqOffset(pchannel);

        if (storedOffset != null) {
            messageConsumer.assignAndSeek(Set.of(pchannel), SeekPosition.STORED_OFFSET_PLUS_ONE,
                    Map.of(tp, storedOffset));
            LOGGER.info("Resumed from stored offset + 1 = {}", storedOffset + 1);
        } else if (!offsetContext.isSnapshotCompleted()) {
            Map<TopicPartition, Long> checkpointOffsets = resolveCheckpointOffsets(pchannel);
            if (checkpointOffsets != null && !checkpointOffsets.isEmpty()) {
                messageConsumer.assignAndSeek(Set.of(pchannel), SeekPosition.DEFAULT, checkpointOffsets);
                LOGGER.info("Resumed from snapshot checkpoint offset for pchannel {}", pchannel);
            } else {
                LOGGER.warn(
                        "No checkpoint offset available for snapshot handoff on pchannel {}, falling back to EARLIEST",
                        pchannel);
                messageConsumer.assignAndSeek(Set.of(pchannel), SeekPosition.EARLIEST, null);
            }
        } else {
            messageConsumer.assignAndSeek(Set.of(pchannel), SeekPosition.EARLIEST, null);
            LOGGER.info("No stored offset, seeking to earliest");
        }
    }

    /**
     * Resolve checkpoint offsets for snapshot-to-streaming handoff.
     *
     * <p>
     * TODO: Wire up EtcdCheckpointReader when the snapshot source is
     * implemented. Returns {@code null} causing the caller to fall back
     * to {@link SeekPosition#EARLIEST}.
     * </p>
     */
    private Map<TopicPartition, Long> resolveCheckpointOffsets(String pchannel) {
        return null;
    }
}

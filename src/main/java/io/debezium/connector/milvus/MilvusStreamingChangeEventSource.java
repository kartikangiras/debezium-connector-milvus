/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;

/**
 * Streaming change event source for Milvus — MQ read layer only.
 *
 * <p>Polls raw {@link RawMilvusMessage}s from Kafka via
 * {@link MilvusMessageConsumer} and tracks the MQ offset in
 * {@link MilvusOffsetContext}. No deserialization is performed at this
 * layer; raw messages are consumed and their offsets tracked.</p>
 *
 * <p>Seek paths supported:</p>
 * <ul>
 *   <li>EARLIEST — seek to beginning</li>
 *   <li>STORED_OFFSET_PLUS_ONE — resume from stored offset + 1</li>
 *   <li>DEFAULT — use checkpoint/snapshot offsets</li>
 * </ul>
 */
public class MilvusStreamingChangeEventSource implements StreamingChangeEventSource<MilvusPartition, MilvusOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MilvusStreamingChangeEventSource.class);

    private final MilvusConnectorConfig connectorConfig;
    private final MilvusMessageConsumer messageConsumer;
    private final Duration pollTimeout;

    public MilvusStreamingChangeEventSource(MilvusConnectorConfig connectorConfig,
                                            MilvusMessageConsumer messageConsumer) {
        this.connectorConfig = connectorConfig;
        this.messageConsumer = messageConsumer;
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

        Long storedOffset = offsetContext.getMqOffset(pchannel);

        if (storedOffset != null) {
            // Normal resume: stored offset + 1 to avoid re-processing the last committed message.
            messageConsumer.assignAndSeek(Set.of(pchannel), SeekPosition.STORED_OFFSET_PLUS_ONE, Map.of(tp, storedOffset));
            LOGGER.info("Resumed from stored offset + 1 = {}", storedOffset + 1);
        }
        else if (!offsetContext.isSnapshotCompleted()) {
            // Snapshot handoff: DEFAULT (checkpoint offset from etcd, injected by snapshot source).
            Map<TopicPartition, Long> checkpointOffsets = resolveCheckpointOffsets(pchannel);
            if (checkpointOffsets != null && !checkpointOffsets.isEmpty()) {
                messageConsumer.assignAndSeek(Set.of(pchannel), SeekPosition.DEFAULT, checkpointOffsets);
                LOGGER.info("Resumed from snapshot checkpoint offset for pchannel {}", pchannel);
            }
            else {
                LOGGER.warn("No checkpoint offset available for snapshot handoff on pchannel {}, falling back to EARLIEST", pchannel);
                messageConsumer.assignAndSeek(Set.of(pchannel), SeekPosition.EARLIEST, null);
            }
        }
        else {
            // No stored offset, no snapshot in progress: full replay.
            messageConsumer.assignAndSeek(Set.of(pchannel), SeekPosition.EARLIEST, null);
            LOGGER.info("No stored offset, seeking to earliest");
        }

        while (context.isRunning()) {
            if (context.isPaused()) {
                context.waitStreamingPaused();
                continue;
            }

            try {
                List<RawMilvusMessage> messages = messageConsumer.poll(pollTimeout);
                if (messages.isEmpty()) {
                    continue;
                }

                for (RawMilvusMessage message : messages) {
                    offsetContext.setMqPosition(message.getTopic(), message.getPartition(), message.getOffset());
                    LOGGER.info("Consumed raw message: topic={} partition={} offset={} size={}b",
                            message.getTopic(), message.getPartition(),
                            message.getOffset(), message.getValue().length);
                }
            }
            catch (Exception e) {
                LOGGER.error("Error during streaming execution", e);
                throw e;
            }
        }

        LOGGER.info("Milvus streaming change event source stopped");
    }

    /**
     * Resolve checkpoint offsets for snapshot-to-streaming handoff.
     *
     * <p>TODO: Wire up EtcdCheckpointReader when the snapshot source is
     * implemented. Returns {@code null} today, causing the caller to fall back
     * to {@link SeekPosition#EARLIEST}.</p>
     */
    private Map<TopicPartition, Long> resolveCheckpointOffsets(String pchannel) {
        return null;
    }
}

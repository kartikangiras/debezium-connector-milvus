/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;

/**
 * Kafka-based implementation of {@link MilvusMessageConsumer}.
 *
 * <p>
 * Uses manual partition assignment (no consumer group rebalance) to align
 * with Milvus pchannel semantics. The consumer is created from the connector
 * configuration and assigned to the specific topic/partition.
 * </p>
 *
 * <p>
 * <b>Important design note:</b> This is intentionally <em>not</em> a
 * group-managed consumer. It does not use
 * {@link KafkaConsumer#subscribe(java.util.Collection)}, does not rely on
 * rebalances, and does not use broker-managed group offsets for recovery
 * decisions. Ownership and resume position are controlled by Debezium internal
 * task state via explicit {@link #assignAndSeek(Map)}. This bypasses
 * consumer-group semantics deliberately, which is documented here because
 * future maintainers may find it surprising or not understand the
 * justification.
 * </p>
 *
 * <p>
 * Supported seek paths:
 * </p>
 * <ul>
 * <li>{@link SeekPosition#EARLIEST} — seek to the beginning of each topic</li>
 * <li>{@link SeekPosition#STORED_OFFSET_PLUS_ONE} — seek to stored offset +
 * 1</li>
 * <li>{@link SeekPosition#DEFAULT} — use the provided checkpoint offsets,
 * typically mapped to snapshot mode behavior</li>
 * </ul>
 */
public class KafkaMilvusMessageConsumer implements MilvusMessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMilvusMessageConsumer.class);

    private final MilvusConnectorConfig config;
    private KafkaConsumer<?, ?> kafkaConsumer;
    private boolean closed = false;

    public KafkaMilvusMessageConsumer(MilvusConnectorConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");
    }

    /**
     * Package-private constructor for unit testing that allows injecting a
     * pre-configured Kafka consumer.
     */
    KafkaMilvusMessageConsumer(MilvusConnectorConfig config, KafkaConsumer<?, ?> kafkaConsumer) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.kafkaConsumer = Objects.requireNonNull(kafkaConsumer, "kafkaConsumer must not be null");
    }

    /**
     * Lazily creates the underlying {@link KafkaConsumer} with the connector's
     * Kafka configuration.
     *
     * <p>
     * Key settings for correctness:
     * </p>
     * <ul>
     * <li>{@code enable.auto.commit=false} — Debezium manages offsets via
     * Connect storage</li>
     * <li>{@code isolation.level=read_committed} — avoid reading transactional
     * messages mid-transaction</li>
     * <li>{@code auto.offset.reset=none} — force explicit seek; fail if offset
     * missing</li>
     * <li>{@code max.poll.interval.ms=300000} — prevent rebalance during slow
     * snapshot phases</li>
     * </ul>
     */
    public synchronized void initialize() {
        if (closed) {
            throw new IllegalStateException("KafkaMilvusMessageConsumer is closed");
        }
        if (kafkaConsumer != null) {
            return;
        }
        Properties props = new Properties();
        props.put("bootstrap.servers", config.getKafkaBootstrapServers());
        props.put("group.id", config.getKafkaConsumerGroupId());
        props.put("enable.auto.commit", "false");
        props.put("isolation.level", "read_committed");
        props.put("auto.offset.reset", "none");
        props.put("max.poll.interval.ms", String.valueOf(config.getKafkaMaxPollIntervalMs()));
        props.put("key.deserializer", config.getKafkaKeyDeserializer());
        props.put("value.deserializer", config.getKafkaValueDeserializer());

        this.kafkaConsumer = new KafkaConsumer<>(props);
        LOGGER.info("Kafka consumer initialized for bootstrap servers: {}", config.getKafkaBootstrapServers());
    }

    @Override
    public void assignAndSeek(Map<TopicPartition, Long> offsets) {
        if (closed) {
            throw new IllegalStateException("KafkaMilvusMessageConsumer is closed");
        }
        initialize();
        Objects.requireNonNull(offsets, "offsets must not be null");
        if (offsets.isEmpty()) {
            throw new IllegalArgumentException("offsets map must not be empty");
        }

        Set<TopicPartition> partitions = offsets.keySet();
        kafkaConsumer.assign(partitions);
        LOGGER.info("Assigned partitions: {}", partitions);

        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            long offset = entry.getValue();
            kafkaConsumer.seek(tp, offset);
            LOGGER.info("Seeked partition {} to offset {}", tp, offset);
        }
    }

    /**
     * Assign partitions and seek using one of the supported strategies.
     *
     * @param pchannels     set of physical channel names (Kafka topics)
     * @param position      seek strategy to apply
     * @param storedOffsets stored offsets for
     *                      {@link SeekPosition#STORED_OFFSET_PLUS_ONE} or
     *                      checkpoint offsets for {@link SeekPosition#DEFAULT}
     */
    @Override
    public void assignAndSeek(Set<String> pchannels, SeekPosition position,
                              Map<TopicPartition, Long> storedOffsets) {
        if (closed) {
            throw new IllegalStateException("KafkaMilvusMessageConsumer is closed");
        }
        initialize();
        Objects.requireNonNull(pchannels, "pchannels must not be null");
        Objects.requireNonNull(position, "position must not be null");

        if (pchannels.isEmpty()) {
            throw new IllegalArgumentException("pchannels must not be empty");
        }

        // Derive TopicPartitions from pchannels. Partition index is configurable
        // (default 0) to accommodate future Milvus topologies.
        int partitionIndex = config.getKafkaPartitionIndex();
        Set<TopicPartition> partitions = pchannels.stream()
                .map(pchannel -> new TopicPartition(pchannel, partitionIndex))
                .collect(Collectors.toSet());

        kafkaConsumer.assign(partitions);
        LOGGER.info("Assigned partitions using {} strategy: {}", position, partitions);

        switch (position) {
            case EARLIEST:
                kafkaConsumer.seekToBeginning(partitions);
                LOGGER.info("Seeked to earliest for all assigned partitions");
                break;

            case STORED_OFFSET_PLUS_ONE:
                if (storedOffsets == null || storedOffsets.isEmpty()) {
                    throw new IllegalArgumentException(
                            "storedOffsets must not be null or empty for STORED_OFFSET_PLUS_ONE");
                }
                for (TopicPartition tp : partitions) {
                    Long stored = storedOffsets.get(tp);
                    if (stored == null) {
                        throw new DebeziumException(
                                "No stored offset for partition " + tp + "; cannot seek to stored offset+1");
                    }
                    long seekOffset = stored + 1;
                    kafkaConsumer.seek(tp, seekOffset);
                    LOGGER.info("Seeked partition {} to stored offset + 1 = {}", tp, seekOffset);
                }
                break;

            case DEFAULT:
                // Default maps to snapshot mode behavior: use the provided checkpoint
                // offsets (typically from etcd via EtcdCheckpointReader).
                if (storedOffsets == null || storedOffsets.isEmpty()) {
                    throw new IllegalArgumentException(
                            "storedOffsets must not be null or empty for DEFAULT (snapshot checkpoint) seek");
                }
                for (TopicPartition tp : partitions) {
                    Long checkpointOffset = storedOffsets.get(tp);
                    if (checkpointOffset == null) {
                        throw new DebeziumException(
                                "No checkpoint offset for partition " + tp + "; cannot seek to default position");
                    }
                    kafkaConsumer.seek(tp, checkpointOffset);
                    LOGGER.info("Seeked partition {} to checkpoint offset = {}", tp, checkpointOffset);
                }
                break;

            default:
                throw new IllegalArgumentException("Unknown seek position: " + position);
        }
    }

    @Override
    public List<RawMilvusMessage> poll(Duration timeout) {
        if (closed) {
            throw new IllegalStateException("KafkaMilvusMessageConsumer is closed");
        }
        initialize();
        Objects.requireNonNull(timeout, "timeout must not be null");
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be positive");
        }

        try {
            ConsumerRecords<?, ?> records = kafkaConsumer.poll(timeout);
            if (records == null || records.isEmpty()) {
                return Collections.emptyList();
            }

            List<RawMilvusMessage> result = new ArrayList<>(records.count());
            for (ConsumerRecord<?, ?> record : records) {
                result.add(RawMilvusMessage.fromKafkaRecord(record));
            }
            return Collections.unmodifiableList(result);
        }
        catch (RetriableException e) {
            LOGGER.warn("Retriable Kafka exception during poll", e);
            throw new DebeziumException("Retriable Kafka error during poll: " + e.getMessage(), e);
        }
        catch (Exception e) {
            LOGGER.error("Fatal Kafka exception during poll", e);
            throw new DebeziumException("Fatal Kafka error during poll: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        LOGGER.info("Closing KafkaMilvusMessageConsumer");
        this.closed = true;
        if (kafkaConsumer != null) {
            try {
                kafkaConsumer.close();
            }
            catch (Exception e) {
                LOGGER.warn("Exception while closing Kafka consumer", e);
            }
        }
    }
}

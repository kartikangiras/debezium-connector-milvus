/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.errors.ConnectException;

/**
 * Immutable wrapper for a raw message consumed from the Milvus MQ layer.
 *
 * <p>Decouples the consumer interface from Kafka-specific {@link ConsumerRecord}
 * so that alternative transports (e.g. Woodpecker in Milvus 2.6) can share the
 * same streaming pipeline.</p>
 */
public class RawMilvusMessage {

    private final String topic;
    private final int partition;
    private final long offset;
    private final byte[] key;
    private final byte[] value;
    private final long timestamp;

    public RawMilvusMessage(String topic, int partition, long offset,
                            byte[] key, byte[] value, long timestamp) {
        this.topic = Objects.requireNonNull(topic, "topic must not be null");
        this.partition = partition;
        this.offset = offset;
        this.key = key;
        this.value = Objects.requireNonNull(value, "value must not be null");
        this.timestamp = timestamp;
    }

    /**
     * Convenience factory that adapts a Kafka {@link ConsumerRecord}.
     *
     * @param record the Kafka consumer record (any key/value types)
     * @return a new {@link RawMilvusMessage} with key/value converted to byte arrays
     */
    public static RawMilvusMessage fromKafkaRecord(ConsumerRecord<?, ?> record) {
        return new RawMilvusMessage(
                record.topic(),
                record.partition(),
                record.offset(),
                toBytes(record.key()),
                toBytes(record.value()),
                record.timestamp());
    }

    /**
     * Converts a deserialized Kafka record key or value to a {@code byte[]}.
     * Only {@code byte[]}, {@link ByteBuffer}, and {@link String} are supported.
     */
    private static byte[] toBytes(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof ByteBuffer buf) {
            byte[] bytes = new byte[buf.remaining()];
            buf.duplicate().get(bytes);
            return bytes;
        }
        if (value instanceof String s) {
            return s.getBytes(StandardCharsets.UTF_8);
        }
        throw new ConnectException(
                "Unsupported Kafka record value type: " + value.getClass().getName());
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "RawMilvusMessage{topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                ", timestamp=" + timestamp +
                '}';
    }
}

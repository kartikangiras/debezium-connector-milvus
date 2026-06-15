/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

public class RawMilvusMessageTest {

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldCreateFromKafkaRecord() {
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "by-dev-rootcoord-dml_0", 0, 42L,
                "key".getBytes(), "value".getBytes());

        RawMilvusMessage msg = RawMilvusMessage.fromKafkaRecord(record);

        assertThat(msg.getTopic()).isEqualTo("by-dev-rootcoord-dml_0");
        assertThat(msg.getPartition()).isEqualTo(0);
        assertThat(msg.getOffset()).isEqualTo(42L);
        assertThat(msg.getKey()).isEqualTo("key".getBytes());
        assertThat(msg.getValue()).isEqualTo("value".getBytes());
        assertThat(msg.getTimestamp()).isEqualTo(record.timestamp());
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldCreateFromKafkaRecordWithTimestamp() {
        long ts = 1234567890L;
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "topic", 1, 100L, ts, TimestampType.CREATE_TIME,
                0, 0, "key".getBytes(), "value".getBytes(),
                new RecordHeaders(), null);

        RawMilvusMessage msg = RawMilvusMessage.fromKafkaRecord(record);

        assertThat(msg.getTimestamp()).isEqualTo(ts);
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldCreateDirectly() {
        RawMilvusMessage msg = new RawMilvusMessage(
                "topic", 2, 99L, null, "value".getBytes(), 0L);

        assertThat(msg.getTopic()).isEqualTo("topic");
        assertThat(msg.getPartition()).isEqualTo(2);
        assertThat(msg.getOffset()).isEqualTo(99L);
        assertThat(msg.getKey()).isNull();
        assertThat(msg.getValue()).isEqualTo("value".getBytes());
        assertThat(msg.getTimestamp()).isEqualTo(0L);
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldThrowOnNullTopic() {
        assertThatNullPointerException()
                .isThrownBy(() -> new RawMilvusMessage(null, 0, 0L, null, new byte[0], 0L))
                .withMessageContaining("topic");
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldThrowOnNullValue() {
        assertThatNullPointerException()
                .isThrownBy(() -> new RawMilvusMessage("topic", 0, 0L, null, null, 0L))
                .withMessageContaining("value");
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldIncludeFieldsInToString() {
        RawMilvusMessage msg = new RawMilvusMessage(
                "topic", 3, 7L, "key".getBytes(), "value".getBytes(), 111L);

        String str = msg.toString();

        assertThat(str).contains("topic='topic'");
        assertThat(str).contains("partition=3");
        assertThat(str).contains("offset=7");
        assertThat(str).contains("timestamp=111");
    }
}

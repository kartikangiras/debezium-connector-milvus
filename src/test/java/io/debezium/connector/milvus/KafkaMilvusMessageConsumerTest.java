/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;

public class KafkaMilvusMessageConsumerTest {

    private static final String TOPIC = "by-dev-rootcoord-dml_0";
    private static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    private MilvusConnectorConfig config;
    @SuppressWarnings("rawtypes")
    private KafkaConsumer kafkaConsumer;
    private KafkaMilvusMessageConsumer consumer;

    @BeforeEach
    void setUp() {
        Configuration configuration = Configuration.from(Map.of(
                "milvus.uri", "http://localhost:19530",
                "topic.prefix", "milvus-test",
                "milvus.kafka.bootstrap.servers", "localhost:9092",
                "milvus.kafka.consumer.group.id", "test-group"));
        config = new MilvusConnectorConfig(configuration);
        kafkaConsumer = mock(KafkaConsumer.class);
        consumer = new KafkaMilvusMessageConsumer(config, kafkaConsumer);
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldAssignAndSeekDirectly() {
        Map<TopicPartition, Long> offsets = Map.of(TP, 42L);

        consumer.assignAndSeek(offsets);

        verify(kafkaConsumer).assign(Set.of(TP));
        verify(kafkaConsumer).seek(TP, 42L);
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldThrowOnEmptyOffsetsForDirectAssign() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> consumer.assignAndSeek(Collections.emptyMap()))
                .withMessageContaining("empty");
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldThrowOnNullOffsetsForDirectAssign() {
        assertThatThrownBy(() -> consumer.assignAndSeek(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("offsets");
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldSeekToEarliest() {
        consumer.assignAndSeek(Set.of(TOPIC), SeekPosition.EARLIEST, null);

        verify(kafkaConsumer).assign(Set.of(TP));
        verify(kafkaConsumer).seekToBeginning(Set.of(TP));
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldSeekToLatestAndEagerlyResolvePosition() {
        when(kafkaConsumer.position(TP)).thenReturn(42L);

        consumer.assignAndSeek(Set.of(TOPIC), SeekPosition.LATEST, null);

        verify(kafkaConsumer).assign(Set.of(TP));
        verify(kafkaConsumer).seekToEnd(Set.of(TP));
        verify(kafkaConsumer).position(TP);
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldSeekToStoredOffsetPlusOne() {
        Map<TopicPartition, Long> storedOffsets = Map.of(TP, 99L);

        consumer.assignAndSeek(Set.of(TOPIC), SeekPosition.STORED_OFFSET_PLUS_ONE, storedOffsets);

        verify(kafkaConsumer).assign(Set.of(TP));
        verify(kafkaConsumer).seek(TP, 100L);
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldThrowWhenStoredOffsetMissingForStoredOffsetPlusOne() {
        Map<TopicPartition, Long> storedOffsets = Map.of(new TopicPartition("other", 0), 1L);

        assertThatThrownBy(
                () -> consumer.assignAndSeek(Set.of(TOPIC), SeekPosition.STORED_OFFSET_PLUS_ONE, storedOffsets))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("No stored offset");
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldThrowWhenStoredOffsetsNullForStoredOffsetPlusOne() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> consumer.assignAndSeek(Set.of(TOPIC), SeekPosition.STORED_OFFSET_PLUS_ONE, null))
                .withMessageContaining("storedOffsets");
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldSeekToDefaultCheckpointOffsets() {
        Map<TopicPartition, Long> checkpointOffsets = Map.of(TP, 55L);

        consumer.assignAndSeek(Set.of(TOPIC), SeekPosition.DEFAULT, checkpointOffsets);

        verify(kafkaConsumer).assign(Set.of(TP));
        verify(kafkaConsumer).seek(TP, 55L);
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldThrowWhenCheckpointOffsetMissingForDefault() {
        Map<TopicPartition, Long> checkpointOffsets = Map.of(new TopicPartition("other", 0), 1L);

        assertThatThrownBy(() -> consumer.assignAndSeek(Set.of(TOPIC), SeekPosition.DEFAULT, checkpointOffsets))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("No checkpoint offset");
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldThrowWhenCheckpointOffsetsNullForDefault() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> consumer.assignAndSeek(Set.of(TOPIC), SeekPosition.DEFAULT, null))
                .withMessageContaining("storedOffsets");
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldThrowOnEmptyPchannels() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> consumer.assignAndSeek(Collections.emptySet(), SeekPosition.EARLIEST, null))
                .withMessageContaining("empty");
    }

    @Test
    @FixFor("debezium/dbz#2068")
    @SuppressWarnings("rawtypes")
    void shouldReturnEmptyListWhenNoRecords() {
        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(new ConsumerRecords(Collections.emptyMap()));

        List<RawMilvusMessage> result = consumer.poll(Duration.ofMillis(100));

        assertThat(result).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#2068")
    @SuppressWarnings({ "rawtypes", "unchecked" })
    void shouldReturnMessagesFromPoll() {
        ConsumerRecord<?, ?> record = new ConsumerRecord<>(TOPIC, 0, 1L, "key".getBytes(),
                "value".getBytes());
        Map recordsMap = new HashMap<>();
        recordsMap.put(TP, List.of(record));
        ConsumerRecords consumerRecords = new ConsumerRecords(recordsMap);

        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(consumerRecords);

        List<RawMilvusMessage> result = consumer.poll(Duration.ofMillis(100));

        assertThat(result).hasSize(1);
        RawMilvusMessage msg = result.get(0);
        assertThat(msg.getTopic()).isEqualTo(TOPIC);
        assertThat(msg.getPartition()).isEqualTo(0);
        assertThat(msg.getOffset()).isEqualTo(1L);
        assertThat(msg.getValue()).isEqualTo("value".getBytes());
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldThrowOnNegativePollTimeout() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> consumer.poll(Duration.ofMillis(-1)))
                .withMessageContaining("positive");
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldThrowOnZeroPollTimeout() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> consumer.poll(Duration.ZERO))
                .withMessageContaining("positive");
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldThrowDebeziumExceptionOnRetriableKafkaError() {
        when(kafkaConsumer.poll(any(Duration.class))).thenThrow(new TimeoutException());

        assertThatThrownBy(() -> consumer.poll(Duration.ofMillis(100)))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Retriable Kafka error");
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldThrowDebeziumExceptionOnFatalKafkaError() {
        RuntimeException fatal = new RuntimeException("broker down");
        when(kafkaConsumer.poll(any(Duration.class))).thenThrow(fatal);

        assertThatThrownBy(() -> consumer.poll(Duration.ofMillis(100)))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Fatal Kafka error")
                .hasCause(fatal);
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldCloseKafkaConsumer() {
        consumer.close();

        verify(kafkaConsumer).close();
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldNotThrowOnCloseWithoutInit() {
        KafkaMilvusMessageConsumer freshConsumer = new KafkaMilvusMessageConsumer(config);
        assertThatNoException().isThrownBy(freshConsumer::close);
    }

    @Test
    @FixFor("debezium/dbz#2068")
    @SuppressWarnings({ "rawtypes", "unchecked" })
    void shouldHandleMultiplePollCalls() {
        ConsumerRecord<?, ?> r1 = new ConsumerRecord<>(TOPIC, 0, 1L, "k1".getBytes(), "v1".getBytes());
        ConsumerRecord<?, ?> r2 = new ConsumerRecord<>(TOPIC, 0, 2L, "k2".getBytes(), "v2".getBytes());

        Map map1 = new HashMap<>();
        map1.put(TP, List.of(r1));
        Map map2 = new HashMap<>();
        map2.put(TP, List.of(r2));

        when(kafkaConsumer.poll(any(Duration.class)))
                .thenReturn(new ConsumerRecords(map1))
                .thenReturn(new ConsumerRecords(map2))
                .thenReturn(new ConsumerRecords(Collections.emptyMap()));

        List<RawMilvusMessage> batch1 = consumer.poll(Duration.ofMillis(100));
        List<RawMilvusMessage> batch2 = consumer.poll(Duration.ofMillis(100));
        List<RawMilvusMessage> batch3 = consumer.poll(Duration.ofMillis(100));

        assertThat(batch1).hasSize(1);
        assertThat(batch1.get(0).getOffset()).isEqualTo(1L);
        assertThat(batch2).hasSize(1);
        assertThat(batch2.get(0).getOffset()).isEqualTo(2L);
        assertThat(batch3).isEmpty();

        verify(kafkaConsumer, times(3)).poll(any(Duration.class));
    }
}

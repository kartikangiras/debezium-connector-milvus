/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.pipeline.source.spi.ChangeEventSource;

public class MilvusStreamingChangeEventSourceTest {

    private static final String TOPIC = "by-dev-rootcoord-dml_0";

    private MilvusConnectorConfig config;
    private MilvusMessageConsumer messageConsumer;
    private MilvusProtoDeserializer deserializer;
    private TimetickOrderingEngine orderingEngine;
    private ChangeEventSource.ChangeEventSourceContext context;
    private MilvusPartition partition;
    private MilvusOffsetContext offsetContext;
    private MilvusStreamingChangeEventSource source;

    @BeforeEach
    void setUp() {
        Configuration configuration = Configuration.from(Map.of(
                "milvus.uri", "http://localhost:19530",
                "topic.prefix", "milvus-test",
                "milvus.kafka.bootstrap.servers", "localhost:9092",
                "poll.interval.ms", "100"));
        config = new MilvusConnectorConfig(configuration);
        messageConsumer = mock(MilvusMessageConsumer.class);
        deserializer = mock(MilvusProtoDeserializer.class);
        orderingEngine = new TimetickOrderingEngine(config);
        context = mock(ChangeEventSource.ChangeEventSourceContext.class);
        partition = MilvusPartition.create("milvus-test", TOPIC);
        offsetContext = new MilvusOffsetContext(new MilvusSourceInfo(config));

        source = new MilvusStreamingChangeEventSource(config, messageConsumer, deserializer, orderingEngine);
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldStopWhenContextNotRunning() throws InterruptedException {
        when(context.isRunning()).thenReturn(false);

        source.execute(context, partition, offsetContext);

        verify(messageConsumer, never()).poll(any(Duration.class));
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldPollAndTrackOffset() throws Exception {
        RawMilvusMessage msg = new RawMilvusMessage(TOPIC, 0, 1L, null, "payload".getBytes(), 0L);

        when(context.isRunning()).thenReturn(true, false);
        when(messageConsumer.poll(any(Duration.class))).thenReturn(List.of(msg));
        when(deserializer.deserialize(any(RawMilvusMessage.class))).thenReturn(List.of());

        source.execute(context, partition, offsetContext);

        verify(messageConsumer).poll(any(Duration.class));
        assertThat(offsetContext.getMqOffset(TOPIC)).isEqualTo(1L);
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldHandleEmptyPoll() throws Exception {
        when(context.isRunning()).thenReturn(true, false);
        when(messageConsumer.poll(any(Duration.class))).thenReturn(List.of());

        source.execute(context, partition, offsetContext);

        verify(messageConsumer).poll(any(Duration.class));
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldHandlePauseAndResume() throws Exception {
        when(context.isRunning()).thenReturn(true, true, false);
        when(context.isPaused()).thenReturn(true, false);
        when(messageConsumer.poll(any(Duration.class))).thenReturn(List.of());

        source.execute(context, partition, offsetContext);

        verify(context).waitStreamingPaused();
        verify(messageConsumer, times(1)).poll(any(Duration.class));
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldBreakLoopOnInterrupt() throws Exception {
        when(context.isRunning()).thenReturn(true);
        when(messageConsumer.poll(any(Duration.class))).then(invocation -> {
            Thread.sleep(10);
            return List.of();
        });

        Thread testThread = new Thread(() -> {
            try {
                source.execute(context, partition, offsetContext);
            }
            catch (InterruptedException e) {
                // expected
            }
        });
        testThread.start();
        Thread.sleep(30);
        testThread.interrupt();
        testThread.join(1000);

        assertThat(testThread.isAlive()).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldResumeFromStoredOffset() throws Exception {
        offsetContext.setMqPosition(TOPIC, 0, 99L);
        offsetContext.postSnapshotCompletion();

        when(context.isRunning()).thenReturn(true, false);
        when(messageConsumer.poll(any(Duration.class))).thenReturn(List.of());

        source.execute(context, partition, offsetContext);

        verify(messageConsumer).assignAndSeek(
                Set.of(TOPIC),
                SeekPosition.STORED_OFFSET_PLUS_ONE,
                Map.of(new TopicPartition(TOPIC, 0), 99L));
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldSeekToEarliestWhenSnapshotCompletedAndNoStoredOffset() throws Exception {
        offsetContext.postSnapshotCompletion();

        when(context.isRunning()).thenReturn(true, false);
        when(messageConsumer.poll(any(Duration.class))).thenReturn(List.of());

        source.execute(context, partition, offsetContext);

        verify(messageConsumer).assignAndSeek(
                Set.of(TOPIC),
                SeekPosition.EARLIEST,
                null);
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldFallbackToEarliestForSnapshotHandoffWhenNoCheckpoint() throws Exception {
        // snapshot not completed, no stored offset -> attempts DEFAULT, falls back to
        // EARLIEST
        when(context.isRunning()).thenReturn(true, false);
        when(messageConsumer.poll(any(Duration.class))).thenReturn(List.of());

        source.execute(context, partition, offsetContext);

        verify(messageConsumer).assignAndSeek(
                Set.of(TOPIC),
                SeekPosition.EARLIEST,
                null);
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldDeserializeAndBufferEvents() throws Exception {
        RawMilvusMessage msg = new RawMilvusMessage(TOPIC, 0, 1L, null, "payload".getBytes(), 0L);

        MilvusChangeEvent.Insert insert = new MilvusChangeEvent.Insert(
                "coll", TOPIC, TOPIC, 100, Map.of("id", 1L));
        MilvusChangeEvent.TimeTick tick = new MilvusChangeEvent.TimeTick(
                null, TOPIC, TOPIC, 200);

        when(context.isRunning()).thenReturn(true, false);
        when(messageConsumer.poll(any(Duration.class))).thenReturn(List.of(msg));
        when(deserializer.deserialize(any(RawMilvusMessage.class)))
                .thenReturn(List.of(insert, tick));

        source.execute(context, partition, offsetContext);

        // The insert should have been buffered and the timetick should have updated
        // the watermark. Since watermark=200 >= TSO=100, the insert should have been
        // flushed.
        assertThat(orderingEngine.getBufferedEventCount()).isZero();
        assertThat(orderingEngine.getGlobalWatermark()).isEqualTo(200L);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.milvus.util.ProtoMessageFactory;
import io.debezium.connector.milvus.util.TestHelper;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.util.Clock;

/**
 * End-to-end integration test for the D0 Milvus streaming pipeline.
 *
 * <p>
 * Runs under {@code maven-failsafe-plugin} ({@code *IT.java}). A Kafka cluster
 * is started by {@code docker-maven-plugin} during {@code pre-integration-test}
 * and the {@code kafka.bootstrap.servers} system property is passed to
 * {@code maven-failsafe-plugin}.
 * </p>
 *
 * <p>
 * Tests exercise the real {@link KafkaMilvusMessageConsumer},
 * {@link MilvusProtoDeserializer} and {@link TimetickOrderingEngine} — and, for
 * the watermark / round-trip assertion, the real
 * {@link MilvusStreamingChangeEventSource} driven against the live Kafka topic
 * via a mocked {@link ChangeEventSource.ChangeEventSourceContext}.
 * </p>
 *
 * <p>Scenarios:</p>
 * <ul>
 * <li>{@code shouldDeserializeInsertAndAdvanceWatermark}</li>
 * <li>{@code shouldDeserializeDeleteEvent}</li>
 * <li>{@code shouldHandleMultipleVchannels}</li>
 * <li>{@code shouldDetectStallAndForceFlush}</li>
 * <li>{@code shouldThrowBufferFullOnOverflow}</li>
 * <li>{@code shouldDeserializeProtoSingleWireFormat}</li>
 * <li>{@code shouldDeserializeMsgPackBatchWireFormat}</li>
 * </ul>
 */
class MilvusStreamingPipelineIT extends AbstractAsyncEngineConnectorTest {

    private static final String TOPIC_PREFIX_LOCAL = TestHelper.TOPIC_PREFIX;

    private String bootstrap;
    private String topic;
    private final Set<KafkaMilvusMessageConsumer> openConsumers = new HashSet<>();

    @BeforeEach
    void setUp() {
        bootstrap = TestHelper.bootstrapServers();
        topic = uniqueTopic();
        TestHelper.ensureTopic(bootstrap, topic);
    }

    @AfterEach
    void tearDown() {
        for (KafkaMilvusMessageConsumer c : openConsumers) {
            try {
                c.close();
            }
            catch (Exception ignored) {
                // best-effort
            }
        }
        openConsumers.clear();
    }

    // -- helpers -------------------------------------------------------------

    private static String uniqueTopic() {
        return "by-dev-rootcoord-dml_" + UUID.randomUUID().toString().substring(0, 8);
    }

    private MilvusConnectorConfig config(String wireFormat, Long stallMs, Integer bufferMaxEvents) {
        io.debezium.config.Configuration.Builder b = TestHelper.defaultConfig(bootstrap).edit()
                .with("milvus.wire.format", wireFormat)
                .with("milvus.kafka.bootstrap.servers", bootstrap);
        if (stallMs != null) {
            b.with("milvus.timetick.stall.timeout.ms", stallMs.toString());
        }
        if (bufferMaxEvents != null) {
            b.with("milvus.buffer.max.events", bufferMaxEvents.toString());
        }
        return TestHelper.connectorConfig(b.build());
    }

    private MilvusProtoDeserializer deserializer(MilvusConnectorConfig cfg) {
        return new MilvusProtoDeserializer(cfg.getWireFormat(),
                new MilvusColumnarPivot(new MilvusValueConverter(cfg)));
    }

    /**
     * Create a {@link KafkaMilvusMessageConsumer} assigned to the supplied
     * pchannels and seeked to EARLIEST, tracked for orderly shutdown.
     */
    private KafkaMilvusMessageConsumer consumerAtEarliest(MilvusConnectorConfig cfg, Set<String> pchannels) {
        KafkaMilvusMessageConsumer consumer = TestHelper.kafkaConsumer(cfg);
        openConsumers.add(consumer);
        consumer.assignAndSeek(pchannels, SeekPosition.EARLIEST, null);
        return consumer;
    }

    /**
     * Drain all raw messages from a consumer, polling until two consecutive
     * empty polls are observed (bounded by {@code atMost}).
     */
    private List<RawMilvusMessage> drainUntilIdle(KafkaMilvusMessageConsumer consumer, Duration atMost) {
        List<RawMilvusMessage> all = new ArrayList<>();
        int[] idleCount = { 0 };
        Awaitility.await("drain raw messages from Kafka")
                .atMost(atMost)
                .pollDelay(Duration.ofMillis(50))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> {
                    List<RawMilvusMessage> batch = consumer.poll(Duration.ofMillis(500));
                    if (batch.isEmpty()) {
                        idleCount[0]++;
                        return !all.isEmpty() && idleCount[0] >= 2;
                    }
                    idleCount[0] = 0;
                    all.addAll(batch);
                    return false;
                });
        // Catch late-delivered batches between the last non-empty and idle polls.
        List<RawMilvusMessage> tail = consumer.poll(Duration.ofMillis(200));
        if (!tail.isEmpty()) {
            all.addAll(tail);
        }
        return all;
    }

    /**
     * Deserialize raw messages and route them into the ordering engine (buffer DML,
     * updateWatermark for TimeTick) WITHOUT flushing. The caller flushes.
     */
    private void routeOnly(List<RawMilvusMessage> raws, MilvusConnectorConfig cfg,
                           TimetickOrderingEngine engine)
            throws MilvusWireFormatMismatchException, MilvusBufferFullException {
        MilvusProtoDeserializer deser = deserializer(cfg);
        for (RawMilvusMessage raw : raws) {
            List<MilvusChangeEvent> events = deser.deserialize(raw);
            for (MilvusChangeEvent ev : events) {
                if (ev instanceof MilvusChangeEvent.TimeTick tt) {
                    String vc = (tt.getVchannel() != null && !tt.getVchannel().isEmpty())
                            ? tt.getVchannel()
                            : raw.getTopic();
                    engine.updateWatermark(vc, tt.getTso());
                }
                else {
                    engine.buffer(ev);
                }
            }
        }
    }

    /**
     * Drain + route + flush for a single pchannel (the common case).
     */
    private List<MilvusChangeEvent> consumeDeserializeAndFlush(MilvusConnectorConfig cfg, String pchannel,
                                                               TimetickOrderingEngine engine)
            throws Exception {
        KafkaMilvusMessageConsumer consumer = consumerAtEarliest(cfg, Set.of(pchannel));
        List<RawMilvusMessage> raws = drainUntilIdle(consumer, Duration.ofSeconds(15));
        routeOnly(raws, cfg, engine);
        return engine.flush();
    }

    private ChangeEventSource.ChangeEventSourceContext runningFor(int iterations) {
        ChangeEventSource.ChangeEventSourceContext ctx = mock(ChangeEventSource.ChangeEventSourceContext.class);
        Boolean[] values = new Boolean[iterations];
        Arrays.fill(values, true);
        when(ctx.isRunning()).thenReturn(true, values).thenReturn(false);
        return ctx;
    }

    // -- scenarios -----------------------------------------------------------

    @Test
    void shouldDeserializeInsertAndAdvanceWatermark() throws Exception {
        byte[] insertBytes = ProtoMessageFactory.insertSimpleRow("books", 100L, 42L, "hello");
        byte[] tickBytes = ProtoMessageFactory.timeTick(110L);

        TestHelper.publishProtoMessages(bootstrap, topic, List.of(insertBytes, tickBytes));

        MilvusConnectorConfig cfg = config(MilvusProtoDeserializer.FORMAT_PROTO_SINGLE, 2_000L, null);

        TimetickOrderingEngine engine = new TimetickOrderingEngine(cfg);
        MilvusMessageConsumer consumer = TestHelper.kafkaConsumer(cfg);
        openConsumers.add((KafkaMilvusMessageConsumer) consumer);
        MilvusStreamingChangeEventSource source = new MilvusStreamingChangeEventSource(
                cfg, consumer, deserializer(cfg), engine);

        MilvusPartition partition = MilvusPartition.create(TOPIC_PREFIX_LOCAL, topic);
        MilvusOffsetContext offsetContext = new MilvusOffsetContext(new MilvusSourceInfo(cfg));

        source.execute(runningFor(4), partition, offsetContext);

        assertThat(engine.getGlobalWatermark()).isEqualTo(110L);
        assertThat(engine.getBufferedEventCount()).isZero();
        assertThat(offsetContext.getMqOffset(topic)).isNotNull();
    }

    @Test
    void shouldDeserializeDeleteEvent() throws Exception {
        byte[] deleteBytes = ProtoMessageFactory.delete("books", 100L, 7L, 8L);
        byte[] tickBytes = ProtoMessageFactory.timeTick(110L);

        TestHelper.publishProtoMessages(bootstrap, topic, List.of(deleteBytes, tickBytes));

        MilvusConnectorConfig cfg = config(MilvusProtoDeserializer.FORMAT_PROTO_SINGLE, 2_000L, null);

        TimetickOrderingEngine engine = new TimetickOrderingEngine(cfg);
        List<MilvusChangeEvent> flushed = consumeDeserializeAndFlush(cfg, topic, engine);

        assertThat(flushed).hasSize(1);
        assertThat(flushed.get(0)).isInstanceOf(MilvusChangeEvent.Delete.class);
        MilvusChangeEvent.Delete del = (MilvusChangeEvent.Delete) flushed.get(0);
        assertThat(del.getCollectionName()).isEqualTo("books");
        assertThat(del.getTso()).isEqualTo(100L);
        assertThat(del.getPrimaryKeys()).isEqualTo(List.of(7L, 8L));
        assertThat(engine.getGlobalWatermark()).isEqualTo(110L);
        assertThat(engine.getBufferedEventCount()).isZero();
    }

    @Test
    void shouldHandleMultipleVchannels() throws Exception {
        // Two pchannels → two vchannels routed into one engine.
        // Watermark = min(timetick_v0, timetick_v1).
        String topic0 = topic;
        String topic1 = uniqueTopic();
        TestHelper.ensureTopic(bootstrap, topic1);

        // pchannel_0: insert@100 + timetick@110 (vchannel=topic0)
        TestHelper.publishProtoMessages(bootstrap, topic0, List.of(
                ProtoMessageFactory.insertSimpleRow("coll0", 100L, 1L, "a"),
                ProtoMessageFactory.timeTick(110L)));
        // pchannel_1: insert@90 + timetick@120 (vchannel=topic1)
        TestHelper.publishProtoMessages(bootstrap, topic1, List.of(
                ProtoMessageFactory.insertSimpleRow("coll1", 90L, 2L, "b"),
                ProtoMessageFactory.timeTick(120L)));

        MilvusConnectorConfig cfg = config(MilvusProtoDeserializer.FORMAT_PROTO_SINGLE, 2_000L, null);
        TimetickOrderingEngine engine = new TimetickOrderingEngine(cfg);

        // Pre-warm the engine with both vchannels at TSO 0 so that inserts
        // from vchannels whose timetick has not yet arrived are NOT dropped
        // as "late". The real connector does this from stored offsets.
        engine.preWarm(Map.of(topic0, 0L, topic1, 0L));
        assertThat(engine.computeWatermark()).isZero();

        // Drain and route each pchannel separately into the same engine.
        routeOnly(
                drainUntilIdle(consumerAtEarliest(cfg, Set.of(topic0)), Duration.ofSeconds(15)),
                cfg, engine);
        routeOnly(
                drainUntilIdle(consumerAtEarliest(cfg, Set.of(topic1)), Duration.ofSeconds(15)),
                cfg, engine);
        List<MilvusChangeEvent> flushed = engine.flush();

        // Watermark = min(110, 120) = 110; both inserts (TSO 90, 100) flush in
        // ascending TSO order.
        assertThat(engine.getGlobalWatermark()).isEqualTo(110L);
        assertThat(flushed).hasSize(2);
        assertThat(flushed.get(0).getTso()).isEqualTo(90L);
        assertThat(flushed.get(0).getCollectionName()).isEqualTo("coll1");
        assertThat(flushed.get(1).getTso()).isEqualTo(100L);
        assertThat(flushed.get(1).getCollectionName()).isEqualTo("coll0");
        assertThat(engine.getBufferedEventCount()).isZero();
    }

    @Test
    void shouldDetectStallAndForceFlush() throws Exception {
        byte[] insertBytes = ProtoMessageFactory.insertSimpleRow("books", 100L, 42L, "hello");
        TestHelper.publishProtoMessages(bootstrap, topic, List.of(insertBytes));

        MilvusConnectorConfig cfg = config(MilvusProtoDeserializer.FORMAT_PROTO_SINGLE, 2_000L, null);

        // Fake clock for deterministic stall detection.
        AtomicLong now = new AtomicLong(0L);
        Clock fake = now::get;
        TimetickOrderingEngine engine = new TimetickOrderingEngine(cfg, fake);

        KafkaMilvusMessageConsumer consumer = consumerAtEarliest(cfg, Set.of(topic));
        List<RawMilvusMessage> raws = drainUntilIdle(consumer, Duration.ofSeconds(15));
        routeOnly(raws, cfg, engine);

        // No timetick published → watermark=0; flush() returns empty; not stalled
        // because the fake clock hasn't advanced past the stall timeout.
        assertThat(engine.getBufferedEventCount()).isEqualTo(1);
        assertThat(engine.getGlobalWatermark()).isZero();
        assertThat(engine.isStalled()).isFalse();
        assertThat(engine.flush()).isEmpty();

        // Advance fake clock past stall timeout (2000 ms).
        now.addAndGet(2_500L);
        assertThat(engine.isStalled()).isTrue();

        List<MilvusChangeEvent> forceFlushed = engine.forceFlush();
        assertThat(forceFlushed).hasSize(1);
        assertThat(forceFlushed.get(0)).isInstanceOf(MilvusChangeEvent.Insert.class);
        assertThat(((MilvusChangeEvent.Insert) forceFlushed.get(0)).getData())
                .containsEntry("id", 42L)
                .containsEntry("title", "hello");
        assertThat(engine.getBufferedEventCount()).isZero();
        assertThat(engine.getForceFlushCount()).isEqualTo(1L);
    }

    @Test
    void shouldThrowBufferFullOnOverflow() throws Exception {
        MilvusConnectorConfig cfg = config(MilvusProtoDeserializer.FORMAT_PROTO_SINGLE, 2_000L, 2);

        byte[] e1 = ProtoMessageFactory.insertSimpleRow("c", 1L, 1L, "a");
        byte[] e2 = ProtoMessageFactory.insertSimpleRow("c", 2L, 2L, "b");
        byte[] e3 = ProtoMessageFactory.insertSimpleRow("c", 3L, 3L, "c");
        TestHelper.publishProtoMessages(bootstrap, topic, List.of(e1, e2, e3));

        KafkaMilvusMessageConsumer consumer = consumerAtEarliest(cfg, Set.of(topic));
        List<RawMilvusMessage> raws = drainUntilIdle(consumer, Duration.ofSeconds(15));
        MilvusProtoDeserializer deser = deserializer(cfg);
        List<MilvusChangeEvent> events = new ArrayList<>();
        for (RawMilvusMessage r : raws) {
            events.addAll(deser.deserialize(r));
        }
        assertThat(events).hasSize(3);

        TimetickOrderingEngine engine = new TimetickOrderingEngine(cfg);
        engine.buffer(events.get(0));
        engine.buffer(events.get(1));
        assertThatThrownBy(() -> engine.buffer(events.get(2)))
                .isInstanceOf(MilvusBufferFullException.class);
    }

    @Test
    void shouldDeserializeProtoSingleWireFormat() throws Exception {
        float[] vector = { 1.5f, 2.5f, 3.5f, 4.5f };
        byte[] insertBytes = ProtoMessageFactory.insertAllFieldTypes("books", 100L,
                42L, "hello", vector);
        byte[] tickBytes = ProtoMessageFactory.timeTick(110L);

        TestHelper.publishProtoMessages(bootstrap, topic, List.of(insertBytes, tickBytes));

        MilvusConnectorConfig cfg = config(MilvusProtoDeserializer.FORMAT_PROTO_SINGLE, 2_000L, null);
        TimetickOrderingEngine engine = new TimetickOrderingEngine(cfg);
        List<MilvusChangeEvent> flushed = consumeDeserializeAndFlush(cfg, topic, engine);

        assertThat(flushed).hasSize(1);
        assertThat(flushed.get(0)).isInstanceOf(MilvusChangeEvent.Insert.class);
        Map<String, Object> data = ((MilvusChangeEvent.Insert) flushed.get(0)).getData();

        assertThat(data).containsEntry("id", 42L);
        assertThat(data).containsEntry("count", 7);
        assertThat(data).containsEntry("title", "hello");
        assertThat(data).containsEntry("price", 1.5f);
        assertThat(data).containsEntry("score", 99.9d);
        assertThat(data).containsEntry("active", true);
        assertThat(data).containsEntry("meta", "{\"k\":1}");
        assertThat((float[]) data.get("embedding")).containsExactly(vector);
    }

    @Test
    void shouldDeserializeMsgPackBatchWireFormat() throws Exception {
        byte[] batch = ProtoMessageFactory.msgpackBatch(topic, topic, "books",
                100L, 101L, 110L, 42L, 7L);
        TestHelper.publishProtoMessages(bootstrap, topic, List.of(batch));

        MilvusConnectorConfig cfg = config(MilvusProtoDeserializer.FORMAT_MSGPACK_BATCH, 2_000L, null);
        TimetickOrderingEngine engine = new TimetickOrderingEngine(cfg);
        List<MilvusChangeEvent> flushed = consumeDeserializeAndFlush(cfg, topic, engine);

        assertThat(engine.getGlobalWatermark()).isEqualTo(110L);
        assertThat(flushed).hasSize(2);
        assertThat(flushed.get(0)).isInstanceOf(MilvusChangeEvent.Insert.class);
        assertThat(((MilvusChangeEvent.Insert) flushed.get(0)).getData()).containsEntry("id", 42L);
        assertThat(flushed.get(1)).isInstanceOf(MilvusChangeEvent.Delete.class);
        assertThat(((MilvusChangeEvent.Delete) flushed.get(1)).getPrimaryKeys()).isEqualTo(List.of(7L));
        assertThat(engine.getBufferedEventCount()).isZero();
    }
}
/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.util.Clock;

/**
 * Tests for {@link TimetickOrderingEngine}.
 */
class TimetickOrderingEngineTest {

    private static final String VC0 = "by-dev-rootcoord-dml_0_v0";
    private static final String VC1 = "by-dev-rootcoord-dml_0_v1";
    private static final String PCHANNEL = "by-dev-rootcoord-dml_0";

    private MilvusConnectorConfig config;
    private ControllableClock clock;

    @BeforeEach
    void setUp() {
        Configuration configuration = Configuration.from(Map.of(
                "milvus.uri", "http://localhost:19530",
                "topic.prefix", "milvus-test",
                "milvus.kafka.bootstrap.servers", "localhost:9092",
                "milvus.timetick.stall.timeout.ms", "5000",
                "milvus.buffer.max.events", "100",
                "milvus.buffer.max.bytes", "1048576"));
        config = new MilvusConnectorConfig(configuration);
        clock = new ControllableClock();
    }

    private TimetickOrderingEngine newEngine() {
        return new TimetickOrderingEngine(config, clock);
    }

    @Test
    @FixFor("debezium/dbz#2129")
    void shouldFlushInStrictTsoOrderAcrossVchannels() throws MilvusBufferFullException {
        TimetickOrderingEngine engine = newEngine();

        engine.buffer(insertEvent("coll", PCHANNEL, VC0, 100));
        engine.buffer(insertEvent("coll", PCHANNEL, VC1, 85));
        engine.buffer(insertEvent("coll", PCHANNEL, VC0, 92));
        engine.buffer(insertEvent("coll", PCHANNEL, VC1, 110));

        engine.updateWatermark(VC0, 105);
        engine.updateWatermark(VC1, 100);

        List<MilvusChangeEvent> flushed = engine.flush();

        assertThat(flushed).hasSize(3);
        List<Long> tsos = flushed.stream().map(MilvusChangeEvent::getTso).collect(Collectors.toList());
        assertThat(tsos).containsExactly(85L, 92L, 100L);

        assertThat(engine.getBufferedEventCount()).isEqualTo(1);
    }

    @Test
    @FixFor("debezium/dbz#2129")
    void shouldNotFlushBeforeMinWatermarkCrossesEventTso() throws MilvusBufferFullException {
        TimetickOrderingEngine engine = newEngine();

        engine.updateWatermark(VC0, 0);
        engine.updateWatermark(VC1, 0);

        engine.buffer(insertEvent("coll", PCHANNEL, VC0, 90));

        engine.updateWatermark(VC0, 100);
        engine.updateWatermark(VC1, 80);

        List<MilvusChangeEvent> flushed = engine.flush();
        assertThat(flushed).isEmpty();

        engine.updateWatermark(VC1, 95);

        flushed = engine.flush();
        assertThat(flushed).hasSize(1);
        assertThat(flushed.get(0).getTso()).isEqualTo(90L);
    }

    @Test
    @FixFor("debezium/dbz#2129")
    void shouldDetectStallAfterTimeout() throws MilvusBufferFullException {
        TimetickOrderingEngine engine = newEngine();

        engine.buffer(insertEvent("coll", PCHANNEL, VC0, 100));
        engine.updateWatermark(VC0, 50);

        assertThat(engine.isStalled()).isFalse();

        clock.advanceMs(6000);

        assertThat(engine.isStalled()).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2129")
    void shouldForceFlushOnStall() throws MilvusBufferFullException {
        TimetickOrderingEngine engine = newEngine();

        engine.buffer(insertEvent("coll", PCHANNEL, VC0, 100));
        engine.buffer(insertEvent("coll", PCHANNEL, VC0, 150));
        engine.updateWatermark(VC0, 50);

        clock.advanceMs(6000);
        assertThat(engine.isStalled()).isTrue();

        List<MilvusChangeEvent> flushed = engine.forceFlush();

        assertThat(flushed).hasSize(2);
        assertThat(flushed.get(0).getTso()).isEqualTo(100L);
        assertThat(flushed.get(1).getTso()).isEqualTo(150L);
        assertThat(engine.getBufferedEventCount()).isZero();
        assertThat(engine.getForceFlushCount()).isEqualTo(1);

        assertThat(engine.isStalled()).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2129")
    void shouldPreWarmAndFlushImmediately() throws MilvusBufferFullException {
        TimetickOrderingEngine engine = newEngine();

        engine.preWarm(Map.of(VC0, 200L, VC1, 180L));

        assertThat(engine.getGlobalWatermark()).isEqualTo(180L);

        engine.buffer(insertEvent("coll", PCHANNEL, VC0, 170));
        assertThat(engine.getBufferedEventCount()).isZero();
        assertThat(engine.getLateMessagesDropped()).isEqualTo(1);

        engine.buffer(insertEvent("coll", PCHANNEL, VC0, 190));
        assertThat(engine.getBufferedEventCount()).isEqualTo(1);

        engine.updateWatermark(VC0, 250);
        engine.updateWatermark(VC1, 200);

        List<MilvusChangeEvent> flushed = engine.flush();
        assertThat(flushed).hasSize(1);
        assertThat(flushed.get(0).getTso()).isEqualTo(190L);
    }

    @Test
    @FixFor("debezium/dbz#2129")
    void shouldThrowWhenEventCountLimitExceeded() throws MilvusBufferFullException {
        TimetickOrderingEngine engine = newEngine();

        for (int i = 0; i < 100; i++) {
            engine.buffer(insertEvent("coll", PCHANNEL, VC0, 1000 + i));
        }

        assertThatThrownBy(() -> engine.buffer(insertEvent("coll", PCHANNEL, VC0, 1100)))
                .isInstanceOf(MilvusBufferFullException.class)
                .hasMessageContaining("100")
                .hasMessageContaining("events");
    }

    @Test
    @FixFor("debezium/dbz#2129")
    void shouldThrowWhenByteLimitExceeded() throws MilvusBufferFullException {
        Configuration largeDataConfig = Configuration.from(Map.of(
                "milvus.uri", "http://localhost:19530",
                "topic.prefix", "milvus-test",
                "milvus.kafka.bootstrap.servers", "localhost:9092",
                "milvus.timetick.stall.timeout.ms", "5000",
                "milvus.buffer.max.events", "100000",
                "milvus.buffer.max.bytes", "2048"));

        MilvusConnectorConfig tinyConfig = new MilvusConnectorConfig(largeDataConfig);
        TimetickOrderingEngine engine = new TimetickOrderingEngine(tinyConfig, clock);

        byte[] largeVector = new byte[1024];
        MilvusChangeEvent event = new MilvusChangeEvent.Insert(
                "coll", PCHANNEL, VC0, 100,
                Map.of("id", 1L, "vector", largeVector));

        engine.buffer(event);

        MilvusChangeEvent event2 = new MilvusChangeEvent.Insert(
                "coll", PCHANNEL, VC0, 101,
                Map.of("id", 2L, "vector", new byte[1024]));

        assertThatThrownBy(() -> engine.buffer(event2))
                .isInstanceOf(MilvusBufferFullException.class)
                .hasMessageContaining("bytes");
    }

    @Test
    @FixFor("debezium/dbz#2129")
    void shouldForceFlushAllBufferedEvents() throws MilvusBufferFullException {
        TimetickOrderingEngine engine = newEngine();

        engine.buffer(insertEvent("coll", PCHANNEL, VC0, 50));
        engine.buffer(insertEvent("coll", PCHANNEL, VC0, 30));
        engine.buffer(insertEvent("coll", PCHANNEL, VC1, 40));

        List<MilvusChangeEvent> flushed = engine.forceFlush();

        assertThat(flushed).hasSize(3);
        List<Long> tsos = flushed.stream().map(MilvusChangeEvent::getTso).collect(Collectors.toList());
        assertThat(tsos).containsExactly(30L, 40L, 50L);

        assertThat(engine.getBufferedEventCount()).isZero();
        assertThat(engine.getBufferedBytes()).isZero();
        assertThat(engine.getGlobalWatermark()).isEqualTo(50L);
    }

    @Test
    @FixFor("debezium/dbz#2129")
    void shouldHandleMultipleEventsAtSameTso() throws MilvusBufferFullException {
        TimetickOrderingEngine engine = newEngine();

        engine.buffer(insertEvent("coll", PCHANNEL, VC0, 100));
        engine.buffer(insertEvent("coll", PCHANNEL, VC0, 100));
        engine.buffer(insertEvent("coll", PCHANNEL, VC1, 100));

        engine.updateWatermark(VC0, 100);
        engine.updateWatermark(VC1, 100);

        List<MilvusChangeEvent> flushed = engine.flush();
        assertThat(flushed).hasSize(3);
        assertThat(flushed).allMatch(e -> e.getTso() == 100L);
    }

    @Test
    @FixFor("debezium/dbz#2129")
    void shouldWorkWithSingleVchannel() throws MilvusBufferFullException {
        TimetickOrderingEngine engine = newEngine();

        engine.buffer(insertEvent("coll", PCHANNEL, VC0, 50));
        engine.buffer(insertEvent("coll", PCHANNEL, VC0, 30));
        engine.buffer(insertEvent("coll", PCHANNEL, VC0, 70));

        engine.updateWatermark(VC0, 60);

        List<MilvusChangeEvent> flushed = engine.flush();
        assertThat(flushed).hasSize(2);
        List<Long> tsos = flushed.stream().map(MilvusChangeEvent::getTso).collect(Collectors.toList());
        assertThat(tsos).containsExactly(30L, 50L);
    }

    @Test
    @FixFor("debezium/dbz#2129")
    void shouldDropLateEventsAfterWatermarkAdvance() throws MilvusBufferFullException {
        TimetickOrderingEngine engine = newEngine();

        engine.updateWatermark(VC0, 100);

        engine.buffer(insertEvent("coll", PCHANNEL, VC0, 80));

        assertThat(engine.getBufferedEventCount()).isZero();
        assertThat(engine.getLateMessagesDropped()).isEqualTo(1);
    }

    @Test
    @FixFor("debezium/dbz#2129")
    void shouldDropLateEventsAtExactWatermark() throws MilvusBufferFullException {
        TimetickOrderingEngine engine = newEngine();

        engine.updateWatermark(VC0, 100);

        engine.buffer(insertEvent("coll", PCHANNEL, VC0, 100));

        assertThat(engine.getBufferedEventCount()).isZero();
        assertThat(engine.getLateMessagesDropped()).isEqualTo(1);
    }

    @Test
    @FixFor("debezium/dbz#2129")
    void shouldTrackStalledVchannels() throws MilvusBufferFullException {
        TimetickOrderingEngine engine = newEngine();

        engine.buffer(insertEvent("coll", PCHANNEL, VC0, 100));
        engine.updateWatermark(VC0, 200);
        engine.updateWatermark(VC1, 50);

        assertThat(engine.getStalledVchannels()).contains(VC1);
        assertThat(engine.getStalledVchannels()).doesNotContain(VC0);
    }

    @Test
    @FixFor("debezium/dbz#2129")
    void shouldPreWarmWithNullOrEmptyMap() {
        TimetickOrderingEngine engine = newEngine();

        engine.preWarm(null);
        assertThat(engine.getGlobalWatermark()).isZero();

        engine.preWarm(Map.of());
        assertThat(engine.getGlobalWatermark()).isZero();
    }

    @Test
    @FixFor("debezium/dbz#2129")
    void shouldReturnVchannelTimeticksCopy() throws MilvusBufferFullException {
        TimetickOrderingEngine engine = newEngine();

        engine.updateWatermark(VC0, 100);
        engine.updateWatermark(VC1, 200);

        Map<String, Long> timeticks = engine.getVchannelTimeticks();
        assertThat(timeticks).containsEntry(VC0, 100L);
        assertThat(timeticks).containsEntry(VC1, 200L);

        assertThatThrownBy(() -> timeticks.put("vc2", 300L))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    @FixFor("debezium/dbz#2129")
    void shouldNotRollBackWatermarkOnLowerTimetick() throws MilvusBufferFullException {
        TimetickOrderingEngine engine = newEngine();

        engine.updateWatermark(VC0, 200);
        engine.updateWatermark(VC0, 150);

        assertThat(engine.getVchannelTimeticks().get(VC0)).isEqualTo(200L);
    }

    @Test
    @FixFor("debezium/dbz#2129")
    void shouldResetStallTimerOnWatermarkAdvance() throws MilvusBufferFullException {
        TimetickOrderingEngine engine = newEngine();

        engine.buffer(insertEvent("coll", PCHANNEL, VC0, 100));
        engine.updateWatermark(VC0, 50);

        clock.advanceMs(4000);
        assertThat(engine.isStalled()).isFalse();

        engine.updateWatermark(VC0, 60);

        clock.advanceMs(4000);
        assertThat(engine.isStalled()).isFalse();

        clock.advanceMs(2000);
        assertThat(engine.isStalled()).isTrue();
    }

    private static MilvusChangeEvent.Insert insertEvent(String collection, String pchannel,
                                                        String vchannel, long tso) {
        return new MilvusChangeEvent.Insert(collection, pchannel, vchannel, tso,
                Map.of("id", tso, "name", "row-" + tso));
    }

    /**
     * A controllable clock for testing stall detection without real time waits.
     */
    private static class ControllableClock implements Clock {
        private final AtomicLong currentTimeMs = new AtomicLong(System.currentTimeMillis());

        @Override
        public long currentTimeInMillis() {
            return currentTimeMs.get();
        }

        @Override
        public long currentTimeInNanos() {
            return currentTimeMs.get() * 1_000_000L;
        }

        void advanceMs(long ms) {
            currentTimeMs.addAndGet(ms);
        }
    }
}

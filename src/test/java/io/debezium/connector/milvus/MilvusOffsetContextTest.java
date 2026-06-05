/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;

class MilvusOffsetContextTest {

    private static final Map<String, String> BASE_CONFIG = Map.of(
            "milvus.uri", "http://localhost:19530",
            "topic.prefix", "milvus-test");

    private MilvusSourceInfo newSourceInfo() {
        CommonConnectorConfig config = new MilvusConnectorConfig(
                io.debezium.config.Configuration.from(BASE_CONFIG));
        return new MilvusSourceInfo(config);
    }

    @Test
    void shouldStartWithEmptyOffset() {
        MilvusOffsetContext context = new MilvusOffsetContext(newSourceInfo());

        assertThat(context.isInitialSnapshotRunning()).isTrue();
        assertThat(context.isSnapshotCompleted()).isFalse();
    }

    @Test
    void shouldLoadFromStoredOffset() {
        Map<String, String> stored = new HashMap<>();
        stored.put("mq_topic", "by-dev-rootcoord-dml_0");
        stored.put("mq_partition", "0");
        stored.put("mq_offset", "12345");
        stored.put("vchannel_timeticks", "{\"by-dev-rootcoord-dml_0_v0\":100}");
        stored.put("snapshot_completed", "true");

        MilvusOffsetContext context = new MilvusOffsetContext(newSourceInfo(), true, stored);

        assertThat(context.getOffset().get("mq_topic")).isEqualTo("by-dev-rootcoord-dml_0");
        assertThat(context.isSnapshotCompleted()).isTrue();
        assertThat(context.isInitialSnapshotRunning()).isFalse();
    }

    @Test
    void shouldSetMqPosition() {
        MilvusOffsetContext context = new MilvusOffsetContext(newSourceInfo());
        context.setMqPosition("by-dev-rootcoord-dml_0", 0, 12345);

        Map<String, ?> offset = context.getOffset();
        assertThat(offset.get("mq_topic")).isEqualTo("by-dev-rootcoord-dml_0");
        assertThat(offset.get("mq_partition")).isEqualTo("0");
        assertThat(offset.get("mq_offset")).isEqualTo("12345");
    }

    @Test
    void shouldTrackSnapshotState() {
        MilvusOffsetContext context = new MilvusOffsetContext(newSourceInfo());

        context.preSnapshotStart(false);
        assertThat(context.isInitialSnapshotRunning()).isTrue();

        context.preSnapshotCompletion();
        context.postSnapshotCompletion();
        assertThat(context.isSnapshotCompleted()).isTrue();
        assertThat(context.isInitialSnapshotRunning()).isFalse();
        assertThat(context.getOffset().get("snapshot_completed")).isEqualTo("true");
    }

    @Test
    void shouldRoundtripThroughLoader() {
        MilvusOffsetContext original = new MilvusOffsetContext(newSourceInfo());
        original.setMqPosition("topic", 0, 999);
        original.preSnapshotCompletion();
        original.postSnapshotCompletion();

        MilvusOffsetContext.Loader loader = new MilvusOffsetContext.Loader(newSourceInfo());
        MilvusOffsetContext loaded = loader.load(original.getOffset());

        assertThat(loaded.getOffset().get("snapshot_completed")).isEqualTo("true");
        assertThat(loaded.isSnapshotCompleted()).isTrue();
    }

    @Test
    void shouldHandleEmptyOffsetViaLoader() {
        MilvusOffsetContext.Loader loader = new MilvusOffsetContext.Loader(newSourceInfo());
        MilvusOffsetContext loaded = loader.load(null);

        assertThat(loaded.isInitialSnapshotRunning()).isTrue();
    }
}

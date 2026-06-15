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
import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;

public class MilvusOffsetContextTest {

    private static final Map<String, String> BASE_CONFIG = Map.of(
            "milvus.uri", "http://localhost:19530",
            "topic.prefix", "milvus-test");

    private MilvusSourceInfo newSourceInfo() {
        CommonConnectorConfig config = new MilvusConnectorConfig(
                Configuration.from(BASE_CONFIG));
        return new MilvusSourceInfo(config);
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldStartWithEmptyOffset() {
        MilvusOffsetContext context = new MilvusOffsetContext(newSourceInfo());

        assertThat(context.isInitialSnapshotRunning()).isTrue();
        assertThat(context.isSnapshotCompleted()).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldLoadFromStoredOffset() {
        Map<String, Object> stored = new HashMap<>();
        stored.put("mq_offset_by-dev-rootcoord-dml_0", 12345L);
        stored.put("vchannel_timetick_by-dev-rootcoord-dml_0_v0", 100L);
        stored.put("snapshot_completed", "true");

        MilvusOffsetContext context = new MilvusOffsetContext(newSourceInfo(), true, stored);

        assertThat(context.getMqOffset("by-dev-rootcoord-dml_0")).isEqualTo(12345L);
        assertThat(context.getVchannelTimetick("by-dev-rootcoord-dml_0_v0")).isEqualTo(100L);
        assertThat(context.isSnapshotCompleted()).isTrue();
        assertThat(context.isInitialSnapshotRunning()).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldSetMqPosition() {
        MilvusOffsetContext context = new MilvusOffsetContext(newSourceInfo());
        context.setMqPosition("by-dev-rootcoord-dml_0", 0, 12345);

        Map<String, ?> offset = context.getOffset();
        assertThat(offset.get("mq_offset_by-dev-rootcoord-dml_0")).isEqualTo(12345L);
        assertThat(context.getMqOffset("by-dev-rootcoord-dml_0")).isEqualTo(12345L);
    }

    @Test
    @FixFor("debezium/dbz#2068")
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
    @FixFor("debezium/dbz#2068")
    void shouldRoundtripThroughLoader() {
        MilvusOffsetContext original = new MilvusOffsetContext(newSourceInfo());
        original.setMqPosition("topic", 0, 999);
        original.preSnapshotCompletion();
        original.postSnapshotCompletion();

        MilvusOffsetContext.Loader loader = new MilvusOffsetContext.Loader(newSourceInfo());
        MilvusOffsetContext loaded = loader.load(original.getOffset());

        assertThat(loaded.getOffset().get("snapshot_completed")).isEqualTo("true");
        assertThat(loaded.isSnapshotCompleted()).isTrue();
        assertThat(loaded.getMqOffset("topic")).isEqualTo(999L);
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldHandleEmptyOffsetViaLoader() {
        MilvusOffsetContext.Loader loader = new MilvusOffsetContext.Loader(newSourceInfo());
        MilvusOffsetContext loaded = loader.load(null);

        assertThat(loaded.isInitialSnapshotRunning()).isTrue();
    }
}

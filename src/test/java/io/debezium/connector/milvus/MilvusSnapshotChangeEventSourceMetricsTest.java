/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.doc.FixFor;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

public class MilvusSnapshotChangeEventSourceMetricsTest {

    private static final String PCHANNEL = "by-dev-rootcoord-dml_0";
    private static final long GUARANTEE_TS = 440000000000L;

    private MilvusSnapshotChangeEventSourceMetrics metrics;
    private MilvusPartition partition;

    @BeforeEach
    void setUp() {
        Configuration configuration = Configuration.from(Map.of(
                "milvus.uri", "http://localhost:19530",
                "topic.prefix", "milvus-test",
                "milvus.kafka.bootstrap.servers", "localhost:9092"));
        MilvusConnectorConfig connectorConfig = new MilvusConnectorConfig(configuration);
        CdcSourceTaskContext<MilvusConnectorConfig> taskContext = new CdcSourceTaskContext<>(
                configuration, connectorConfig, Collections.emptyMap());
        ChangeEventQueueMetrics queueMetrics = mock(ChangeEventQueueMetrics.class);
        EventMetadataProvider metadataProvider = mock(EventMetadataProvider.class);

        metrics = new MilvusSnapshotChangeEventSourceMetrics(taskContext, queueMetrics, metadataProvider);
        partition = MilvusPartition.create("milvus-test", PCHANNEL);
    }

    @Test
    @FixFor("debezium/dbz#2282")
    void shouldExposeZeroGuaranteeTsoBeforeAnyUpdate() {
        assertThat(metrics.getGuaranteeTso()).isZero();
    }

    @Test
    @FixFor("debezium/dbz#2282")
    void shouldExposeGuaranteeTsoAfterUpdate() {
        metrics.setGuaranteeTso(GUARANTEE_TS);

        assertThat(metrics.getGuaranteeTso()).isEqualTo(GUARANTEE_TS);
    }

    @Test
    @FixFor("debezium/dbz#2282")
    void shouldExposeZeroSnapshotStartTsBeforeSnapshotStarted() {
        assertThat(metrics.getSnapshotStartTs()).isZero();
    }

    @Test
    @FixFor("debezium/dbz#2282")
    void shouldRecordSnapshotStartTimeWhenSnapshotStarts() {
        long before = System.currentTimeMillis();

        metrics.snapshotStarted(partition);

        long after = System.currentTimeMillis();
        assertThat(metrics.getSnapshotStartTs()).isBetween(before, after);
    }

    @Test
    @FixFor("debezium/dbz#2282")
    void shouldClearGuaranteeTsoAndSnapshotStartTsOnReset() {
        metrics.setGuaranteeTso(GUARANTEE_TS);
        metrics.snapshotStarted(partition);

        metrics.reset();

        assertThat(metrics.getGuaranteeTso()).isZero();
        assertThat(metrics.getSnapshotStartTs()).isZero();
    }
}

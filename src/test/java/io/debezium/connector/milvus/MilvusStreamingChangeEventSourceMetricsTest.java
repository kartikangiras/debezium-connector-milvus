/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.doc.FixFor;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

public class MilvusStreamingChangeEventSourceMetricsTest {

    private static final String PCHANNEL = "by-dev-rootcoord-dml_0";

    private MilvusStreamingChangeEventSourceMetrics metrics;

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
        CapturedTablesSupplier capturedTablesSupplier = () -> List.of();

        metrics = new MilvusStreamingChangeEventSourceMetrics(
                taskContext, queueMetrics, metadataProvider, capturedTablesSupplier);
    }

    @Test
    @FixFor("debezium/dbz#2282")
    void shouldReturnZeroLagBeforeAnyWatermark() {
        assertThat(metrics.getMilliSecondsBehindSource()).isZero();
    }

    @Test
    @FixFor("debezium/dbz#2282")
    void shouldComputeLagFromGlobalWatermarkAfterUpdate() {
        long nowMs = System.currentTimeMillis();
        long fiveSecondsAgoMs = nowMs - 5000;
        long tso = fiveSecondsAgoMs << 18;

        metrics.updateGlobalWatermark(tso);

        long lag = metrics.getMilliSecondsBehindSource();
        assertThat(lag).isGreaterThanOrEqualTo(4900L);
        assertThat(lag).isLessThan(15000L);
    }

    @Test
    @FixFor("debezium/dbz#2282")
    void shouldReturnEmptySourceEventPositionBeforeAnyUpdate() {
        assertThat(metrics.getSourceEventPosition()).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#2282")
    void shouldExposeSourceEventPositionAfterUpdate() {
        metrics.updateSourceEventPosition(PCHANNEL, 12345L);

        assertThat(metrics.getSourceEventPosition())
                .containsEntry("pchannel", PCHANNEL)
                .containsEntry("watermark", "12345");
    }

    @Test
    @FixFor("debezium/dbz#2282")
    void shouldOverwritePreviousSourceEventPositionOnSubsequentUpdate() {
        metrics.updateSourceEventPosition(PCHANNEL, 100L);
        metrics.updateSourceEventPosition(PCHANNEL, 200L);

        assertThat(metrics.getSourceEventPosition())
                .containsEntry("pchannel", PCHANNEL)
                .containsEntry("watermark", "200");
    }

    @Test
    @FixFor("debezium/dbz#2282")
    void shouldClearWatermarkAndSourceEventPositionOnReset() {
        metrics.updateGlobalWatermark(123L << 18);
        metrics.updateSourceEventPosition(PCHANNEL, 999L);
        metrics.positionResolved(true);

        metrics.reset();

        assertThat(metrics.getMilliSecondsBehindSource()).isZero();
        assertThat(metrics.getSourceEventPosition()).isEmpty();
        assertThat(metrics.isPositionResolved()).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2282")
    void shouldNotBePositionResolvedBeforeAnyUpdate() {
        assertThat(metrics.isPositionResolved()).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2282")
    void shouldBePositionResolvedAfterMarkedResolved() {
        metrics.positionResolved(true);

        assertThat(metrics.isPositionResolved()).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2282")
    void shouldNotBePositionResolvedAfterMarkedUnresolvedAgain() {
        metrics.positionResolved(true);
        metrics.positionResolved(false);

        assertThat(metrics.isPositionResolved()).isFalse();
    }
}

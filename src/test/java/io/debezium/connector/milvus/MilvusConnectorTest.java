/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import java.util.Map;

import org.junit.jupiter.api.Test;

class MilvusConnectorTest {

    private Map<String, String> baseConfig() {
        return Map.of(
                "milvus.uri", "http://localhost:19530",
                "topic.prefix", "milvus-test");
    }

    @Test
    void shouldExposeConnectorVersion() {
        MilvusConnector connector = new MilvusConnector();
        assertThat(connector.version()).isNotNull();
        assertThat(connector.version()).isNotBlank();
    }

    @Test
    void shouldReturnCorrectTaskClass() {
        MilvusConnector connector = new MilvusConnector();
        assertThat(connector.taskClass()).isEqualTo(MilvusConnectorTask.class);
    }

    @Test
    void shouldExposeConfigDef() {
        MilvusConnector connector = new MilvusConnector();
        org.apache.kafka.common.config.ConfigDef configDef = connector.config();
        assertThat(configDef).isNotNull();
        assertThat(configDef.names()).contains("milvus.uri");
    }

    @Test
    void shouldStartAndStopCleanly() {
        MilvusConnector connector = new MilvusConnector();

        // Start — config validation only (no network)
        assertThatNoException().isThrownBy(() -> connector.start(baseConfig()));

        // Task configs should pass through the original config
        java.util.List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertThat(taskConfigs).hasSize(1);
        assertThat(taskConfigs.get(0)).containsEntry("milvus.uri", "http://localhost:19530");

        // Stop
        assertThatNoException().isThrownBy(connector::stop);
    }

    @Test
    void shouldReturnSingleTaskConfigEvenWithMaxTasksGreaterThanOne() {
        MilvusConnector connector = new MilvusConnector();
        connector.start(baseConfig());

        java.util.List<Map<String, String>> taskConfigs = connector.taskConfigs(5);
        // Milvus connector supports only a single task
        assertThat(taskConfigs).hasSize(1);
        connector.stop();
    }

    @Test
    void shouldStopCleanlyWhenNotStarted() {
        MilvusConnector connector = new MilvusConnector();
        // Stopping before start should not throw
        assertThatNoException().isThrownBy(connector::stop);
    }
}

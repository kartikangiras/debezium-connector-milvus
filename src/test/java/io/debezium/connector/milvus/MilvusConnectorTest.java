/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

public class MilvusConnectorTest {

    private Map<String, String> baseConfig() {
        return Map.of(
                "milvus.uri", "http://localhost:19530",
                "topic.prefix", "milvus-test");
    }

    @Test
    @FixFor("debezium/dbz#2028")
    void shouldExposeConnectorVersion() {
        MilvusConnector connector = new MilvusConnector();
        assertThat(connector.version()).isNotNull();
        assertThat(connector.version()).isNotBlank();
    }

    @Test
    @FixFor("debezium/dbz#2028")
    void shouldReturnCorrectTaskClass() {
        MilvusConnector connector = new MilvusConnector();
        assertThat(connector.taskClass()).isEqualTo(MilvusConnectorTask.class);
    }

    @Test
    @FixFor("debezium/dbz#2028")
    void shouldExposeConfigDef() {
        MilvusConnector connector = new MilvusConnector();
        ConfigDef configDef = connector.config();
        assertThat(configDef).isNotNull();
        assertThat(configDef.names()).contains("milvus.uri");
    }

    @Test
    @FixFor("debezium/dbz#2028")
    void shouldStartAndStopCleanly() {
        MilvusConnector connector = new MilvusConnector();

        // Start — config validation only (no network)
        assertThatNoException().isThrownBy(() -> connector.start(baseConfig()));

        // Task configs should pass through the original config
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertThat(taskConfigs).hasSize(1);
        assertThat(taskConfigs.get(0)).containsEntry("milvus.uri", "http://localhost:19530");

        // Stop
        assertThatNoException().isThrownBy(connector::stop);
    }

    @Test
    @FixFor("debezium/dbz#2028")
    void shouldReturnSingleTaskConfigEvenWithMaxTasksGreaterThanOne() {
        MilvusConnector connector = new MilvusConnector();
        connector.start(baseConfig());

        List<Map<String, String>> taskConfigs = connector.taskConfigs(5);
        // Milvus connector supports only a single task
        assertThat(taskConfigs).hasSize(1);
        connector.stop();
    }

    @Test
    @FixFor("debezium/dbz#2028")
    void shouldStopCleanlyWhenNotStarted() {
        MilvusConnector connector = new MilvusConnector();
        // Stopping before start should not throw
        assertThatNoException().isThrownBy(connector::stop);
    }
}

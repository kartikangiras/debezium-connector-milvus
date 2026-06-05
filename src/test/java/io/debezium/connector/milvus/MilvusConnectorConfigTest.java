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

import io.debezium.config.Configuration;

class MilvusConnectorConfigTest {

    private static final String TEST_URI = "http://localhost:19530";
    private static final String TEST_TOPIC_PREFIX = "milvus-test";

    private Map<String, String> baseConfig() {
        return Map.of(
                "milvus.uri", TEST_URI,
                "topic.prefix", TEST_TOPIC_PREFIX);
    }

    @Test
    void shouldLoadRequiredConfig() {
        Configuration config = Configuration.from(baseConfig());
        MilvusConnectorConfig connectorConfig = new MilvusConnectorConfig(config);

        assertThat(connectorConfig.getMilvusUri()).isEqualTo(TEST_URI);
        assertThat(connectorConfig.getLogicalName()).isEqualTo(TEST_TOPIC_PREFIX);
    }

    @Test
    void shouldApplyDefaults() {
        Configuration config = Configuration.from(baseConfig());
        MilvusConnectorConfig connectorConfig = new MilvusConnectorConfig(config);

        assertThat(connectorConfig.getMilvusDatabase()).isEqualTo("default");
        assertThat(connectorConfig.getMetadataTimeoutMs()).isEqualTo(5000L);
        assertThat(connectorConfig.isStartupValidationEnabled()).isTrue();
        assertThat(connectorConfig.getEtcdRootPath()).isEqualTo("by-dev");
        assertThat(connectorConfig.getSnapshotMode().getValue()).isEqualTo("initial");
        assertThat(connectorConfig.getWireFormat()).isEqualTo("auto");
        assertThat(connectorConfig.getTimetickStallTimeoutMs()).isEqualTo(30000L);
        assertThat(connectorConfig.getUpsertMode()).isEqualTo("passthrough");
    }

    @Test
    void shouldParseOverrideConfig() {
        Map<String, String> props = new java.util.HashMap<>(baseConfig());
        props.put("milvus.database", "testdb");
        props.put("milvus.collection.include.list", "articles,products");
        props.put("milvus.metadata.timeout.ms", "10000");
        props.put("milvus.startup.validation.enabled", "false");
        props.put("milvus.etcd.root.path", "custom-root");
        props.put("snapshot.mode", "never");
        props.put("milvus.kafka.bootstrap.servers", "kafka:9092");
        props.put("milvus.wire.format", "msgpack_batch");
        props.put("milvus.timetick.stall.timeout.ms", "60000");
        props.put("milvus.upsert.mode", "correlate");

        Configuration config = Configuration.from(props);
        MilvusConnectorConfig connectorConfig = new MilvusConnectorConfig(config);

        assertThat(connectorConfig.getMilvusDatabase()).isEqualTo("testdb");
        assertThat(connectorConfig.getCollectionIncludeList()).containsExactly("articles", "products");
        assertThat(connectorConfig.getMetadataTimeoutMs()).isEqualTo(10000L);
        assertThat(connectorConfig.isStartupValidationEnabled()).isFalse();
        assertThat(connectorConfig.getEtcdRootPath()).isEqualTo("custom-root");
        assertThat(connectorConfig.getSnapshotMode().getValue()).isEqualTo("never");
        assertThat(connectorConfig.getKafkaBootstrapServers()).isEqualTo("kafka:9092");
        assertThat(connectorConfig.getWireFormat()).isEqualTo("msgpack_batch");
        assertThat(connectorConfig.getTimetickStallTimeoutMs()).isEqualTo(60000L);
        assertThat(connectorConfig.getUpsertMode()).isEqualTo("correlate");
    }

    @Test
    void shouldExposeConfigDef() {
        assertThatNoException().isThrownBy(() -> {
            org.apache.kafka.common.config.ConfigDef configDef = MilvusConnectorConfig.configDef();
            assertThat(configDef).isNotNull();
            assertThat(configDef.names()).contains("milvus.uri", "topic.prefix", "milvus.database");
        });
    }
}

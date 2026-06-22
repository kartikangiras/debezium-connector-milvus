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

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;

public class MilvusConnectorTaskTest {

    private Map<String, String> baseConfig() {
        return Map.of(
                "milvus.uri", "http://localhost:19530",
                "topic.prefix", "milvus-test");
    }

    @Test
    @FixFor("debezium/dbz#2028")
    void shouldExposeVersion() {
        MilvusConnectorTask task = new MilvusConnectorTask();
        assertThat(task.version()).isEqualTo("3.6.0-SNAPSHOT");
    }

    @Test
    @FixFor("debezium/dbz#2028")
    void shouldStartAndStopCleanly() {
        MilvusConnectorTask task = new MilvusConnectorTask();
        Configuration config = Configuration.from(baseConfig());

        task.preStart(config);
        assertThatNoException().isThrownBy(() -> task.start(config));
        assertThatNoException().isThrownBy(task::doStop);
    }

    @Test
    @FixFor("debezium/dbz#2028")
    void shouldReturnEmptyListWhenPolling() throws InterruptedException {
        MilvusConnectorTask task = new MilvusConnectorTask();
        Configuration config = Configuration.from(baseConfig());

        task.preStart(config);
        task.start(config);
        List<SourceRecord> records = task.doPoll();

        assertThat(records).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#2028")
    void shouldReturnEmptyListWhenPollingAfterStop() throws InterruptedException {
        MilvusConnectorTask task = new MilvusConnectorTask();
        Configuration config = Configuration.from(baseConfig());

        task.preStart(config);
        task.start(config);
        task.doStop();
        List<SourceRecord> records = task.doPoll();

        assertThat(records).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#2028")
    void shouldExposeConfigurationFields() {
        MilvusConnectorTask task = new MilvusConnectorTask();
        assertThat(task.getAllConfigurationFields()).isSameAs(MilvusConnectorConfig.ALL_FIELDS);
    }
}

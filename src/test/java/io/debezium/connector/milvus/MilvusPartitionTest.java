/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;

public class MilvusPartitionTest {

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldCreatePartitionWithCorrectIdentity() {
        MilvusPartition partition = MilvusPartition.create("test-server", "by-dev-rootcoord-dml_0");

        assertThat(partition.getLogicalName()).isEqualTo("test-server");
        assertThat(partition.getPchannel()).isEqualTo("by-dev-rootcoord-dml_0");
        assertThat(partition.getSourcePartition())
                .containsEntry("logicalName", "test-server")
                .containsEntry("pchannel", "by-dev-rootcoord-dml_0");
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldImplementEqualityBasedOnLogicalNameAndPchannel() {
        MilvusPartition p1 = MilvusPartition.create("server", "channel-a");
        MilvusPartition p2 = MilvusPartition.create("server", "channel-a");
        MilvusPartition p3 = MilvusPartition.create("server", "channel-b");
        MilvusPartition p4 = MilvusPartition.create("other", "channel-a");

        assertThat(p1).isEqualTo(p2);
        assertThat(p1.hashCode()).isEqualTo(p2.hashCode());
        assertThat(p1).isNotEqualTo(p3);
        assertThat(p1).isNotEqualTo(p4);
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldProvideDeterministicOrdering() {
        MilvusPartition.Provider provider = new MilvusPartition.Provider(
                createTestConfig(),
                List.of("channel-c", "channel-a", "channel-b"));

        List<MilvusPartition> partitions = provider.getPartitionList();
        assertThat(partitions).hasSize(3);
        assertThat(partitions.get(0).getPchannel()).isEqualTo("channel-a");
        assertThat(partitions.get(1).getPchannel()).isEqualTo("channel-b");
        assertThat(partitions.get(2).getPchannel()).isEqualTo("channel-c");
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldDeduplicatePchannels() {
        MilvusPartition.Provider provider = new MilvusPartition.Provider(
                createTestConfig(),
                List.of("channel-a", "channel-a", "channel-b"));

        assertThat(provider.getPartitions()).hasSize(2);
    }

    private MilvusConnectorConfig createTestConfig() {
        Map<String, String> props = Map.of(
                "milvus.uri", "http://localhost:19530",
                "topic.prefix", "test");
        return new MilvusConnectorConfig(Configuration.from(props));
    }
}

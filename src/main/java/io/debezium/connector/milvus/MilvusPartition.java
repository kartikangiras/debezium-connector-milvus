/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.AbstractPartition;
import io.debezium.util.Collect;

/**
 * Represents a single pchannel as a source partition.
 *
 * <p>One MilvusPartition maps to exactly one pchannel (physical Kafka topic).
 * Multiple vchannels mux onto a single pchannel and are demuxed by the
 * deserializer at message level.</p>
 */
public class MilvusPartition extends AbstractPartition {

    private final String pchannel;
    private final Map<String, String> sourcePartition;
    private final int hashCode;

    private MilvusPartition(String logicalName, String pchannel) {
        super(logicalName);
        this.pchannel = Objects.requireNonNull(pchannel);
        this.sourcePartition = Collect.hashMapOf("logicalName", logicalName, "pchannel", pchannel);
        this.hashCode = Objects.hash(logicalName, pchannel);
    }

    public static MilvusPartition create(String logicalName, String pchannel) {
        return new MilvusPartition(logicalName, pchannel);
    }

    public String getPchannel() {
        return pchannel;
    }

    public String getLogicalName() {
        return databaseName;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return sourcePartition;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MilvusPartition other = (MilvusPartition) obj;
        return databaseName.equals(other.databaseName) && pchannel.equals(other.pchannel);
    }

    @Override
    public String toString() {
        return "MilvusPartition{logicalName='" + databaseName + "', pchannel='" + pchannel + "'}";
    }

    public static class Provider implements Partition.Provider<MilvusPartition> {

        private final java.util.List<MilvusPartition> partitions;

        public Provider(MilvusConnectorConfig connectorConfig,
                        java.util.List<String> pchannelNames) {
            String logicalName = connectorConfig.getLogicalName();
            this.partitions = pchannelNames.stream()
                    .sorted()
                    .map(pchannel -> MilvusPartition.create(logicalName, pchannel))
                    .toList();
        }

        @Override
        public Set<MilvusPartition> getPartitions() {
            return Collections.unmodifiableSet(new java.util.LinkedHashSet<>(partitions));
        }

        public java.util.List<MilvusPartition> getPartitionList() {
            return Collections.unmodifiableList(partitions);
        }
    }
}

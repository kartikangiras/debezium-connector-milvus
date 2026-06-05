/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.SnapshotType;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Offset context for the Milvus connector.
 *
 * <p>Tracks the Kafka MQ position (topic, partition, offset) and per-vchannel
 * timetick watermarks alongside the standard snapshot state.</p>
 */
public class MilvusOffsetContext extends CommonOffsetContext<MilvusSourceInfo> {

    private static final String MQ_TOPIC_KEY = "mq_topic";
    private static final String MQ_PARTITION_KEY = "mq_partition";
    private static final String MQ_OFFSET_KEY = "mq_offset";
    private static final String VCHANNEL_TIMETICKS_KEY = "vchannel_timeticks";

    private final Map<String, Object> offset;
    private TransactionContext transactionContext;

    public MilvusOffsetContext(MilvusSourceInfo sourceInfo) {
        super(sourceInfo, false);
        setSnapshot(SnapshotType.INITIAL);
        this.offset = new HashMap<>();
        this.transactionContext = new TransactionContext();
    }

    public MilvusOffsetContext(MilvusSourceInfo sourceInfo, boolean snapshotCompleted) {
        super(sourceInfo, snapshotCompleted);
        if (!snapshotCompleted) {
            setSnapshot(SnapshotType.INITIAL);
        }
        this.offset = new HashMap<>();
        this.transactionContext = new TransactionContext();
    }

    public MilvusOffsetContext(MilvusSourceInfo sourceInfo, boolean snapshotCompleted,
                               Map<String, ?> storedOffset) {
        super(sourceInfo, snapshotCompleted);
        this.offset = new HashMap<>(storedOffset);
        this.transactionContext = TransactionContext.load(storedOffset);
    }

    public void setMqPosition(String topic, int partition, long offset) {
        this.offset.put(MQ_TOPIC_KEY, topic);
        this.offset.put(MQ_PARTITION_KEY, String.valueOf(partition));
        this.offset.put(MQ_OFFSET_KEY, String.valueOf(offset));
    }

    public void setVchannelTimeticks(Map<String, Long> timeticks) {
        this.offset.put(VCHANNEL_TIMETICKS_KEY, serializeTimeticks(timeticks));
    }

    public boolean isSnapshotCompleted() {
        return snapshotCompleted;
    }

    @Override
    public Map<String, ?> getOffset() {
        Map<String, Object> result = new HashMap<>(offset);
        result.put(SNAPSHOT_COMPLETED_KEY, String.valueOf(snapshotCompleted));
        return result;
    }

    @Override
    public Schema getSourceInfoSchema() {
        return new MilvusSourceInfoStructMaker().schema();
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    @Override
    public void event(DataCollectionId collectionId, Instant timestamp) {
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    private static String serializeTimeticks(Map<String, Long> timeticks) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Long> entry : timeticks.entrySet()) {
            if (!first) {
                sb.append(",");
            }
            sb.append("\"").append(entry.getKey()).append("\":")
                    .append(entry.getValue());
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }

    public static class Loader implements OffsetContext.Loader<MilvusOffsetContext> {

        private final MilvusSourceInfo sourceInfo;

        public Loader(MilvusSourceInfo sourceInfo) {
            this.sourceInfo = sourceInfo;
        }

        @Override
        public MilvusOffsetContext load(Map<String, ?> offset) {
            if (offset == null || offset.isEmpty()) {
                return new MilvusOffsetContext(sourceInfo);
            }
            Object completed = offset.get(SNAPSHOT_COMPLETED_KEY);
            boolean snapshotCompleted = completed != null && Boolean.parseBoolean(completed.toString());
            return new MilvusOffsetContext(sourceInfo, snapshotCompleted, offset);
        }
    }
}

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
import io.debezium.util.Strings;

/**
 * Offset context for the Milvus connector.
 *
 * <p>Tracks the Kafka MQ position per pchannel and per-vchannel timetick
 * watermarks as flat primitive key-value pairs (no JSON serialization),
 * following the same pattern as PostgresOffsetContext.</p>
 */
public class MilvusOffsetContext extends CommonOffsetContext<MilvusSourceInfo> {

    private static final String MQ_OFFSET_PREFIX = "mq_offset_";
    private static final String VCHANNEL_TIMETICK_PREFIX = "vchannel_timetick_";

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

    /**
     * Store the MQ offset for a pchannel, keyed by topic name.
     *
     * <p>Offsets are stored as flat primitive values so the Connect offset
     * storage never has to parse JSON. Each pchannel gets its own key
     * {@code mq_offset_<topic>}.</p>
     */
    public void setMqPosition(String topic, int partition, long offset) {
        this.offset.put(MQ_OFFSET_PREFIX + topic, offset);
    }

    /**
     * Retrieve the stored MQ offset for a given pchannel, or {@code null} if
     * none is present.
     */
    public Long getMqOffset(String pchannel) {
        Object value = this.offset.get(MQ_OFFSET_PREFIX + pchannel);
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(value.toString());
    }

    /**
     * Store per-vchannel timetick watermarks as flat primitive values.
     *
     * <p>Each vchannel gets its own key {@code vchannel_timetick_<vchannel>}.</p>
     */
    public void setVchannelTimeticks(Map<String, Long> timeticks) {
        for (Map.Entry<String, Long> entry : timeticks.entrySet()) {
            this.offset.put(VCHANNEL_TIMETICK_PREFIX + entry.getKey(), entry.getValue());
        }
    }

    /**
     * Retrieve the stored timetick for a given vchannel, or {@code null} if
     * none is present.
     */
    public Long getVchannelTimetick(String vchannel) {
        Object value = this.offset.get(VCHANNEL_TIMETICK_PREFIX + vchannel);
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(value.toString());
    }

    /**
     * Update the source info fields for a change event about to be dispatched.
     *
     * @param dbName         Milvus database name (populates {@code source.db})
     * @param collectionName Milvus collection name
     * @param pchannel       physical channel
     * @param vchannel       virtual channel
     * @param tso            timestamp oracle from the event
     */
    public void updateForEvent(String dbName, String collectionName, String pchannel,
                               String vchannel, long tso) {
        sourceInfo.setDatabaseName(dbName);
        sourceInfo.setCollectionName(collectionName);
        sourceInfo.setPchannel(pchannel);
        sourceInfo.setVchannel(vchannel);
        sourceInfo.setTso(tso);
        sourceInfo.setTimestamp(java.time.Instant.now());
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
            boolean snapshotCompleted = completed != null && Strings.asBoolean(completed.toString(), false);
            return new MilvusOffsetContext(sourceInfo, snapshotCompleted, offset);
        }
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus.checkpoint;

/**
 * Represents a channel checkpoint stored in etcd.
 *
 * <ul>
 * <li>{@code msgID} — the Kafka offset bytes for seek position</li>
 * <li>{@code timestamp} — the TSO used as {@code guarantee_ts} for snapshot
 * alignment</li>
 * </ul>
 */
public class ChannelCheckpoint {

    private final String pchannel;
    private final long msgId;
    private final long timestamp;

    public ChannelCheckpoint(String pchannel, long msgId, long timestamp) {
        this.pchannel = pchannel;
        this.msgId = msgId;
        this.timestamp = timestamp;
    }

    public String getPchannel() {
        return pchannel;
    }

    public long getMsgId() {
        return msgId;
    }

    /**
     * The TSO timestamp from the checkpoint, used as guarantee_ts
     * for snapshot/streaming handoff alignment.
     */
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "ChannelCheckpoint{pchannel='" + pchannel +
                "', msgId=" + msgId +
                ", timestamp=" + timestamp + "}";
    }
}
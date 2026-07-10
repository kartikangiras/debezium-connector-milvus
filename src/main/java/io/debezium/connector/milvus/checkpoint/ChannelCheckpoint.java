/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus.checkpoint;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import milvus.proto.msg.Msg.MsgPosition;

/**
 * Represents a channel checkpoint stored in etcd.
 *
 * <ul>
 * <li>{@code msgId} — the raw Kafka offset bytes for seek position (the
 * {@code msgID} field of {@link MsgPosition})</li>
 * <li>{@code timestamp} — the TSO used as {@code guarantee_ts} for snapshot
 * alignment</li>
 * </ul>
 */
public class ChannelCheckpoint {

    private final String pchannel;
    private final byte[] msgId;
    private final long timestamp;

    public ChannelCheckpoint(String pchannel, byte[] msgId, long timestamp) {
        this.pchannel = pchannel;
        this.msgId = msgId != null ? msgId.clone() : null;
        this.timestamp = timestamp;
    }

    public String getPchannel() {
        return pchannel;
    }

    /**
     * The raw {@code MsgPosition.msgID} bytes. For Kafka-backed Milvus clusters
     * these bytes encode the Kafka offset that should be used for streaming
     * resume.
     */
    public byte[] getMsgId() {
        return msgId != null ? msgId.clone() : null;
    }

    /**
     * The TSO timestamp from the checkpoint, used as guarantee_ts
     * for snapshot/streaming handoff alignment.
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Attempts to decode the {@link #getMsgId()} bytes into a Kafka offset.
     *
     * <p>Milvus stores the Kafka message id in {@code MsgPosition.msgID}. For
     * Kafka this is commonly an 8-byte little-endian long. If the byte array
     * does not match a known encoding, a {@link NumberFormatException} is
     * thrown.</p>
     *
     * @return the decoded Kafka offset
     * @throws NumberFormatException if the msgId bytes cannot be decoded
     */
    public long getKafkaOffset() {
        if (msgId == null || msgId.length == 0) {
            throw new NumberFormatException("msgId is null or empty");
        }
        if (msgId.length == 8) {
            return ByteBuffer.wrap(msgId).order(ByteOrder.LITTLE_ENDIAN).getLong();
        }
        // Some Milvus/Kafka deployments encode the offset as a UTF-8 decimal string.
        try {
            return Long.parseLong(new String(msgId, java.nio.charset.StandardCharsets.UTF_8));
        }
        catch (NumberFormatException e) {
            throw new NumberFormatException("Unable to decode msgId of length " + msgId.length + " as Kafka offset");
        }
    }

    /**
     * Creates a checkpoint from a Milvus {@link MsgPosition} proto.
     *
     * @param pchannel the physical channel name
     * @param position the position read from etcd
     * @return a new checkpoint
     */
    public static ChannelCheckpoint fromMsgPosition(String pchannel, MsgPosition position) {
        return new ChannelCheckpoint(
                pchannel,
                position.getMsgID().toByteArray(),
                position.getTimestamp());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ChannelCheckpoint)) {
            return false;
        }
        ChannelCheckpoint that = (ChannelCheckpoint) o;
        return timestamp == that.timestamp
                && java.util.Objects.equals(pchannel, that.pchannel)
                && Arrays.equals(msgId, that.msgId);
    }

    @Override
    public int hashCode() {
        int result = java.util.Objects.hash(pchannel, timestamp);
        result = 31 * result + Arrays.hashCode(msgId);
        return result;
    }

    @Override
    public String toString() {
        return "ChannelCheckpoint{pchannel='" + pchannel +
                "', msgIdLength=" + (msgId != null ? msgId.length : 0) +
                ", timestamp=" + timestamp + "}";
    }
}

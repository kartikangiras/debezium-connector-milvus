/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;

import io.debezium.doc.FixFor;

import milvus.proto.msg.Msg;

public class ChannelCheckpointTest {

    @Test
    @FixFor("debezium/dbz#2131")
    void shouldCreateCheckpointFromMsgPosition() {
        Msg.MsgPosition position = Msg.MsgPosition.newBuilder()
                .setChannelName("by-dev-rootcoord-dml_0")
                .setMsgID(ByteString.copyFromUtf8("12345"))
                .setTimestamp(9876543210L)
                .build();

        ChannelCheckpoint checkpoint = ChannelCheckpoint.fromMsgPosition("by-dev-rootcoord-dml_0", position);

        assertThat(checkpoint.getPchannel()).isEqualTo("by-dev-rootcoord-dml_0");
        assertThat(checkpoint.getMsgId()).isEqualTo("12345".getBytes(StandardCharsets.UTF_8));
        assertThat(checkpoint.getTimestamp()).isEqualTo(9876543210L);
    }

    @Test
    @FixFor("debezium/dbz#2131")
    void shouldNotDefensivelyCopyMsgId() {
        byte[] original = { 1, 2, 3, 4 };
        ChannelCheckpoint checkpoint = new ChannelCheckpoint("pchannel", original, 100L);

        byte[] first = checkpoint.getMsgId();
        first[0] = 99;
        byte[] second = checkpoint.getMsgId();

        assertThat(second[0]).isEqualTo((byte) 99);
    }

    @Test
    @FixFor("debezium/dbz#2131")
    void shouldDecodeEightByteLittleEndianOffset() {
        byte[] msgId = ByteBuffer.allocate(8)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putLong(12345L)
                .array();
        ChannelCheckpoint checkpoint = new ChannelCheckpoint("pchannel", msgId, 100L);

        assertThat(checkpoint.getKafkaOffset()).isEqualTo(12345L);
    }

    @Test
    @FixFor("debezium/dbz#2131")
    void shouldDecodeStringOffset() {
        ChannelCheckpoint checkpoint = new ChannelCheckpoint(
                "pchannel",
                "67890".getBytes(StandardCharsets.UTF_8),
                100L);

        assertThat(checkpoint.getKafkaOffset()).isEqualTo(67890L);
    }

    @Test
    @FixFor("debezium/dbz#2131")
    void shouldThrowOnNullMsgId() {
        ChannelCheckpoint checkpoint = new ChannelCheckpoint("pchannel", null, 100L);

        assertThatThrownBy(checkpoint::getKafkaOffset)
                .isInstanceOf(NumberFormatException.class)
                .hasMessageContaining("msgId is null or empty");
    }

    @Test
    @FixFor("debezium/dbz#2131")
    void shouldThrowOnUndecodableOffset() {
        ChannelCheckpoint checkpoint = new ChannelCheckpoint(
                "pchannel",
                new byte[]{ 0x01, 0x02, 0x03 },
                100L);

        assertThatThrownBy(checkpoint::getKafkaOffset)
                .isInstanceOf(NumberFormatException.class)
                .hasMessageContaining("Unable to decode msgId");
    }

    @Test
    @FixFor("debezium/dbz#2131")
    void shouldImplementEqualsAndHashCode() {
        byte[] msgId = { 1, 2, 3 };
        ChannelCheckpoint a = new ChannelCheckpoint("pchannel", msgId, 100L);
        ChannelCheckpoint b = new ChannelCheckpoint("pchannel", msgId.clone(), 100L);
        ChannelCheckpoint c = new ChannelCheckpoint("other", msgId, 100L);
        ChannelCheckpoint d = new ChannelCheckpoint("pchannel", msgId, 200L);

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
        assertThat(a).isNotEqualTo(c);
        assertThat(a).isNotEqualTo(d);
    }
}

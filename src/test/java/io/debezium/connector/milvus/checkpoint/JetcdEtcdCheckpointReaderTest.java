/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.protobuf.ByteString;

import io.debezium.DebeziumException;
import io.debezium.doc.FixFor;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;

import milvus.proto.msg.Msg;

@ExtendWith(MockitoExtension.class)
public class JetcdEtcdCheckpointReaderTest {

    private static final String CHECKPOINT_TEMPLATE = "by-dev/data-coord/checkpoint/binlog/channel/%s";
    private static final long TIMEOUT_MS = 5000L;

    @Mock
    private Client client;

    @Mock
    private KV kv;

    @Mock
    private GetResponse response;

    @Mock
    private KeyValue keyValue;

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldReturnCheckpointWhenKeyExists() throws Exception {
        Msg.MsgPosition position = Msg.MsgPosition.newBuilder()
                .setMsgID(ByteString.copyFromUtf8("12345"))
                .setTimestamp(9876543210L)
                .build();
        byte[] value = position.toByteArray();

        when(client.getKVClient()).thenReturn(kv);
        when(kv.get(any(ByteSequence.class))).thenReturn(CompletableFuture.completedFuture(response));
        when(response.getKvs()).thenReturn(List.of(keyValue));
        when(keyValue.getValue()).thenReturn(ByteSequence.from(value));

        JetcdEtcdCheckpointReader reader = new JetcdEtcdCheckpointReader(client, CHECKPOINT_TEMPLATE, TIMEOUT_MS);

        Optional<ChannelCheckpoint> result = reader.read("by-dev-rootcoord-dml_0");

        assertThat(result).isPresent();
        ChannelCheckpoint checkpoint = result.get();
        assertThat(checkpoint.getPchannel()).isEqualTo("by-dev-rootcoord-dml_0");
        assertThat(checkpoint.getTimestamp()).isEqualTo(9876543210L);
        assertThat(checkpoint.getKafkaOffset()).isEqualTo(12345L);
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldReturnEmptyWhenKeyMissing() throws Exception {
        when(client.getKVClient()).thenReturn(kv);
        when(kv.get(any(ByteSequence.class))).thenReturn(CompletableFuture.completedFuture(response));
        when(response.getKvs()).thenReturn(Collections.emptyList());

        JetcdEtcdCheckpointReader reader = new JetcdEtcdCheckpointReader(client, CHECKPOINT_TEMPLATE, TIMEOUT_MS);

        Optional<ChannelCheckpoint> result = reader.read("by-dev-rootcoord-dml_0");

        assertThat(result).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldThrowOnInvalidProtobufValue() throws Exception {
        when(client.getKVClient()).thenReturn(kv);
        when(kv.get(any(ByteSequence.class))).thenReturn(CompletableFuture.completedFuture(response));
        when(response.getKvs()).thenReturn(List.of(keyValue));
        when(keyValue.getValue()).thenReturn(ByteSequence.from(new byte[]{ 0x01, 0x02, 0x03 }));

        JetcdEtcdCheckpointReader reader = new JetcdEtcdCheckpointReader(client, CHECKPOINT_TEMPLATE, TIMEOUT_MS);

        assertThatThrownBy(() -> reader.read("by-dev-rootcoord-dml_0"))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Failed to decode MsgPosition checkpoint");
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldWrapEtcdFailureInDebeziumException() throws Exception {
        when(client.getKVClient()).thenReturn(kv);
        when(kv.get(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.failedFuture(new ExecutionException("etcd unreachable", new RuntimeException())));

        JetcdEtcdCheckpointReader reader = new JetcdEtcdCheckpointReader(client, CHECKPOINT_TEMPLATE, TIMEOUT_MS);

        assertThatThrownBy(() -> reader.read("by-dev-rootcoord-dml_0"))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Failed to read checkpoint");
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldReportAccessibleWhenEtcdResponds() throws Exception {
        when(client.getKVClient()).thenReturn(kv);
        when(kv.get(any(ByteSequence.class))).thenReturn(CompletableFuture.completedFuture(response));

        JetcdEtcdCheckpointReader reader = new JetcdEtcdCheckpointReader(client, CHECKPOINT_TEMPLATE, TIMEOUT_MS);

        assertThat(reader.isAccessible()).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldReportInaccessibleWhenEtcdFails() throws Exception {
        when(client.getKVClient()).thenReturn(kv);
        when(kv.get(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("connection refused")));

        JetcdEtcdCheckpointReader reader = new JetcdEtcdCheckpointReader(client, CHECKPOINT_TEMPLATE, TIMEOUT_MS);

        assertThat(reader.isAccessible()).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2068")
    void shouldCloseClient() {
        JetcdEtcdCheckpointReader reader = new JetcdEtcdCheckpointReader(client, CHECKPOINT_TEMPLATE, TIMEOUT_MS);
        reader.close();
        verify(client).close();
    }
}

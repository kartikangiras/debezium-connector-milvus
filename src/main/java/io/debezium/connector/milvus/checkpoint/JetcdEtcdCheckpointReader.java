/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus.checkpoint;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.milvus.MilvusConnectorConfig;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;

import milvus.proto.msg.Msg.MsgPosition;

/**
 * {@link EtcdCheckpointReader} implementation backed by the jetcd client.
 *
 * <p>
 * <b>Warning — direct etcd access:</b> Milvus considers its etcd key layout an
 * internal implementation detail. The Milvus gRPC API is the preferred source
 * for collection metadata and channel assignments; this class accesses etcd
 * <em>only</em> because the Milvus API does not expose channel checkpoint data.
 * The key path is configurable via {@code milvus.etcd.checkpoint.path} so that
 * operators can adapt to future Milvus versions without a code change. If a
 * checkpoint API is added to Milvus, this implementation should be deprecated.
 * </p>
 */
@EtcdInternalAPI("Reads channel checkpoints from Milvus etcd internals because no public API exposes them.")
public class JetcdEtcdCheckpointReader implements EtcdCheckpointReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(JetcdEtcdCheckpointReader.class);

    private static final String DEFAULT_CHECKPOINT_RELATIVE_PATH = "data-coord/checkpoint/binlog/channel/%s";

    private final Client client;
    private final String checkpointPathTemplate;
    private final long timeoutMs;

    public JetcdEtcdCheckpointReader(MilvusConnectorConfig config) {
        this.checkpointPathTemplate = resolveCheckpointPathTemplate(config);
        this.timeoutMs = config.getMetadataTimeoutMs();
        try {
            this.client = Client.builder()
                    .endpoints(config.getEtcdEndpoints().toArray(new String[0]))
                    .build();
        }
        catch (Exception e) {
            throw new DebeziumException("Failed to create etcd checkpoint reader for endpoints "
                    + config.getEtcdEndpoints(), e);
        }
    }

    /**
     * Package-private constructor for unit testing that allows injecting a
     * pre-configured jetcd client.
     */
    JetcdEtcdCheckpointReader(Client client, String checkpointPathTemplate, long timeoutMs) {
        this.client = client;
        this.checkpointPathTemplate = checkpointPathTemplate;
        this.timeoutMs = timeoutMs;
    }

    private static String resolveCheckpointPathTemplate(MilvusConnectorConfig config) {
        String override = config.getEtcdCheckpointPath();
        if (override != null && !override.isBlank()) {
            // Operator-supplied template must contain a %s placeholder for the pchannel.
            if (!override.contains("%s")) {
                throw new DebeziumException(
                        "milvus.etcd.checkpoint.path must contain a '%s' placeholder for the pchannel name");
            }
            return override;
        }
        String root = config.getEtcdRootPath();
        if (root.endsWith("/")) {
            root = root.substring(0, root.length() - 1);
        }
        return root + "/" + DEFAULT_CHECKPOINT_RELATIVE_PATH;
    }

    @Override
    public Optional<ChannelCheckpoint> read(String pchannel) {
        String key = buildKey(pchannel);
        LOGGER.debug("Reading checkpoint from etcd key: {}", key);

        KV kv = client.getKVClient();
        ByteSequence keyBytes = ByteSequence.from(key, StandardCharsets.UTF_8);

        try {
            CompletableFuture<GetResponse> future = kv.get(keyBytes);
            GetResponse response = future.get(timeoutMs, TimeUnit.MILLISECONDS);

            List<KeyValue> kvs = response.getKvs();
            if (kvs == null || kvs.isEmpty()) {
                LOGGER.debug("No checkpoint found in etcd for pchannel {} at key {}", pchannel, key);
                return Optional.empty();
            }

            KeyValue kvEntry = kvs.get(0);
            byte[] value = kvEntry.getValue().getBytes();
            return Optional.of(decodeCheckpoint(pchannel, value));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DebeziumException("Interrupted while reading checkpoint for pchannel " + pchannel, e);
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new DebeziumException("Failed to read checkpoint for pchannel " + pchannel + " from etcd", cause);
        }
        catch (TimeoutException e) {
            throw new DebeziumException("Timeout while reading checkpoint for pchannel " + pchannel + " from etcd", e);
        }
    }

    @Override
    public boolean isAccessible() {
        try {
            client.getKVClient()
                    .get(ByteSequence.from("/", StandardCharsets.UTF_8))
                    .get(timeoutMs, TimeUnit.MILLISECONDS);
            return true;
        }
        catch (Exception e) {
            LOGGER.warn("etcd cluster is not accessible", e);
            return false;
        }
    }

    @Override
    public void close() {
        if (client != null) {
            try {
                client.close();
            }
            catch (Exception e) {
                LOGGER.warn("Exception while closing etcd checkpoint reader", e);
            }
        }
    }

    private String buildKey(String pchannel) {
        return String.format(checkpointPathTemplate, pchannel);
    }

    private ChannelCheckpoint decodeCheckpoint(String pchannel, byte[] value) {
        try {
            MsgPosition position = MsgPosition.parseFrom(value);
            return ChannelCheckpoint.fromMsgPosition(pchannel, position);
        }
        catch (Exception e) {
            throw new DebeziumException(
                    "Failed to decode MsgPosition checkpoint for pchannel " + pchannel, e);
        }
    }
}

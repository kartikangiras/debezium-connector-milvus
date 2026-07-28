/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import io.debezium.config.Configuration;
import io.debezium.connector.milvus.KafkaMilvusMessageConsumer;
import io.debezium.connector.milvus.MilvusConnectorConfig;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;

public final class TestHelper {

    public static final String TOPIC_PREFIX = "milvus_test";
    public static final int PARTITION = 0;

    private TestHelper() {
    }

    public static MilvusClientV2 createMilvusClient() {
        return new MilvusClientV2(ConnectConfig.builder()
                .uri(MilvusTestContainer.milvusUri())
                .token("root:Milvus")
                .connectTimeoutMs(5_000L)
                .build());
    }

    public static Configuration defaultConfig() {
        String bootstrapServers = MilvusTestContainer.kafkaBootstrapServers();
        return defaultConfig(bootstrapServers);
    }

    public static Configuration defaultConfig(String bootstrapServers) {
        Map<String, String> props = new HashMap<>();
        props.put("milvus.uri", MilvusTestContainer.milvusUri());
        props.put("milvus.token", "root:Milvus");
        props.put("topic.prefix", TOPIC_PREFIX);
        props.put("milvus.kafka.bootstrap.servers", Objects.requireNonNull(bootstrapServers));
        props.put("milvus.kafka.consumer.group.id",
                "debezium-milvus-it-" + UUID.randomUUID());
        props.put("milvus.wire.format", "proto_single");
        props.put("poll.interval.ms", "100");
        props.put("milvus.timetick.stall.timeout.ms", "2000");
        props.put("tombstones.on.delete", "false");
        props.put("bootstrap.servers", bootstrapServers);
        return Configuration.from(props);
    }

    public static MilvusConnectorConfig connectorConfig(Configuration config) {
        return new MilvusConnectorConfig(config);
    }

    public static void ensureTopic(String bootstrapServers, String topic) {
        ensureTopic(bootstrapServers, topic, 1, (short) 1);
    }

    public static void ensureTopic(String bootstrapServers, String topic, int partitions, short replication) {
        Map<String, Object> adminProps = new HashMap<>();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 60_000);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            Set<String> existing = admin.listTopics().names().get(60, TimeUnit.SECONDS);
            if (!existing.contains(topic)) {
                admin.createTopics(List.of(new NewTopic(topic, partitions, replication)))
                        .all().get(60, TimeUnit.SECONDS);
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to ensure Kafka topic " + topic, e);
        }
    }

    public static KafkaMilvusMessageConsumer kafkaConsumer(MilvusConnectorConfig config) {
        return new KafkaMilvusMessageConsumer(config);
    }

    /**
     * Builds an index on {@code vectorFieldName} and loads {@code collectionName} into memory.
     *
     * <p>Milvus requires a collection to be indexed and loaded before it can be queried
     * (including plain scalar {@code query()} calls used by the connector's snapshot path,
     * not just vector search). Collections created only from a schema are neither indexed
     * nor loaded, so tests that expect the snapshot to actually query Milvus must call this
     * after creating the collection.</p>
     */
    public static void loadCollection(MilvusClientV2 client, String collectionName, String vectorFieldName) {
        IndexParam indexParam = IndexParam.builder()
                .fieldName(vectorFieldName)
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .metricType(IndexParam.MetricType.L2)
                .build();
        client.createIndex(CreateIndexReq.builder()
                .collectionName(collectionName)
                .indexParams(List.of(indexParam))
                .build());
        client.loadCollection(LoadCollectionReq.builder()
                .collectionName(collectionName)
                .build());
    }
}

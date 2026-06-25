/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus.util;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import io.debezium.config.Configuration;
import io.debezium.connector.milvus.KafkaMilvusMessageConsumer;
import io.debezium.connector.milvus.MilvusConnectorConfig;
import io.debezium.connector.milvus.RawMilvusMessage;

/**
 * Static helpers for the Milvus connector integration tests.
 *
 * <p>
 * Mirrors the pattern used by the Postgres/Ingres/SQLite {@code TestHelper}
 * classes: build a default {@link Configuration} from system-property values
 * passed in by the {@code docker-maven-plugin} via {@code maven-failsafe-plugin},
 * and provide Kafka admin/producer/consumer utilities that target the
 * integration-test Kafka cluster.
 * </p>
 */
public final class TestHelper {

    public static final String TOPIC_PREFIX = "milvus-test";
    public static final String PCHANNEL = "by-dev-rootcoord-dml_0";
    public static final int PARTITION = 0;

    private TestHelper() {
    }

    /**
     * Default connector config pointing at the integration-test Milvus/Kafka endpoints.
     * The Kafka bootstrap servers come from the {@code kafka.bootstrap.servers} system
     * property substituted by {@code maven-failsafe-plugin} via {@code docker.host.address}.
     */
    public static Configuration defaultConfig() {
        String bootstrapServers = System.getProperty("kafka.bootstrap.servers", "localhost:9092");
        return defaultConfig(bootstrapServers);
    }

    /**
     * Default connector config pointing at a specific Kafka cluster.
     */
    public static Configuration defaultConfig(String bootstrapServers) {
        Map<String, String> props = new HashMap<>();
        props.put("milvus.uri", "http://localhost:19530");
        props.put("topic.prefix", TOPIC_PREFIX);
        props.put("milvus.kafka.bootstrap.servers", Objects.requireNonNull(bootstrapServers));
        props.put("milvus.kafka.consumer.group.id",
                "debezium-milvus-it-" + UUID.randomUUID());
        props.put("milvus.wire.format", "proto_single");
        props.put("poll.interval.ms", "100");
        props.put("milvus.timetick.stall.timeout.ms", "2000");
        return Configuration.from(props);
    }

    /** Build a {@link MilvusConnectorConfig} from a test {@link Configuration}. */
    public static MilvusConnectorConfig connectorConfig(Configuration config) {
        return new MilvusConnectorConfig(config);
    }

    /** Resolve the test Kafka bootstrap servers from system property with a sane fallback. */
    public static String bootstrapServers() {
        return System.getProperty("kafka.bootstrap.servers", "localhost:9092");
    }

    /**
     * Create a Kafka topic if it does not already exist. Idempotent so safe to
     * call before each test.
     */
    public static void ensureTopic(String bootstrapServers, String topic) {
        ensureTopic(bootstrapServers, topic, 1, (short) 1);
    }

    public static void ensureTopic(String bootstrapServers, String topic, int partitions, short replication) {
        Map<String, Object> adminProps = new HashMap<>();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            Set<String> existing = admin.listTopics().names().get(30, TimeUnit.SECONDS);
            if (!existing.contains(topic)) {
                admin.createTopics(List.of(new NewTopic(topic, partitions, replication)))
                        .all().get(30, TimeUnit.SECONDS);
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to ensure Kafka topic " + topic, e);
        }
    }

    /**
     * Publish a batch of raw byte[] payloads to the given Kafka topic on the
     * default partition (0). Each payload is sent as the record value with a
     * null key.
     */
    public static void publishProtoMessages(String bootstrapServers, String topic, List<byte[]> payloads) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30_000);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            for (byte[] payload : payloads) {
                producer.send(new ProducerRecord<>(topic, PARTITION, null, payload));
            }
            producer.flush();
        }
    }

    /**
     * Consume all available records from the given topics starting from the
     * earliest offset, using manual assignment (no consumer-group rebalance),
     * and return them as {@link RawMilvusMessage}s. The poll loop runs until
     * no records are returned for two consecutive polls (draining the test data).
     *
     * <p>
     * Consumer group id is randomized so that {@code auto.offset.reset=earliest}
     * always applies on first assignment.
     * </p>
     */
    public static List<RawMilvusMessage> consumeEarliest(String bootstrapServers, Set<String> pchannels,
                                                         long pollTimeoutMs, int maxIdlePolls) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "debezium-milvus-it-consume-" + UUID.randomUUID());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);

        List<RawMilvusMessage> all = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            Set<TopicPartition> tps = new java.util.LinkedHashSet<>();
            for (String pchannel : pchannels) {
                tps.add(new TopicPartition(pchannel, PARTITION));
            }
            consumer.assign(tps);
            consumer.seekToBeginning(tps);

            int idle = 0;
            while (idle < maxIdlePolls) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(pollTimeoutMs));
                if (records == null || records.isEmpty()) {
                    idle++;
                    continue;
                }
                idle = 0;
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    all.add(RawMilvusMessage.fromKafkaRecord(record));
                }
            }
        }
        return all;
    }

    /**
     * Build a {@link KafkaMilvusMessageConsumer} for the given connector config
     * with {@code auto.offset.reset=earliest} wiring handled by {@link #assignEarliest}.
     */
    public static KafkaMilvusMessageConsumer kafkaConsumer(MilvusConnectorConfig config) {
        return new KafkaMilvusMessageConsumer(config);
    }
}
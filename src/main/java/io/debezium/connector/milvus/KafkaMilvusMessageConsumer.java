/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka-based implementation of {@link MilvusMessageConsumer}.
 *
 * <p>Uses manual partition assignment (no consumer group rebalance) to align
 * with Milvus pchannel semantics. The consumer is created from the connector
 * configuration and assigned to the specific topic/partition.</p>
 */
public class KafkaMilvusMessageConsumer implements MilvusMessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMilvusMessageConsumer.class);

    private final MilvusConnectorConfig config;
    private KafkaConsumer<byte[], byte[]> kafkaConsumer;

    public KafkaMilvusMessageConsumer(MilvusConnectorConfig config) {
        this.config = config;
    }

    @Override
    public void assign(String pchannel) {
        LOGGER.info("Assigning consumer to pchannel: {}", pchannel);
        // TODO: create KafkaConsumer with manual assignment
    }

    @Override
    public void seek(String pchannel, long offset) {
        LOGGER.info("Seeking pchannel {} to offset {}", pchannel, offset);
        // TODO: seek to the given offset
    }

    @Override
    public List<ConsumerRecord<byte[], byte[]>> poll(long timeoutMs) {
        // TODO: poll from KafkaConsumer
        return Collections.emptyList();
    }

    @Override
    public void close() {
        LOGGER.info("Closing KafkaMilvusMessageConsumer");
        if (kafkaConsumer != null) {
            kafkaConsumer.close();
        }
    }
}

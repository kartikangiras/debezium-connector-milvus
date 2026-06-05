/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Deserializes raw Kafka bytes into a typed {@link RawMilvusMessage}.
 *
 * <p>Supports both protobuf and MsgPack wire formats. The wire format is
 * determined by the connector configuration ({@code milvus.wire.format}).</p>
 *
 * <p>TODO: replace the return type with a fully-typed {@code MilvusChangeEvent}
 * once protobuf / MsgPack deserialization is implemented.</p>
 */
public class MilvusProtoDeserializer {

    private final String wireFormat;

    public MilvusProtoDeserializer(String wireFormat) {
        this.wireFormat = wireFormat;
    }

    /**
     * Deserialize a raw Kafka record into a typed change event.
     *
     * @param record the raw Kafka consumer record
     * @return the deserialized message, or {@code null} if deserialization is not yet implemented
     */
    public RawMilvusMessage deserialize(ConsumerRecord<byte[], byte[]> record) {
        // TODO: implement protobuf / MsgPack deserialization
        return null;
    }
}
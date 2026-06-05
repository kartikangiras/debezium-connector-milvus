/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import io.debezium.pipeline.source.spi.StreamingChangeEventSource;

/**
 * Streaming change event source for Milvus.
 *
 * <p>Main loop:</p>
 * <ol>
 *   <li>Poll raw messages from {@link MilvusMessageConsumer}</li>
 *   <li>Deserialize via {@link MilvusProtoDeserializer}</li>
 *   <li>Order via {@link TimetickOrderingEngine}</li>
 *   <li>Dispatch via {@link io.debezium.pipeline.EventDispatcher}</li>
 * </ol>
 */
public class MilvusStreamingChangeEventSource implements StreamingChangeEventSource<MilvusPartition, MilvusOffsetContext> {

    @Override
    public void execute(ChangeEventSourceContext context, MilvusPartition partition, MilvusOffsetContext offsetContext)
            throws InterruptedException {
    }
}

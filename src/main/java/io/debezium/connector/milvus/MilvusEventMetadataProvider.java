/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Provides event metadata for Milvus change events.
 *
 * <p>Returns minimal metadata; enriched as further event context
 * becomes available.</p>
 */
public class MilvusEventMetadataProvider implements EventMetadataProvider {

    @Override
    public Instant getEventTimestamp(DataCollectionId source, OffsetContext offset,
                                     Object key, Struct value) {
        return Instant.now();
    }

    @Override
    public Map<String, String> getEventSourcePosition(DataCollectionId source,
                                                      OffsetContext offset,
                                                      Object key, Struct value) {
        return Map.of();
    }

    @Override
    public String getTransactionId(DataCollectionId source, OffsetContext offset,
                                   Object key, Struct value) {
        return null;
    }
}

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
        if (value == null) {
            return null;
        }
        Struct sourceStruct = value.getStruct(io.debezium.data.Envelope.FieldName.SOURCE);
        if (sourceStruct == null) {
            return null;
        }
        Long tsMs = sourceStruct.getInt64(io.debezium.connector.AbstractSourceInfo.TIMESTAMP_KEY);
        return tsMs != null ? Instant.ofEpochMilli(tsMs) : null;
    }

    @Override
    public Map<String, String> getEventSourcePosition(DataCollectionId source,
                                                      OffsetContext offset,
                                                      Object key, Struct value) {
        if (value == null) {
            return Map.of();
        }
        Struct sourceStruct = value.getStruct(io.debezium.data.Envelope.FieldName.SOURCE);
        if (sourceStruct == null) {
            return Map.of();
        }
        Map<String, String> position = new java.util.LinkedHashMap<>();
        Long tso = sourceStruct.getInt64("tso");
        if (tso != null) {
            position.put("tso", tso.toString());
        }
        String pchannel = sourceStruct.getString("pchannel");
        if (pchannel != null) {
            position.put("pchannel", pchannel);
        }
        String vchannel = sourceStruct.getString("vchannel");
        if (vchannel != null) {
            position.put("vchannel", vchannel);
        }
        return position;
    }

    @Override
    public String getTransactionId(DataCollectionId source, OffsetContext offset,
                                   Object key, Struct value) {
        return null;
    }
}

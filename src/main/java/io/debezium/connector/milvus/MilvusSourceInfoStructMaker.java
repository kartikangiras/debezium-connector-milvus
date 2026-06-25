/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.AbstractSourceInfoStructMaker;

/**
 * Source info struct maker for Milvus connector.
 *
 * <p>Produces the {@code source} block for every Debezium event.</p>
 */
public class MilvusSourceInfoStructMaker extends AbstractSourceInfoStructMaker<MilvusSourceInfo> {

    private static final String COLLECTION_KEY = "collection";
    private static final String PCHANNEL_KEY = "pchannel";
    private static final String VCHANNEL_KEY = "vchannel";
    private static final String TSO_KEY = "tso";

    @Override
    public Schema schema() {
        return SchemaBuilder.struct()
                .name("io.debezium.connector.milvus.Source")
                .version(1)
                .field(AbstractSourceInfo.DEBEZIUM_VERSION_KEY, Schema.STRING_SCHEMA)
                .field(AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY, Schema.STRING_SCHEMA)
                .field(AbstractSourceInfo.SERVER_NAME_KEY, Schema.STRING_SCHEMA)
                .field(AbstractSourceInfo.TIMESTAMP_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .field(AbstractSourceInfo.TIMESTAMP_US_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .field(AbstractSourceInfo.TIMESTAMP_NS_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .field(AbstractSourceInfo.SNAPSHOT_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(AbstractSourceInfo.SEQUENCE_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(AbstractSourceInfo.DATABASE_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(COLLECTION_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(PCHANNEL_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(VCHANNEL_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(TSO_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .build();
    }

    @Override
    public Struct struct(MilvusSourceInfo sourceInfo) {
        Struct struct = super.commonStruct(sourceInfo);
        struct.put(COLLECTION_KEY, sourceInfo.getCollectionName());
        struct.put(PCHANNEL_KEY, sourceInfo.getPchannel());
        struct.put(VCHANNEL_KEY, sourceInfo.getVchannel());
        struct.put(TSO_KEY, sourceInfo.getTso());
        return struct;
    }
}

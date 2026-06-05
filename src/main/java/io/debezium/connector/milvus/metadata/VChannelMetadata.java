/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus.metadata;

/**
 * VChannel metadata mapping a logical vchannel to its physical pchannel.
 */
public class VChannelMetadata {

    private final String vchannel;
    private final String pchannel;
    private final String collectionName;
    private final long collectionId;

    public VChannelMetadata(String vchannel, String pchannel,
                            String collectionName, long collectionId) {
        this.vchannel = vchannel;
        this.pchannel = pchannel;
        this.collectionName = collectionName;
        this.collectionId = collectionId;
    }

    public String getVchannel() {
        return vchannel;
    }

    public String getPchannel() {
        return pchannel;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public long getCollectionId() {
        return collectionId;
    }

    @Override
    public String toString() {
        return "VChannelMetadata{vchannel='" + vchannel +
                "', pchannel='" + pchannel +
                "', collection='" + collectionName + "'}";
    }
}
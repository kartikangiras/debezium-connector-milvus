/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.time.Instant;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.common.BaseSourceInfo;

/**
 * Source info for a single Milvus change event.
 *
 * <p>Holds the per-event metadata that populates the {@code source} block in
 * Debezium records, including collection, channel, TSO, and database name.</p>
 */
public class MilvusSourceInfo extends BaseSourceInfo {

    private Instant timestamp;
    private String databaseName;
    private String collectionName;
    private String pchannel;
    private String vchannel;
    private long tso;

    public MilvusSourceInfo(CommonConnectorConfig config) {
        super(config);
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public void setPchannel(String pchannel) {
        this.pchannel = pchannel;
    }

    public void setVchannel(String vchannel) {
        this.vchannel = vchannel;
    }

    public void setTso(long tso) {
        this.tso = tso;
    }

    @Override
    protected Instant timestamp() {
        return timestamp;
    }

    @Override
    protected String database() {
        return databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String getPchannel() {
        return pchannel;
    }

    public String getVchannel() {
        return vchannel;
    }

    public long getTso() {
        return tso;
    }
}

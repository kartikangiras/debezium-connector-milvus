/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.util.Map;

import io.milvus.grpc.DataType;

/**
 * Sealed hierarchy representing a single change event from Milvus.
 *
 * <p>
 * Subtypes mirror the Milvus CDC message types:
 * </p>
 * <ul>
 * <li>{@link Insert} — vector insert</li>
 * <li>{@link Delete} — vector delete</li>
 * <li>{@link TimeTick} — timetick watermark</li>
 * <li>{@link DDL} — schema / collection DDL</li>
 * </ul>
 */
public sealed class MilvusChangeEvent {

    private final String collectionName;
    private final String pchannel;
    private final String vchannel;
    private final long tso;

    private MilvusChangeEvent(String collectionName, String pchannel, String vchannel, long tso) {
        this.collectionName = collectionName;
        this.pchannel = pchannel;
        this.vchannel = vchannel;
        this.tso = tso;
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

    /**
     * Insert change event carrying a new row/vector.
     */
    public static final class Insert extends MilvusChangeEvent {
        private final Map<String, Object> data;
        /** Per-field Milvus DataType, keyed by field name. */
        private final Map<String, DataType> fieldTypes;

        public Insert(String collectionName, String pchannel, String vchannel, long tso,
                      Map<String, Object> data) {
            this(collectionName, pchannel, vchannel, tso, data, Map.of());
        }

        public Insert(String collectionName, String pchannel, String vchannel, long tso,
                      Map<String, Object> data, Map<String, DataType> fieldTypes) {
            super(collectionName, pchannel, vchannel, tso);
            this.data = data;
            this.fieldTypes = fieldTypes != null ? fieldTypes : Map.of();
        }

        public Map<String, Object> getData() {
            return data;
        }

        /**
         * Returns the per-field Milvus {@link DataType}, keyed by field name.
         * Never null; may be empty if type information was not available at parse time.
         */
        public Map<String, DataType> getFieldTypes() {
            return fieldTypes;
        }
    }

    /**
     * Delete change event carrying primary keys to remove.
     */
    public static final class Delete extends MilvusChangeEvent {
        private final Object primaryKeys;

        public Delete(String collectionName, String pchannel, String vchannel, long tso,
                      Object primaryKeys) {
            super(collectionName, pchannel, vchannel, tso);
            this.primaryKeys = primaryKeys;
        }

        public Object getPrimaryKeys() {
            return primaryKeys;
        }
    }

    /**
     * TimeTick watermark event used to advance the TSO ordering engine.
     */
    public static final class TimeTick extends MilvusChangeEvent {
        public TimeTick(String collectionName, String pchannel, String vchannel, long tso) {
            super(collectionName, pchannel, vchannel, tso);
        }
    }

    /**
     * DDL event representing schema or collection changes.
     */
    public static final class DDL extends MilvusChangeEvent {
        private final String ddlType;
        private final String ddlStatement;

        public DDL(String collectionName, String pchannel, String vchannel, long tso,
                   String ddlType, String ddlStatement) {
            super(collectionName, pchannel, vchannel, tso);
            this.ddlType = ddlType;
            this.ddlStatement = ddlStatement;
        }

        public String getDdlType() {
            return ddlType;
        }

        public String getDdlStatement() {
            return ddlStatement;
        }
    }
}
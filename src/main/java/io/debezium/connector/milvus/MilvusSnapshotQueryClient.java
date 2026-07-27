/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.milvus.grpc.DataType;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.response.QueryResp;

/**
 * Snapshot query client backed by the Milvus v2 {@link MilvusClientV2}.
 *
 * <p>
 * Issues paginated {@code query} calls with {@code consistency_level=Strong}.
 * The {@code STRONG} consistency level in Milvus guarantees that the query reflects
 * all data written before the query is issued, which — when combined with the etcd
 * channel checkpoint's TSO — provides the point-in-time consistent snapshot view
 * required by DDD-42 §6.
 * </p>
 *
 * <p>
 * The v2 client is used (rather than the v1 service client used for metadata)
 * because the v2 API exposes consistency level as a first-class query parameter.
 * Note that in SDK 2.6.0, the guarantee_ts is managed internally by the server
 * based on the consistency level; the client-side anchor is established by reading
 * the etcd checkpoint's TSO in {@link MilvusSnapshotChangeEventSource}.
 * </p>
 */
public class MilvusSnapshotQueryClient implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MilvusSnapshotQueryClient.class);

    /** Close timeout in milliseconds passed to {@link MilvusClientV2#close(long)}. */
    private static final long CLOSE_TIMEOUT_MS = 5_000L;

    /**
     * Configuration used to build the client lazily on first use.
     * {@code null} when the client was injected via the test constructor.
     */
    private final MilvusConnectorConfig config;

    private volatile MilvusClientV2 client;

    /**
     * Production constructor — stores the config; the actual {@link MilvusClientV2}
     * connection is deferred until the first {@link #queryPage} call.  This avoids
     * failing connector startup with a connectivity error at a point where
     * {@code RetriableException} semantics are not yet active.
     */
    public MilvusSnapshotQueryClient(MilvusConnectorConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.client = null;
    }

    /**
     * Package-private constructor for unit tests — allows injecting a pre-built
     * (or mocked) {@link MilvusClientV2} without a live Milvus endpoint.
     */
    MilvusSnapshotQueryClient(MilvusClientV2 client) {
        this.config = null;
        this.client = Objects.requireNonNull(client, "client must not be null");
    }

    /**
     * Returns the shared client, creating it on the first call when using the
     * production constructor.
     */
    private MilvusClientV2 client() {
        if (client == null) {
            synchronized (this) {
                if (client == null) {
                    ConnectConfig.ConnectConfigBuilder builder = ConnectConfig.builder()
                            .uri(config.getMilvusUri())
                            .connectTimeoutMs(config.getMetadataTimeoutMs());
                    if (config.getMilvusToken() != null && !config.getMilvusToken().isBlank()) {
                        builder.token(config.getMilvusToken());
                    }
                    LOGGER.info("Lazily connecting Milvus v2 snapshot query client to {}", config.getMilvusUri());
                    try {
                        client = new MilvusClientV2(builder.build());
                    }
                    catch (Exception e) {
                        throw new DebeziumException(
                                "Failed to create Milvus v2 snapshot query client for URI " + config.getMilvusUri(), e);
                    }
                }
            }
        }
        return client;
    }

    /**
     * Issue a single paginated query against {@code collectionName}.
     *
     * <p>The query uses {@code consistency_level=Strong} to ensure all committed
     * data is visible. Results are returned as a list of field-name to value maps,
     * one entry per row.</p>
     *
     * @param collectionName Milvus collection to query
     * @param outputFields   list of field names to project; pass {@code List.of("*")} for all fields
     * @param filterExpr     Milvus boolean expression for row selection; use
     *                       {@link #allRowsFilter(String, DataType)} to build a "select all" filter
     * @param guaranteeTs    TSO timestamp from the etcd checkpoint (informational; logged for traceability)
     * @param batchSize      maximum number of rows to return in this page
     * @param offset         zero-based offset for pagination
     * @return list of rows, each represented as a {@code field-name → value} map
     */
    public List<Map<String, Object>> queryPage(String collectionName,
                                               List<String> outputFields,
                                               String filterExpr,
                                               long guaranteeTs,
                                               int batchSize,
                                               long offset) {
        LOGGER.debug("Querying collection={}, offset={}, limit={}, guaranteeTs={}, filter={}",
                collectionName, offset, batchSize, guaranteeTs, filterExpr);

        QueryReq req = QueryReq.builder()
                .collectionName(collectionName)
                .filter(filterExpr)
                .outputFields(outputFields)
                .limit((long) batchSize)
                .offset(offset)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build();

        try {
            QueryResp resp = client().query(req);
            List<QueryResp.QueryResult> results = resp.getQueryResults();
            if (results == null || results.isEmpty()) {
                return List.of();
            }
            return results.stream()
                    .map(QueryResp.QueryResult::getEntity)
                    .collect(Collectors.toList());
        }
        catch (Exception e) {
            throw new DebeziumException(
                    "Milvus snapshot query failed for collection " + collectionName
                            + " at offset " + offset + " with guarantee_ts=" + guaranteeTs,
                    e);
        }
    }

    /**
     * Build a "select all rows" filter expression for the given primary-key field and type.
     *
     * <p>
     * Milvus requires a non-empty filter expression for query calls. The expression is
     * constructed to logically match every value of the primary-key column:
     * <ul>
     *   <li>Integer types ({@code Int8}, {@code Int16}, {@code Int32}, {@code Int64}):
     *       {@code "{pk} >= 0 or {pk} < 0"} — a tautology covering all integers.</li>
     *   <li>String types ({@code VarChar}, {@code String}):
     *       {@code "{pk} like \"%\""} — matches any non-null string value.</li>
     *   <li>All other types: falls back to the integer tautology form.</li>
     * </ul>
     * </p>
     *
     * @param pkFieldName the primary-key field name
     * @param pkType      the Milvus {@link DataType} of the primary-key field
     * @return a Milvus boolean expression string
     */
    public static String allRowsFilter(String pkFieldName, DataType pkType) {
        if (pkType == DataType.VarChar || pkType == DataType.String) {
            return pkFieldName + " like \"%\"";
        }
        return pkFieldName + " >= 0 or " + pkFieldName + " < 0";
    }

    @Override
    public void close() {
        if (client != null) {
            try {
                client.close(CLOSE_TIMEOUT_MS);
            }
            catch (Exception e) {
                LOGGER.warn("Exception while closing Milvus v2 snapshot query client", e);
            }
        }
    }
}

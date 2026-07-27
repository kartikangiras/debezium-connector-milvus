/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.milvus.checkpoint.ChannelCheckpoint;
import io.debezium.connector.milvus.checkpoint.EtcdCheckpointReader;
import io.debezium.connector.milvus.metadata.CollectionMetadata;
import io.debezium.connector.milvus.metadata.MilvusCollectionSchema;
import io.debezium.connector.milvus.metadata.MilvusMetadataClient;
import io.debezium.connector.milvus.metadata.VChannelMetadata;
import io.debezium.data.Envelope;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import io.milvus.grpc.DataType;

/**
 * Snapshot change event source for Milvus.
 *
 * <p>
 * Performs an initial snapshot by:
 * <ol>
 *   <li>Reading the etcd channel checkpoint for the pchannel to obtain the
 *       {@code guarantee_ts} TSO and the Kafka offset to resume streaming from.</li>
 *   <li>Storing the checkpoint Kafka offset in the {@link MilvusOffsetContext} so
 *       the streaming source can seek to it after snapshot completion.</li>
 *   <li>Iterating over all collections in the configured Milvus database (filtered
 *       by the include/exclude lists in {@link MilvusConnectorConfig}).</li>
 *   <li>For each collection: querying all rows via the Milvus v2 SDK with
 *       {@code consistency_level=Strong} and {@code guarantee_ts = checkpoint.timestamp}.</li>
 *   <li>Emitting each row as a Debezium {@code op=r} (read / snapshot) event through
 *       the {@link EventDispatcher}.</li>
 *   <li>Marking {@code snapshot_completed=true} in the offset context.</li>
 * </ol>
 * </p>
 *
 * <p>
 * If no etcd checkpoint exists (e.g. the Milvus cluster has not yet written to the
 * pchannel), the snapshot completes immediately without emitting any rows. The streaming
 * source will then fall back to the LATEST Kafka position.
 * </p>
 */
public class MilvusSnapshotChangeEventSource
        extends AbstractSnapshotChangeEventSource<MilvusPartition, MilvusOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MilvusSnapshotChangeEventSource.class);

    private final MilvusConnectorConfig connectorConfig;
    private final EtcdCheckpointReader checkpointReader;
    private final MilvusSnapshotQueryClient queryClient;
    private final MilvusMetadataClient metadataClient;
    private final EventDispatcher<MilvusPartition, TableId> dispatcher;
    private final MilvusDatabaseSchema databaseSchema;
    private final SnapshotProgressListener<MilvusPartition> snapshotProgressListener;

    public MilvusSnapshotChangeEventSource(MilvusConnectorConfig connectorConfig,
                                           SnapshotProgressListener<MilvusPartition> snapshotProgressListener,
                                           NotificationService<MilvusPartition, MilvusOffsetContext> notificationService,
                                           EtcdCheckpointReader checkpointReader,
                                           MilvusSnapshotQueryClient queryClient,
                                           MilvusMetadataClient metadataClient,
                                           EventDispatcher<MilvusPartition, TableId> dispatcher,
                                           MilvusDatabaseSchema databaseSchema) {
        super(connectorConfig, snapshotProgressListener, notificationService);
        this.connectorConfig = connectorConfig;
        this.checkpointReader = checkpointReader;
        this.queryClient = queryClient;
        this.metadataClient = metadataClient;
        this.dispatcher = dispatcher;
        this.databaseSchema = databaseSchema;
        this.snapshotProgressListener = snapshotProgressListener;
    }

    @Override
    protected SnapshotResult<MilvusOffsetContext> doExecute(ChangeEventSource.ChangeEventSourceContext context,
                                                            MilvusOffsetContext offsetContext,
                                                            SnapshotContext<MilvusPartition, MilvusOffsetContext> snapshotContext,
                                                            SnapshottingTask snapshottingTask)
            throws Exception {

        if (offsetContext == null) {
            offsetContext = new MilvusOffsetContext(new MilvusSourceInfo(connectorConfig));
        }

        String pchannel = snapshotContext.partition.getPchannel();
        LOGGER.info("Starting Milvus snapshot for pchannel={}", pchannel);

        long guaranteeTs;
        Optional<ChannelCheckpoint> checkpointOpt = checkpointReader.read(pchannel);
        if (checkpointOpt.isPresent()) {
            ChannelCheckpoint checkpoint = checkpointOpt.get();
            guaranteeTs = checkpoint.getTimestamp();
            long kafkaOffset = checkpoint.getKafkaOffset();
            LOGGER.info("Etcd checkpoint for pchannel={}: guaranteeTs={}, kafkaOffset={}",
                    pchannel, guaranteeTs, kafkaOffset);
            offsetContext.setMqPosition(pchannel, connectorConfig.getKafkaPartitionIndex(), kafkaOffset);
            offsetContext.setCheckpointTimestamp(guaranteeTs);
        }
        else {
            guaranteeTs = 0L;
            LOGGER.warn("No etcd checkpoint found for pchannel={}.  Snapshotting with guaranteeTs=0 "
                    + "and streaming will resume from LATEST.", pchannel);
        }

        String dbName = connectorConfig.getMilvusDatabase();
        List<CollectionMetadata> collections = metadataClient.collections();
        List<String> includeList = connectorConfig.getCollectionIncludeList();
        List<String> excludeList = connectorConfig.getCollectionExcludeList();
        List<TableId> snapshottedTables = new ArrayList<>();
        for (CollectionMetadata collectionMeta : collections) {
            String collectionName = collectionMeta.getName();
            if (isIncluded(collectionName, includeList, excludeList)) {
                snapshottedTables.add(new TableId(null, dbName, collectionName));
            }
        }
        snapshotProgressListener.monitoredDataCollectionsDetermined(snapshotContext.partition, snapshottedTables);

        int collectionCount = 0;
        long totalRows = 0;

        for (CollectionMetadata collectionMeta : collections) {
            if (!context.isRunning()) {
                LOGGER.info("Snapshot interrupted after {} collections", collectionCount);
                break;
            }

            String collectionName = collectionMeta.getName();
            if (!isIncluded(collectionName, includeList, excludeList)) {
                LOGGER.debug("Skipping collection {} (filtered by include/exclude list)", collectionName);
                continue;
            }

            TableId tableId = new TableId(null, dbName, collectionName);

            LOGGER.info("Snapshotting collection={} ({}/{})",
                    collectionName, collectionCount + 1, snapshottedTables.size());

            notificationService.initialSnapshotNotificationService()
                    .notifyTableInProgress(snapshotContext.partition, offsetContext, collectionName);

            long rows = snapshotCollection(context, snapshotContext.partition, offsetContext,
                    dbName, collectionName, pchannel, guaranteeTs);
            totalRows += rows;
            collectionCount++;

            snapshotProgressListener.dataCollectionSnapshotCompleted(snapshotContext.partition, tableId, rows);
            notificationService.initialSnapshotNotificationService()
                    .notifyCompletedTableSuccessfully(snapshotContext.partition, offsetContext, collectionName);

            LOGGER.info("Snapshot of collection={} complete: rows={}", collectionName, rows);
        }

        LOGGER.info("Milvus snapshot complete: collections={}, totalRows={}, guaranteeTs={}",
                collectionCount, totalRows, guaranteeTs);

        offsetContext.markSnapshotCompleted();

        return SnapshotResult.completed(offsetContext);
    }

    /**
     * Snapshot a single collection by paging through all rows and emitting each
     * as {@code op=r}.
     *
     * @return the number of rows emitted
     */
    private long snapshotCollection(ChangeEventSource.ChangeEventSourceContext context,
                                    MilvusPartition partition,
                                    MilvusOffsetContext offsetContext,
                                    String dbName,
                                    String collectionName,
                                    String pchannel,
                                    long guaranteeTs)
            throws InterruptedException {

        MilvusCollectionSchema schema = metadataClient.schema(collectionName);
        String pkFieldName = schema.getPrimaryKeyField();
        List<String> outputFields = new ArrayList<>();
        List<FieldDefinition> fieldDefs = new ArrayList<>();
        DataType pkDataType = DataType.Int64;

        for (MilvusCollectionSchema.FieldSchema f : schema.getFields()) {
            outputFields.add(f.getName());
            DataType dt = DataType.forNumber(f.getDataType());
            if (dt == null) {
                dt = DataType.None;
            }
            fieldDefs.add(new FieldDefinition(f.getName(), null, dt));
            if (f.getName().equals(pkFieldName)) {
                pkDataType = dt;
            }
        }

        Map<String, DataType> typeByName = new HashMap<>(fieldDefs.size() * 2);
        for (FieldDefinition fd : fieldDefs) {
            typeByName.put(fd.fieldName(), fd.dataType());
        }

        String vchannel = resolveVchannel(collectionName, pchannel);

        TableId tableId = new TableId(null, dbName, collectionName);
        databaseSchema.registerCollection(dbName, collectionName, fieldDefs);

        String[] columnNames = databaseSchema.getColumnNames(tableId);
        if (columnNames == null || columnNames.length == 0) {
            LOGGER.warn("No columns registered for collection={}; skipping", collectionName);
            return 0;
        }

        String filter = MilvusSnapshotQueryClient.allRowsFilter(pkFieldName, pkDataType);
        int batchSize = connectorConfig.getSnapshotBatchSize() > 0
                ? connectorConfig.getSnapshotBatchSize()
                : 1000;
        long offset = 0;
        long rowsEmitted = 0;

        while (context.isRunning()) {
            List<Map<String, Object>> page = queryClient.queryPage(
                    collectionName, outputFields, filter, guaranteeTs, batchSize, offset);

            if (page.isEmpty()) {
                break;
            }

            for (Map<String, Object> rowMap : page) {
                if (!context.isRunning()) {
                    break;
                }

                MilvusRow row = toMilvusRow(rowMap, columnNames, typeByName);

                offsetContext.updateForEvent(dbName, collectionName, pchannel, vchannel, guaranteeTs);

                MilvusChangeEvent.Insert snapshotInsert = new MilvusChangeEvent.Insert(
                        collectionName, pchannel, vchannel, guaranteeTs, row);

                MilvusChangeRecordEmitter emitter = new MilvusChangeRecordEmitter(
                        partition, offsetContext, Clock.SYSTEM, connectorConfig,
                        snapshotInsert, Envelope.Operation.READ,
                        columnNames, pkFieldName);

                dispatcher.dispatchDataChangeEvent(partition, tableId, emitter);
                rowsEmitted++;
            }

            snapshotProgressListener.rowsScanned(partition, tableId, rowsEmitted);

            if (page.size() < batchSize) {
                break;
            }
            offset += batchSize;
        }

        return rowsEmitted;
    }

    /**
     * Convert a {@code Map<String, Object>} query result row into a {@link MilvusRow}.
     *
     * <p>Fields are ordered according to {@code columnNames} (derived from the registered
     * schema) to guarantee stable column ordering across batches. The {@code typeByName}
     * map is built once per collection by {@link #snapshotCollection} and reused for
     * every row to avoid O(rows * fields) rebuild cost.</p>
     */
    private MilvusRow toMilvusRow(Map<String, Object> rowMap,
                                  String[] columnNames,
                                  Map<String, DataType> typeByName) {
        String[] fieldNames = columnNames;
        Object[] fieldValues = new Object[fieldNames.length];
        DataType[] fieldTypes = new DataType[fieldNames.length];

        for (int i = 0; i < fieldNames.length; i++) {
            fieldValues[i] = rowMap.get(fieldNames[i]);
            DataType dt = typeByName.getOrDefault(fieldNames[i], DataType.None);
            fieldTypes[i] = dt != null ? dt : DataType.None;
        }

        return new MilvusRow(fieldNames, fieldValues, fieldTypes);
    }

    /**
     * Resolve the virtual channel (vchannel) for a collection being snapshotted.
     *
     * <p>A vchannel is the logical channel that maps a collection to its physical
     * channel (pchannel). During streaming the vchannel is obtained from the Milvus
     * insert request's shard name; for the snapshot we resolve it from the collection
     * metadata via {@link MilvusMetadataClient#channels(String)}.</p>
     *
     * <p>{@code vchannel} is purely informational: it is only surfaced in the
     * {@code source.vchannel} field of emitted records (see
     * {@link MilvusOffsetContext#updateForEvent}). Snapshot/streaming dedup and
     * resume are keyed by pchannel and Kafka offset, and streaming's own
     * per-vchannel watermark tracking is populated exclusively from live Milvus
     * timetick events, so this value never feeds into correctness-critical logic.</p>
     *
     * <p>When the metadata client cannot resolve a vchannel for this collection
     * (e.g. because etcd is temporarily unavailable), {@code null} is returned.
     * A {@code null} vchannel causes the {@code source.vchannel} field to be absent
     * in the emitted record but has no impact on snapshot correctness, dedup, or
     * streaming resume, since none of those paths key on vchannel.</p>
     */
    private String resolveVchannel(String collectionName, String pchannel) {
        try {
            List<VChannelMetadata> channels = metadataClient.channels(collectionName);
            for (VChannelMetadata ch : channels) {
                if (pchannel == null || pchannel.equals(ch.getPchannel())) {
                    return ch.getVchannel();
                }
            }
            if (!channels.isEmpty()) {
                return channels.get(0).getVchannel();
            }
            LOGGER.warn("No vchannel metadata found for collection={}; "
                    + "source.vchannel will be unresolved (null) for its snapshot rows.", collectionName);
        }
        catch (Exception e) {
            LOGGER.warn("Failed to resolve vchannel for collection={}; "
                    + "source.vchannel will be unresolved (null) for its snapshot rows: {}",
                    collectionName, e.getMessage());
        }
        return null;
    }

    /**
     * Determine whether a collection should be included in the snapshot.
     *
     * <p>Both the include and exclude lists support literal collection names as well
     * as regular expressions compiled via {@link Strings#setOfRegex(String)}, matching
     * the behaviour of other Debezium connectors.  For example, {@code "orders.*"} matches
     * every collection whose name begins with {@code orders}.</p>
     *
     * <p>Evaluation order:
     * <ol>
     *   <li>If the exclude list is non-empty and the collection matches any entry,
     *       the collection is excluded.</li>
     *   <li>If the include list is non-empty, the collection must match at least
     *       one entry to be included.</li>
     *   <li>If neither list constrains the collection it is included.</li>
     * </ol></p>
     */
    private static boolean isIncluded(String collectionName,
                                      List<String> includeList,
                                      List<String> excludeList) {
        if (excludeList != null && !excludeList.isEmpty()) {
            Set<Pattern> excludePatterns = Strings.setOfRegex(String.join(",", excludeList));
            if (excludePatterns.stream().anyMatch(p -> p.matcher(collectionName).matches())) {
                return false;
            }
        }
        if (includeList != null && !includeList.isEmpty()) {
            Set<Pattern> includePatterns = Strings.setOfRegex(String.join(",", includeList));
            return includePatterns.stream().anyMatch(p -> p.matcher(collectionName).matches());
        }
        return true;
    }

    @Override
    protected SnapshotContext<MilvusPartition, MilvusOffsetContext> prepare(MilvusPartition partition, boolean onDemand)
            throws Exception {
        return new MilvusSnapshotContext(partition);
    }

    @Override
    public SnapshottingTask getSnapshottingTask(MilvusPartition partition, MilvusOffsetContext offsetContext) {
        MilvusConnectorConfig.SnapshotMode snapshotMode = connectorConfig.getSnapshotMode();

        boolean snapshotNeeded;
        switch (snapshotMode) {
            case INITIAL:
                snapshotNeeded = (offsetContext == null || !offsetContext.isSnapshotCompleted());
                break;
            case NEVER:
                snapshotNeeded = false;
                break;
            case WHEN_NEEDED:
                snapshotNeeded = (offsetContext == null || !offsetContext.isSnapshotCompleted());
                break;
            case RECOVERY:
                snapshotNeeded = (offsetContext == null);
                break;
            default:
                snapshotNeeded = false;
                break;
        }

        return new SnapshottingTask(snapshotNeeded, snapshotNeeded,
                List.of(), Map.of(), false);
    }

    @Override
    public SnapshottingTask getBlockingSnapshottingTask(MilvusPartition partition, MilvusOffsetContext offsetContext,
                                                        SnapshotConfiguration snapshotConfiguration) {
        return new SnapshottingTask(false, false,
                List.of(), Map.of(), false);
    }

    /**
     * Minimal snapshot context holding the partition being snapshotted.
     */
    private static class MilvusSnapshotContext
            extends AbstractSnapshotChangeEventSource.SnapshotContext<MilvusPartition, MilvusOffsetContext> {

        MilvusSnapshotContext(MilvusPartition partition) throws Exception {
            super(partition);
        }
    }
}

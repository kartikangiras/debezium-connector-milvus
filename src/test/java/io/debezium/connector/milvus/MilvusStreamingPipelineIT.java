/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.connector.milvus.metadata.MilvusClientV2MetadataClient;
import io.debezium.connector.milvus.metadata.MilvusMetadataClient;
import io.debezium.connector.milvus.util.MilvusTestContainer;
import io.debezium.connector.milvus.util.TestHelper;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.LoggingContext;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.request.InsertReq;

@Tag("integration")
class MilvusStreamingPipelineIT extends AbstractAsyncEngineConnectorTest {

    private static final String PCHANNEL = "by-dev-rootcoord-dml_0";
    private static final int DIM = 4;
    private static final Gson GSON = new Gson();

    private static MilvusClientV2 milvusClient;
    private static MilvusMetadataClient metadataClient;
    private static String bootstrap;

    private String collectionName;
    private final Set<KafkaMilvusMessageConsumer> openConsumers = new HashSet<>();

    @BeforeAll
    static void startInfrastructure() {
        MilvusTestContainer.startAll();
        bootstrap = MilvusTestContainer.kafkaBootstrapServers();

        metadataClient = new MilvusClientV2MetadataClient(
                new MilvusConnectorConfig(TestHelper.defaultConfig()));

        Awaitility.await("Milvus MQ ready")
                .atMost(Duration.ofMinutes(5))
                .pollInterval(Duration.ofSeconds(5))
                .until(() -> {
                    try {
                        if (milvusClient == null) {
                            milvusClient = TestHelper.createMilvusClient();
                        }
                        // listCollections() succeeds before Kafka MQ is ready.
                        // createCollection exercises the full Milvus→Kafka path,
                        // so we use it as the true readiness probe.
                        String sentinel = "_dbz_ready_probe_";
                        CreateCollectionReq.FieldSchema idField = CreateCollectionReq.FieldSchema.builder()
                                .name("id").dataType(DataType.Int64)
                                .isPrimaryKey(true).autoID(false).build();
                        CreateCollectionReq.FieldSchema vecField = CreateCollectionReq.FieldSchema.builder()
                                .name("vec").dataType(DataType.FloatVector)
                                .dimension(2).build();
                        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                                .fieldSchemaList(Arrays.asList(idField, vecField)).build();
                        milvusClient.createCollection(CreateCollectionReq.builder()
                                .collectionName(sentinel).collectionSchema(schema).build());
                        milvusClient.dropCollection(
                                DropCollectionReq.builder()
                                        .collectionName(sentinel).build());
                        return true;
                    }
                    catch (Exception e) {
                        if (milvusClient != null) {
                            try {
                                milvusClient.close(1);
                            }
                            catch (Exception ignored) {
                            }
                            milvusClient = null;
                        }
                        return false;
                    }
                });
    }

    @AfterAll
    static void stopInfrastructure() {
        try {
            milvusClient.close(10);
        }
        catch (Exception ignored) {
        }
        try {
            if (metadataClient != null) {
                metadataClient.close();
            }
        }
        catch (Exception ignored) {
        }
        MilvusTestContainer.stopAll();
    }

    @BeforeEach
    void setUp() {
        collectionName = "test_" + UUID.randomUUID().toString().substring(0, 8);
        TestHelper.ensureTopic(bootstrap, PCHANNEL);
    }

    @AfterEach
    void tearDown() {
        for (KafkaMilvusMessageConsumer c : openConsumers) {
            try {
                c.close();
            }
            catch (Exception ignored) {
            }
        }
        openConsumers.clear();
        try {
            milvusClient.dropCollection(DropCollectionReq.builder()
                    .collectionName(collectionName).build());
        }
        catch (Exception ignored) {
        }
    }

    private Configuration rawConfig(String wireFormat, Long stallMs, Integer bufferMaxEvents) {
        Configuration.Builder b = TestHelper.defaultConfig(bootstrap).edit()
                .with("milvus.wire.format", wireFormat)
                .with("milvus.kafka.bootstrap.servers", bootstrap);
        if (stallMs != null) {
            b.with("milvus.timetick.stall.timeout.ms", stallMs.toString());
        }
        if (bufferMaxEvents != null) {
            b.with("milvus.buffer.max.events", bufferMaxEvents.toString());
        }
        return b.build();
    }

    private MilvusConnectorConfig config(String wireFormat, Long stallMs, Integer bufferMaxEvents) {
        return new MilvusConnectorConfig(rawConfig(wireFormat, stallMs, bufferMaxEvents));
    }

    private MilvusProtoDeserializer deserializer(MilvusConnectorConfig cfg) {
        return new MilvusProtoDeserializer(cfg.getWireFormat(),
                new MilvusColumnarPivot(new MilvusValueConverter(cfg)));
    }

    /**
     * Returns the current end offset of the pchannel, used to seek each test's pipeline
     * to exactly where the topic stands before that test's data changes. This prevents
     * cross-test event contamination (e.g. DELETE events from a prior test crashing a
     * test that never expected to see a delete for an "unknown" collection).
     */
    private long kafkaEndOffset(String bootstrapServers, String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.deserializer", ByteArrayDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            TopicPartition tp = new TopicPartition(topic, 0);
            consumer.assign(List.of(tp));
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(List.of(tp));
            return endOffsets.getOrDefault(tp, 0L);
        }
    }

    private KafkaMilvusMessageConsumer consumerAtOffset(MilvusConnectorConfig cfg,
                                                        Set<String> pchannels,
                                                        long startOffset) {
        KafkaMilvusMessageConsumer consumer = TestHelper.kafkaConsumer(cfg);
        openConsumers.add(consumer);
        // Build a checkpoint map: {TopicPartition → startOffset - 1} so that
        // STORED_OFFSET_PLUS_ONE seeks to exactly startOffset.
        Map<TopicPartition, Long> checkpoints = new HashMap<>();
        for (String pchannel : pchannels) {
            checkpoints.put(new TopicPartition(pchannel, 0), startOffset - 1);
        }
        if (startOffset == 0) {
            consumer.assignAndSeek(pchannels, SeekPosition.EARLIEST, null);
        }
        else {
            consumer.assignAndSeek(pchannels, SeekPosition.STORED_OFFSET_PLUS_ONE, checkpoints);
        }
        return consumer;
    }

    /**
     * Runs {@link MilvusStreamingChangeEventSource#execute} in a daemon thread
     * while concurrently polling the change-event queue for events.
     *
     * <p>The source uses an {@link AtomicBoolean} context so it keeps running
     * until we explicitly stop it after the expected events have arrived (or
     * the timeout expires).  This avoids the race where {@code execute()} exits
     * its fixed-iteration loop before Milvus has asynchronously published the
     * DML events to Kafka.</p>
     */
    private List<DataChangeEvent> runSourceAndPoll(Pipeline pipeline,
                                                   int expectedCount,
                                                   Duration timeout)
            throws Exception {
        AtomicBoolean running = new AtomicBoolean(true);
        ChangeEventSource.ChangeEventSourceContext ctx = mock(ChangeEventSource.ChangeEventSourceContext.class);
        when(ctx.isRunning()).thenAnswer(inv -> running.get());
        when(ctx.isPaused()).thenReturn(false);

        Thread sourceThread = new Thread(() -> {
            try {
                pipeline.source.execute(ctx, pipeline.partition, pipeline.offsetContext);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "milvus-source-thread");
        sourceThread.setDaemon(true);
        sourceThread.start();

        try {
            return pollEvents(pipeline.queue, expectedCount, timeout);
        }
        finally {
            running.set(false);
            sourceThread.join(10_000);
        }
    }

    private static class Pipeline {
        final ChangeEventQueue<DataChangeEvent> queue;
        final MilvusDatabaseSchema schema;
        final MilvusStreamingChangeEventSource source;
        final MilvusPartition partition;
        final MilvusOffsetContext offsetContext;

        Pipeline(ChangeEventQueue<DataChangeEvent> queue,
                 MilvusDatabaseSchema schema,
                 MilvusStreamingChangeEventSource source,
                 MilvusPartition partition,
                 MilvusOffsetContext offsetContext) {
            this.queue = queue;
            this.schema = schema;
            this.source = source;
            this.partition = partition;
            this.offsetContext = offsetContext;
        }
    }

    private Pipeline createFullPipeline(MilvusConnectorConfig cfg, Set<String> pchannels,
                                        long startOffset) {
        Configuration rawCfg = cfg.getConfig();

        ChangeEventQueue<DataChangeEvent> queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(Duration.ofMillis(cfg.getPollIntervalMs()))
                .maxBatchSize(cfg.getMaxBatchSize())
                .maxQueueSize(cfg.getMaxQueueSize())
                .maxQueueSizeInBytes(cfg.getMaxQueueSizeInBytes())
                .loggingContextSupplier(() -> LoggingContext.forConnector(
                        Module.name(), cfg.getLogicalName(), "streaming"))
                .build();

        CdcSourceTaskContext<MilvusConnectorConfig> taskCtx = new CdcSourceTaskContext<>(
                rawCfg, cfg, Collections.emptyMap());

        MilvusDatabaseSchema schema = MilvusDatabaseSchema.create(cfg, taskCtx, metadataClient);
        TopicNamingStrategy<TableId> naming = cfg.getTopicNamingStrategy(
                CommonConnectorConfig.TOPIC_NAMING_STRATEGY);

        MilvusEventDispatcher dispatcher = new MilvusEventDispatcher(
                cfg, naming, schema, queue,
                cfg.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                new MilvusEventMetadataProvider(),
                cfg.schemaNameAdjuster(),
                new DebeziumHeaderProducer(taskCtx));

        TimetickOrderingEngine engine = new TimetickOrderingEngine(cfg);
        KafkaMilvusMessageConsumer consumer = consumerAtOffset(cfg, pchannels, startOffset);

        MilvusStreamingChangeEventSource source = new MilvusStreamingChangeEventSource(
                cfg, consumer, deserializer(cfg), engine, dispatcher, schema);

        MilvusPartition partition = MilvusPartition.create(TestHelper.TOPIC_PREFIX, PCHANNEL);

        // Seed the offset context so the streaming source resumes from exactly
        // startOffset instead of falling back to EARLIEST and reading events
        // produced by earlier tests on the shared pchannel.
        MilvusOffsetContext offsetContext = new MilvusOffsetContext(new MilvusSourceInfo(cfg));
        if (startOffset > 0) {
            offsetContext.setMqPosition(PCHANNEL, cfg.getKafkaPartitionIndex(), startOffset - 1);
        }

        return new Pipeline(queue, schema, source, partition, offsetContext);
    }

    private List<DataChangeEvent> pollEvents(ChangeEventQueue<DataChangeEvent> queue,
                                             int expectedCount,
                                             Duration timeout)
            throws InterruptedException {
        List<DataChangeEvent> all = new ArrayList<>();
        Awaitility.await("poll dispatched events from queue")
                .atMost(timeout)
                .pollDelay(Duration.ofMillis(100))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    try {
                        List<DataChangeEvent> batch = queue.poll();
                        all.addAll(batch);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return all.size() >= expectedCount;
                });
        return all;
    }

    private void createSimpleCollection(String name) {
        CreateCollectionReq.FieldSchema idField = CreateCollectionReq.FieldSchema.builder()
                .name("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build();
        CreateCollectionReq.FieldSchema titleField = CreateCollectionReq.FieldSchema.builder()
                .name("title")
                .dataType(DataType.VarChar)
                .maxLength(256)
                .build();
        CreateCollectionReq.FieldSchema vecField = CreateCollectionReq.FieldSchema.builder()
                .name("vector")
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build();
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(Arrays.asList(idField, titleField, vecField))
                .build();
        milvusClient.createCollection(CreateCollectionReq.builder()
                .collectionName(name)
                .collectionSchema(schema)
                .build());
    }

    private void createAllFieldTypesCollection(String name) {
        CreateCollectionReq.FieldSchema idField = CreateCollectionReq.FieldSchema.builder()
                .name("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build();
        CreateCollectionReq.FieldSchema countField = CreateCollectionReq.FieldSchema.builder()
                .name("count")
                .dataType(DataType.Int32)
                .build();
        CreateCollectionReq.FieldSchema titleField = CreateCollectionReq.FieldSchema.builder()
                .name("title")
                .dataType(DataType.VarChar)
                .maxLength(256)
                .build();
        CreateCollectionReq.FieldSchema priceField = CreateCollectionReq.FieldSchema.builder()
                .name("price")
                .dataType(DataType.Float)
                .build();
        CreateCollectionReq.FieldSchema scoreField = CreateCollectionReq.FieldSchema.builder()
                .name("score")
                .dataType(DataType.Double)
                .build();
        CreateCollectionReq.FieldSchema activeField = CreateCollectionReq.FieldSchema.builder()
                .name("active")
                .dataType(DataType.Bool)
                .build();
        CreateCollectionReq.FieldSchema metaField = CreateCollectionReq.FieldSchema.builder()
                .name("meta")
                .dataType(DataType.JSON)
                .build();
        CreateCollectionReq.FieldSchema embeddingField = CreateCollectionReq.FieldSchema.builder()
                .name("embedding")
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build();
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(Arrays.asList(idField, countField, titleField, priceField,
                        scoreField, activeField, metaField, embeddingField))
                .build();
        milvusClient.createCollection(CreateCollectionReq.builder()
                .collectionName(name)
                .collectionSchema(schema)
                .build());
    }

    private JsonObject rowWithIdAndTitle(long id, String title) {
        JsonObject row = new JsonObject();
        row.addProperty("id", id);
        row.addProperty("title", title);
        List<Float> vec = Arrays.asList(1.0f, 2.0f, 3.0f, 4.0f);
        row.add("vector", GSON.toJsonTree(vec));
        return row;
    }

    private JsonObject allFieldTypesRow() {
        JsonObject row = new JsonObject();
        row.addProperty("id", 42L);
        row.addProperty("count", 7);
        row.addProperty("title", "hello");
        row.addProperty("price", 1.5f);
        row.addProperty("score", 99.9d);
        row.addProperty("active", true);
        JsonObject meta = new JsonObject();
        meta.addProperty("k", 1);
        row.add("meta", meta);
        List<Float> vec = Arrays.asList(1.5f, 2.5f, 3.5f, 4.5f);
        row.add("embedding", GSON.toJsonTree(vec));
        return row;
    }

    @Test
    void shouldCaptureInsert() throws Exception {
        long startOffset = kafkaEndOffset(bootstrap, PCHANNEL);
        createSimpleCollection(collectionName);

        milvusClient.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(Collections.singletonList(rowWithIdAndTitle(42L, "hello")))
                .build());

        MilvusConnectorConfig cfg = config(MilvusProtoDeserializer.FORMAT_PROTO_SINGLE, 5_000L, null);
        Pipeline pipeline = createFullPipeline(cfg, Set.of(PCHANNEL), startOffset);

        List<DataChangeEvent> events = runSourceAndPoll(pipeline, 1, Duration.ofSeconds(60));
        assertThat(events).hasSize(1);

        SourceRecord record = events.get(0).getRecord();
        assertThat(record).isNotNull();
        assertThat(record.topic()).startsWith("milvus-test");

        Schema keySchema = record.keySchema();
        assertThat(keySchema).isNotNull();
        assertThat(keySchema.type()).isEqualTo(Schema.Type.STRUCT);
        Struct key = (Struct) record.key();
        assertThat(key).isNotNull();
        assertThat(key.get("id")).isEqualTo(42L);

        Schema valueSchema = record.valueSchema();
        assertThat(valueSchema).isNotNull();
        assertThat(valueSchema.type()).isEqualTo(Schema.Type.STRUCT);
        Struct value = (Struct) record.value();
        assertThat(value).isNotNull();

        assertThat(value.getString("op")).isEqualTo("c");
        assertThat(value.get("before")).isNull();

        Struct after = value.getStruct("after");
        assertThat(after).isNotNull();
        assertThat(after.get("id")).isEqualTo(42L);
        assertThat(after.get("title")).isEqualTo("hello");

        Struct source = value.getStruct("source");
        assertThat(source).isNotNull();
        assertThat(source.getString("connector")).isEqualTo("milvus");
        assertThat(source.getString("collection")).isEqualTo(collectionName);
        assertThat(source.getInt64("tso")).isPositive();
    }

    @Test
    void shouldCaptureInsertAndDelete() throws Exception {
        long startOffset = kafkaEndOffset(bootstrap, PCHANNEL);
        createSimpleCollection(collectionName);

        milvusClient.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(Collections.singletonList(rowWithIdAndTitle(42L, "hello")))
                .build());

        milvusClient.delete(DeleteReq.builder()
                .collectionName(collectionName)
                .ids(Collections.singletonList(42L))
                .build());

        MilvusConnectorConfig cfg = config(MilvusProtoDeserializer.FORMAT_PROTO_SINGLE, 5_000L, null);
        Pipeline pipeline = createFullPipeline(cfg, Set.of(PCHANNEL), startOffset);

        List<DataChangeEvent> events = runSourceAndPoll(pipeline, 2, Duration.ofSeconds(60));
        assertThat(events).hasSize(2);

        SourceRecord insertRecord = events.get(0).getRecord();
        Struct insertValue = (Struct) insertRecord.value();
        assertThat(insertValue.getString("op")).isEqualTo("c");
        Struct insertAfter = insertValue.getStruct("after");
        assertThat(insertAfter.get("id")).isEqualTo(42L);
        assertThat(insertAfter.get("title")).isEqualTo("hello");
        Struct insertSource = insertValue.getStruct("source");
        assertThat(insertSource.getString("collection")).isEqualTo(collectionName);
        assertThat(insertSource.getInt64("tso")).isPositive();

        SourceRecord deleteRecord = events.get(1).getRecord();
        Struct deleteValue = (Struct) deleteRecord.value();
        assertThat(deleteValue.getString("op")).isEqualTo("d");
        assertThat(deleteValue.get("after")).isNull();

        Struct deleteBefore = deleteValue.getStruct("before");
        assertThat(deleteBefore).isNotNull();
    }

    @Test
    void shouldCaptureAllFieldTypes() throws Exception {
        long startOffset = kafkaEndOffset(bootstrap, PCHANNEL);
        createAllFieldTypesCollection(collectionName);

        milvusClient.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(Collections.singletonList(allFieldTypesRow()))
                .build());

        MilvusConnectorConfig cfg = config(MilvusProtoDeserializer.FORMAT_PROTO_SINGLE, 5_000L, null);
        Pipeline pipeline = createFullPipeline(cfg, Set.of(PCHANNEL), startOffset);

        List<DataChangeEvent> events = runSourceAndPoll(pipeline, 1, Duration.ofSeconds(60));
        assertThat(events).hasSize(1);

        SourceRecord record = events.get(0).getRecord();
        Struct value = (Struct) record.value();
        assertThat(value.getString("op")).isEqualTo("c");

        Struct after = value.getStruct("after");
        assertThat(after).isNotNull();

        assertThat(after.get("id")).isEqualTo(42L);
        assertThat(after.get("count")).isEqualTo(7);
        assertThat(after.get("title")).isEqualTo("hello");
        assertThat(after.get("price")).isEqualTo(1.5f);
        assertThat(after.get("score")).isEqualTo(99.9d);
        assertThat(after.get("active")).isEqualTo(true);
        assertThat(after.get("meta")).isEqualTo("{\"k\":1}");

        Schema afterSchema = after.schema();
        assertThat(afterSchema.field("id").schema().type()).isEqualTo(Schema.Type.INT64);
        assertThat(afterSchema.field("count").schema().type()).isEqualTo(Schema.Type.INT32);
        assertThat(afterSchema.field("title").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(afterSchema.field("price").schema().type()).isEqualTo(Schema.Type.FLOAT32);
        assertThat(afterSchema.field("score").schema().type()).isEqualTo(Schema.Type.FLOAT64);
        assertThat(afterSchema.field("active").schema().type()).isEqualTo(Schema.Type.BOOLEAN);

        assertThat(after.get("embedding")).isInstanceOf(byte[].class);
    }
}

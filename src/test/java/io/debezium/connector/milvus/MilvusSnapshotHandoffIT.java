/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

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

import io.debezium.config.Configuration;
import io.debezium.connector.milvus.MilvusConnectorConfig.SnapshotMode;
import io.debezium.connector.milvus.util.MilvusTestContainer;
import io.debezium.connector.milvus.util.TestHelper;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;

@Tag("integration")
class MilvusSnapshotHandoffIT extends AbstractAsyncEngineConnectorTest {

    private static final String PCHANNEL = "by-dev-rootcoord-dml_0";
    private static final int DIM = 4;
    private static final Gson GSON = new Gson();
    private static final Duration RECORD_TIMEOUT = Duration.ofSeconds(15);

    private static MilvusClientV2 milvusClient;
    private static String bootstrap;
    private String collectionName;

    @BeforeAll
    static void startInfrastructure() {
        MilvusTestContainer.startAll();
        bootstrap = MilvusTestContainer.kafkaBootstrapServers();
        Awaitility.await("Milvus MQ ready")
                .atMost(Duration.ofMinutes(5))
                .pollInterval(Duration.ofSeconds(5))
                .until(() -> {
                    try {
                        if (milvusClient == null) {
                            milvusClient = TestHelper.createMilvusClient();
                        }
                        String sentinel = "_dbz_ready_probe_";
                        CreateCollectionReq.FieldSchema idField = CreateCollectionReq.FieldSchema.builder()
                                .name("id").dataType(DataType.Int64)
                                .isPrimaryKey(true).autoID(false).build();
                        CreateCollectionReq.FieldSchema vecField = CreateCollectionReq.FieldSchema.builder()
                                .name("vec").dataType(DataType.FloatVector).dimension(2).build();
                        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                                .fieldSchemaList(Arrays.asList(idField, vecField)).build();
                        milvusClient.createCollection(CreateCollectionReq.builder()
                                .collectionName(sentinel).collectionSchema(schema).build());
                        milvusClient.dropCollection(DropCollectionReq.builder()
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
        TestHelper.ensureTopic(bootstrap, PCHANNEL);
    }

    @AfterAll
    static void stopInfrastructure() {
        try {
            milvusClient.close(10);
        }
        catch (Exception ignored) {
        }
        MilvusTestContainer.stopAll();
    }

    @BeforeEach
    void setUp() {
        collectionName = "snap_" + UUID.randomUUID().toString().substring(0, 8);
    }

    @AfterEach
    void tearDown() {
        try {
            milvusClient.dropCollection(DropCollectionReq.builder()
                    .collectionName(collectionName).build());
        }
        catch (Exception ignored) {
        }
    }

    private Configuration connectorConfig(SnapshotMode snapshotMode) {
        return TestHelper.defaultConfig(bootstrap).edit()
                .with(MilvusConnectorConfig.PCHANNEL_NAME, PCHANNEL)
                .with(MilvusConnectorConfig.WIRE_FORMAT, MilvusProtoDeserializer.FORMAT_PROTO_SINGLE)
                .with(MilvusConnectorConfig.TIMETICK_STALL_TIMEOUT_MS, 5_000L)
                .with(MilvusConnectorConfig.SNAPSHOT_MODE_FIELD, snapshotMode.getValue())
                .build();
    }

    private void createSimpleCollection(String name) {
        CreateCollectionReq.FieldSchema idField = CreateCollectionReq.FieldSchema.builder()
                .name("id").dataType(DataType.Int64).isPrimaryKey(true).autoID(false).build();
        CreateCollectionReq.FieldSchema titleField = CreateCollectionReq.FieldSchema.builder()
                .name("title").dataType(DataType.VarChar).maxLength(256).build();
        CreateCollectionReq.FieldSchema vecField = CreateCollectionReq.FieldSchema.builder()
                .name("vector").dataType(DataType.FloatVector).dimension(DIM).build();
        milvusClient.createCollection(CreateCollectionReq.builder()
                .collectionName(name)
                .collectionSchema(CreateCollectionReq.CollectionSchema.builder()
                        .fieldSchemaList(Arrays.asList(idField, titleField, vecField)).build())
                .build());
    }

    private JsonObject simpleRow(long id, String title) {
        JsonObject row = new JsonObject();
        row.addProperty("id", id);
        row.addProperty("title", title);
        row.add("vector", GSON.toJsonTree(Arrays.asList(1.0f, 2.0f, 3.0f, 4.0f)));
        return row;
    }

    private String expectedTopic() {
        return TestHelper.TOPIC_PREFIX + "."
                + MilvusConnectorConfig.MILVUS_DATABASE.defaultValueAsString()
                + "." + collectionName;
    }

    private List<SourceRecord> topicRecords(String topic) {
        List<SourceRecord> result = new ArrayList<>();
        for (SourceRecord r : consumedLines) {
            if (topic.equals(r.topic())) {
                result.add(r);
            }
        }
        return result;
    }

    private List<SourceRecord> awaitTopicRecords(String topic, int expectedCount) {
        Awaitility.await("records for topic " + topic)
                .atMost(RECORD_TIMEOUT).pollInterval(Duration.ofMillis(200))
                .until(() -> topicRecords(topic).size() >= expectedCount);
        return topicRecords(topic);
    }

    @Test
    void shouldTakeSnapshotThenHandoffToStreaming() throws Exception {
        createSimpleCollection(collectionName);
        milvusClient.insert(InsertReq.builder().collectionName(collectionName)
                .data(Arrays.asList(simpleRow(1001L, "snap-row-1"),
                        simpleRow(1002L, "snap-row-2"), simpleRow(1003L, "snap-row-3")))
                .build());

        start(MilvusConnector.class, connectorConfig(SnapshotMode.INITIAL));
        waitForStreamingRunning("milvus", TestHelper.TOPIC_PREFIX);

        String topic = expectedTopic();

        milvusClient.insert(InsertReq.builder().collectionName(collectionName)
                .data(Arrays.asList(simpleRow(2001L, "stream-row-1"),
                        simpleRow(2002L, "stream-row-2")))
                .build());

        List<SourceRecord> records = awaitTopicRecords(topic, 2);
        assertThat(records).as("At least 2 streaming records must arrive").hasSizeGreaterThanOrEqualTo(2);

        List<SourceRecord> creates = records.stream()
                .filter(r -> "c".equals(((Struct) r.value()).getString("op")))
                .collect(Collectors.toList());
        assertThat(creates).as("Expected 2 op=c streaming records").hasSize(2);
        for (SourceRecord r : creates) {
            Struct after = ((Struct) r.value()).getStruct("after");
            assertThat(after).isNotNull();
            assertThat(after.get("title")).asString().startsWith("stream-row-");
        }

        List<SourceRecord> reads = topicRecords(topic).stream()
                .filter(r -> "r".equals(((Struct) r.value()).getString("op")))
                .collect(Collectors.toList());
        if (!reads.isEmpty()) {
            assertThat(reads).hasSize(3);
            for (SourceRecord r : reads) {
                Struct value = (Struct) r.value();
                assertThat(value.get("before")).isNull();
                assertThat(value.getStruct("after")).isNotNull();
            }
        }
    }

    @Test
    void shouldSkipSnapshotWithNeverMode() throws Exception {
        createSimpleCollection(collectionName);
        milvusClient.insert(InsertReq.builder().collectionName(collectionName)
                .data(Collections.singletonList(simpleRow(9001L, "should-never-appear")))
                .build());

        start(MilvusConnector.class, connectorConfig(SnapshotMode.NEVER));
        waitForStreamingRunning("milvus", TestHelper.TOPIC_PREFIX);

        milvusClient.insert(InsertReq.builder().collectionName(collectionName)
                .data(Collections.singletonList(simpleRow(100L, "streaming-only")))
                .build());

        String topic = expectedTopic();
        List<SourceRecord> records = awaitTopicRecords(topic, 1);
        assertThat(records).hasSize(1);
        Struct value = (Struct) records.get(0).value();
        assertThat(value.getString("op")).isEqualTo("c");
        assertThat(value.getStruct("after").get("id")).isEqualTo(100L);

        long readCount = topicRecords(topic).stream()
                .filter(r -> "r".equals(((Struct) r.value()).getString("op"))).count();
        assertThat(readCount).as("No snapshot records with mode=never").isZero();
    }

    @Test
    void shouldNotDuplicateRowsAcrossSnapshotAndStreaming() throws Exception {
        createSimpleCollection(collectionName);
        milvusClient.insert(InsertReq.builder().collectionName(collectionName)
                .data(Collections.singletonList(simpleRow(1L, "one-and-only")))
                .build());

        start(MilvusConnector.class, connectorConfig(SnapshotMode.INITIAL));
        waitForStreamingRunning("milvus", TestHelper.TOPIC_PREFIX);

        String topic = expectedTopic();

        milvusClient.insert(InsertReq.builder().collectionName(collectionName)
                .data(Collections.singletonList(simpleRow(2L, "streaming-row")))
                .build());

        List<SourceRecord> allRecords = awaitTopicRecords(topic, 1);

        List<SourceRecord> creates = allRecords.stream()
                .filter(r -> "c".equals(((Struct) r.value()).getString("op")))
                .collect(Collectors.toList());
        assertThat(creates).as("Streaming insert must be present").isNotEmpty();
        Struct createVal = (Struct) creates.get(0).value();
        assertThat(createVal.getStruct("after").get("id")).isEqualTo(2L);

        long duplicateCount = allRecords.stream()
                .filter(r -> {
                    Struct v = (Struct) r.value();
                    return "c".equals(v.getString("op"))
                            && v.getStruct("after").get("id").equals(1L);
                }).count();
        assertThat(duplicateCount).as("Snapshot row must not appear as streaming").isZero();
    }
}

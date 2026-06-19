/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.doc.FixFor;

public class MilvusWireFormatMismatchExceptionTest {

    @Test
    @FixFor("debezium/dbz#2124")
    void shouldContainExpectedAndDetectedFormatInMessage() {
        MilvusWireFormatMismatchException ex = new MilvusWireFormatMismatchException(
                "msgpack_batch", "proto_single", "by-dev-rootcoord-dml_0", 0, 42L,
                "payload not msgpack");

        assertThat(ex.getMessage())
                .contains("expected=msgpack_batch")
                .contains("detected=proto_single")
                .contains("payload not msgpack");
        assertThat(ex.getExpectedFormat()).isEqualTo("msgpack_batch");
        assertThat(ex.getDetectedFormat()).isEqualTo("proto_single");
    }

    @Test
    @FixFor("debezium/dbz#2124")
    void shouldContainTopicPartitionAndOffsetInMessage() {
        MilvusWireFormatMismatchException ex = new MilvusWireFormatMismatchException(
                "proto_single", "unknown", "by-dev-rootcoord-dml_1", 3, 777L, "unrecognizable payload");

        assertThat(ex.getMessage())
                .contains("topic=by-dev-rootcoord-dml_1")
                .contains("partition=3")
                .contains("offset=777");
        assertThat(ex.getTopic()).isEqualTo("by-dev-rootcoord-dml_1");
        assertThat(ex.getPartition()).isEqualTo(3);
        assertThat(ex.getOffset()).isEqualTo(777L);
    }

    @Test
    @FixFor("debezium/dbz#2124")
    void shouldExtendDebeziumException() {
        MilvusWireFormatMismatchException ex = new MilvusWireFormatMismatchException(
                "auto", "unknown", "topic", 0, 0L, "noop");

        assertThat(ex).isInstanceOf(DebeziumException.class);
    }

    @Test
    @FixFor("debezium/dbz#2124")
    void shouldPreserveCauseWhenProvided() {
        Exception cause = new RuntimeException("InvalidProtocolBufferException");
        MilvusWireFormatMismatchException ex = new MilvusWireFormatMismatchException(
                "proto_single", "unknown", "topic", 0, 1L, "malformed protobuf", cause);

        assertThat(ex.getCause()).isSameAs(cause);
    }
}

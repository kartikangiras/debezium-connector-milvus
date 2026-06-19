/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import io.debezium.DebeziumException;

/**
 * Raised when a raw Milvus MQ message cannot be parsed using the expected wire
 * format, or when a probe cannot recognize the payload at all.
 *
 * <p>
 * Carries the MQ coordinates (topic / partition / offset) so that operators can
 * locate the offending message, plus the expected and detected format names for
 * fast diagnosis. All deserialization failures in
 * {@link MilvusProtoDeserializer} and {@link MilvusWireFormatDetector} are
 * funnelled through this exception rather than generic runtime exceptions.
 * </p>
 */
public class MilvusWireFormatMismatchException extends DebeziumException {

    private static final long serialVersionUID = 1L;

    private final String expectedFormat;
    private final String detectedFormat;
    private final String topic;
    private final int partition;
    private final long offset;

    public MilvusWireFormatMismatchException(String expectedFormat, String detectedFormat,
                                             String topic, int partition, long offset,
                                             String detail) {
        super(formatMessage(expectedFormat, detectedFormat, topic, partition, offset, detail));
        this.expectedFormat = expectedFormat;
        this.detectedFormat = detectedFormat;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    public MilvusWireFormatMismatchException(String expectedFormat, String detectedFormat,
                                             String topic, int partition, long offset,
                                             String detail, Throwable cause) {
        super(formatMessage(expectedFormat, detectedFormat, topic, partition, offset, detail), cause);
        this.expectedFormat = expectedFormat;
        this.detectedFormat = detectedFormat;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    private static String formatMessage(String expectedFormat, String detectedFormat,
                                        String topic, int partition, long offset, String detail) {
        return String.format(
                "Wire format mismatch: expected=%s, detected=%s at topic=%s partition=%d offset=%d. %s",
                expectedFormat, detectedFormat, topic, partition, offset, detail);
    }

    public String getExpectedFormat() {
        return expectedFormat;
    }

    public String getDetectedFormat() {
        return detectedFormat;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean;

/**
 * Milvus-specific streaming metrics exposed via JMX in addition to the
 * standard {@link StreamingChangeEventSourceMetricsMXBean} attributes.
 */
public interface MilvusStreamingChangeEventSourceMetricsMXBean extends StreamingChangeEventSourceMetricsMXBean {

    /**
     * @return {@code true} once the Kafka consumer's starting position has
     *         been resolved and assigned for the current streaming run,
     *         {@code false} before that or after streaming has stopped
     */
    boolean isPositionResolved();
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetricsMXBean;

/**
 * Milvus-specific snapshot metrics exposed via JMX in addition to the
 * standard {@link SnapshotChangeEventSourceMetricsMXBean} attributes.
 */
public interface MilvusSnapshotChangeEventSourceMetricsMXBean extends SnapshotChangeEventSourceMetricsMXBean {

    /**
     * @return the etcd checkpoint {@code guarantee_ts} TSO used as the
     *         snapshot's anchor point, or 0 if no checkpoint was available
     */
    long getGuaranteeTso();

    /**
     * @return the system clock timestamp (ms since epoch) when the snapshot
     *         phase started, or 0 if the snapshot has not yet started
     */
    long getSnapshotStartTs();
}

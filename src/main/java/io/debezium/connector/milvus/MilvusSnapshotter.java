/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.debezium.snapshot.SnapshotterService;
import io.debezium.snapshot.spi.SnapshotLock;
import io.debezium.snapshot.spi.SnapshotQuery;
import io.debezium.spi.snapshot.Snapshotter;

/**
 * Snapshotter for the Milvus connector.
 *
 * <p>Triggers a data snapshot when no prior offset exists (fresh connector start)
 * or when a snapshot is already in progress (resumed after a crash mid-snapshot).
 * The second gate — {@link MilvusSnapshotChangeEventSource#getSnapshottingTask} —
 * respects the configured {@code snapshot.mode} (e.g. {@code never}) and whether
 * the offset context already has {@code snapshot_completed=true}.</p>
 *
 * <p>Schema snapshot is deliberately skipped: Milvus schemas are inferred
 * dynamically from the first Insert event (streaming) or from the Milvus
 * metadata API (snapshot), so no separate DDL-style schema snapshot is needed.</p>
 */
public class MilvusSnapshotter implements Snapshotter {

    @Override
    public String name() {
        return "milvus";
    }

    @Override
    public void configure(Map<String, ?> config) {
    }

    @Override
    public boolean shouldSnapshotData(boolean offsetExists, boolean snapshotInProgress) {
        return !offsetExists || snapshotInProgress;
    }

    @Override
    public boolean shouldSnapshotSchema(boolean offsetExists, boolean snapshotInProgress) {
        return false;
    }

    @Override
    public boolean shouldStream() {
        return true;
    }

    @Override
    public boolean shouldSnapshotOnSchemaError() {
        return false;
    }

    @Override
    public boolean shouldSnapshotOnDataError() {
        return false;
    }

    /**
     * Creates a minimal {@link SnapshotterService} using this snapshotter
     * with no-op query and lock implementations.
     */
    public static SnapshotterService createService() {
        return new SnapshotterService(
                new MilvusSnapshotter(),
                new NoOpSnapshotQuery(),
                new NoOpSnapshotLock());
    }

    /**
     * No-op snapshot query — Milvus snapshot queries will be wired
     * via the SDK when the snapshot layer is implemented.
     */
    static class NoOpSnapshotQuery implements SnapshotQuery {

        @Override
        public String name() {
            return "milvus";
        }

        @Override
        public void configure(Map<String, ?> config) {
        }

        @Override
        public Optional<String> snapshotQuery(String tableName, List<String> columns) {
            return Optional.empty();
        }
    }

    /**
     * No-op snapshot lock — Milvus does not require table locks for
     * point-in-time snapshots.
     */
    static class NoOpSnapshotLock implements SnapshotLock {

        @Override
        public String name() {
            return "milvus";
        }

        @Override
        public void configure(Map<String, ?> config) {
        }

        @Override
        public Optional<String> tableLockingStatement(Duration lockTimeout, String tableId) {
            return Optional.empty();
        }
    }
}

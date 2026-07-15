/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

/**
 * Unit tests for {@link MilvusSnapshotter}.
 *
 * <p>Verifies the snapshot eligibility logic in {@link MilvusSnapshotter#shouldSnapshotData}
 * and confirms that schema and on-error snapshots are correctly suppressed.</p>
 */
public class MilvusSnapshotterTest {

    private final MilvusSnapshotter snapshotter = new MilvusSnapshotter();

    @Test
    @FixFor("debezium/dbz#2230")
    void shouldSnapshotDataWhenNoOffsetExists() {
        assertThat(snapshotter.shouldSnapshotData(false, false)).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2230")
    void shouldSnapshotDataWhenSnapshotIsInProgress() {
        assertThat(snapshotter.shouldSnapshotData(true, true)).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2230")
    void shouldNotSnapshotDataWhenOffsetExistsAndSnapshotNotInProgress() {
        assertThat(snapshotter.shouldSnapshotData(true, false)).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2230")
    void shouldNeverSnapshotSchema() {
        assertThat(snapshotter.shouldSnapshotSchema(false, false)).isFalse();
        assertThat(snapshotter.shouldSnapshotSchema(true, false)).isFalse();
        assertThat(snapshotter.shouldSnapshotSchema(true, true)).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2230")
    void shouldAlwaysStream() {
        assertThat(snapshotter.shouldStream()).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2230")
    void shouldNotSnapshotOnSchemaError() {
        assertThat(snapshotter.shouldSnapshotOnSchemaError()).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2230")
    void shouldNotSnapshotOnDataError() {
        assertThat(snapshotter.shouldSnapshotOnDataError()).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2230")
    void nameShouldBeMilvus() {
        assertThat(snapshotter.name()).isEqualTo("milvus");
    }
}

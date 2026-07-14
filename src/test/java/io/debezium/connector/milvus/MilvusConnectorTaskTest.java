/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

public class MilvusConnectorTaskTest {

    @Test
    @FixFor("debezium/dbz#2028")
    void shouldExposeVersion() {
        MilvusConnectorTask task = new MilvusConnectorTask();
        assertThat(task.version()).isEqualTo(Module.version());
    }

    @Test
    @FixFor("debezium/dbz#2028")
    void shouldExposeConfigurationFields() {
        MilvusConnectorTask task = new MilvusConnectorTask();
        assertThat(task.getAllConfigurationFields()).isSameAs(MilvusConnectorConfig.ALL_FIELDS);
    }

    @Test
    @FixFor("debezium/dbz#2028")
    void shouldStopCleanlyWithoutStart() {
        MilvusConnectorTask task = new MilvusConnectorTask();
        assertThatNoException().isThrownBy(task::doStop);
    }

    @Test
    @FixFor("debezium/dbz#2028")
    void shouldReturnErrorHandlerBeforeStart() {
        MilvusConnectorTask task = new MilvusConnectorTask();
        assertThat(task.getErrorHandler()).isEmpty();
    }
}

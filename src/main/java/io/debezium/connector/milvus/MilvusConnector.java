/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.common.BaseSourceConnector;
import io.debezium.util.Strings;

/**
 * Source connector entrypoint for the Milvus Debezium connector.
 */
public class MilvusConnector extends BaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(MilvusConnector.class);

    private Map<String, String> properties;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Class<MilvusConnectorTask> taskClass() {
        return MilvusConnectorTask.class;
    }

    @Override
    public void start(Map<String, String> props) {
        LOGGER.info("Starting Milvus connector");
        this.properties = Collections.unmodifiableMap(props);
        Configuration config = Configuration.from(props);
        if (Strings.isNullOrBlank(config.getString(MilvusConnectorConfig.MILVUS_URI))) {
            throw new IllegalStateException("milvus.uri must be specified");
        }
        if (Strings.isNullOrBlank(config.getString(MilvusConnectorConfig.TOPIC_PREFIX))) {
            throw new IllegalStateException("topic.prefix must be specified");
        }
        LOGGER.info("Milvus connector started successfully");
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks > 1) {
            LOGGER.warn("Milvus connector supports only a single task; ignoring maxTasks={}", maxTasks);
        }
        return Collections.singletonList(properties);
    }

    @Override
    public void stop() {
        LOGGER.info("Stopping Milvus connector");
        // TODO: close MetadataClient, Connection, or other opened resources
    }

    @Override
    public ConfigDef config() {
        return MilvusConnectorConfig.configDef();
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return Map.of();
    }
}

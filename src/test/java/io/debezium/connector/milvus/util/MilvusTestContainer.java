/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus.util;

/**
 * Manages the Milvus integration-test infrastructure.
 *
 * <p>
 * Containers are started by the fabric8 docker-maven-plugin in the
 * {@code pre-integration-test} phase and stopped in {@code post-integration-test}.
 * This class simply reads the endpoints injected by the plugin via Maven
 * failsafe system properties ({@code kafka.bootstrap.servers} and
 * {@code milvus.uri}).
 * </p>
 */
public final class MilvusTestContainer {

    private MilvusTestContainer() {
    }

    /** No-op: containers are started by the docker-maven-plugin. */
    public static synchronized void startAll() {
    }

    /** No-op: containers are stopped by the docker-maven-plugin. */
    public static synchronized void stopAll() {
    }

    /**
     * Returns the Kafka bootstrap servers injected by the docker-maven-plugin via
     * the {@code kafka.bootstrap.servers} system property.
     */
    public static String kafkaBootstrapServers() {
        String val = System.getProperty("kafka.bootstrap.servers");
        if (val == null || val.isBlank()) {
            throw new IllegalStateException(
                    "System property 'kafka.bootstrap.servers' is not set. "
                            + "Run integration tests via 'mvn verify' so the docker-maven-plugin sets it.");
        }
        return val;
    }

    /**
     * Returns the Milvus URI injected by the docker-maven-plugin via
     * the {@code milvus.uri} system property.
     */
    public static String milvusUri() {
        String val = System.getProperty("milvus.uri");
        if (val == null || val.isBlank()) {
            throw new IllegalStateException(
                    "System property 'milvus.uri' is not set. "
                            + "Run integration tests via 'mvn verify' so the docker-maven-plugin sets it.");
        }
        return val;
    }
}

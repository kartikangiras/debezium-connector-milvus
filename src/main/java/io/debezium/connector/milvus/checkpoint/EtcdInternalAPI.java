/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus.checkpoint;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks code that reads from Milvus etcd internals directly.
 *
 * <p>Milvus considers its etcd key layout and value encoding an internal
 * implementation detail that can change between minor versions. Prefer the
 * Milvus gRPC API wherever possible and use direct etcd access only when no
 * public API exposes the required data (e.g., channel checkpoints).</p>
 */
@Documented
@Retention(RetentionPolicy.SOURCE)
@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.CONSTRUCTOR })
public @interface EtcdInternalAPI {

    /**
     * Describes the stability risk or mitigation for this direct etcd usage.
     */
    String value() default "";
}

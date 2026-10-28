/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka.transcode;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.metadata.TableInfo;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.time.Duration;
import java.util.function.Supplier;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Bounded cache of Kafka write plans keyed by table ID and schema ID, with idle expiration.
 *
 * <p>Table and schema IDs identify an immutable schema. Compilation is atomic for each key.
 */
@Internal
@ThreadSafe
final class KafkaCompiledWritePlanCache {

    static final long DEFAULT_MAXIMUM_SIZE = 1_000L;
    private static final Duration EXPIRE_AFTER_ACCESS = Duration.ofMinutes(5);

    private final Cache<String, KafkaTopicWritePlan> plans;

    public KafkaCompiledWritePlanCache() {
        this(DEFAULT_MAXIMUM_SIZE);
    }

    @VisibleForTesting
    KafkaCompiledWritePlanCache(long maximumSize) {
        this(maximumSize, Ticker.systemTicker());
    }

    @VisibleForTesting
    KafkaCompiledWritePlanCache(long maximumSize, Ticker ticker) {
        checkArgument(
                maximumSize > 0, "Kafka compiled write plan cache size must be greater than zero.");
        this.plans =
                Caffeine.newBuilder()
                        .maximumSize(maximumSize)
                        .expireAfterAccess(EXPIRE_AFTER_ACCESS)
                        .ticker(ticker)
                        .build();
    }

    /**
     * Returns the cached write plan for the given table and schema IDs, or compiles a new one if
     * none is cached.
     */
    public KafkaTopicWritePlan getOrCompile(
            TableInfo tableInfo, Supplier<KafkaTopicWritePlan> planCompiler) {
        checkNotNull(planCompiler);
        return plans.get(key(tableInfo), ignored -> checkNotNull(planCompiler.get()));
    }

    /** Invalidates the cached write plan for the given table and schema IDs, if any. */
    public void invalidate(TableInfo tableInfo) {
        plans.invalidate(key(tableInfo));
    }

    /**
     * Returns the cached write plan for the given table and schema IDs, or {@code null} if none is
     * cached.
     */
    @Nullable
    KafkaTopicWritePlan getIfPresent(TableInfo tableInfo) {
        return plans.getIfPresent(key(tableInfo));
    }

    @VisibleForTesting
    long estimatedSize() {
        plans.cleanUp();
        return plans.estimatedSize();
    }

    private static String key(TableInfo tableInfo) {
        checkNotNull(tableInfo);
        return tableInfo.getTableId() + "-" + tableInfo.getSchemaId();
    }
}

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
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Bounded, single-flight cache for immutable Kafka topic write plans.
 *
 * <p>The bounded map is keyed by {@link TablePath}, while each slot fences its plan with the exact
 * {@link TableInfo} instance from the authoritative metadata cache. This avoids the recursive and
 * allocation-heavy {@link TableInfo#hashCode()} on every Produce request. An equivalent but
 * separately constructed {@code TableInfo} is deliberately treated as a cache miss. In the
 * production path, {@code KafkaTableInfoCache} returns the same instance only when the complete
 * {@code GetTableInfo} response is unchanged.
 *
 * <p>Consequently a delete/recreate, schema change, or property-only Kafka contract change receives
 * a different instance and cannot reuse an older plan. Slots are synchronized individually so a
 * concurrent cold start compiles once per table version.
 */
@Internal
@ThreadSafe
final class KafkaCompiledWritePlanCache {

    static final long DEFAULT_MAXIMUM_SIZE = 1_000L;

    private final long maximumSize;
    private final ConcurrentMap<TablePath, CacheSlot> slots;
    private final Object slotCreationLock = new Object();

    KafkaCompiledWritePlanCache() {
        this(DEFAULT_MAXIMUM_SIZE);
    }

    KafkaCompiledWritePlanCache(long maximumSize) {
        checkArgument(
                maximumSize > 0, "Kafka compiled write plan cache size must be greater than zero.");
        this.maximumSize = maximumSize;
        this.slots = new ConcurrentHashMap<>();
    }

    KafkaTopicWritePlan getIfPresent(TableInfo tableInfo) {
        checkNotNull(tableInfo);
        CacheSlot slot = slots.get(tableInfo.getTablePath());
        if (slot == null) {
            return null;
        }
        synchronized (slot) {
            if (slots.get(tableInfo.getTablePath()) != slot) {
                return null;
            }
            return slot.tableInfo == tableInfo ? slot.plan : null;
        }
    }

    KafkaTopicWritePlan getOrCompile(
            TableInfo tableInfo, Supplier<KafkaTopicWritePlan> planCompiler) {
        checkNotNull(tableInfo);
        checkNotNull(planCompiler);

        TablePath tablePath = tableInfo.getTablePath();
        while (true) {
            CacheSlot slot = getOrCreateSlot(tablePath);
            synchronized (slot) {
                if (slots.get(tablePath) != slot) {
                    continue;
                }
                if (slot.tableInfo == tableInfo && slot.plan != null) {
                    return slot.plan;
                }
                KafkaTopicWritePlan plan = checkNotNull(planCompiler.get());
                slot.tableInfo = tableInfo;
                slot.plan = plan;
                return plan;
            }
        }
    }

    void invalidate(TableInfo tableInfo) {
        checkNotNull(tableInfo);
        TablePath tablePath = tableInfo.getTablePath();
        CacheSlot slot = slots.get(tablePath);
        if (slot == null) {
            return;
        }
        synchronized (slot) {
            if (slot.tableInfo != tableInfo) {
                return;
            }
            slot.tableInfo = null;
            slot.plan = null;
            slots.remove(tablePath, slot);
        }
    }

    long estimatedSize() {
        return slots.size();
    }

    private CacheSlot getOrCreateSlot(TablePath tablePath) {
        CacheSlot slot = slots.get(tablePath);
        if (slot != null) {
            return slot;
        }
        synchronized (slotCreationLock) {
            slot = slots.get(tablePath);
            if (slot != null) {
                return slot;
            }
            if (slots.size() >= maximumSize) {
                evictOneSlot();
            }
            slot = new CacheSlot();
            slots.put(tablePath, slot);
            return slot;
        }
    }

    private void evictOneSlot() {
        for (Map.Entry<TablePath, CacheSlot> entry : slots.entrySet()) {
            CacheSlot slot = entry.getValue();
            synchronized (slot) {
                if (slots.remove(entry.getKey(), slot)) {
                    return;
                }
            }
        }
    }

    private static final class CacheSlot {
        private TableInfo tableInfo;
        private KafkaTopicWritePlan plan;
    }
}

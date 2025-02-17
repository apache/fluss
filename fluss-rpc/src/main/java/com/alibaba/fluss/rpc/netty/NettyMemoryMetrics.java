/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.rpc.netty;

import com.alibaba.fluss.metrics.Gauge;
import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.PoolArenaMetric;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.PooledByteBufAllocator;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.PooledByteBufAllocatorMetric;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** A Netty memory metrics class to collect metrics from Netty {@link PooledByteBufAllocator}. */
public class NettyMemoryMetrics {

    private final MetricGroup metricGroup;
    private final PooledByteBufAllocator pooledAllocator;
    private final boolean verboseMetricsEnabled;

    public static final String NETTY_MEMORY_METRIC_GROUP = "nettyMemory";
    public static final String NETTY_MEMORY_USED_HEAP_MEMORY = "usedHeapMemory";
    public static final String NETTY_MEMORY_USED_DIRECT_MEMORY = "usedDirectMemory";
    public static final String NETTY_MEMORY_USED_HEAP_ARENAS = "numHeapArenas";
    public static final String NETTY_MEMORY_USED_DIRECT_ARENAS = "numDirectArenas";
    public static final String NETTY_MEMORY_TINY_CACHE_SIZE = "tinyCacheSize";
    public static final String NETTY_MEMORY_SMALL_CACHE_SIZE = "smallCacheSize";
    public static final String NETTY_MEMORY_NORMAL_CACHE_SIZE = "normalCacheSize";
    public static final String NETTY_MEMORY_NUM_THREAD_LOCAL_CACHES = "numThreadLocalCaches";
    public static final String NETTY_MEMORY_CHUNK_SIZE = "chunkSize";
    public static final String NETTY_MEMORY_HEAP_ARENA = "heapArena";
    public static final String NETTY_MEMORY_DIRECT_ARENA = "directArena";

    public static final Set<String> VERBOSE_METRICS = new HashSet<>();

    static {
        VERBOSE_METRICS.addAll(
                Arrays.asList(
                        "numAllocations",
                        "numTinyAllocations",
                        "numSmallAllocations",
                        "numNormalAllocations",
                        "numHugeAllocations",
                        "numDeallocations",
                        "numTinyDeallocations",
                        "numSmallDeallocations",
                        "numNormalDeallocations",
                        "numHugeDeallocations",
                        "numActiveAllocations",
                        "numActiveTinyAllocations",
                        "numActiveSmallAllocations",
                        "numActiveNormalAllocations",
                        "numActiveHugeAllocations",
                        "numActiveBytes"));
    }

    public NettyMemoryMetrics(
            MetricGroup metricGroup,
            PooledByteBufAllocator pooledAllocator,
            boolean verboseMetricsEnabled) {
        this.metricGroup = metricGroup.addGroup(NETTY_MEMORY_METRIC_GROUP);
        this.pooledAllocator = pooledAllocator;
        this.verboseMetricsEnabled = verboseMetricsEnabled;
        registerMetrics();
    }

    private void registerMetrics() {
        PooledByteBufAllocatorMetric pooledAllocatorMetric = pooledAllocator.metric();
        metricGroup.<Long, Gauge<Long>>gauge(
                NETTY_MEMORY_USED_HEAP_MEMORY, pooledAllocatorMetric::usedHeapMemory);
        metricGroup.<Long, Gauge<Long>>gauge(
                NETTY_MEMORY_USED_DIRECT_MEMORY, pooledAllocatorMetric::usedDirectMemory);
        metricGroup.<Integer, Gauge<Integer>>gauge(
                NETTY_MEMORY_USED_HEAP_ARENAS, pooledAllocatorMetric::numHeapArenas);
        metricGroup.<Integer, Gauge<Integer>>gauge(
                NETTY_MEMORY_USED_DIRECT_ARENAS, pooledAllocatorMetric::numDirectArenas);
        metricGroup.<Integer, Gauge<Integer>>gauge(
                NETTY_MEMORY_TINY_CACHE_SIZE, pooledAllocatorMetric::tinyCacheSize);
        metricGroup.<Integer, Gauge<Integer>>gauge(
                NETTY_MEMORY_SMALL_CACHE_SIZE, pooledAllocatorMetric::smallCacheSize);
        metricGroup.<Integer, Gauge<Integer>>gauge(
                NETTY_MEMORY_NORMAL_CACHE_SIZE, pooledAllocatorMetric::normalCacheSize);
        metricGroup.<Integer, Gauge<Integer>>gauge(
                NETTY_MEMORY_NUM_THREAD_LOCAL_CACHES, pooledAllocatorMetric::numThreadLocalCaches);
        metricGroup.<Integer, Gauge<Integer>>gauge(
                NETTY_MEMORY_CHUNK_SIZE, pooledAllocatorMetric::chunkSize);
        if (verboseMetricsEnabled) {
            int heapArenaIndex = 0;
            for (PoolArenaMetric poolArenaMetric : pooledAllocatorMetric.heapArenas()) {
                registerArenaMetric(
                        metricGroup, poolArenaMetric, NETTY_MEMORY_HEAP_ARENA + heapArenaIndex);
                heapArenaIndex++;
            }
            int directArenaIndex = 0;
            for (PoolArenaMetric poolArenaMetric : pooledAllocatorMetric.directArenas()) {
                registerArenaMetric(
                        metricGroup, poolArenaMetric, NETTY_MEMORY_DIRECT_ARENA + directArenaIndex);
                directArenaIndex++;
            }
        }
    }

    private void registerArenaMetric(
            final MetricGroup metricGroup,
            final PoolArenaMetric arenaMetric,
            final String arenaName) {
        MetricGroup arenaGroup = metricGroup.addGroup(arenaName);
        for (String methodName : VERBOSE_METRICS) {
            Method method;
            try {
                method = PoolArenaMetric.class.getMethod(methodName);
            } catch (Exception e) {
                // Failed to find metric related method, ignore this metric.
                continue;
            }
            if (!Modifier.isPublic(method.getModifiers())) {
                // Ignore non-public methods.
                continue;
            }
            Class<?> returnType = method.getReturnType();
            String metricName = method.getName();
            if (returnType.equals(int.class)) {
                arenaGroup.<Integer, Gauge<Integer>>gauge(
                        metricName,
                        () -> {
                            try {
                                return (Integer) method.invoke(arenaMetric);
                            } catch (Exception e) {
                                return -1; // Swallow the exceptions.
                            }
                        });
            } else if (returnType.equals(long.class)) {
                arenaGroup.<Long, Gauge<Long>>gauge(
                        metricName,
                        () -> {
                            try {
                                return (Long) method.invoke(arenaMetric);
                            } catch (Exception e) {
                                return -1L; // Swallow the exceptions.
                            }
                        });
            }
        }
    }

    public static void createNettyMemoryMetrics(
            MetricGroup metricGroup,
            PooledByteBufAllocator pooledAllocator,
            boolean verboseMetricsEnabled) {
        new NettyMemoryMetrics(metricGroup, pooledAllocator, verboseMetricsEnabled);
    }
}

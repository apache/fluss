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

package org.apache.fluss.server.metrics.group;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;
import org.apache.fluss.server.kv.rocksdb.RocksDBStatistics;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link TabletServerMetricGroup}. */
class TabletServerMetricGroupTest {

    @Test
    void testTotalMemoryCountsSharedCacheOnceAcrossRocksDBInstances() {
        TabletServerMetricGroup metricGroup =
                new TabletServerMetricGroup(
                        NOPMetricRegistry.INSTANCE, "cluster", "rack", "host", 0);
        TablePath tablePath = TablePath.of("database", "table");
        RocksDBStatistics firstStatistics = mock(RocksDBStatistics.class);
        RocksDBStatistics secondStatistics = mock(RocksDBStatistics.class);
        when(firstStatistics.getTotalMemoryUsage()).thenReturn(10L);
        when(secondStatistics.getTotalMemoryUsage()).thenReturn(20L);

        metricGroup
                .addTableBucketMetricGroup(
                        PhysicalTablePath.of(tablePath), new TableBucket(1L, 0), true)
                .registerRocksDBStatistics(firstStatistics);
        metricGroup
                .addTableBucketMetricGroup(
                        PhysicalTablePath.of(tablePath), new TableBucket(1L, 1), true)
                .registerRocksDBStatistics(secondStatistics);
        metricGroup.setSharedBlockCacheMetrics(() -> 100L, () -> 5L, 256L);

        assertThat(gaugeValue(metricGroup, MetricNames.ROCKSDB_MEMORY_USAGE_TOTAL)).isEqualTo(130L);
        assertThat(gaugeValue(metricGroup, MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_USAGE))
                .isEqualTo(100L);
        assertThat(gaugeValue(metricGroup, MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_PINNED_USAGE))
                .isEqualTo(5L);
        assertThat(gaugeValue(metricGroup, MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_CAPACITY))
                .isEqualTo(256L);
    }

    @Test
    void testSharedWriteBufferMetrics() {
        TabletServerMetricGroup metricGroup =
                new TabletServerMetricGroup(
                        NOPMetricRegistry.INSTANCE, "cluster", "rack", "host", 0);
        metricGroup.setSharedWriteBufferMetrics(() -> 40L, 128L);

        assertThat(gaugeValue(metricGroup, MetricNames.ROCKSDB_SHARED_WRITE_BUFFER_USAGE))
                .isEqualTo(40L);
        assertThat(gaugeValue(metricGroup, MetricNames.ROCKSDB_SHARED_WRITE_BUFFER_CAPACITY))
                .isEqualTo(128L);
    }

    @Test
    void testWalMemoryPoolMetrics() {
        TabletServerMetricGroup metricGroup =
                new TabletServerMetricGroup(
                        NOPMetricRegistry.INSTANCE, "cluster", "rack", "host", 0);
        metricGroup.registerKvWalMemoryPoolMetrics(() -> 100L, 256L);

        assertThat(gaugeValue(metricGroup, MetricNames.KV_WAL_MEMORY_POOL_USAGE)).isEqualTo(100L);
        assertThat(gaugeValue(metricGroup, MetricNames.KV_WAL_MEMORY_POOL_CAPACITY))
                .isEqualTo(256L);
    }

    @Test
    void testNativeRocksDBCounterLabels() {
        CollectingRegistry registry = new CollectingRegistry();
        TabletServerMetricGroup metrics =
                new TabletServerMetricGroup(registry, "cluster", "rack", "host", 0);
        assertThat(registry.counters).hasSize(10);
        registry.labels.forEach(
                labels -> {
                    assertThat(labels)
                            .doesNotContainKeys("database", "table", "bucket", "bucket_id");
                    assertThat(labels).containsKeys("cluster_id", "server_id");
                });
        assertThat(registry.labels)
                .filteredOn(labels -> labels.containsKey("operation"))
                .extracting(labels -> labels.get("operation"))
                .containsOnly("get", "multiGet");
        assertThat(registry.value(MetricNames.ROCKSDB_NATIVE_READ_TIME_US_TOTAL, "get")).isZero();
        assertThat(registry.value(MetricNames.ROCKSDB_NATIVE_READ_COUNT_TOTAL, "get")).isZero();
        assertThat(registry.value(MetricNames.ROCKSDB_NATIVE_READ_COUNT_TOTAL, "multiGet"))
                .isZero();
        assertThat(registry.value(MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_HITS_TOTAL, "data"))
                .isZero();
    }

    private static Object gaugeValue(TabletServerMetricGroup metricGroup, String metricName) {
        return ((Gauge<?>) metricGroup.getMetrics().get(metricName)).getValue();
    }

    private static class CollectingRegistry extends NOPMetricRegistry {
        private final Map<String, Counter> counters = new HashMap<>();
        private final List<Map<String, String>> labels = new ArrayList<>();

        @Override
        public void register(Metric metric, String name, AbstractMetricGroup group) {
            if (name.equals(MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_HITS_TOTAL)
                    || name.equals(MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_MISSES_TOTAL)
                    || name.equals(MetricNames.ROCKSDB_NATIVE_READ_COUNT_TOTAL)
                    || name.equals(MetricNames.ROCKSDB_NATIVE_READ_TIME_US_TOTAL)) {
                Map<String, String> variables = group.getAllVariables();
                String label = variables.getOrDefault("operation", variables.get("block_type"));
                counters.put(name + ":" + label, (Counter) metric);
                labels.add(new HashMap<>(variables));
            }
        }

        private long value(String name, String label) {
            return counters.get(name + ":" + label).getCount();
        }
    }
}

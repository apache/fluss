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
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;
import org.apache.fluss.server.kv.rocksdb.RocksDBStatistics;

import org.junit.jupiter.api.Test;

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

    private static Object gaugeValue(TabletServerMetricGroup metricGroup, String metricName) {
        return ((Gauge<?>) metricGroup.getMetrics().get(metricName)).getValue();
    }
}

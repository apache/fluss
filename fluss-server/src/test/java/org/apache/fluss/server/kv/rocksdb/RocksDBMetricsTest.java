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

package org.apache.fluss.server.kv.rocksdb;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;
import org.apache.fluss.server.kv.KvManager;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;

import io.github.fluss_contrib.rocksdb.Cache;
import io.github.fluss_contrib.rocksdb.FlushOptions;
import io.github.fluss_contrib.rocksdb.HyperClockCache;
import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.Statistics;
import io.github.fluss_contrib.rocksdb.TickerType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the server aggregation, collection lifecycle and read execution contracts. */
class RocksDBMetricsTest {
    @BeforeAll
    static void loadLibrary() {
        RocksDB.loadLibrary();
    }

    @Test
    void testReadAggregationAndFailedAttempt(@TempDir Path directory) throws Exception {
        CollectingRegistry registry = new CollectingRegistry();
        RocksDBMetrics metrics =
                new TabletServerMetricGroup(registry, "cluster", "rack", "host", 0)
                        .getRocksDBMetrics();
        assertThat(registry.counters).hasSize(12);
        registry.labels.forEach(
                labels -> {
                    assertThat(labels)
                            .doesNotContainKeys("database", "table", "bucket", "bucket_id");
                    assertThat(labels).containsKeys("cluster_id", "server_id");
                });
        try (RocksDBKv first = open(directory.resolve("first"), null);
                RocksDBKv second = open(directory.resolve("second"), null)) {
            first.initializeMetrics(metrics, false);
            second.initializeMetrics(metrics, false);
            byte[] key = {1};
            first.put(key, new byte[] {2});
            assertThat(first.get(key)).containsExactly((byte) 2);
            assertThat(second.get(key)).isNull();
            assertThat(first.multiGet(Arrays.asList(key, new byte[] {3})))
                    .containsExactly(new byte[] {2}, null);
            try (RocksDBKv.PrefixLookup lookup = first.openPrefixLookup(key)) {
                assertThat(lookup.readAll()).containsExactly(new byte[] {2});
                lookup.close();
                assertThatThrownBy(lookup::readAll).hasMessageContaining("closed");
            }
            assertThat(registry.value(MetricNames.ROCKSDB_READ_EXECUTION_COUNT_TOTAL, "get"))
                    .isEqualTo(2);
            assertThat(registry.value(MetricNames.ROCKSDB_READ_EXECUTION_COUNT_TOTAL, "multiGet"))
                    .isEqualTo(1);
            assertThat(registry.value(MetricNames.ROCKSDB_READ_EXECUTION_COUNT_TOTAL, "prefixScan"))
                    .isEqualTo(2);
            assertThat(registry.value(MetricNames.ROCKSDB_READ_EXECUTION_TIME_US_TOTAL, "get"))
                    .isPositive();
            assertThat(registry.value(MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_HITS_TOTAL, "data"))
                    .isZero();
            assertThat(registry.value(MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_MISSES_TOTAL, "data"))
                    .isZero();
        }
        assertThat(registry.counters).hasSize(12);
    }

    @Test
    void testTimeAccumulatesBeforeConversion() {
        CollectingRegistry registry = new CollectingRegistry();
        RocksDBMetrics metrics =
                new TabletServerMetricGroup(registry, "cluster", "rack", "host", 0)
                        .getRocksDBMetrics();
        metrics.recordRead(RocksDBMetrics.ReadOperation.GET, 600);
        metrics.recordRead(RocksDBMetrics.ReadOperation.GET, 600);
        assertThat(registry.value(MetricNames.ROCKSDB_READ_EXECUTION_TIME_US_TOTAL, "get"))
                .isEqualTo(1);
    }

    @Test
    void testFailedCacheSamplePreservesCursorAndClosedDbTotals() {
        CollectingRegistry registry = new CollectingRegistry();
        RocksDBMetrics metrics =
                new TabletServerMetricGroup(registry, "cluster", "rack", "host", 0)
                        .getRocksDBMetrics();
        try (TestingStatistics first = new TestingStatistics();
                TestingStatistics second = new TestingStatistics()) {
            Runnable closeFirst = metrics.registerSharedCache(first);
            Runnable closeSecond = metrics.registerSharedCache(second);
            first.hits = 10;
            second.hits = 20;
            assertThat(registry.value(MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_HITS_TOTAL, "data"))
                    .isEqualTo(30);
            first.fail = true;
            first.hits = 15;
            second.hits = 25;
            assertThat(registry.value(MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_HITS_TOTAL, "data"))
                    .isEqualTo(35);
            first.fail = false;
            assertThat(registry.value(MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_HITS_TOTAL, "data"))
                    .isEqualTo(40);
            first.hits = 17;
            closeFirst.run();
            closeFirst.run();
            first.close();
            assertThat(registry.value(MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_HITS_TOTAL, "data"))
                    .isEqualTo(42);
            second.hits = 1;
            assertThat(registry.value(MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_HITS_TOTAL, "data"))
                    .isEqualTo(43);
            closeSecond.run();
        }
    }

    @Test
    void testNativeCacheCountersSurviveDbClose(@TempDir Path directory) throws Exception {
        CollectingRegistry registry = new CollectingRegistry();
        RocksDBMetrics metrics =
                new TabletServerMetricGroup(registry, "cluster", "rack", "host", 0)
                        .getRocksDBMetrics();
        try (Cache cache = new HyperClockCache(1024 * 1024, 0, -1, false)) {
            long expectedHits;
            long expectedMisses;
            try (RocksDBKv kv = open(directory, cache)) {
                kv.initializeMetrics(metrics, true);
                byte[] key = {1};
                kv.put(key, new byte[] {2});
                try (FlushOptions flush = new FlushOptions().setWaitForFlush(true)) {
                    kv.getDb().flush(flush);
                }
                kv.get(key);
                kv.get(key);
                expectedHits = kv.getStatistics().getTickerCount(TickerType.BLOCK_CACHE_DATA_HIT);
                expectedMisses =
                        kv.getStatistics().getTickerCount(TickerType.BLOCK_CACHE_DATA_MISS);
                assertThat(expectedHits).isPositive();
                // Do not scrape before close: the final collection must retain unsampled events.
            }
            assertThat(registry.value(MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_HITS_TOTAL, "data"))
                    .isEqualTo(expectedHits);
            assertThat(registry.value(MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_MISSES_TOTAL, "data"))
                    .isEqualTo(expectedMisses);
        }
    }

    private static RocksDBKv open(Path directory, Cache cache) throws Exception {
        RocksDBResourceContainer resources =
                new RocksDBResourceContainer(
                        new Configuration(),
                        directory.toFile(),
                        true,
                        KvManager.getDefaultRateLimiter(),
                        cache);
        return new RocksDBKvBuilder(directory.toFile(), resources, resources.getColumnOptions())
                .build();
    }

    private static class TestingStatistics extends Statistics {
        private long hits;
        private boolean fail;

        @Override
        public long getTickerCount(TickerType ticker) {
            if (fail) {
                throw new IllegalStateException("Statistics unavailable");
            }
            return ticker == TickerType.BLOCK_CACHE_DATA_HIT ? hits : 0;
        }
    }

    private static class CollectingRegistry extends NOPMetricRegistry {
        private final Map<String, Counter> counters = new HashMap<>();
        private final List<Map<String, String>> labels = new ArrayList<>();

        @Override
        public void register(Metric metric, String name, AbstractMetricGroup group) {
            if (name.equals(MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_HITS_TOTAL)
                    || name.equals(MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_MISSES_TOTAL)
                    || name.equals(MetricNames.ROCKSDB_READ_EXECUTION_COUNT_TOTAL)
                    || name.equals(MetricNames.ROCKSDB_READ_EXECUTION_TIME_US_TOTAL)) {
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

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

import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TabletServerMetricGroup}. */
class TabletServerMetricGroupTest {

    @Test
    void testSharedBlockCacheMetrics() {
        TestingMetricRegistry registry = new TestingMetricRegistry();
        TabletServerMetricGroup metricGroup =
                new TabletServerMetricGroup(registry, "cluster", "rack", "host", 0);

        metricGroup.setSharedBlockCacheUsageSuppliers(() -> 123L, () -> 45L);

        assertThat(registry.<Long>getGaugeValue(MetricNames.ROCKSDB_MEMORY_USAGE_TOTAL))
                .isEqualTo(123L);
        assertThat(registry.<Long>getGaugeValue(MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_USAGE))
                .isEqualTo(123L);
        assertThat(
                        registry.<Long>getGaugeValue(
                                MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_PINNED_USAGE))
                .isEqualTo(45L);
    }

    private static class TestingMetricRegistry implements MetricRegistry {
        private final Map<String, Metric> metrics = new HashMap<>();

        @Override
        public int getNumberReporters() {
            return 0;
        }

        @Override
        public void register(Metric metric, String metricName, AbstractMetricGroup group) {
            metrics.put(metricName, metric);
        }

        @Override
        public void unregister(Metric metric, String metricName, AbstractMetricGroup group) {
            metrics.remove(metricName);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return CompletableFuture.completedFuture(null);
        }

        @SuppressWarnings("unchecked")
        private <T> T getGaugeValue(String metricName) {
            return ((Gauge<T>) metrics.get(metricName)).getValue();
        }
    }
}

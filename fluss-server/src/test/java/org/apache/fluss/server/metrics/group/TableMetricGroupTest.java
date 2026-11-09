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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.MeterView;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.ThreadSafeSimpleCounter;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.kv.rocksdb.RocksDBKvBuilder;
import org.apache.fluss.server.kv.rocksdb.RocksDBResourceContainer;
import org.apache.fluss.server.kv.rocksdb.RocksDBStatistics;
import org.apache.fluss.server.kv.rocksdb.RocksDBStatisticsRegistry;
import org.apache.fluss.server.utils.ResourceGuard;

import io.github.fluss_contrib.rocksdb.HistogramType;
import io.github.fluss_contrib.rocksdb.Statistics;
import io.github.fluss_contrib.rocksdb.TickerType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/** Test for {@link TableMetricGroup}. */
class TableMetricGroupTest {

    @Test
    void testSharedStatisticsRemainAvailableWhenSelectedInstanceCloses(@TempDir File directory)
            throws Exception {
        RocksDBStatisticsRegistry registry = new RocksDBStatisticsRegistry();
        TableMetricGroup group =
                new TableMetricGroup(
                        NOPMetricRegistry.INSTANCE,
                        TablePath.of("db", "table"),
                        true,
                        TestingMetricGroups.TABLET_SERVER_METRICS);
        AtomicBoolean closeFirstReader = new AtomicBoolean(true);
        try (RocksDBKv first = createKv(new File(directory, "first"), registry);
                RocksDBKv second = createKv(new File(directory, "second"), registry)) {
            group.addBucketMetricGroup(null, new TableBucket(1, 0))
                    .registerRocksDBStatistics(
                            statisticsClosingOnFirstRead(first, closeFirstReader));
            group.addBucketMetricGroup(null, new TableBucket(1, 1))
                    .registerRocksDBStatistics(
                            statisticsClosingOnFirstRead(second, closeFirstReader));
            first.put(new byte[] {1}, new byte[] {2});
            second.put(new byte[] {3}, new byte[] {4});
            Statistics statistics = first.getStatistics();
            long expected = statistics.getTickerCount(TickerType.BYTES_WRITTEN);
            assertThat(expected).isPositive();

            assertThat(gaugeValue(group, MetricNames.ROCKSDB_BYTES_WRITTEN_TOTAL))
                    .isEqualTo(expected);
            assertThat(closeFirstReader).isFalse();
            assertThat(first.getResourceGuard().isClosed())
                    .isNotEqualTo(second.getResourceGuard().isClosed());
            assertThat(statistics.isOwningHandle()).isTrue();
            assertThat(gaugeValue(group, MetricNames.ROCKSDB_BYTES_WRITTEN_TOTAL))
                    .isEqualTo(expected);
        } finally {
            group.close();
        }
    }

    private RocksDBStatistics statisticsClosingOnFirstRead(
            RocksDBKv kv, AtomicBoolean closeFirstReader) {
        ResourceGuard guard =
                new ResourceGuard() {
                    @Override
                    public Lease acquireResource() throws IOException {
                        // Close the selected RocksDB instance immediately before acquiring its
                        // lease.
                        if (closeFirstReader.compareAndSet(true, false)) {
                            try {
                                kv.close();
                            } catch (Exception e) {
                                throw new IOException(e);
                            }
                        }
                        return kv.getResourceGuard().acquireResource();
                    }

                    @Override
                    public boolean isClosed() {
                        return kv.getResourceGuard().isClosed();
                    }
                };
        return new RocksDBStatistics(
                kv.getDb(),
                kv.getStatistics(),
                guard,
                kv.getDefaultColumnFamilyHandle(),
                kv.getBlockCache());
    }

    @Test
    void testSharedStatisticsAreCountedOnceButMemoryUsesEveryDb(@TempDir File directory)
            throws Exception {
        RocksDBStatisticsRegistry registry = new RocksDBStatisticsRegistry();
        TableMetricGroup group =
                new TableMetricGroup(
                        NOPMetricRegistry.INSTANCE,
                        TablePath.of("db", "table"),
                        true,
                        TestingMetricGroups.TABLET_SERVER_METRICS);
        try (RocksDBKv first = createKv(new File(directory, "first"), registry);
                RocksDBKv second = createKv(new File(directory, "second"), registry)) {
            RocksDBStatistics firstStats =
                    new RocksDBStatistics(
                            first.getDb(),
                            first.getStatistics(),
                            first.getResourceGuard(),
                            first.getDefaultColumnFamilyHandle(),
                            first.getBlockCache());
            RocksDBStatistics secondStats =
                    new RocksDBStatistics(
                            second.getDb(),
                            second.getStatistics(),
                            second.getResourceGuard(),
                            second.getDefaultColumnFamilyHandle(),
                            second.getBlockCache());
            group.addBucketMetricGroup(null, new TableBucket(1, 0))
                    .registerRocksDBStatistics(firstStats);
            group.addBucketMetricGroup("partition", new TableBucket(1, 2L, 1))
                    .registerRocksDBStatistics(secondStats);
            first.put(new byte[] {1}, new byte[] {2});
            second.put(new byte[] {3}, new byte[] {4, 5});
            first.get(new byte[] {1});
            second.get(new byte[] {3});

            Statistics statistics = first.getStatistics();
            assertThat(statistics).isSameAs(second.getStatistics());
            assertThat(statistics.getTickerCount(TickerType.BYTES_WRITTEN)).isPositive();
            assertThat(gaugeValue(group, MetricNames.ROCKSDB_BYTES_WRITTEN_TOTAL))
                    .isEqualTo(statistics.getTickerCount(TickerType.BYTES_WRITTEN));
            assertThat(gaugeValue(group, MetricNames.ROCKSDB_BYTES_READ_TOTAL))
                    .isEqualTo(statistics.getTickerCount(TickerType.BYTES_READ));
            assertThat(gaugeValue(group, MetricNames.ROCKSDB_GET_LATENCY_MICROS_P99))
                    .isEqualTo(
                            Math.round(
                                    statistics
                                            .getHistogramData(HistogramType.DB_GET)
                                            .getPercentile99()));
            assertThat(gaugeValue(group, MetricNames.ROCKSDB_MEMTABLE_MEMORY_USAGE_TOTAL))
                    .isEqualTo(
                            firstStats.getMemTableMemoryUsage()
                                    + secondStats.getMemTableMemoryUsage());

            assertThat(gaugeValue(group, MetricNames.ROCKSDB_WRITE_STALL_MICROS_MAX))
                    .isEqualTo(gaugeValue(group, MetricNames.ROCKSDB_WRITE_STALL_MICROS_TOTAL));
            assertThat(gaugeValue(group, MetricNames.ROCKSDB_GET_LATENCY_MICROS_MAX))
                    .isEqualTo(gaugeValue(group, MetricNames.ROCKSDB_GET_LATENCY_MICROS_P99));
            assertThat(gaugeValue(group, MetricNames.ROCKSDB_WRITE_LATENCY_MICROS_MAX))
                    .isEqualTo(gaugeValue(group, MetricNames.ROCKSDB_WRITE_LATENCY_MICROS_P99));
            assertThat(gaugeValue(group, MetricNames.ROCKSDB_COMPACTION_TIME_MICROS_MAX))
                    .isEqualTo(gaugeValue(group, MetricNames.ROCKSDB_COMPACTION_TIME_MICROS_P99));

            // Reporters can track metric registrations by object identity.
            assertThat(group.getMetrics().get(MetricNames.ROCKSDB_WRITE_STALL_MICROS_MAX))
                    .isNotSameAs(
                            group.getMetrics().get(MetricNames.ROCKSDB_WRITE_STALL_MICROS_TOTAL));
            assertThat(group.getMetrics().get(MetricNames.ROCKSDB_GET_LATENCY_MICROS_MAX))
                    .isNotSameAs(
                            group.getMetrics().get(MetricNames.ROCKSDB_GET_LATENCY_MICROS_P99));
            assertThat(group.getMetrics().get(MetricNames.ROCKSDB_WRITE_LATENCY_MICROS_MAX))
                    .isNotSameAs(
                            group.getMetrics().get(MetricNames.ROCKSDB_WRITE_LATENCY_MICROS_P99));
            assertThat(group.getMetrics().get(MetricNames.ROCKSDB_COMPACTION_TIME_MICROS_MAX))
                    .isNotSameAs(
                            group.getMetrics().get(MetricNames.ROCKSDB_COMPACTION_TIME_MICROS_P99));

            first.close();
            assertThat(gaugeValue(group, MetricNames.ROCKSDB_BYTES_WRITTEN_TOTAL))
                    .isEqualTo(statistics.getTickerCount(TickerType.BYTES_WRITTEN));
            group.removeBucketMetricGroup(new TableBucket(1, 0));
            second.put(new byte[] {6}, new byte[] {7});
            assertThat(gaugeValue(group, MetricNames.ROCKSDB_BYTES_WRITTEN_TOTAL))
                    .isEqualTo(statistics.getTickerCount(TickerType.BYTES_WRITTEN));
        } finally {
            group.close();
        }
    }

    private RocksDBKv createKv(File directory, RocksDBStatisticsRegistry registry)
            throws Exception {
        RocksDBResourceContainer container =
                new RocksDBResourceContainer(new Configuration(), directory, true);
        container.useSharedStatistics(registry.acquire(1));
        return new RocksDBKvBuilder(directory, container, container.getColumnOptions()).build();
    }

    private long gaugeValue(TableMetricGroup group, String name) {
        return ((Number) ((Gauge<?>) group.getMetrics().get(name)).getValue()).longValue();
    }

    @Test
    void testRemoteKvCopyRateMetric() {
        MetricRegistry registry = mock(MetricRegistry.class);
        TableMetricGroup group =
                new TableMetricGroup(
                        registry,
                        TablePath.of("db", "pk_table"),
                        true,
                        TestingMetricGroups.TABLET_SERVER_METRICS);
        try {
            Counter counter = group.remoteKvCopyBytes();
            assertThat(counter).isInstanceOf(ThreadSafeSimpleCounter.class);
            ArgumentCaptor<AbstractMetricGroup> metricGroup =
                    ArgumentCaptor.forClass(AbstractMetricGroup.class);
            ArgumentCaptor<MeterView> meter = ArgumentCaptor.forClass(MeterView.class);
            verify(registry)
                    .register(
                            meter.capture(),
                            eq(MetricNames.REMOTE_KV_COPY_BYTES_RATE),
                            metricGroup.capture());
            verify(registry, never()).register(eq(counter), any(), any());
            assertThat(metricGroup.getValue().getMetrics())
                    .containsEntry(MetricNames.REMOTE_KV_COPY_BYTES_RATE, meter.getValue())
                    .doesNotContainKey("remoteKvCopyBytes");
            assertThat(metricGroup.getValue().getAllVariables())
                    .containsEntry("database", "db")
                    .containsEntry("table", "pk_table");
            assertThat(metricGroup.getValue().getScopeComponents()).endsWith("kv");
            counter.inc(120L);
            assertThat(meter.getValue().getCount()).isEqualTo(120L);
            meter.getValue().update();
            assertThat(meter.getValue().getRate()).isEqualTo(2.0);
            group.close();
            verify(registry, never()).unregister(eq(counter), any(), any());
            verify(registry)
                    .unregister(
                            meter.getValue(),
                            MetricNames.REMOTE_KV_COPY_BYTES_RATE,
                            metricGroup.getValue());
        } finally {
            group.close();
        }
    }

    @Test
    void testLogTableDoesNotExposeRemoteKvCopyRateMetric() {
        MetricRegistry registry = mock(MetricRegistry.class);
        TableMetricGroup group =
                new TableMetricGroup(
                        registry,
                        TablePath.of("db", "log_table"),
                        false,
                        TestingMetricGroups.TABLET_SERVER_METRICS);
        try {
            group.remoteKvCopyBytes().inc(120L);
            assertThat(group.remoteKvCopyBytes().getCount()).isZero();
            verify(registry, never()).register(any(), eq("remoteKvCopyBytes"), any());
            verify(registry, never())
                    .register(any(), eq(MetricNames.REMOTE_KV_COPY_BYTES_RATE), any());
        } finally {
            group.close();
        }
    }

    @Test
    void testKvBackpressureMetricsExposeOnlyCoreSignals() {
        TableMetricGroup metricGroup =
                new TableMetricGroup(
                        NOPMetricRegistry.INSTANCE,
                        TablePath.of("db", "pk_table"),
                        true,
                        TestingMetricGroups.TABLET_SERVER_METRICS);

        assertThat(metricGroup.getMetrics())
                .containsKeys(
                        MetricNames.KV_BACKPRESSURE_MAX_PRESSURE,
                        MetricNames.KV_BACKPRESSURE_REJECTIONS_TOTAL)
                .doesNotContainKey("kvBackpressureAffectedBuckets");
    }
}

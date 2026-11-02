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

import io.github.fluss_contrib.rocksdb.HistogramData;
import io.github.fluss_contrib.rocksdb.HistogramType;
import io.github.fluss_contrib.rocksdb.Statistics;
import io.github.fluss_contrib.rocksdb.TickerType;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests table isolation, concurrent acquisition, and native statistics lifetime. */
class RocksDBStatisticsRegistryTest {

    @Test
    void testTableAndServerIsolationAndLastReferenceRelease() {
        RocksDBStatisticsRegistry registry = new RocksDBStatisticsRegistry();
        try (RocksDBStatisticsRegistry.Lease first = registry.acquire(1);
                RocksDBStatisticsRegistry.Lease second = registry.acquire(1);
                RocksDBStatisticsRegistry.Lease otherTable = registry.acquire(2);
                RocksDBStatisticsRegistry.Lease otherServer =
                        new RocksDBStatisticsRegistry().acquire(1)) {
            Statistics statistics = first.getStatistics();
            assertThat(second.getStatistics()).isSameAs(statistics);
            assertThat(otherTable.getStatistics()).isNotSameAs(statistics);
            assertThat(otherServer.getStatistics()).isNotSameAs(statistics);
            first.close();
            first.close();
            assertThat(statistics.isOwningHandle()).isTrue();
            second.close();
            assertThat(statistics.isOwningHandle()).isFalse();
            try (RocksDBStatisticsRegistry.Lease reopened = registry.acquire(1)) {
                assertThat(reopened.getStatistics()).isNotSameAs(statistics);
                assertThat(reopened.getStatistics().isOwningHandle()).isTrue();
            }
        }
    }

    @Test
    void testConcurrentAcquisitionAndRelease() throws Exception {
        RocksDBStatisticsRegistry registry = new RocksDBStatisticsRegistry();
        ExecutorService executor = Executors.newFixedThreadPool(8);
        List<RocksDBStatisticsRegistry.Lease> leases = new ArrayList<>();
        try {
            List<Future<RocksDBStatisticsRegistry.Lease>> results = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                results.add(executor.submit(() -> registry.acquire(1)));
            }
            for (Future<RocksDBStatisticsRegistry.Lease> result : results) {
                leases.add(result.get(30, TimeUnit.SECONDS));
            }
            Statistics statistics = leases.get(0).getStatistics();
            List<Future<?>> releases = new ArrayList<>();
            for (RocksDBStatisticsRegistry.Lease lease : leases) {
                assertThat(lease.getStatistics()).isSameAs(statistics);
                releases.add(
                        executor.submit(
                                () -> {
                                    lease.close();
                                    lease.close();
                                }));
            }
            for (Future<?> release : releases) {
                release.get(30, TimeUnit.SECONDS);
            }
            assertThat(statistics.isOwningHandle()).isFalse();
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(30, TimeUnit.SECONDS);
            for (RocksDBStatisticsRegistry.Lease lease : leases) {
                lease.close();
            }
        }
    }

    @Test
    void testFailedCacheSamplePreservesCursorAndClosedDbTotals() {
        TestingStatistics first = new TestingStatistics();
        TestingStatistics second = new TestingStatistics();
        Queue<Statistics> statistics = new ArrayDeque<>(Arrays.asList(first, second));
        RocksDBStatisticsRegistry registry =
                new RocksDBStatisticsRegistry(true, statistics::remove);
        try (RocksDBStatisticsRegistry.Lease firstLease = registry.acquire(1);
                RocksDBStatisticsRegistry.Lease secondLease = registry.acquire(2)) {
            first.hits = 10;
            second.hits = 20;
            assertThat(registry.getSharedBlockCacheTickerCount(TickerType.BLOCK_CACHE_DATA_HIT))
                    .isEqualTo(30);
            first.fail = true;
            first.hits = 15;
            second.hits = 25;
            assertThat(registry.getSharedBlockCacheTickerCount(TickerType.BLOCK_CACHE_DATA_HIT))
                    .isEqualTo(35);
            first.fail = false;
            assertThat(registry.getSharedBlockCacheTickerCount(TickerType.BLOCK_CACHE_DATA_HIT))
                    .isEqualTo(40);
            first.hits = 17;
            firstLease.close();
            firstLease.close();
            assertThat(first.isOwningHandle()).isFalse();
            assertThat(registry.getSharedBlockCacheTickerCount(TickerType.BLOCK_CACHE_DATA_HIT))
                    .isEqualTo(42);
            second.hits = 1;
            assertThat(registry.getSharedBlockCacheTickerCount(TickerType.BLOCK_CACHE_DATA_HIT))
                    .isEqualTo(43);
        }
    }

    @Test
    void testSharedStatisticsAreSampledUntilLastLeaseCloses() {
        TestingStatistics statistics = new TestingStatistics();
        RocksDBStatisticsRegistry registry = new RocksDBStatisticsRegistry(true, () -> statistics);
        try (RocksDBStatisticsRegistry.Lease first = registry.acquire(1)) {
            statistics.hits = 10;
            assertThat(registry.getSharedBlockCacheTickerCount(TickerType.BLOCK_CACHE_DATA_HIT))
                    .isEqualTo(10);
            try (RocksDBStatisticsRegistry.Lease second = registry.acquire(1)) {
                statistics.hits = 15;
                assertThat(registry.getSharedBlockCacheTickerCount(TickerType.BLOCK_CACHE_DATA_HIT))
                        .isEqualTo(15);
                first.close();
                first.close();
                statistics.hits = 20;
                assertThat(registry.getSharedBlockCacheTickerCount(TickerType.BLOCK_CACHE_DATA_HIT))
                        .isEqualTo(20);
                statistics.hits = 25;
            }
            assertThat(statistics.isOwningHandle()).isFalse();
            assertThat(registry.getSharedBlockCacheTickerCount(TickerType.BLOCK_CACHE_DATA_HIT))
                    .isEqualTo(25);
        }
    }

    @Test
    void testNativeReadTotalsSurviveFailuresResetAndReopenWithoutSharedCache() {
        TestingStatistics first = new TestingStatistics();
        TestingStatistics second = new TestingStatistics();
        TestingStatistics reopened = new TestingStatistics();
        Queue<Statistics> statistics = new ArrayDeque<>(Arrays.asList(first, second, reopened));
        RocksDBStatisticsRegistry registry =
                new RocksDBStatisticsRegistry(false, statistics::remove);
        try (RocksDBStatisticsRegistry.Lease firstLease = registry.acquire(1);
                RocksDBStatisticsRegistry.Lease duplicateLease = registry.acquire(1);
                RocksDBStatisticsRegistry.Lease secondLease = registry.acquire(2)) {
            first.reads = 2;
            first.readMicros = 13;
            second.reads = 3;
            second.readMicros = 29;
            first.hits = 10;
            assertThat(registry.getSharedBlockCacheTickerCount(TickerType.BLOCK_CACHE_DATA_HIT))
                    .isZero();
            assertThat(registry.getNativeReadCount(HistogramType.DB_GET)).isEqualTo(5);
            assertThat(registry.getNativeReadTimeMicros(HistogramType.DB_GET)).isEqualTo(42);
            assertThat(registry.getNativeReadCount(HistogramType.DB_MULTIGET)).isZero();
            assertThat(registry.getNativeReadTimeMicros(HistogramType.DB_MULTIGET)).isZero();

            first.fail = true;
            first.reads = 4;
            first.readMicros = 23;
            second.reads = 5;
            second.readMicros = 39;
            assertThat(registry.getNativeReadCount(HistogramType.DB_GET)).isEqualTo(7);
            assertThat(registry.getNativeReadTimeMicros(HistogramType.DB_GET)).isEqualTo(52);
            first.fail = false;
            assertThat(registry.getNativeReadCount(HistogramType.DB_GET)).isEqualTo(9);
            assertThat(registry.getNativeReadTimeMicros(HistogramType.DB_GET)).isEqualTo(62);

            firstLease.close();
            assertThat(first.isOwningHandle()).isTrue();
            duplicateLease.close();
            assertThat(first.isOwningHandle()).isFalse();
            second.reads = 1;
            second.readMicros = 100;
            assertThat(registry.getNativeReadCount(HistogramType.DB_GET)).isEqualTo(10);
            assertThat(registry.getNativeReadTimeMicros(HistogramType.DB_GET)).isEqualTo(162);
            second.reads = 2;
            second.readMicros = 1;
            assertThat(registry.getNativeReadCount(HistogramType.DB_GET)).isEqualTo(12);
            assertThat(registry.getNativeReadTimeMicros(HistogramType.DB_GET)).isEqualTo(163);
            try (RocksDBStatisticsRegistry.Lease reopenedLease = registry.acquire(1)) {
                assertThat(reopenedLease.getStatistics()).isSameAs(reopened);
                reopened.reads = 2;
                reopened.readMicros = 7;
                reopened.multiReads = 3;
                reopened.multiReadMicros = 11;
            }
            assertThat(registry.getNativeReadCount(HistogramType.DB_GET)).isEqualTo(14);
            assertThat(registry.getNativeReadTimeMicros(HistogramType.DB_GET)).isEqualTo(170);
            assertThat(registry.getNativeReadCount(HistogramType.DB_MULTIGET)).isEqualTo(3);
            assertThat(registry.getNativeReadTimeMicros(HistogramType.DB_MULTIGET)).isEqualTo(11);
        }
    }

    private static class TestingStatistics extends Statistics {
        private long hits;
        private long reads;
        private long readMicros;
        private long multiReads;
        private long multiReadMicros;
        private boolean fail;

        @Override
        public HistogramData getHistogramData(HistogramType histogram) {
            if (fail) {
                throw new IllegalStateException("Statistics unavailable");
            }
            boolean get = histogram == HistogramType.DB_GET;
            return new HistogramData(
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    get ? reads : multiReads,
                    get ? readMicros : multiReadMicros,
                    0);
        }

        @Override
        public long getTickerCount(TickerType ticker) {
            if (fail) {
                throw new IllegalStateException("Statistics unavailable");
            }
            return ticker == TickerType.BLOCK_CACHE_DATA_HIT ? hits : 0;
        }
    }
}

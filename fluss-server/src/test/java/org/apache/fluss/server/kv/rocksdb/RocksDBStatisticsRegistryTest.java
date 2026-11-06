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

import io.github.fluss_contrib.rocksdb.Statistics;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
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
}

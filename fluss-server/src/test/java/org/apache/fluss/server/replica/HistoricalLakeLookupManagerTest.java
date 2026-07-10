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

package org.apache.fluss.server.replica;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.HistoricalLookupThrottledException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.entity.LookupResultForBucket;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.entity.LookupDataForBucket;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.record.TestData.PARTITION_TABLE_ID;
import static org.apache.fluss.record.TestData.PARTITION_TABLE_INFO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link HistoricalLakeLookupManager}. */
class HistoricalLakeLookupManagerTest {

    private static final TableBucket HISTORICAL_BUCKET = new TableBucket(PARTITION_TABLE_ID, 1L, 0);

    @Test
    void testHistoricalLookupThrottledWhenPermitsExhausted() throws Exception {
        ManualExecutor executor = new ManualExecutor();
        HistoricalLakeLookupManager manager = createManager(1, executor);

        CompletableFuture<LookupResultForBucket> first =
                manager.lookup(lookupData(HISTORICAL_BUCKET), PARTITION_TABLE_INFO);
        assertThat(first).isNotDone();
        assertThat(executor.numQueuedTasks()).isEqualTo(1);

        TableBucket secondBucket = new TableBucket(PARTITION_TABLE_ID, 2L, 0);
        LookupResultForBucket second =
                manager.lookup(lookupData(secondBucket), PARTITION_TABLE_INFO)
                        .get(1, TimeUnit.SECONDS);

        assertThat(second.failed()).isTrue();
        assertThat(second.getError().error()).isEqualTo(Errors.HISTORICAL_LOOKUP_THROTTLED);
        assertThat(second.getError().exception())
                .isInstanceOf(HistoricalLookupThrottledException.class);
        assertThat(executor.numQueuedTasks()).isEqualTo(1);
    }

    @Test
    void testHistoricalLookupReleasesPermitOnFailure() throws Exception {
        ManualExecutor executor = new ManualExecutor();
        HistoricalLakeLookupManager manager = createManager(1, executor);

        CompletableFuture<LookupResultForBucket> first =
                manager.lookup(lookupData(HISTORICAL_BUCKET), PARTITION_TABLE_INFO);
        executor.runNext();
        LookupResultForBucket firstResult = first.get(1, TimeUnit.SECONDS);
        assertThat(firstResult.failed()).isTrue();
        assertThat(firstResult.getError().error()).isNotEqualTo(Errors.HISTORICAL_LOOKUP_THROTTLED);

        CompletableFuture<LookupResultForBucket> second =
                manager.lookup(lookupData(HISTORICAL_BUCKET), PARTITION_TABLE_INFO);
        assertThat(second).isNotDone();
        assertThat(executor.numQueuedTasks()).isEqualTo(1);
    }

    @Test
    void testHistoricalLookupMaxQueuedRequestsUsesExplicitConfig() throws Exception {
        ManualExecutor executor = new ManualExecutor();
        HistoricalLakeLookupManager manager = createManager(2, executor);

        CompletableFuture<LookupResultForBucket> first =
                manager.lookup(
                        lookupData(new TableBucket(PARTITION_TABLE_ID, 1L, 0)),
                        PARTITION_TABLE_INFO);
        CompletableFuture<LookupResultForBucket> second =
                manager.lookup(
                        lookupData(new TableBucket(PARTITION_TABLE_ID, 2L, 0)),
                        PARTITION_TABLE_INFO);
        LookupResultForBucket third =
                manager.lookup(
                                lookupData(new TableBucket(PARTITION_TABLE_ID, 3L, 0)),
                                PARTITION_TABLE_INFO)
                        .get(1, TimeUnit.SECONDS);

        assertThat(first).isNotDone();
        assertThat(second).isNotDone();
        assertThat(executor.numQueuedTasks()).isEqualTo(2);
        assertThat(third.getError().error()).isEqualTo(Errors.HISTORICAL_LOOKUP_THROTTLED);
    }

    @Test
    void testRejectNonPositiveHistoricalLookupMaxQueuedRequests() {
        Configuration conf = conf(0);
        ManualExecutor executor = new ManualExecutor();

        assertThatThrownBy(() -> new HistoricalLakeLookupManager(conf, null, executor))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        ConfigOptions.NETTY_SERVER_MAX_QUEUED_HISTORICAL_REQUESTS.key());
    }

    private static HistoricalLakeLookupManager createManager(
            int maxQueuedHistoricalRequests, ManualExecutor executor) {
        return new HistoricalLakeLookupManager(conf(maxQueuedHistoricalRequests), null, executor);
    }

    private static Configuration conf(int maxQueuedHistoricalRequests) {
        Configuration conf = new Configuration();
        conf.set(
                ConfigOptions.NETTY_SERVER_MAX_QUEUED_HISTORICAL_REQUESTS,
                maxQueuedHistoricalRequests);
        return conf;
    }

    private static LookupDataForBucket lookupData(TableBucket tableBucket) {
        return new LookupDataForBucket(
                tableBucket, Collections.singletonList(new byte[] {1}), "2024");
    }

    private static final class ManualExecutor extends AbstractExecutorService {
        private final BlockingQueue<Runnable> tasks = new LinkedBlockingQueue<>();
        private volatile boolean shutdown;

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
            List<Runnable> remainingTasks = new ArrayList<>();
            tasks.drainTo(remainingTasks);
            return remainingTasks;
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown && tasks.isEmpty();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return isTerminated();
        }

        @Override
        public void execute(Runnable command) {
            if (shutdown) {
                throw new RejectedExecutionException();
            }
            tasks.add(command);
        }

        private void runNext() throws Exception {
            Runnable task = tasks.poll(1, TimeUnit.SECONDS);
            assertThat(task).isNotNull();
            task.run();
        }

        private int numQueuedTasks() {
            return tasks.size();
        }
    }
}

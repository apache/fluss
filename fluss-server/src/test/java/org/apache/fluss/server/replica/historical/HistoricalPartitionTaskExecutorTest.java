/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.fluss.server.replica.historical;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link HistoricalPartitionTaskExecutor}. */
class HistoricalPartitionTaskExecutorTest {

    @Test
    void testSharedRequestLimit() throws Exception {
        ManualExecutor executor = new ManualExecutor();
        HistoricalPartitionTaskExecutor taskExecutor =
                new HistoricalPartitionTaskExecutor(configuration(1), executor);

        CompletableFuture<String> first = taskExecutor.submit(() -> "accepted", () -> "throttled");
        CompletableFuture<String> second = taskExecutor.submit(() -> "accepted", () -> "throttled");

        assertThat(first).isNotDone();
        assertThat(second).isCompletedWithValue("throttled");
        assertThat(taskExecutor.numInflightRequests()).isOne();

        executor.runNext();
        assertThat(first).isCompletedWithValue("accepted");
        assertThat(taskExecutor.numInflightRequests()).isZero();
    }

    @Test
    void testReleasesPermitAfterFailure() throws Exception {
        ManualExecutor executor = new ManualExecutor();
        HistoricalPartitionTaskExecutor taskExecutor =
                new HistoricalPartitionTaskExecutor(configuration(1), executor);

        CompletableFuture<String> failed =
                taskExecutor.submit(
                        () -> {
                            throw new IllegalStateException("expected");
                        },
                        () -> "throttled");
        executor.runNext();

        assertThatThrownBy(failed::join).hasCauseInstanceOf(IllegalStateException.class);
        assertThat(taskExecutor.numInflightRequests()).isZero();
        assertThat(taskExecutor.submit(() -> "accepted", () -> "throttled")).isNotDone();
    }

    @Test
    void testSerializesSameKeyAndAllowsDifferentKeysToRunConcurrently() throws Exception {
        ManualExecutor executor = new ManualExecutor();
        HistoricalPartitionTaskExecutor taskExecutor =
                new HistoricalPartitionTaskExecutor(configuration(3), executor);
        List<String> executionOrder = new ArrayList<>();

        CompletableFuture<String> first =
                taskExecutor.submitOrdered(
                        "partition-1",
                        () -> {
                            executionOrder.add("partition-1-first");
                            return "first";
                        },
                        () -> "throttled");
        CompletableFuture<String> second =
                taskExecutor.submitOrdered(
                        "partition-1",
                        () -> {
                            executionOrder.add("partition-1-second");
                            return "second";
                        },
                        () -> "throttled");
        CompletableFuture<String> otherPartition =
                taskExecutor.submitOrdered(
                        "partition-2",
                        () -> {
                            executionOrder.add("partition-2");
                            return "other";
                        },
                        () -> "throttled");

        assertThat(executor.numQueuedTasks()).isEqualTo(2);
        executor.runNext();
        assertThat(first).isCompletedWithValue("first");
        assertThat(second).isNotDone();
        assertThat(executor.numQueuedTasks()).isEqualTo(2);

        executor.runNext();
        executor.runNext();
        assertThat(otherPartition).isCompletedWithValue("other");
        assertThat(second).isCompletedWithValue("second");
        assertThat(executionOrder)
                .containsExactly("partition-1-first", "partition-2", "partition-1-second");
    }

    @Test
    void testOrderedTaskContinuesAfterPreviousFailure() throws Exception {
        ManualExecutor executor = new ManualExecutor();
        HistoricalPartitionTaskExecutor taskExecutor =
                new HistoricalPartitionTaskExecutor(configuration(2), executor);

        CompletableFuture<String> failed =
                taskExecutor.submitOrdered(
                        "partition",
                        () -> {
                            throw new IllegalStateException("expected");
                        },
                        () -> "throttled");
        CompletableFuture<String> next =
                taskExecutor.submitOrdered("partition", () -> "accepted", () -> "throttled");

        assertThat(executor.numQueuedTasks()).isOne();
        executor.runNext();
        assertThatThrownBy(failed::join).hasCauseInstanceOf(IllegalStateException.class);
        assertThat(executor.numQueuedTasks()).isOne();

        executor.runNext();
        assertThat(next).isCompletedWithValue("accepted");
        assertThat(taskExecutor.numInflightRequests()).isZero();
    }

    @Test
    void testRejectNonPositiveRequestLimit() {
        assertThatThrownBy(
                        () ->
                                new HistoricalPartitionTaskExecutor(
                                        configuration(0), new ManualExecutor()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        ConfigOptions.NETTY_SERVER_MAX_QUEUED_HISTORICAL_REQUESTS.key());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void testRejectNonPositiveThreadPoolSize(int maxThreadPoolSize) {
        Configuration conf = configuration(1);
        conf.set(ConfigOptions.SERVER_HISTORICAL_PARTITION_THREAD_POOL_MAX_SIZE, maxThreadPoolSize);

        assertThatThrownBy(() -> new HistoricalPartitionTaskExecutor(conf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        ConfigOptions.SERVER_HISTORICAL_PARTITION_THREAD_POOL_MAX_SIZE.key());
    }

    private static Configuration configuration(int maxQueuedHistoricalRequests) {
        Configuration conf = new Configuration();
        conf.set(
                ConfigOptions.NETTY_SERVER_MAX_QUEUED_HISTORICAL_REQUESTS,
                maxQueuedHistoricalRequests);
        return conf;
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

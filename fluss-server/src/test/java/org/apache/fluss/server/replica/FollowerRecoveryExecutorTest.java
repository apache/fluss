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

import org.apache.fluss.exception.LogStorageException;
import org.apache.fluss.rpc.protocol.ApiError;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests per-directory concurrency and temporary recovery pool completion barriers. */
class FollowerRecoveryExecutorTest {
    @Test
    void testPreservesStorageFailureMapping() {
        FollowerRecoveryExecutor recovery = new FollowerRecoveryExecutor(true, 1);
        LogStorageException failure = new LogStorageException("injected storage failure");
        Map<File, List<Runnable>> tasks = new HashMap<>();
        add(
                tasks,
                "a",
                () -> {
                    throw failure;
                });
        assertThatThrownBy(() -> recovery.recoverDirectories(tasks))
                .isSameAs(failure)
                .satisfies(
                        thrown ->
                                assertThat(ApiError.fromThrowable(thrown).error())
                                        .isEqualTo(ApiError.fromThrowable(failure).error()));
    }

    @Test
    void testAlreadyInterruptedCallerFailsEvenWhenTasksAreComplete() {
        ExecutorService executor =
                new ThreadPoolExecutor(
                        1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>()) {
                    @Override
                    public void execute(Runnable command) {
                        command.run();
                    }
                };
        FollowerRecoveryExecutor recovery =
                new FollowerRecoveryExecutor(true, 1, (directory, size) -> executor);
        Map<File, List<Runnable>> tasks = new HashMap<>();
        add(tasks, "a", () -> {});
        try {
            Thread.currentThread().interrupt();
            assertThatThrownBy(() -> recovery.recoverDirectories(tasks))
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(executor.isTerminated()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void testRejectsNonPositiveThreadsPerDirectory() {
        assertThatThrownBy(() -> new FollowerRecoveryExecutor(true, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FollowerRecoveryExecutor(false, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void testIndependentDirectoryCapsAndTotalConcurrency(int threadsPerDirectory) throws Exception {
        FollowerRecoveryExecutor recovery = new FollowerRecoveryExecutor(true, threadsPerDirectory);
        CountDownLatch started = new CountDownLatch(2 * threadsPerDirectory);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger totalActive = new AtomicInteger();
        AtomicInteger maxTotal = new AtomicInteger();
        List<AtomicInteger> perDirectoryMax = new ArrayList<>();
        List<Thread> workers = new CopyOnWriteArrayList<>();
        Map<File, List<Runnable>> tasks = new HashMap<>();
        for (int directory = 0; directory < 2; directory++) {
            AtomicInteger active = new AtomicInteger();
            AtomicInteger max = new AtomicInteger();
            perDirectoryMax.add(max);
            for (int replica = 0; replica < 3; replica++) {
                add(
                        tasks,
                        "dir-" + directory,
                        () -> {
                            workers.add(Thread.currentThread());
                            max.accumulateAndGet(active.incrementAndGet(), Math::max);
                            maxTotal.accumulateAndGet(totalActive.incrementAndGet(), Math::max);
                            started.countDown();
                            await(release);
                            totalActive.decrementAndGet();
                            active.decrementAndGet();
                        });
            }
        }
        CompletableFuture<Void> result =
                CompletableFuture.runAsync(() -> recovery.recoverDirectories(tasks));
        try {
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(totalActive.get()).isEqualTo(2 * threadsPerDirectory);
            assertThat(result).isNotDone();
            release.countDown();
            result.get(10, TimeUnit.SECONDS);
            assertThat(maxTotal.get()).isEqualTo(2 * threadsPerDirectory);
            assertThat(perDirectoryMax)
                    .allSatisfy(max -> assertThat(max.get()).isEqualTo(threadsPerDirectory));
            for (Thread worker : workers) {
                worker.join(10000);
                assertThat(worker.isAlive()).isFalse();
            }
        } finally {
            release.countDown();
            result.get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void testFailureDrainsAllTasksInDirectoryAndReplicaOrder() throws Exception {
        FollowerRecoveryExecutor recovery = new FollowerRecoveryExecutor(true, 2);
        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        RuntimeException first = new RuntimeException("a replica 0");
        RuntimeException second = new RuntimeException("a replica 1");
        RuntimeException third = new RuntimeException("b replica 0");
        Map<File, List<Runnable>> tasks = new HashMap<>();
        add(
                tasks,
                "b",
                () -> {
                    blocked.countDown();
                    await(release);
                    throw third;
                });
        add(
                tasks,
                "a",
                () -> {
                    await(blocked);
                    throw first;
                });
        add(
                tasks,
                "a",
                () -> {
                    throw second;
                });
        CompletableFuture<Throwable> result = new CompletableFuture<>();
        Thread coordinator = startRecovery(recovery, tasks, result, new AtomicBoolean());
        try {
            assertThat(blocked.await(10, TimeUnit.SECONDS)).isTrue();
            waitUntil(
                    () -> coordinator.getState() == Thread.State.WAITING,
                    Duration.ofSeconds(10),
                    "Coordinator did not drain");
            assertThat(result).isNotDone();
            release.countDown();
            assertThat(result.get(10, TimeUnit.SECONDS)).isSameAs(first);
            assertThat(first.getSuppressed()).containsExactly(second, third);
        } finally {
            release.countDown();
            coordinator.join(10000);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPartialCreationOrSubmissionFailureDrainsAndTerminates(boolean creationFailure)
            throws Exception {
        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean exited = new AtomicBoolean();
        ThreadPoolExecutor executor =
                new ThreadPoolExecutor(
                        1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>()) {
                    private int submissions;

                    @Override
                    public void execute(Runnable command) {
                        if (!creationFailure && ++submissions == 2) {
                            throw new RejectedExecutionException("injected submission failure");
                        }
                        super.execute(command);
                    }
                };
        FollowerRecoveryExecutor recovery =
                new FollowerRecoveryExecutor(
                        true,
                        1,
                        (directory, size) -> {
                            if (creationFailure && directory.getName().equals("b")) {
                                throw new RejectedExecutionException(
                                        "injected pool creation failure");
                            }
                            return executor;
                        });
        Map<File, List<Runnable>> tasks = new HashMap<>();
        add(
                tasks,
                "a",
                () -> {
                    blocked.countDown();
                    await(release);
                    exited.set(true);
                });
        add(
                tasks,
                creationFailure ? "b" : "a",
                () -> {
                    throw new AssertionError("unaccepted task ran");
                });
        CompletableFuture<Throwable> result = new CompletableFuture<>();
        Thread coordinator = startRecovery(recovery, tasks, result, new AtomicBoolean());
        try {
            assertThat(blocked.await(10, TimeUnit.SECONDS)).isTrue();
            waitUntil(
                    () -> coordinator.getState() == Thread.State.WAITING,
                    Duration.ofSeconds(10),
                    "Coordinator did not drain accepted work");
            assertThat(result).isNotDone();
            release.countDown();
            assertThat(result.get(10, TimeUnit.SECONDS))
                    .isInstanceOf(RejectedExecutionException.class);
            assertThat(exited).isTrue();
            assertThat(executor.isTerminated()).isTrue();
        } finally {
            release.countDown();
            coordinator.join(10000);
        }
    }

    @Test
    void testInterruptDrainsAndRestoresInterruptFlag() throws Exception {
        FollowerRecoveryExecutor recovery = new FollowerRecoveryExecutor(true, 1);
        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean exited = new AtomicBoolean();
        Map<File, List<Runnable>> tasks = new HashMap<>();
        add(
                tasks,
                "a",
                () -> {
                    blocked.countDown();
                    await(release);
                    exited.set(true);
                });
        CompletableFuture<Throwable> result = new CompletableFuture<>();
        AtomicBoolean interrupted = new AtomicBoolean();
        Thread coordinator = startRecovery(recovery, tasks, result, interrupted);
        try {
            assertThat(blocked.await(10, TimeUnit.SECONDS)).isTrue();
            waitUntil(
                    () -> coordinator.getState() == Thread.State.WAITING,
                    Duration.ofSeconds(10),
                    "Coordinator did not await task");
            coordinator.interrupt();
            waitUntil(
                    () ->
                            !coordinator.isInterrupted()
                                    && coordinator.getState() == Thread.State.WAITING,
                    Duration.ofSeconds(10),
                    "Interrupted coordinator did not keep draining");
            assertThat(result).isNotDone();
            assertThat(exited).isFalse();
            release.countDown();
            assertThat(result.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(interrupted).isTrue();
            assertThat(exited).isTrue();
        } finally {
            release.countDown();
            coordinator.join(10000);
        }
    }

    @Test
    void testWaitsForActualTerminationAfterFutureCompletionAndInterrupt() throws Exception {
        CountDownLatch taskReturned = new CountDownLatch(1);
        CountDownLatch releaseWorkerExit = new CountDownLatch(1);
        ThreadPoolExecutor executor =
                new ThreadPoolExecutor(
                        1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>()) {
                    @Override
                    protected void afterExecute(Runnable runnable, Throwable error) {
                        taskReturned.countDown();
                        await(releaseWorkerExit);
                    }
                };
        FollowerRecoveryExecutor recovery =
                new FollowerRecoveryExecutor(true, 1, (directory, size) -> executor);
        Map<File, List<Runnable>> tasks = new HashMap<>();
        add(tasks, "a", () -> {});
        CompletableFuture<Throwable> result = new CompletableFuture<>();
        AtomicBoolean interrupted = new AtomicBoolean();
        Thread coordinator = startRecovery(recovery, tasks, result, interrupted);
        try {
            assertThat(taskReturned.await(10, TimeUnit.SECONDS)).isTrue();
            waitUntil(
                    () -> coordinator.getState() == Thread.State.TIMED_WAITING,
                    Duration.ofSeconds(10),
                    "Coordinator did not await termination");
            coordinator.interrupt();
            waitUntil(
                    () ->
                            !coordinator.isInterrupted()
                                    && coordinator.getState() == Thread.State.TIMED_WAITING,
                    Duration.ofSeconds(10),
                    "Interrupted termination wait returned early");
            assertThat(result).isNotDone();
            releaseWorkerExit.countDown();
            assertThat(result.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(interrupted).isTrue();
            assertThat(executor.isTerminated()).isTrue();
        } finally {
            releaseWorkerExit.countDown();
            coordinator.join(10000);
        }
    }

    @Test
    void testEachBatchOwnsFreshPoolsAndEmptyBatchAllocatesNone() {
        List<ExecutorService> pools = new ArrayList<>();
        FollowerRecoveryExecutor recovery =
                new FollowerRecoveryExecutor(
                        true,
                        1,
                        (directory, size) -> {
                            ExecutorService pool =
                                    new ThreadPoolExecutor(
                                            1,
                                            1,
                                            0,
                                            TimeUnit.MILLISECONDS,
                                            new LinkedBlockingQueue<>());
                            pools.add(pool);
                            return pool;
                        });
        Map<File, List<Runnable>> tasks = new HashMap<>();
        recovery.recoverDirectories(tasks);
        assertThat(pools).isEmpty();
        add(tasks, "a", () -> {});
        recovery.recoverDirectories(tasks);
        recovery.recoverDirectories(tasks);
        assertThat(pools)
                .hasSize(2)
                .doesNotHaveDuplicates()
                .allSatisfy(pool -> assertThat(pool.isTerminated()).isTrue());
        FollowerRecoveryExecutor disabled =
                new FollowerRecoveryExecutor(
                        false,
                        1,
                        (directory, size) -> {
                            throw new AssertionError("Disabled recovery allocated a pool");
                        });
        disabled.recover(
                new ArrayList<>(),
                ignored -> {
                    throw new AssertionError();
                });
    }

    private static void add(Map<File, List<Runnable>> tasks, String directory, Runnable task) {
        tasks.computeIfAbsent(new File(directory), ignored -> new ArrayList<>()).add(task);
    }

    private static Thread startRecovery(
            FollowerRecoveryExecutor recovery,
            Map<File, List<Runnable>> tasks,
            CompletableFuture<Throwable> result,
            AtomicBoolean interrupted) {
        Thread coordinator =
                new Thread(
                        () -> {
                            Throwable failure = null;
                            try {
                                recovery.recoverDirectories(tasks);
                            } catch (Throwable t) {
                                failure = t;
                            }
                            interrupted.set(Thread.currentThread().isInterrupted());
                            result.complete(failure);
                        });
        coordinator.start();
        return coordinator;
    }

    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }
}

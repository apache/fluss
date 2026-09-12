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

package org.apache.fluss.memory;

import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.RecordTooLargeException;
import org.apache.fluss.exception.TimeoutException;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Regression tests for batch allocation progress and cleanup. */
class MemoryAllocationTest {

    @Test
    void testCumulativeAllocationExceedsCapacity() throws Exception {
        try (LazyMemorySegmentPool pool = pool()) {
            assertThatThrownBy(
                            () -> {
                                try (MemoryAllocation allocation = pool.newAllocation()) {
                                    allocation.nextSegment();
                                    allocation.nextSegment();
                                    allocation.nextSegment();
                                }
                            })
                    .isInstanceOf(RecordTooLargeException.class);
            assertThat(pool.freePages()).isEqualTo(2);
            try (MemoryAllocation retry = pool.newAllocation()) {
                assertThat(retry.allocatePages(2)).hasSize(2);
            }
            assertThat(pool.freePages()).isEqualTo(2);
        }
    }

    @Test
    void testWaitForActiveOwner() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (LazyMemorySegmentPool pool = pool();
                MemoryAllocation owner = pool.newAllocation()) {
            owner.allocatePages(2);
            Future<?> waiter =
                    executor.submit(
                            () -> {
                                try (MemoryAllocation allocation = pool.newAllocation()) {
                                    return allocation.nextSegment();
                                }
                            });
            awaitWaiters(pool, 1);
            assertThat(waiter.isDone()).isFalse();
            owner.close();
            assertThat(waiter.get(10, TimeUnit.SECONDS)).isNotNull();
            assertThat(pool.freePages()).isEqualTo(2);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testDeadlockAbortsYoungerHolderAndRetrySucceeds() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (LazyMemorySegmentPool pool = pool();
                MemoryAllocation older = pool.newAllocation();
                MemoryAllocation younger = pool.newAllocation()) {
            older.nextSegment();
            younger.nextSegment();
            Future<?> completion =
                    executor.submit(
                            () -> {
                                try (MemoryAllocation allocation = older) {
                                    return allocation.nextSegment();
                                }
                            });
            awaitWaiters(pool, 1);
            assertThatThrownBy(younger::nextSegment)
                    .isInstanceOf(TimeoutException.class)
                    .hasMessageContaining("cannot make progress");
            // Arbitration must not recycle pages that the aborted caller may still access.
            assertThat(pool.freePages()).isZero();
            younger.close();
            assertThat(completion.get(10, TimeUnit.SECONDS)).isNotNull();
            try (MemoryAllocation retry = pool.newAllocation()) {
                assertThat(retry.allocatePages(2)).hasSize(2);
            }
            assertThat(pool.freePages()).isEqualTo(2);
            assertThat(pool.queued()).isZero();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testDeadlockWakesAlreadyWaitingVictim() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (LazyMemorySegmentPool pool = pool();
                MemoryAllocation older = pool.newAllocation();
                MemoryAllocation younger = pool.newAllocation()) {
            older.nextSegment();
            younger.nextSegment();
            Future<?> victim =
                    executor.submit(
                            () -> {
                                try (MemoryAllocation allocation = younger) {
                                    assertThatThrownBy(allocation::nextSegment)
                                            .isInstanceOf(TimeoutException.class);
                                }
                            });
            awaitWaiters(pool, 1);
            assertThat(older.nextSegment()).isNotNull();
            victim.get(10, TimeUnit.SECONDS);
            older.close();
            assertThat(pool.freePages()).isEqualTo(2);
            assertThat(pool.queued()).isZero();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testWaitForUnscopedPagesAndPartialReturn() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (LazyMemorySegmentPool pool = pool();
                MemoryAllocation allocation = pool.newAllocation()) {
            List<MemorySegment> external = pool.allocatePages(1);
            MemorySegment first = allocation.nextSegment();
            Future<?> waiter = executor.submit(allocation::nextSegment);
            awaitWaiters(pool, 1);
            assertThat(waiter.isDone()).isFalse();
            pool.returnAll(external);
            assertThat(waiter.get(10, TimeUnit.SECONDS)).isNotNull();
            allocation.returnPage(first);
            assertThat(pool.freePages()).isEqualTo(1);
            allocation.close();
            allocation.close();
            assertThat(pool.freePages()).isEqualTo(2);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testTimeoutReturnsHeldPages() throws Exception {
        try (LazyMemorySegmentPool pool = new LazyMemorySegmentPool(2, 128, 10, 128);
                MemoryAllocation active = pool.newAllocation()) {
            active.nextSegment();
            assertThatThrownBy(
                            () -> {
                                try (MemoryAllocation allocation = pool.newAllocation()) {
                                    allocation.nextSegment();
                                    allocation.nextSegment();
                                }
                            })
                    .isInstanceOf(TimeoutException.class)
                    .hasMessageContaining("Timed out");
            assertThat(pool.freePages()).isEqualTo(1);
            assertThat(pool.queued()).isZero();
        }
    }

    @Test
    void testEmptyWaiterDoesNotPreventDeadlockRecovery() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (LazyMemorySegmentPool pool = pool();
                MemoryAllocation older = pool.newAllocation();
                MemoryAllocation younger = pool.newAllocation()) {
            older.nextSegment();
            younger.nextSegment();
            Future<?> empty =
                    executor.submit(
                            () -> {
                                try (MemoryAllocation allocation = pool.newAllocation()) {
                                    return allocation.nextSegment();
                                }
                            });
            awaitWaiters(pool, 1);
            Future<?> survivor =
                    executor.submit(
                            () -> {
                                try (MemoryAllocation allocation = older) {
                                    return allocation.nextSegment();
                                }
                            });
            awaitWaiters(pool, 2);
            assertThatThrownBy(younger::nextSegment).isInstanceOf(TimeoutException.class);
            younger.close();
            assertThat(empty.get(10, TimeUnit.SECONDS)).isNotNull();
            assertThat(survivor.get(10, TimeUnit.SECONDS)).isNotNull();
            assertThat(pool.freePages()).isEqualTo(2);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testPoolCloseWakesAllocation() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (LazyMemorySegmentPool pool = pool();
                MemoryAllocation active = pool.newAllocation()) {
            active.nextSegment();
            Future<?> waiter =
                    executor.submit(
                            () -> {
                                try (MemoryAllocation allocation = pool.newAllocation()) {
                                    allocation.nextSegment();
                                    assertThatThrownBy(allocation::nextSegment)
                                            .isInstanceOf(FlussRuntimeException.class)
                                            .hasMessageContaining("pool closed");
                                }
                                return null;
                            });
            awaitWaiters(pool, 1);
            pool.close();
            waiter.get(10, TimeUnit.SECONDS);
            active.close();
            assertThat(pool.queued()).isZero();
            assertThat(pool.freePages()).isEqualTo(2);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testInterruptedAllocationReturnsPages() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch released = new CountDownLatch(1);
        try (LazyMemorySegmentPool pool = pool();
                MemoryAllocation active = pool.newAllocation()) {
            active.nextSegment();
            Future<?> waiter =
                    executor.submit(
                            () -> {
                                try (MemoryAllocation allocation = pool.newAllocation()) {
                                    allocation.nextSegment();
                                    allocation.nextSegment();
                                } finally {
                                    released.countDown();
                                }
                                return null;
                            });
            awaitWaiters(pool, 1);
            waiter.cancel(true);
            assertThat(released.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(pool.queued()).isZero();
            assertThat(pool.freePages()).isEqualTo(1);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testConcurrentAllocationsMakeProgress() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try (LazyMemorySegmentPool pool = new LazyMemorySegmentPool(4, 128, Long.MAX_VALUE, 128)) {
            List<Future<?>> tasks = new ArrayList<>();
            for (int thread = 0; thread < 4; thread++) {
                tasks.add(
                        executor.submit(
                                () -> {
                                    int completed = 0;
                                    while (completed < 25) {
                                        try (MemoryAllocation allocation = pool.newAllocation()) {
                                            for (int page = 0; page < 4; page++) {
                                                allocation.nextSegment();
                                                Thread.yield();
                                            }
                                            completed++;
                                        } catch (TimeoutException retryable) {
                                            // Each operation fits alone; retry after releasing all
                                            // held pages.
                                        }
                                    }
                                    return null;
                                }));
            }
            for (Future<?> task : tasks) {
                task.get(10, TimeUnit.SECONDS);
            }
            assertThat(pool.freePages()).isEqualTo(4);
            assertThat(pool.queued()).isZero();
        } finally {
            executor.shutdownNow();
        }
    }

    private static LazyMemorySegmentPool pool() {
        return new LazyMemorySegmentPool(2, 128, Long.MAX_VALUE, 128);
    }

    private static void awaitWaiters(LazyMemorySegmentPool pool, int expected) throws Exception {
        retry(Duration.ofSeconds(10), () -> assertThat(pool.queued()).isEqualTo(expected));
    }
}

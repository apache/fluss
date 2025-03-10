/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.replica.delay;

import com.alibaba.fluss.exception.FlussRuntimeException;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link com.alibaba.fluss.server.replica.delay.DelayedOperation}. */
public class DelayedOperationTest {
    private DelayedOperationManager<DelayedOperation> delayedOperationManager;
    private ExecutorService executorService;

    @BeforeEach
    void setup() {
        delayedOperationManager = new DelayedOperationManager<>("test", 0, 1000);
    }

    @AfterEach
    void tearDown() {
        delayedOperationManager.shutdown();
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    @Test
    void testLockInTryCompleteElseWatch() {
        DelayedOperation op =
                new DelayedOperation(100000L) {
                    @Override
                    public void onExpiration() {}

                    @Override
                    public void onComplete() {}

                    @Override
                    public boolean tryComplete() {
                        assertThat(((ReentrantLock) lock).isHeldByCurrentThread()).isTrue();
                        return false;
                    }

                    @Override
                    public boolean safeTryComplete() {
                        Assertions.fail("tryCompleteElseWatch should not use safeTryComplete");
                        return super.safeTryComplete();
                    }
                };

        delayedOperationManager.tryCompleteElseWatch(op, Collections.singletonList("key"));
    }

    @Test
    void testSafeTryCompleteOrElse() {
        DelayedOperation opFalse =
                new DelayedOperation(100000L) {
                    @Override
                    public void onExpiration() {}

                    @Override
                    public void onComplete() {}

                    @Override
                    public boolean tryComplete() {
                        assertThat(((ReentrantLock) lock).isHeldByCurrentThread()).isTrue();
                        return false;
                    }
                };

        final boolean[] pass = new boolean[1];
        assertThat(opFalse.safeTryCompleteOrElse(() -> pass[0] = true)).isFalse();
        assertThat(pass[0]).isTrue();

        DelayedOperation opTrue =
                new DelayedOperation(100000L) {
                    @Override
                    public void onExpiration() {}

                    @Override
                    public void onComplete() {}

                    @Override
                    public boolean tryComplete() {
                        assertThat(((ReentrantLock) lock).isHeldByCurrentThread()).isTrue();
                        return true;
                    }
                };

        assertThat(
                        opTrue.safeTryCompleteOrElse(
                                () -> Assertions.fail("this method should NOT be executed")))
                .isTrue();
    }

    @Test
    void testRequestSatisfaction() {
        TestDelayedOperation r1 = new TestDelayedOperation(100000L);
        TestDelayedOperation r2 = new TestDelayedOperation(100000L);

        // With no waiting requests, nothing should be satisfied.
        assertThat(delayedOperationManager.checkAndComplete("test1")).isEqualTo(0);
        // r1 not satisfied and hence watched.
        assertThat(
                        delayedOperationManager.tryCompleteElseWatch(
                                r1, Collections.singletonList("test1")))
                .isFalse();
        // Still nothing satisfied.
        assertThat(delayedOperationManager.checkAndComplete("test1")).isEqualTo(0);
        // r2 not satisfied and hence watched.
        assertThat(
                        delayedOperationManager.tryCompleteElseWatch(
                                r2, Collections.singletonList("test2")))
                .isFalse();
        // Still nothing satisfied.
        assertThat(delayedOperationManager.checkAndComplete("test2")).isEqualTo(0);

        r1.completable = true;
        // r1 satisfied.
        assertThat(delayedOperationManager.checkAndComplete("test1")).isEqualTo(1);
        // Nothing satisfied.
        assertThat(delayedOperationManager.checkAndComplete("test1")).isEqualTo(0);

        r2.completable = true;
        // r2 satisfied.
        assertThat(delayedOperationManager.checkAndComplete("test2")).isEqualTo(1);
        // Nothing satisfied.
        assertThat(delayedOperationManager.checkAndComplete("test2")).isEqualTo(0);
    }

    @Test
    void testRequestExpiry() {
        long expiration = 20L;
        long start = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        TestDelayedOperation r1 = new TestDelayedOperation(expiration);
        TestDelayedOperation r2 = new TestDelayedOperation(200000L);
        // r1 not satisfied and hence watched.
        assertThat(
                        delayedOperationManager.tryCompleteElseWatch(
                                r1, Collections.singletonList("test1")))
                .isFalse();
        // r2 not satisfied and hence watched.
        assertThat(
                        delayedOperationManager.tryCompleteElseWatch(
                                r2, Collections.singletonList("test2")))
                .isFalse();
        r1.awaitExpiration();
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) - start;
        // r1 completed due to expiration.
        assertThat(r1.isCompleted()).isTrue();
        // r2 hasn't completed.
        assertThat(r2.isCompleted()).isFalse();
        assertThat(elapsed >= expiration).isTrue();
    }

    @Test
    void testRequestPurge() {
        TestDelayedOperation r1 = new TestDelayedOperation(100000L);
        TestDelayedOperation r2 = new TestDelayedOperation(100000L);
        TestDelayedOperation r3 = new TestDelayedOperation(100000L);
        delayedOperationManager.tryCompleteElseWatch(r1, Collections.singletonList("test1"));
        delayedOperationManager.tryCompleteElseWatch(r2, Arrays.asList("test1", "test2"));
        delayedOperationManager.tryCompleteElseWatch(r3, Arrays.asList("test1", "test2", "test3"));

        // The delayed operation manager should have 3 total delayed operations.
        assertThat(delayedOperationManager.numDelayed()).isEqualTo(3);
        // The delayed operation manager should have 6 watched elements.
        assertThat(delayedOperationManager.watched()).isEqualTo(6);

        // complete the operations, it should immediately be purged from the delayed operation.
        r2.completable = true;
        r2.tryComplete();
        assertThat(delayedOperationManager.numDelayed()).isEqualTo(2);

        r3.completable = true;
        r3.tryComplete();
        assertThat(delayedOperationManager.numDelayed()).isEqualTo(1);

        // checking a watch should purge the watch list.
        delayedOperationManager.checkAndComplete("test1");
        assertThat(delayedOperationManager.watched()).isEqualTo(4);

        delayedOperationManager.checkAndComplete("test2");
        assertThat(delayedOperationManager.watched()).isEqualTo(2);

        delayedOperationManager.checkAndComplete("test3");
        assertThat(delayedOperationManager.watched()).isEqualTo(1);
    }

    @Test
    void testShouldCancelForKeyReturningCancelledOperations() {
        delayedOperationManager.tryCompleteElseWatch(
                new TestDelayedOperation(100000L), Collections.singletonList("key"));
        delayedOperationManager.tryCompleteElseWatch(
                new TestDelayedOperation(100000L), Collections.singletonList("key"));
        delayedOperationManager.tryCompleteElseWatch(
                new TestDelayedOperation(100000L), Collections.singletonList("key2"));

        List<DelayedOperation> cancelledOperations = delayedOperationManager.cancelForKey("key");
        assertThat(cancelledOperations.size()).isEqualTo(2);
        assertThat(delayedOperationManager.numDelayed()).isEqualTo(1);
        assertThat(delayedOperationManager.watched()).isEqualTo(1);
    }

    @Test
    void testShouldReturnNilOperationsOnCancelForKeyWhenKeyDoesntExist() {
        List<DelayedOperation> cancelledOperations = delayedOperationManager.cancelForKey("key");
        assertThat(cancelledOperations.size()).isEqualTo(0);
    }

    /**
     * Test `tryComplete` with multiple threads to verify that there are no timing windows when
     * completion is not performed even if the thread that makes the operation completable may not
     * be able to acquire the operation lock. Since it is difficult to test all scenarios, this test
     * uses random delays with a large number of threads.
     */
    @Test
    void testTryCompleteWithMultipleThreads() throws Exception {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(3);
        executorService = executor;
        Random random = new Random();
        int maxDelayMs = 10;
        int completionAttempts = 20;

        class TestDelayedOperationV2 extends TestDelayedOperation {
            final String key;
            final AtomicInteger completionAttemptsRemaining;

            public TestDelayedOperationV2(int index) {
                super(10000L);
                this.key = "key" + index;
                this.completionAttemptsRemaining = new AtomicInteger(completionAttempts);
            }

            @Override
            public boolean tryComplete() {
                boolean shouldComplete = completable;
                try {
                    Thread.sleep(random.nextInt(maxDelayMs));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                if (shouldComplete) {
                    return forceComplete();
                } else {
                    return false;
                }
            }
        }

        // Create and watch operations
        List<TestDelayedOperationV2> ops = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            TestDelayedOperationV2 op = new TestDelayedOperationV2(index);
            delayedOperationManager.tryCompleteElseWatch(op, Collections.singletonList(op.key));
            ops.add(op);
        }

        // Schedule tryComplete at random intervals
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < completionAttempts; i++) {
            for (TestDelayedOperationV2 op : ops) {
                long delayMs = random.nextInt(maxDelayMs);
                Future<?> future =
                        executor.schedule(
                                () -> {
                                    if (op.completionAttemptsRemaining.decrementAndGet() == 0) {
                                        op.completable = true;
                                    }
                                    delayedOperationManager.checkAndComplete(op.key);
                                },
                                delayMs,
                                TimeUnit.MILLISECONDS);
                futures.add(future);
            }
        }

        // Await completion of all scheduled attempts.
        for (Future<?> future : futures) {
            future.get();
        }

        // Assert all operations are completed.
        for (TestDelayedOperationV2 op : ops) {
            assertThat(op.isCompleted()).isTrue();
        }

        // Shutdown the executor.
        executor.shutdown();
        try {
            if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static class TestDelayedOperation extends DelayedOperation {
        protected boolean completable;

        public TestDelayedOperation(long delayMs) {
            super(delayMs);
            this.completable = false;
        }

        public void awaitExpiration() {
            synchronized (this) {
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    throw new FlussRuntimeException("Error while await expiration.", e);
                }
            }
        }

        @Override
        public void onExpiration() {}

        @Override
        public void onComplete() {
            synchronized (this) {
                this.notify();
            }
        }

        @Override
        public boolean tryComplete() {
            if (completable) {
                return forceComplete();
            } else {
                return false;
            }
        }
    }
}

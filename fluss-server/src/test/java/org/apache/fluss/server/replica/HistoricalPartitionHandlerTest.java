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

import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HistoricalPartitionHandler}. */
class HistoricalPartitionHandlerTest {

    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        executor = Executors.newFixedThreadPool(4);
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void testQueueFullRejectsNewRequest() throws Exception {
        HistoricalPartitionHandler handler = new HistoricalPartitionHandler(executor, 2);
        CountDownLatch blockLatch = new CountDownLatch(1);
        CountDownLatch startedLatch = new CountDownLatch(2);
        TableBucket bucket1 = new TableBucket(1, 1L, 0);
        TableBucket bucket2 = new TableBucket(1, 1L, 1);

        // Acquire 2 request permits (simulate 2 in-flight requests)
        assertThat(handler.tryAcquire()).isTrue();
        assertThat(handler.tryAcquire()).isTrue();

        // Submit blocking tasks so permits stay held
        handler.submitWrite(
                bucket1,
                () -> {
                    startedLatch.countDown();
                    try {
                        blockLatch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
        handler.submitWrite(
                bucket2,
                () -> {
                    startedLatch.countDown();
                    try {
                        blockLatch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });

        startedLatch.await(5, TimeUnit.SECONDS);

        // Third request should be rejected
        assertThat(handler.tryAcquire()).isFalse();

        blockLatch.countDown();
    }

    @Test
    void testSameBucketWritesFIFO() throws Exception {
        HistoricalPartitionHandler handler = new HistoricalPartitionHandler(executor, 10);
        TableBucket bucket = new TableBucket(1, 1L, 0);
        List<Integer> executionOrder = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch allDone = new CountDownLatch(3);

        // One request with 3 writes to the same bucket
        assertThat(handler.tryAcquire()).isTrue();
        handler.submitWrite(
                bucket,
                () -> {
                    executionOrder.add(1);
                    allDone.countDown();
                });
        handler.submitWrite(
                bucket,
                () -> {
                    executionOrder.add(2);
                    allDone.countDown();
                });
        handler.submitWrite(
                bucket,
                () -> {
                    executionOrder.add(3);
                    allDone.countDown();
                });

        assertThat(allDone.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(executionOrder).containsExactly(1, 2, 3);
        handler.release();
    }

    @Test
    void testDifferentBucketWritesConcurrent() throws Exception {
        HistoricalPartitionHandler handler = new HistoricalPartitionHandler(executor, 10);
        TableBucket bucket1 = new TableBucket(1, 1L, 0);
        TableBucket bucket2 = new TableBucket(1, 1L, 1);

        CountDownLatch bothStarted = new CountDownLatch(2);
        CountDownLatch releaseLatch = new CountDownLatch(1);
        AtomicBoolean concurrent = new AtomicBoolean(false);

        assertThat(handler.tryAcquire()).isTrue();
        handler.submitWrite(
                bucket1,
                () -> {
                    bothStarted.countDown();
                    try {
                        if (bothStarted.await(3, TimeUnit.SECONDS)) {
                            concurrent.set(true);
                        }
                        releaseLatch.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
        handler.submitWrite(
                bucket2,
                () -> {
                    bothStarted.countDown();
                    try {
                        if (bothStarted.await(3, TimeUnit.SECONDS)) {
                            concurrent.set(true);
                        }
                        releaseLatch.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });

        assertThat(bothStarted.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(concurrent.get()).isTrue();
        releaseLatch.countDown();
    }

    @Test
    void testLookupsConcurrent() throws Exception {
        HistoricalPartitionHandler handler = new HistoricalPartitionHandler(executor, 10);

        CountDownLatch bothStarted = new CountDownLatch(2);
        CountDownLatch releaseLatch = new CountDownLatch(1);
        AtomicBoolean concurrent = new AtomicBoolean(false);

        Runnable lookupTask =
                () -> {
                    bothStarted.countDown();
                    try {
                        if (bothStarted.await(3, TimeUnit.SECONDS)) {
                            concurrent.set(true);
                        }
                        releaseLatch.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                };

        assertThat(handler.tryAcquire()).isTrue();
        handler.submitLookup(lookupTask);
        handler.submitLookup(lookupTask);

        assertThat(bothStarted.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(concurrent.get()).isTrue();
        releaseLatch.countDown();
    }

    @Test
    void testPermitReleasedOnCompletion() throws Exception {
        HistoricalPartitionHandler handler = new HistoricalPartitionHandler(executor, 1);
        CountDownLatch firstDone = new CountDownLatch(1);
        TableBucket bucket = new TableBucket(1, 1L, 0);

        // Fill up the single permit
        assertThat(handler.tryAcquire()).isTrue();
        handler.submitWrite(bucket, firstDone::countDown);

        // Wait for the task to complete
        assertThat(firstDone.await(5, TimeUnit.SECONDS)).isTrue();

        // Release the request permit
        handler.release();

        // Should be able to acquire again
        CountDownLatch secondDone = new CountDownLatch(1);
        AtomicBoolean executed = new AtomicBoolean(false);
        assertThat(handler.tryAcquire()).isTrue();
        handler.submitLookup(
                () -> {
                    executed.set(true);
                    secondDone.countDown();
                });

        assertThat(secondDone.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(executed.get()).isTrue();
        handler.release();
    }

    @Test
    void testPermitReleasedOnException() throws Exception {
        HistoricalPartitionHandler handler = new HistoricalPartitionHandler(executor, 1);
        TableBucket bucket = new TableBucket(1, 1L, 0);
        CountDownLatch errorDone = new CountDownLatch(1);

        // Submit a task that throws an exception
        assertThat(handler.tryAcquire()).isTrue();
        handler.submitWrite(
                bucket,
                () -> {
                    errorDone.countDown();
                    throw new RuntimeException("test error");
                });

        // Wait for the error task to complete
        assertThat(errorDone.await(5, TimeUnit.SECONDS)).isTrue();

        // Release the request permit (caller's responsibility)
        handler.release();

        // Wait a bit for state to settle
        Thread.sleep(50);

        // Permit should be available; submit another task
        CountDownLatch secondDone = new CountDownLatch(1);
        AtomicInteger counter = new AtomicInteger(0);
        assertThat(handler.tryAcquire()).isTrue();
        handler.submitWrite(
                bucket,
                () -> {
                    counter.incrementAndGet();
                    secondDone.countDown();
                });

        assertThat(secondDone.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(counter.get()).isEqualTo(1);
        handler.release();
    }

    @Test
    void testCapacityMinimumIsOne() {
        HistoricalPartitionHandler handler = new HistoricalPartitionHandler(executor, 0);
        assertThat(handler.getCapacity()).isEqualTo(1);

        HistoricalPartitionHandler handler2 = new HistoricalPartitionHandler(executor, -5);
        assertThat(handler2.getCapacity()).isEqualTo(1);
    }
}

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

package org.apache.fluss.kafka.backend.produce;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests bounded admission, ordering, independent lanes and executor shutdown. */
class KafkaProduceConversionExecutorTest {
    @Test
    void testBoundedLanePreservesOrderAndDoesNotBlockAnotherLane() throws Exception {
        KafkaProduceConversionExecutor executor = new KafkaProduceConversionExecutor(2, 2);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<Integer> order = new ArrayList<>();
        AtomicReference<Thread> worker = new AtomicReference<>();
        AtomicBoolean rejectedRan = new AtomicBoolean();
        try {
            CompletableFuture<Integer> first =
                    executor.submit(
                            0,
                            () -> {
                                worker.set(Thread.currentThread());
                                started.countDown();
                                await(release);
                                order.add(1);
                                return CompletableFuture.completedFuture(1);
                            });
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<Integer> second =
                    executor.submit(
                            0,
                            () -> {
                                order.add(2);
                                return CompletableFuture.completedFuture(2);
                            });
            CompletableFuture<Integer> rejected =
                    executor.submit(
                            0,
                            () -> {
                                rejectedRan.set(true);
                                return CompletableFuture.completedFuture(3);
                            });
            assertThatThrownBy(rejected::join).hasCauseInstanceOf(RejectedExecutionException.class);
            assertThat(rejectedRan).isFalse();
            assertThat(worker.get()).isNotSameAs(Thread.currentThread());
            assertThat(second).isNotDone();
            assertThat(
                            executor.submit(1, () -> CompletableFuture.completedFuture(4))
                                    .get(10, TimeUnit.SECONDS))
                    .isEqualTo(4);
            release.countDown();
            assertThat(first.get(10, TimeUnit.SECONDS)).isEqualTo(1);
            assertThat(second.get(10, TimeUnit.SECONDS)).isEqualTo(2);
            assertThat(order).containsExactly(1, 2);
        } finally {
            release.countDown();
            executor.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void testPendingAcknowledgementDoesNotOccupyWorker() throws Exception {
        KafkaProduceConversionExecutor executor = new KafkaProduceConversionExecutor(1, 1);
        CompletableFuture<Integer> acknowledgement = new CompletableFuture<>();
        CountDownLatch submitted = new CountDownLatch(1);
        try {
            CompletableFuture<Integer> first =
                    executor.submit(
                            0,
                            () -> {
                                submitted.countDown();
                                return acknowledgement;
                            });
            assertThat(submitted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(
                            executor.submit(0, () -> CompletableFuture.completedFuture(2))
                                    .get(10, TimeUnit.SECONDS))
                    .isEqualTo(2);
            assertThat(first).isNotDone();
            acknowledgement.complete(1);
            assertThat(first.get(10, TimeUnit.SECONDS)).isEqualTo(1);
        } finally {
            executor.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void testCloseInterruptsWorkerCompletesQueuedRequestsAndRejectsNewWork() throws Exception {
        KafkaProduceConversionExecutor executor = new KafkaProduceConversionExecutor(1, 1);
        CountDownLatch started = new CountDownLatch(1);
        AtomicBoolean queuedRan = new AtomicBoolean();
        CompletableFuture<Void> running =
                executor.submit(
                        0,
                        () -> {
                            started.countDown();
                            await(new CountDownLatch(1));
                            return CompletableFuture.completedFuture(null);
                        });
        try {
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<Void> queued =
                    executor.submit(
                            0,
                            () -> {
                                queuedRan.set(true);
                                return CompletableFuture.completedFuture(null);
                            });
            CompletableFuture<Void> closed = executor.closeAsync();
            assertThat(executor.closeAsync()).isSameAs(closed);
            closed.get(10, TimeUnit.SECONDS);
            assertThatThrownBy(running::join).hasRootCauseInstanceOf(InterruptedException.class);
            assertThatThrownBy(queued::join).hasCauseInstanceOf(RejectedExecutionException.class);
            assertThat(queuedRan).isFalse();
            assertThatThrownBy(
                            () ->
                                    executor.submit(
                                                    0,
                                                    () -> CompletableFuture.completedFuture(null))
                                            .join())
                    .hasCauseInstanceOf(RejectedExecutionException.class);
        } finally {
            executor.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void testFailedOperationDoesNotStopLane() throws Exception {
        KafkaProduceConversionExecutor executor = new KafkaProduceConversionExecutor(1, 1);
        try {
            CompletableFuture<Object> failed =
                    executor.submit(
                            0,
                            () -> {
                                throw new IllegalStateException("conversion failed");
                            });
            assertThatThrownBy(failed::join).hasCauseInstanceOf(IllegalStateException.class);
            assertThat(
                            executor.submit(0, () -> CompletableFuture.completedFuture(1))
                                    .get(10, TimeUnit.SECONDS))
                    .isEqualTo(1);
        } finally {
            executor.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Test worker was not released.");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(failure);
        }
    }
}

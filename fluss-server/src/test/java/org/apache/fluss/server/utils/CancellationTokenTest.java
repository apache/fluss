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

package org.apache.fluss.server.utils;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests cancellation notification and safe callback removal. */
class CancellationTokenTest {
    @Test
    void testCancelBeforeRegistrationAndRepeatedCancel() throws Exception {
        CancellationToken token = new CancellationToken();
        token.cancel();
        AtomicInteger calls = new AtomicInteger();
        try (AutoCloseable ignored = token.register(calls::incrementAndGet)) {
            token.cancel();
            assertThat(calls.get()).isEqualTo(1);
        }
        assertThat(token.isCancelled()).isTrue();
        assertThatThrownBy(token::checkCancelled).isInstanceOf(IOException.class);
    }

    @Test
    void testUnregisteredCallbackIsNotInvoked() throws Exception {
        CancellationToken token = new CancellationToken();
        AtomicInteger calls = new AtomicInteger();
        AutoCloseable registration = token.register(calls::incrementAndGet);
        registration.close();
        registration.close();
        token.cancel();
        assertThat(calls.get()).isZero();
    }

    @Test
    void testUnregisterWaitsForEnteredCallback() throws Exception {
        CancellationToken token = new CancellationToken();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch unregistering = new CountDownLatch(1);
        AtomicBoolean accessed = new AtomicBoolean();
        AtomicBoolean closed = new AtomicBoolean();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        AutoCloseable registration =
                token.register(
                        () -> {
                            entered.countDown();
                            await(release);
                            assertThat(closed.get()).isFalse();
                            accessed.set(true);
                        });
        try {
            Future<?> cancel = executor.submit(token::cancel);
            entered.await();
            Future<?> unregister =
                    executor.submit(
                            () -> {
                                unregistering.countDown();
                                registration.close();
                                assertThat(accessed.get()).isTrue();
                                closed.set(true);
                                return null;
                            });
            unregistering.await();
            assertThat(unregister.isDone()).isFalse();
            release.countDown();
            cancel.get();
            unregister.get();
            token.cancel();
            assertThat(closed.get()).isTrue();
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}

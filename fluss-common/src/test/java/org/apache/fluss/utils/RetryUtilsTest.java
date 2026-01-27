/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.utils;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link RetryUtils}. */
class RetryUtilsTest {

    @Test
    void testSuccessOnFirstAttempt() throws IOException {
        AtomicInteger attempts = new AtomicInteger(0);

        String result =
                RetryUtils.executeWithRetry(
                        () -> {
                            attempts.incrementAndGet();
                            return "success";
                        },
                        "testOp",
                        3,
                        10,
                        100,
                        e -> true);

        assertThat(result).isEqualTo("success");
        assertThat(attempts.get()).isEqualTo(1);
    }

    @Test
    void testSuccessAfterRetries() throws IOException {
        AtomicInteger attempts = new AtomicInteger(0);

        String result =
                RetryUtils.executeWithRetry(
                        () -> {
                            int attempt = attempts.incrementAndGet();
                            if (attempt < 3) {
                                throw new IOException("transient failure");
                            }
                            return "success";
                        },
                        "testOp",
                        5,
                        10,
                        100,
                        e -> e instanceof IOException);

        assertThat(result).isEqualTo("success");
        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    void testExhaustedRetries() {
        AtomicInteger attempts = new AtomicInteger(0);

        assertThatThrownBy(
                        () ->
                                RetryUtils.executeWithRetry(
                                        () -> {
                                            attempts.incrementAndGet();
                                            throw new IOException("persistent failure");
                                        },
                                        "testOp",
                                        3,
                                        10,
                                        100,
                                        e -> e instanceof IOException))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("testOp failed after 3 attempts")
                .hasCauseInstanceOf(IOException.class);

        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    void testNonRetryableException() {
        AtomicInteger attempts = new AtomicInteger(0);

        assertThatThrownBy(
                        () ->
                                RetryUtils.executeWithRetry(
                                        () -> {
                                            attempts.incrementAndGet();
                                            throw new IllegalArgumentException("bad argument");
                                        },
                                        "testOp",
                                        5,
                                        10,
                                        100,
                                        e -> e instanceof IOException))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("testOp failed")
                .hasCauseInstanceOf(IllegalArgumentException.class);

        // Should fail immediately without retries
        assertThat(attempts.get()).isEqualTo(1);
    }

    @Test
    void testIOExceptionPassedThrough() {
        IOException originalException = new IOException("original");

        assertThatThrownBy(
                        () ->
                                RetryUtils.executeWithRetry(
                                        () -> {
                                            throw originalException;
                                        },
                                        "testOp",
                                        1,
                                        10,
                                        100,
                                        e -> false))
                .isInstanceOf(IOException.class)
                .isSameAs(originalException);
    }

    @Test
    void testExecuteIOWithRetry() throws IOException {
        AtomicInteger attempts = new AtomicInteger(0);

        String result =
                RetryUtils.executeIOWithRetry(
                        () -> {
                            int attempt = attempts.incrementAndGet();
                            if (attempt < 2) {
                                throw new IOException("transient");
                            }
                            return "done";
                        },
                        "ioOp",
                        3,
                        10,
                        100);

        assertThat(result).isEqualTo("done");
        assertThat(attempts.get()).isEqualTo(2);
    }

    @Test
    void testExecuteIOWithRetryNonIOException() {
        AtomicInteger attempts = new AtomicInteger(0);

        assertThatThrownBy(
                        () ->
                                RetryUtils.executeIOWithRetry(
                                        () -> {
                                            attempts.incrementAndGet();
                                            throw new RuntimeException("not IO");
                                        },
                                        "ioOp",
                                        3,
                                        10,
                                        100))
                .isInstanceOf(IOException.class)
                .hasCauseInstanceOf(RuntimeException.class);

        // RuntimeException is not retryable for executeIOWithRetry
        assertThat(attempts.get()).isEqualTo(1);
    }

    @Test
    void testBackoffCapped() throws IOException {
        AtomicInteger attempts = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();

        RetryUtils.executeWithRetry(
                () -> {
                    int attempt = attempts.incrementAndGet();
                    if (attempt < 4) {
                        throw new IOException("fail");
                    }
                    return "ok";
                },
                "backoffTest",
                5,
                10, // initial backoff
                50, // max backoff (caps exponential growth)
                e -> e instanceof IOException);

        long elapsed = System.currentTimeMillis() - startTime;
        // 3 retries: 10ms + 20ms + 40ms (capped to 50ms) = 80ms minimum
        // But with cap: 10ms + 20ms + 50ms = 80ms minimum
        // Allow some tolerance for test execution
        assertThat(elapsed).isGreaterThanOrEqualTo(70);
        assertThat(attempts.get()).isEqualTo(4);
    }

    @Test
    void testInterruptedExceptionFromOperation() {
        AtomicInteger attempts = new AtomicInteger(0);

        assertThatThrownBy(
                        () ->
                                RetryUtils.executeWithRetry(
                                        () -> {
                                            attempts.incrementAndGet();
                                            throw new InterruptedException("interrupted");
                                        },
                                        "interruptOp",
                                        5,
                                        10,
                                        100,
                                        e -> true))
                .isInstanceOf(InterruptedIOException.class)
                .hasMessageContaining("interruptOp was interrupted on attempt 1");

        // Should fail immediately without retries
        assertThat(attempts.get()).isEqualTo(1);
        // Verify interrupt status is preserved
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        // Clear interrupt status for other tests
        Thread.interrupted();
    }
}

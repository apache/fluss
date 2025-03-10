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

package com.alibaba.fluss.utils.concurrent;

import com.alibaba.fluss.exception.FlussException;
import com.alibaba.fluss.testutils.common.OneShotLatch;
import com.alibaba.fluss.testutils.common.TestExecutorExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.fluss.testutils.common.FlussAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the utility methods in {@link FutureUtils}. */
class FutureUtilsTest {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            new TestExecutorExtension<>(Executors::newSingleThreadScheduledExecutor);

    @Test
    void testComposeAfterwards() {
        final CompletableFuture<Void> inputFuture = new CompletableFuture<>();
        final OneShotLatch composeLatch = new OneShotLatch();

        final CompletableFuture<Void> composeFuture =
                FutureUtils.composeAfterwards(
                        inputFuture,
                        () -> {
                            composeLatch.trigger();
                            return CompletableFuture.completedFuture(null);
                        });

        assertThat(composeLatch.isTriggered()).isFalse();
        assertThat(composeFuture).isNotDone();

        inputFuture.complete(null);

        assertThat(composeLatch.isTriggered()).isTrue();

        assertThatFuture(composeFuture).eventuallySucceeds();
    }

    @Test
    void testComposeAfterwardsFirstExceptional() {
        final CompletableFuture<Void> inputFuture = new CompletableFuture<>();
        final OneShotLatch composeLatch = new OneShotLatch();
        final FlussException testException = new FlussException("Test exception");

        final CompletableFuture<Void> composeFuture =
                FutureUtils.composeAfterwards(
                        inputFuture,
                        () -> {
                            composeLatch.trigger();
                            return CompletableFuture.completedFuture(null);
                        });

        assertThat(composeLatch.isTriggered()).isFalse();
        assertThat(composeFuture).isNotDone();

        inputFuture.completeExceptionally(testException);

        assertThat(composeLatch.isTriggered()).isTrue();
        assertThat(composeFuture).isDone();

        assertThatFuture(composeFuture)
                .eventuallyFailsWith(ExecutionException.class)
                .withCause(testException);
    }

    @Test
    void testComposeAfterwardsSecondExceptional() {
        final CompletableFuture<Void> inputFuture = new CompletableFuture<>();
        final OneShotLatch composeLatch = new OneShotLatch();
        final FlussException testException = new FlussException("Test exception");

        final CompletableFuture<Void> composeFuture =
                FutureUtils.composeAfterwards(
                        inputFuture,
                        () -> {
                            composeLatch.trigger();
                            return FutureUtils.completedExceptionally(testException);
                        });

        assertThat(composeLatch.isTriggered()).isFalse();
        assertThat(composeFuture).isNotDone();

        inputFuture.complete(null);

        assertThat(composeLatch.isTriggered()).isTrue();
        assertThat(composeFuture).isDone();

        assertThatFuture(composeFuture)
                .eventuallyFailsWith(ExecutionException.class)
                .withCause(testException);
    }

    @Test
    void testComposeAfterwardsBothExceptional() {
        final CompletableFuture<Void> inputFuture = new CompletableFuture<>();
        final FlussException testException1 = new FlussException("Test exception1");
        final FlussException testException2 = new FlussException("Test exception2");
        final OneShotLatch composeLatch = new OneShotLatch();

        final CompletableFuture<Void> composeFuture =
                FutureUtils.composeAfterwards(
                        inputFuture,
                        () -> {
                            composeLatch.trigger();
                            return FutureUtils.completedExceptionally(testException2);
                        });

        assertThat(composeLatch.isTriggered()).isFalse();
        assertThat(composeFuture).isNotDone();

        inputFuture.completeExceptionally(testException1);

        assertThat(composeLatch.isTriggered()).isTrue();
        assertThat(composeFuture).isDone();

        assertThatFuture(composeFuture)
                .eventuallyFailsWith(ExecutionException.class)
                .extracting(Throwable::getCause)
                .isEqualTo(testException1)
                .satisfies(
                        cause -> assertThat(cause.getSuppressed()).containsExactly(testException2));
    }

    @Test
    void testCompleteAll() {
        final CompletableFuture<String> inputFuture1 = new CompletableFuture<>();
        final CompletableFuture<Integer> inputFuture2 = new CompletableFuture<>();

        final List<CompletableFuture<?>> futuresToComplete =
                Arrays.asList(inputFuture1, inputFuture2);
        final FutureUtils.ConjunctFuture<Void> completeFuture =
                FutureUtils.completeAll(futuresToComplete);

        assertThat(completeFuture).isNotDone();
        assertThat(completeFuture.getNumFuturesCompleted()).isZero();
        assertThat(completeFuture.getNumFuturesTotal()).isEqualTo(futuresToComplete.size());

        inputFuture2.complete(42);

        assertThat(completeFuture).isNotDone();
        assertThat(completeFuture.getNumFuturesCompleted()).isOne();

        inputFuture1.complete("foobar");

        assertThat(completeFuture).isDone();
        assertThat(completeFuture.getNumFuturesCompleted()).isEqualTo(2);

        assertThatFuture(completeFuture).eventuallySucceeds();
    }

    @Test
    void testCompleteAllPartialExceptional() {
        final CompletableFuture<String> inputFuture1 = new CompletableFuture<>();
        final CompletableFuture<Integer> inputFuture2 = new CompletableFuture<>();

        final List<CompletableFuture<?>> futuresToComplete =
                Arrays.asList(inputFuture1, inputFuture2);
        final FutureUtils.ConjunctFuture<Void> completeFuture =
                FutureUtils.completeAll(futuresToComplete);

        assertThat(completeFuture).isNotDone();
        assertThat(completeFuture.getNumFuturesCompleted()).isZero();
        assertThat(completeFuture.getNumFuturesTotal()).isEqualTo(futuresToComplete.size());

        final FlussException testException1 = new FlussException("Test exception 1");
        inputFuture2.completeExceptionally(testException1);

        assertThat(completeFuture).isNotDone();
        assertThat(completeFuture.getNumFuturesCompleted()).isOne();

        inputFuture1.complete("foobar");

        assertThat(completeFuture).isDone();
        assertThat(completeFuture.getNumFuturesCompleted()).isEqualTo(2);

        assertThatFuture(completeFuture)
                .eventuallyFailsWith(ExecutionException.class)
                .withCause(testException1);
    }

    @Test
    void testCompleteAllExceptional() {
        final CompletableFuture<String> inputFuture1 = new CompletableFuture<>();
        final CompletableFuture<Integer> inputFuture2 = new CompletableFuture<>();

        final List<CompletableFuture<?>> futuresToComplete =
                Arrays.asList(inputFuture1, inputFuture2);
        final FutureUtils.ConjunctFuture<Void> completeFuture =
                FutureUtils.completeAll(futuresToComplete);

        assertThat(completeFuture).isNotDone();
        assertThat(completeFuture.getNumFuturesCompleted()).isZero();
        assertThat(completeFuture.getNumFuturesTotal()).isEqualTo(futuresToComplete.size());

        final FlussException testException1 = new FlussException("Test exception 1");
        inputFuture1.completeExceptionally(testException1);

        assertThat(completeFuture).isNotDone();
        assertThat(completeFuture.getNumFuturesCompleted()).isOne();

        final FlussException testException2 = new FlussException("Test exception 2");
        inputFuture2.completeExceptionally(testException2);

        assertThat(completeFuture.getNumFuturesCompleted()).isEqualTo(2);
        assertThatFuture(completeFuture)
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(FlussException.class)
                .extracting(Throwable::getCause)
                .satisfies(
                        e -> {
                            final Throwable[] actualSuppressedExceptions = e.getSuppressed();
                            final FlussException expectedSuppressedException =
                                    e.equals(testException1) ? testException2 : testException1;

                            assertThat(actualSuppressedExceptions)
                                    .containsExactly(expectedSuppressedException);
                        });
    }

    @Test
    void testHandleUncaughtExceptionWithCompletedFuture() {
        final CompletableFuture<String> future = CompletableFuture.completedFuture("foobar");
        final TestingUncaughtExceptionHandler uncaughtExceptionHandler =
                new TestingUncaughtExceptionHandler();

        FutureUtils.handleUncaughtException(future, uncaughtExceptionHandler);

        assertThat(uncaughtExceptionHandler.hasBeenCalled()).isFalse();
    }

    @Test
    void testHandleUncaughtExceptionWithNormalCompletion() {
        final CompletableFuture<String> future = new CompletableFuture<>();

        final TestingUncaughtExceptionHandler uncaughtExceptionHandler =
                new TestingUncaughtExceptionHandler();

        FutureUtils.handleUncaughtException(future, uncaughtExceptionHandler);
        future.complete("barfoo");

        assertThat(uncaughtExceptionHandler.hasBeenCalled()).isFalse();
    }

    @Test
    void testHandleUncaughtExceptionWithExceptionallyCompletedFuture() {
        final CompletableFuture<String> future =
                FutureUtils.completedExceptionally(new FlussException("foobar"));

        final TestingUncaughtExceptionHandler uncaughtExceptionHandler =
                new TestingUncaughtExceptionHandler();

        FutureUtils.handleUncaughtException(future, uncaughtExceptionHandler);

        assertThat(uncaughtExceptionHandler.hasBeenCalled()).isTrue();
    }

    @Test
    void testHandleUncaughtExceptionWithExceptionallyCompletion() {
        final CompletableFuture<String> future = new CompletableFuture<>();

        final TestingUncaughtExceptionHandler uncaughtExceptionHandler =
                new TestingUncaughtExceptionHandler();

        FutureUtils.handleUncaughtException(future, uncaughtExceptionHandler);

        assertThat(uncaughtExceptionHandler.hasBeenCalled()).isFalse();

        future.completeExceptionally(new FlussException("barfoo"));
        assertThat(uncaughtExceptionHandler.hasBeenCalled()).isTrue();
    }

    /**
     * Tests the behavior of {@link FutureUtils#handleUncaughtException(CompletableFuture,
     * Thread.UncaughtExceptionHandler)} with a custom fallback exception handler to avoid
     * triggering {@code System.exit}.
     */
    @Test
    void testHandleUncaughtExceptionWithBuggyErrorHandlingCode() {
        final Exception actualProductionCodeError =
                new Exception(
                        "Actual production code error that should be caught by the error handler.");

        final RuntimeException errorHandlingException =
                new RuntimeException("Expected test error in error handling code.");
        final Thread.UncaughtExceptionHandler buggyActualExceptionHandler =
                (thread, ignoredActualException) -> {
                    throw errorHandlingException;
                };

        final AtomicReference<Throwable> caughtErrorHandlingException = new AtomicReference<>();
        final Thread.UncaughtExceptionHandler fallbackExceptionHandler =
                (thread, errorHandlingEx) -> caughtErrorHandlingException.set(errorHandlingEx);

        FutureUtils.handleUncaughtException(
                FutureUtils.completedExceptionally(actualProductionCodeError),
                buggyActualExceptionHandler,
                fallbackExceptionHandler);

        assertThat(caughtErrorHandlingException)
                .hasValueSatisfying(
                        actualError -> {
                            assertThat(actualError)
                                    .isInstanceOf(IllegalStateException.class)
                                    .hasRootCause(errorHandlingException)
                                    .satisfies(
                                            cause ->
                                                    assertThat(cause.getSuppressed())
                                                            .containsExactly(
                                                                    actualProductionCodeError));
                        });
    }

    private static class TestingUncaughtExceptionHandler
            implements Thread.UncaughtExceptionHandler {

        private Throwable exception = null;

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            exception = e;
        }

        private boolean hasBeenCalled() {
            return exception != null;
        }
    }

    @Test
    void testForwardAsync() {
        final CompletableFuture<String> source = new CompletableFuture<>();
        final CompletableFuture<String> target = new CompletableFuture<>();
        final ManuallyTriggeredScheduledExecutor executor =
                new ManuallyTriggeredScheduledExecutor();

        FutureUtils.forwardAsync(source, target, executor);

        final String expectedValue = "foobar";
        source.complete(expectedValue);

        assertThat(target).isNotDone();

        // execute the forward action
        executor.triggerAll();

        assertThatFuture(target).eventuallySucceeds().isEqualTo(expectedValue);
    }

    /** Tests that a future is timed out after the specified timeout. */
    @Test
    void testOrTimeout() {
        final CompletableFuture<String> future = new CompletableFuture<>();
        final long timeout = 10L;

        final String expectedErrorMessage = "testOrTimeout";
        FutureUtils.orTimeout(future, timeout, TimeUnit.MILLISECONDS, expectedErrorMessage);

        assertThatFuture(future)
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(TimeoutException.class)
                .withMessageContaining(expectedErrorMessage);
    }
}

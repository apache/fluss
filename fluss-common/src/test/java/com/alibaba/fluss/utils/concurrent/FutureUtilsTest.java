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

package com.alibaba.fluss.utils.concurrent;

import com.alibaba.fluss.exception.FlussException;
import com.alibaba.fluss.testutils.common.OneShotLatch;
import com.alibaba.fluss.testutils.common.TestExecutorExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.fluss.testutils.common.FlussAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

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
        FutureUtils.ConjunctFuture<Void> emptyConjunctFuture =
                FutureUtils.completeAll(Collections.emptyList());
        assertThatFuture(emptyConjunctFuture).isDone();
        assertThatFuture(emptyConjunctFuture).eventuallySucceeds().isNull();

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

    @Test
    void testCompletedVoidFuture() {
        final CompletableFuture<Void> future = FutureUtils.completedVoidFuture();
        assertThatFuture(future).eventuallySucceeds().isNull();
    }

    @Test
    void testCompleteFromCallable() {
        final CompletableFuture<String> successFuture = new CompletableFuture<>();
        FutureUtils.completeFromCallable(successFuture, () -> "Fluss");
        assertThatFuture(successFuture).eventuallySucceeds().isEqualTo("Fluss");

        final CompletableFuture<String> failureFuture = new CompletableFuture<>();
        FutureUtils.completeFromCallable(
                failureFuture,
                () -> {
                    throw new RuntimeException("mock runtime exception");
                });
        assertThatFuture(failureFuture)
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(RuntimeException.class)
                .withMessageContaining("mock runtime exception");
    }

    @Test
    void testRunIfNotDoneAndGet() throws Exception {
        FutureUtils.runIfNotDoneAndGet(null);
        List<String> list = new ArrayList<>();
        Runnable task = () -> list.add("Fluss");
        FutureTask<String> futureTask = new FutureTask<>(task, "Hello Fluss");
        FutureUtils.runIfNotDoneAndGet(futureTask);
        assertThat(futureTask.get()).isEqualTo("Hello Fluss");
        assertThat(list.contains("Fluss")).isTrue();
    }

    @Test
    void combineAll() throws Exception {
        FutureUtils.ConjunctFuture<Collection<String>> emptyConjunctFuture =
                FutureUtils.combineAll(Collections.emptyList());
        assertThatFuture(emptyConjunctFuture).isDone();
        assertThatFuture(emptyConjunctFuture).eventuallySucceeds().asList().isEmpty();

        CompletableFuture<String> future1 = new CompletableFuture<String>();
        CompletableFuture<String> future2 = new CompletableFuture<String>();
        FutureUtils.ConjunctFuture<Collection<String>> conjunctFuture =
                FutureUtils.combineAll(Arrays.asList(future1, future2));

        assertThatFuture(conjunctFuture).isNotDone();
        assertThat(conjunctFuture.getNumFuturesTotal()).isEqualTo(2);
        assertThat(conjunctFuture.getNumFuturesCompleted()).isZero();

        future1.complete("Hello");
        future2.complete("World");
        assertThatFuture(conjunctFuture).eventuallySucceeds();

        assertThatFuture(conjunctFuture).isDone();
        assertThat(conjunctFuture.getNumFuturesTotal()).isEqualTo(2);
        assertThat(conjunctFuture.getNumFuturesCompleted()).isEqualTo(2);
        assertThat(conjunctFuture.get()).containsExactly("Hello", "World");

        CompletableFuture<String> successFuture = new CompletableFuture<String>();
        CompletableFuture<String> failureFuture = new CompletableFuture<String>();
        FutureUtils.ConjunctFuture<Collection<String>> failureConjunctFuture =
                FutureUtils.combineAll(Arrays.asList(successFuture, failureFuture));

        failureFuture.completeExceptionally(new RuntimeException("mock runtime exception"));

        assertThatFuture(failureConjunctFuture).isDone();
        assertThatFuture(failureConjunctFuture)
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(RuntimeException.class)
                .withMessageContaining("mock runtime exception");
    }

    @Test
    void testWaitForAll() {
        FutureUtils.ConjunctFuture<Void> emptyConjunctFuture =
                FutureUtils.waitForAll(Collections.emptyList());
        assertThatFuture(emptyConjunctFuture).isDone();
        assertThatFuture(emptyConjunctFuture).eventuallySucceeds().isNull();

        List<String> successRes = new ArrayList<>();
        CompletableFuture<String> future1 = new CompletableFuture<String>();
        CompletableFuture<String> future2 = new CompletableFuture<String>();
        FutureUtils.ConjunctFuture<Void> conjunctFuture =
                FutureUtils.waitForAll(
                        Arrays.asList(future1, future2),
                        (val, throwable) -> {
                            if (throwable == null) {
                                successRes.add(val);
                            } else {
                                throw new RuntimeException(throwable);
                            }
                        });

        assertThatFuture(conjunctFuture).isNotDone();
        assertThat(conjunctFuture.getNumFuturesTotal()).isEqualTo(2);
        assertThat(conjunctFuture.getNumFuturesCompleted()).isZero();

        future1.complete("Hello");
        future2.complete("World");
        assertThatFuture(conjunctFuture).eventuallySucceeds();
        assertThatFuture(conjunctFuture).isDone();
        assertThat(conjunctFuture.getNumFuturesTotal()).isEqualTo(2);
        assertThat(conjunctFuture.getNumFuturesCompleted()).isEqualTo(2);

        assertThat(successRes.size()).isEqualTo(2);
        assertThat(successRes.contains("Hello")).isTrue();
        assertThat(successRes.contains("World")).isTrue();

        List<String> failureRes = new ArrayList<>();
        CompletableFuture<String> successFuture = new CompletableFuture<String>();
        CompletableFuture<String> failureFuture = new CompletableFuture<String>();
        FutureUtils.ConjunctFuture<Void> failureConjunctFuture =
                FutureUtils.waitForAll(
                        Arrays.asList(successFuture, failureFuture),
                        (val, throwable) -> {
                            if (throwable == null) {
                                failureRes.add(val);
                            } else {
                                throw new RuntimeException(throwable);
                            }
                        });

        failureFuture.completeExceptionally(new RuntimeException("mock runtime exception"));

        assertThatFuture(failureConjunctFuture).isDone();
        assertThat(conjunctFuture.getNumFuturesTotal()).isEqualTo(2);
        assertThat(conjunctFuture.getNumFuturesCompleted()).isEqualTo(2);
        assertThatFuture(failureConjunctFuture)
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(RuntimeException.class)
                .withMessageContaining("mock runtime exception");
        assertThat(failureRes.isEmpty()).isTrue();
    }

    @Test
    void testCatchingAndLoggingThrowables() {
        Runnable task =
                () -> {
                    throw new RuntimeException("mock runtime exception");
                };

        Runnable catchingRunnable = FutureUtils.catchingAndLoggingThrowables(task);
        assertThatNoException().isThrownBy(catchingRunnable::run);
    }
}

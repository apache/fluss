/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.utils.ExceptionUtils;
import com.alibaba.fluss.utils.FatalExitExceptionHandler;
import com.alibaba.fluss.utils.function.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** A collection of utilities that expand the usage of {@link CompletableFuture}. */
public class FutureUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FutureUtils.class);

    private FutureUtils() {}

    private static final CompletableFuture<Void> COMPLETED_VOID_FUTURE =
            CompletableFuture.completedFuture(null);

    /**
     * Returns a completed future of type {@link Void}.
     *
     * @return a completed future of type {@link Void}
     */
    public static CompletableFuture<Void> completedVoidFuture() {
        return COMPLETED_VOID_FUTURE;
    }

    /**
     * Fakes asynchronous execution by immediately executing the operation and completing the
     * supplied future either normally or exceptionally.
     *
     * @param operation to executed
     * @param <T> type of the result
     */
    public static <T> void completeFromCallable(
            CompletableFuture<T> future, Callable<T> operation) {
        try {
            future.complete(operation.call());
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
    }

    /** Runnable to complete the given future with a {@link TimeoutException}. */
    private static final class Timeout implements Runnable {

        private final CompletableFuture<?> future;
        private final String timeoutMsg;

        private Timeout(CompletableFuture<?> future, @Nullable String timeoutMsg) {
            this.future = checkNotNull(future);
            this.timeoutMsg = timeoutMsg;
        }

        @Override
        public void run() {
            future.completeExceptionally(new TimeoutException(timeoutMsg));
        }
    }

    /**
     * Delay scheduler used to timeout futures.
     *
     * <p>This class creates a singleton scheduler used to run the provided actions.
     */
    private enum Delayer {
        ;
        static final ScheduledThreadPoolExecutor DELAYER =
                new ScheduledThreadPoolExecutor(
                        1, new ExecutorThreadFactory("fluss-completable-future-delay-scheduler"));

        /**
         * Delay the given action by the given delay.
         *
         * @param runnable to execute after the given delay
         * @param delay after which to execute the runnable
         * @param timeUnit time unit of the delay
         * @return Future of the scheduled action
         */
        private static ScheduledFuture<?> delay(Runnable runnable, long delay, TimeUnit timeUnit) {
            checkNotNull(runnable);
            checkNotNull(timeUnit);

            return DELAYER.schedule(runnable, delay, timeUnit);
        }
    }

    /**
     * Times the given future out after the timeout.
     *
     * @param future to time out
     * @param timeout after which the given future is timed out
     * @param timeUnit time unit of the timeout
     * @param timeoutMsg timeout message for exception
     * @param <T> type of the given future
     * @return The timeout enriched future
     */
    public static <T> CompletableFuture<T> orTimeout(
            CompletableFuture<T> future,
            long timeout,
            TimeUnit timeUnit,
            @Nullable String timeoutMsg) {
        return orTimeout(future, timeout, timeUnit, Executors.directExecutor(), timeoutMsg);
    }

    /**
     * Times the given future out after the timeout.
     *
     * @param future to time out
     * @param timeout after which the given future is timed out
     * @param timeUnit time unit of the timeout
     * @param timeoutFailExecutor executor that will complete the future exceptionally after the
     *     timeout is reached
     * @param timeoutMsg timeout message for exception
     * @param <T> type of the given future
     * @return The timeout enriched future
     */
    public static <T> CompletableFuture<T> orTimeout(
            CompletableFuture<T> future,
            long timeout,
            TimeUnit timeUnit,
            Executor timeoutFailExecutor,
            @Nullable String timeoutMsg) {

        if (!future.isDone()) {
            final ScheduledFuture<?> timeoutFuture =
                    Delayer.delay(
                            () -> timeoutFailExecutor.execute(new Timeout(future, timeoutMsg)),
                            timeout,
                            timeUnit);

            future.whenComplete(
                    (T value, Throwable throwable) -> {
                        if (!timeoutFuture.isDone()) {
                            timeoutFuture.cancel(false);
                        }
                    });
        }

        return future;
    }

    // ------------------------------------------------------------------------
    //  Future actions
    // ------------------------------------------------------------------------

    /**
     * Run the given {@code RunnableFuture} if it is not done, and then retrieves its result.
     *
     * @param future to run if not done and get
     * @param <T> type of the result
     * @return the result after running the future
     * @throws ExecutionException if a problem occurred
     * @throws InterruptedException if the current thread has been interrupted
     */
    public static <T> T runIfNotDoneAndGet(RunnableFuture<T> future)
            throws ExecutionException, InterruptedException {

        if (null == future) {
            return null;
        }

        if (!future.isDone()) {
            future.run();
        }

        return future.get();
    }

    // ------------------------------------------------------------------------
    //  composing futures
    // ------------------------------------------------------------------------

    /**
     * Run the given asynchronous action after the completion of the given future. The given future
     * can be completed normally or exceptionally. In case of an exceptional completion, the
     * asynchronous action's exception will be added to the initial exception.
     *
     * @param future to wait for its completion
     * @param composedAction asynchronous action which is triggered after the future's completion
     * @return Future which is completed after the asynchronous action has completed. This future
     *     can contain an exception if an error occurred in the given future or asynchronous action.
     */
    public static CompletableFuture<Void> composeAfterwards(
            CompletableFuture<?> future, Supplier<CompletableFuture<?>> composedAction) {
        final CompletableFuture<Void> resultFuture = new CompletableFuture<>();

        future.whenComplete(
                (Object outerIgnored, Throwable outerThrowable) -> {
                    final CompletableFuture<?> composedActionFuture = composedAction.get();

                    composedActionFuture.whenComplete(
                            (Object innerIgnored, Throwable innerThrowable) -> {
                                if (innerThrowable != null) {
                                    resultFuture.completeExceptionally(
                                            ExceptionUtils.firstOrSuppressed(
                                                    innerThrowable, outerThrowable));
                                } else if (outerThrowable != null) {
                                    resultFuture.completeExceptionally(outerThrowable);
                                } else {
                                    resultFuture.complete(null);
                                }
                            });
                });

        return resultFuture;
    }

    /**
     * Creates a future that is complete once multiple other futures completed. The future fails
     * (completes exceptionally) once one of the futures in the conjunction fails. Upon successful
     * completion, the future returns the collection of the futures' results.
     *
     * <p>The ConjunctFuture gives access to how many Futures in the conjunction have already
     * completed successfully, via {@link ConjunctFuture#getNumFuturesCompleted()}.
     *
     * @param futures The futures that make up the conjunction. No null entries are allowed.
     * @return The ConjunctFuture that completes once all given futures are complete (or one fails).
     */
    public static <T> ConjunctFuture<Collection<T>> combineAll(
            Collection<? extends CompletableFuture<? extends T>> futures) {
        checkNotNull(futures, "futures");

        return new ResultConjunctFuture<>(futures);
    }

    /**
     * Creates a future that is complete once all of the given futures have completed. The future
     * fails (completes exceptionally) once one of the given futures fails.
     *
     * <p>The ConjunctFuture gives access to how many Futures have already completed successfully,
     * via {@link ConjunctFuture#getNumFuturesCompleted()}.
     *
     * @param futures The futures to wait on. No null entries are allowed.
     * @return The WaitingFuture that completes once all given futures are complete (or one fails).
     */
    public static ConjunctFuture<Void> waitForAll(
            Collection<? extends CompletableFuture<?>> futures) {
        checkNotNull(futures, "futures");

        return new WaitingConjunctFuture(futures);
    }

    /**
     * Creates a {@link ConjunctFuture} which is only completed after all given futures have
     * completed. Unlike {@link FutureUtils#waitForAll(Collection)}, the resulting future won't be
     * completed directly if one of the given futures is completed exceptionally. Instead, all
     * occurring exception will be collected and combined to a single exception. If at least on
     * exception occurs, then the resulting future will be completed exceptionally.
     *
     * @param futuresToComplete futures to complete
     * @return Future which is completed after all given futures have been completed.
     */
    public static ConjunctFuture<Void> completeAll(
            Collection<? extends CompletableFuture<?>> futuresToComplete) {
        //noinspection unchecked,rawtypes
        return new CompletionConjunctFuture(futuresToComplete, (ignored, throwable) -> {});
    }

    /**
     * Creates a {@link ConjunctFuture} which is only completed after all given futures have
     * completed. Unlike {@link FutureUtils#waitForAll(Collection)}, the resulting future won't be
     * completed directly if one of the given futures is completed exceptionally. Instead, all
     * occurring exception will be collected and combined to a single exception. If at least on
     * exception occurs, then the resulting future will be completed exceptionally.
     *
     * @param futuresToComplete futures to complete
     * @param completeAction action to be executed after the completion of each future
     * @return Future which is completed after all given futures have been completed.
     */
    public static <T> ConjunctFuture<Void> completeAll(
            Collection<? extends CompletableFuture<T>> futuresToComplete,
            BiConsumer<T, Throwable> completeAction) {
        return new CompletionConjunctFuture<>(futuresToComplete, completeAction);
    }

    /**
     * Run the given action after the completion of the given future. The given future can be
     * completed normally or exceptionally. In case of an exceptional completion the, the action's
     * exception will be added to the initial exception.
     *
     * @param future to wait for its completion
     * @param runnable action which is triggered after the future's completion
     * @return Future which is completed after the action has completed. This future can contain an
     *     exception, if an error occurred in the given future or action.
     */
    public static CompletableFuture<Void> runAfterwards(
            CompletableFuture<?> future, RunnableWithException runnable) {
        return runAfterwardsAsync(future, runnable, Executors.directExecutor());
    }

    /**
     * Run the given action after the completion of the given future. The given future can be
     * completed normally or exceptionally. In case of an exceptional completion the, the action's
     * exception will be added to the initial exception.
     *
     * @param future to wait for its completion
     * @param runnable action which is triggered after the future's completion
     * @return Future which is completed after the action has completed. This future can contain an
     *     exception, if an error occurred in the given future or action.
     */
    public static CompletableFuture<Void> runAfterwardsAsync(
            CompletableFuture<?> future, RunnableWithException runnable) {
        return runAfterwardsAsync(future, runnable, ForkJoinPool.commonPool());
    }

    /**
     * Run the given action after the completion of the given future. The given future can be
     * completed normally or exceptionally. In case of an exceptional completion the action's
     * exception will be added to the initial exception.
     *
     * @param future to wait for its completion
     * @param runnable action which is triggered after the future's completion
     * @param executor to run the given action
     * @return Future which is completed after the action has completed. This future can contain an
     *     exception, if an error occurred in the given future or action.
     */
    public static CompletableFuture<Void> runAfterwardsAsync(
            CompletableFuture<?> future, RunnableWithException runnable, Executor executor) {
        final CompletableFuture<Void> resultFuture = new CompletableFuture<>();

        future.whenCompleteAsync(
                (Object ignored, Throwable throwable) -> {
                    try {
                        runnable.run();
                    } catch (Throwable e) {
                        throwable = ExceptionUtils.firstOrSuppressed(e, throwable);
                    }

                    if (throwable != null) {
                        resultFuture.completeExceptionally(throwable);
                    } else {
                        resultFuture.complete(null);
                    }
                },
                executor);

        return resultFuture;
    }

    /**
     * A future that is complete once multiple other futures completed. The futures are not
     * necessarily of the same type. The ConjunctFuture fails (completes exceptionally) once one of
     * the Futures in the conjunction fails.
     *
     * <p>The advantage of using the ConjunctFuture over chaining all the futures (such as via
     * {@link CompletableFuture#thenCombine(CompletionStage, BiFunction)}) is that ConjunctFuture
     * also tracks how many of the Futures are already complete.
     */
    public abstract static class ConjunctFuture<T> extends CompletableFuture<T> {

        /**
         * Gets the total number of Futures in the conjunction.
         *
         * @return The total number of Futures in the conjunction.
         */
        public abstract int getNumFuturesTotal();

        /**
         * Gets the number of Futures in the conjunction that are already complete.
         *
         * @return The number of Futures in the conjunction that are already complete
         */
        public abstract int getNumFuturesCompleted();
    }

    /**
     * The implementation of the {@link ConjunctFuture} which returns its Futures' result as a
     * collection.
     */
    private static class ResultConjunctFuture<T> extends ConjunctFuture<Collection<T>> {

        /** The total number of futures in the conjunction. */
        private final int numTotal;

        /** The number of futures in the conjunction that are already complete. */
        private final AtomicInteger numCompleted = new AtomicInteger(0);

        /** The set of collected results so far. */
        private final T[] results;

        /**
         * The function that is attached to all futures in the conjunction. Once a future is
         * complete, this function tracks the completion or fails the conjunct.
         */
        private void handleCompletedFuture(int index, T value, Throwable throwable) {
            if (throwable != null) {
                completeExceptionally(throwable);
            } else {
                /**
                 * This {@link #results} update itself is not synchronised in any way and it's fine
                 * because:
                 *
                 * <ul>
                 *   <li>There is a happens-before relationship for each thread (that is completing
                 *       the future) between setting {@link #results} and incrementing {@link
                 *       #numCompleted}.
                 *   <li>Each thread is updating uniquely different field of the {@link #results}
                 *       array.
                 *   <li>There is a happens-before relationship between all of the writing threads
                 *       and the last one thread (thanks to the {@code
                 *       numCompleted.incrementAndGet() == numTotal} check.
                 *   <li>The last thread will be completing the future, so it has transitively
                 *       happens-before relationship with all of preceding updated/writes to {@link
                 *       #results}.
                 *   <li>{@link AtomicInteger#incrementAndGet} is an equivalent of both volatile
                 *       read & write
                 * </ul>
                 */
                results[index] = value;

                if (numCompleted.incrementAndGet() == numTotal) {
                    complete(Arrays.asList(results));
                }
            }
        }

        @SuppressWarnings("unchecked")
        ResultConjunctFuture(Collection<? extends CompletableFuture<? extends T>> resultFutures) {
            this.numTotal = resultFutures.size();
            results = (T[]) new Object[numTotal];

            if (resultFutures.isEmpty()) {
                complete(Collections.emptyList());
            } else {
                int counter = 0;
                for (CompletableFuture<? extends T> future : resultFutures) {
                    final int index = counter;
                    counter++;
                    future.whenComplete(
                            (value, throwable) -> handleCompletedFuture(index, value, throwable));
                }
            }
        }

        @Override
        public int getNumFuturesTotal() {
            return numTotal;
        }

        @Override
        public int getNumFuturesCompleted() {
            return numCompleted.get();
        }
    }

    public static <T> CompletableFuture<T> failedFuture(Throwable t) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(t);
        return future;
    }

    /**
     * Implementation of the {@link ConjunctFuture} interface which waits only for the completion of
     * its futures and does not return their values.
     */
    private static final class WaitingConjunctFuture extends ConjunctFuture<Void> {

        /** Number of completed futures. */
        private final AtomicInteger numCompleted = new AtomicInteger(0);

        /** Total number of futures to wait on. */
        private final int numTotal;

        /**
         * Method which increments the atomic completion counter and completes or fails the
         * WaitingFutureImpl.
         */
        private void handleCompletedFuture(Object ignored, Throwable throwable) {
            if (throwable == null) {
                if (numTotal == numCompleted.incrementAndGet()) {
                    complete(null);
                }
            } else {
                completeExceptionally(throwable);
            }
        }

        private WaitingConjunctFuture(Collection<? extends CompletableFuture<?>> futures) {
            this.numTotal = futures.size();

            if (futures.isEmpty()) {
                complete(null);
            } else {
                for (java.util.concurrent.CompletableFuture<?> future : futures) {
                    future.whenComplete(this::handleCompletedFuture);
                }
            }
        }

        @Override
        public int getNumFuturesTotal() {
            return numTotal;
        }

        @Override
        public int getNumFuturesCompleted() {
            return numCompleted.get();
        }
    }

    /**
     * {@link ConjunctFuture} implementation which is completed after all the given futures have
     * been completed. Exceptional completions of the input futures will be recorded but it won't
     * trigger the early completion of this future.
     */
    private static final class CompletionConjunctFuture<T> extends ConjunctFuture<Void> {

        private final Object lock = new Object();

        private final int numFuturesTotal;

        private final BiConsumer<T, Throwable> completeAction;

        private int futuresCompleted;

        private Throwable globalThrowable;

        private CompletionConjunctFuture(
                Collection<? extends CompletableFuture<T>> futuresToComplete,
                BiConsumer<T, Throwable> completeAction) {
            this.numFuturesTotal = futuresToComplete.size();
            this.completeAction = completeAction;

            futuresCompleted = 0;

            globalThrowable = null;

            if (futuresToComplete.isEmpty()) {
                complete(null);
            } else {
                for (CompletableFuture<T> completableFuture : futuresToComplete) {
                    completableFuture.whenComplete(this::completeFuture);
                }
            }
        }

        private void completeFuture(T value, Throwable throwable) {
            synchronized (lock) {
                try {
                    completeAction.accept(value, throwable);
                } catch (Exception e) {
                    // ignore
                }

                futuresCompleted++;

                if (throwable != null) {
                    globalThrowable = ExceptionUtils.firstOrSuppressed(throwable, globalThrowable);
                }

                if (futuresCompleted == numFuturesTotal) {
                    if (globalThrowable != null) {
                        completeExceptionally(globalThrowable);
                    } else {
                        complete(null);
                    }
                }
            }
        }

        @Override
        public int getNumFuturesTotal() {
            return numFuturesTotal;
        }

        @Override
        public int getNumFuturesCompleted() {
            synchronized (lock) {
                return futuresCompleted;
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Helper methods
    // ------------------------------------------------------------------------

    /**
     * Returns an exceptionally completed {@link CompletableFuture}.
     *
     * @param cause to complete the future with
     * @param <T> type of the future
     * @return An exceptionally completed CompletableFuture
     */
    public static <T> CompletableFuture<T> completedExceptionally(Throwable cause) {
        CompletableFuture<T> result = new CompletableFuture<>();
        result.completeExceptionally(cause);

        return result;
    }

    /**
     * Returns a future which is completed when {@link RunnableWithException} is finished.
     *
     * @param runnable represents the task
     * @param executor to execute the runnable
     * @return Future which is completed when runnable is finished
     */
    public static CompletableFuture<Void> runAsync(
            RunnableWithException runnable, Executor executor) {
        return CompletableFuture.runAsync(
                () -> {
                    try {
                        runnable.run();
                    } catch (Throwable e) {
                        throw new CompletionException(e);
                    }
                },
                executor);
    }

    /**
     * Asserts that the given {@link CompletableFuture} is not completed exceptionally. If the
     * future is completed exceptionally, then it will call the {@link FatalExitExceptionHandler}.
     *
     * @param completableFuture to assert for no exceptions
     */
    public static void assertNoException(CompletableFuture<?> completableFuture) {
        handleUncaughtException(completableFuture, FatalExitExceptionHandler.INSTANCE);
    }

    /**
     * Checks that the given {@link CompletableFuture} is not completed exceptionally. If the future
     * is completed exceptionally, then it will call the given uncaught exception handler.
     *
     * @param completableFuture to assert for no exceptions
     * @param uncaughtExceptionHandler to call if the future is completed exceptionally
     */
    public static void handleUncaughtException(
            CompletableFuture<?> completableFuture,
            Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
        handleUncaughtException(
                completableFuture, uncaughtExceptionHandler, FatalExitExceptionHandler.INSTANCE);
    }

    @VisibleForTesting
    static void handleUncaughtException(
            CompletableFuture<?> completableFuture,
            Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
            Thread.UncaughtExceptionHandler fatalErrorHandler) {
        checkNotNull(completableFuture)
                .whenComplete(
                        (ignored, throwable) -> {
                            if (throwable != null) {
                                final Thread currentThread = Thread.currentThread();
                                try {
                                    uncaughtExceptionHandler.uncaughtException(
                                            currentThread, throwable);
                                } catch (Throwable t) {
                                    final RuntimeException errorHandlerException =
                                            new IllegalStateException(
                                                    "An error occurred while executing the error handling for a "
                                                            + throwable.getClass().getSimpleName()
                                                            + ".",
                                                    t);
                                    errorHandlerException.addSuppressed(throwable);
                                    fatalErrorHandler.uncaughtException(
                                            currentThread, errorHandlerException);
                                }
                            }
                        });
    }

    /**
     * Forwards the value from the source future to the target future using the provided executor.
     *
     * @param source future to forward the value from
     * @param target future to forward the value to
     * @param executor executor to forward the source value to the target future
     * @param <T> type of the value
     */
    public static <T> void forwardAsync(
            CompletableFuture<T> source, CompletableFuture<T> target, Executor executor) {
        source.whenCompleteAsync(forwardTo(target), executor);
    }

    private static <T> BiConsumer<T, Throwable> forwardTo(CompletableFuture<T> target) {
        return (value, throwable) -> doForward(value, throwable, target);
    }

    /**
     * Completes the given future with either the given value or throwable, depending on which
     * parameter is not null.
     *
     * @param value value with which the future should be completed
     * @param throwable throwable with which the future should be completed exceptionally
     * @param target future to complete
     * @param <T> completed future
     */
    public static <T> void doForward(
            @Nullable T value, @Nullable Throwable throwable, CompletableFuture<T> target) {
        if (throwable != null) {
            target.completeExceptionally(throwable);
        } else {
            target.complete(value);
        }
    }

    public static Throwable unwrapCompletionException(Throwable t) {
        if (t instanceof CompletionException) {
            return unwrapCompletionException(t.getCause());
        } else {
            return t;
        }
    }

    /**
     * Wraps a Runnable so that throwables are caught and logged when a Runnable is run.
     *
     * <p>The main usecase for this method is to be used in {@link
     * java.util.concurrent.ScheduledExecutorService#scheduleAtFixedRate(Runnable, long, long,
     * TimeUnit)} calls to ensure that the scheduled task doesn't get cancelled as a result of an
     * uncaught exception.
     *
     * @param runnable The runnable to wrap
     * @return a wrapped Runnable
     */
    public static Runnable catchingAndLoggingThrowables(Runnable runnable) {
        return new CatchingAndLoggingRunnable(runnable);
    }

    private static final class CatchingAndLoggingRunnable implements Runnable {
        private final Runnable runnable;

        private CatchingAndLoggingRunnable(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public void run() {
            try {
                runnable.run();
            } catch (Throwable t) {
                LOG.error("Unexpected throwable caught", t);
            }
        }
    }
}

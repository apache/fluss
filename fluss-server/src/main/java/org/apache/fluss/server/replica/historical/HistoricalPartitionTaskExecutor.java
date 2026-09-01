/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.fluss.server.replica.historical;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.utils.ExecutorUtils;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Executes lookup and write tasks for historical partitions.
 *
 * <p>Lookups and writes share one executor and one request limit. A permit is held from task
 * acceptance until completion, so the limit covers both queued and running work.
 *
 * <p>Normal submissions may run concurrently. Ordered submissions with the same key are chained in
 * acceptance order, while submissions with different keys can still run in parallel.
 */
@Internal
public final class HistoricalPartitionTaskExecutor implements AutoCloseable {

    private static final Duration THREAD_KEEP_ALIVE = Duration.ofMinutes(10);
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);
    private static final String THREAD_NAME_PREFIX = "historical-partition-io";

    private final int maxQueuedHistoricalRequests;
    // Shared by lookup and write tasks, including tasks waiting in the executor queue.
    private final Semaphore requestPermits;
    // Keep accepted requests so close() can cancel work that remains after executor shutdown.
    private final Set<CompletableFuture<?>> pendingRequests;
    private final ExecutorService executor;
    private final Object orderedTasksLock = new Object();

    // The latest accepted task for each ordering key. A completed tail is removed only when it is
    // still the current tail, so an older completion cannot remove a newer task from the chain.
    @GuardedBy("orderedTasksLock")
    private final Map<Object, CompletableFuture<Void>> orderedTaskTails;

    /** Creates a historical-partition task executor from the server configuration. */
    public HistoricalPartitionTaskExecutor(Configuration conf) {
        this(conf, null);
    }

    /** Creates a historical-partition task executor backed by the supplied executor. */
    @VisibleForTesting
    public HistoricalPartitionTaskExecutor(
            Configuration conf, @Nullable ExecutorService historicalPartitionExecutor) {
        checkNotNull(conf, "conf must not be null.");
        this.maxQueuedHistoricalRequests =
                conf.get(ConfigOptions.NETTY_SERVER_MAX_QUEUED_HISTORICAL_REQUESTS);
        checkArgument(
                maxQueuedHistoricalRequests > 0,
                "%s must be greater than 0.",
                ConfigOptions.NETTY_SERVER_MAX_QUEUED_HISTORICAL_REQUESTS.key());
        int maxThreadPoolSize =
                conf.get(ConfigOptions.SERVER_HISTORICAL_PARTITION_THREAD_POOL_MAX_SIZE);
        checkArgument(
                maxThreadPoolSize > 0,
                "%s must be greater than 0.",
                ConfigOptions.SERVER_HISTORICAL_PARTITION_THREAD_POOL_MAX_SIZE.key());
        this.executor =
                historicalPartitionExecutor == null
                        ? createHistoricalPartitionExecutor(maxThreadPoolSize)
                        : historicalPartitionExecutor;
        this.requestPermits = new Semaphore(maxQueuedHistoricalRequests);
        this.pendingRequests = ConcurrentHashMap.newKeySet();
        this.orderedTaskTails = new HashMap<>();
    }

    /**
     * Submits a task or returns the supplied throttled result when the shared request limit is
     * full.
     *
     * <p>The throttled result is completed by the caller thread without entering the executor.
     */
    public <T> CompletableFuture<T> submit(Supplier<T> task, Supplier<T> throttledResult) {
        checkNotNull(task, "task must not be null.");
        checkNotNull(throttledResult, "throttledResult must not be null.");
        if (!requestPermits.tryAcquire()) {
            return CompletableFuture.completedFuture(throttledResult.get());
        }

        CompletableFuture<T> future;
        try {
            future = CompletableFuture.supplyAsync(task, executor);
        } catch (RuntimeException e) {
            requestPermits.release();
            throw e;
        }
        return trackAcceptedRequest(future);
    }

    /**
     * Submits a task after all previously accepted tasks with the same ordering key have finished.
     *
     * <p>A failed task does not break the chain: its completion still releases the next task for
     * that key.
     */
    public <T> CompletableFuture<T> submitOrdered(
            Object orderingKey, Supplier<T> task, Supplier<T> throttledResult) {
        checkNotNull(orderingKey, "orderingKey must not be null.");
        checkNotNull(task, "task must not be null.");
        checkNotNull(throttledResult, "throttledResult must not be null.");
        if (!requestPermits.tryAcquire()) {
            return CompletableFuture.completedFuture(throttledResult.get());
        }

        CompletableFuture<T> future;
        CompletableFuture<Void> tail;
        try {
            synchronized (orderedTasksLock) {
                CompletableFuture<Void> previousTail = orderedTaskTails.get(orderingKey);
                if (previousTail == null) {
                    future = CompletableFuture.supplyAsync(task, executor);
                } else {
                    future = previousTail.thenApplyAsync(ignored -> task.get(), executor);
                }
                // Convert success or failure into a normal completion used only for sequencing.
                tail = future.handle((ignored, error) -> null);
                orderedTaskTails.put(orderingKey, tail);
            }
        } catch (RuntimeException e) {
            requestPermits.release();
            throw e;
        }

        CompletableFuture<Void> currentTail = tail;
        tail.whenComplete(
                (ignored, error) -> {
                    synchronized (orderedTasksLock) {
                        orderedTaskTails.remove(orderingKey, currentTail);
                    }
                });
        return trackAcceptedRequest(future);
    }

    private <T> CompletableFuture<T> trackAcceptedRequest(CompletableFuture<T> future) {
        pendingRequests.add(future);
        future.whenComplete(
                (ignored, error) -> {
                    // Release the permit exactly once when the accepted task reaches a terminal
                    // state, including exceptional completion and cancellation.
                    pendingRequests.remove(future);
                    requestPermits.release();
                });
        return future;
    }

    /** Returns the number of accepted historical requests that have not completed. */
    @VisibleForTesting
    public int numInflightRequests() {
        return maxQueuedHistoricalRequests - requestPermits.availablePermits();
    }

    @Override
    public void close() {
        ExecutorUtils.gracefulShutdown(
                SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS, executor);
        pendingRequests.forEach(future -> future.cancel(true));
    }

    private static ExecutorService createHistoricalPartitionExecutor(int maxThreadPoolSize) {
        ThreadPoolExecutor executor =
                new ThreadPoolExecutor(
                        maxThreadPoolSize,
                        maxThreadPoolSize,
                        THREAD_KEEP_ALIVE.toMillis(),
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        new ExecutorThreadFactory(THREAD_NAME_PREFIX));
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }
}

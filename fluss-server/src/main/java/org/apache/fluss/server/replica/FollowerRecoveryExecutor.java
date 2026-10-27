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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;
import org.apache.fluss.utils.concurrent.FutureUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Runs follower recovery in temporary pools owned by a single recovery batch. */
final class FollowerRecoveryExecutor {
    private final boolean enabled;
    private final BiFunction<File, Integer, ExecutorService> executorFactory;

    FollowerRecoveryExecutor(boolean enabled, int threadsPerDataDir) {
        this(
                enabled,
                threadsPerDataDir,
                (directory, tasks) ->
                        new ThreadPoolExecutor(
                                threadsPerDataDir,
                                threadsPerDataDir,
                                0L,
                                TimeUnit.MILLISECONDS,
                                new ArrayBlockingQueue<>(tasks),
                                new ExecutorThreadFactory(
                                        "follower-recovery-" + directory.getAbsolutePath())));
    }

    @VisibleForTesting
    FollowerRecoveryExecutor(
            boolean enabled,
            int threadsPerDataDir,
            BiFunction<File, Integer, ExecutorService> executorFactory) {
        checkArgument(
                threadsPerDataDir > 0,
                "Follower recovery threads per data directory must be greater than zero.");
        this.enabled = enabled;
        this.executorFactory = executorFactory;
    }

    /** Keeps the direct caller path when disabled; otherwise submits one task per replica. */
    void recover(List<Replica> replicas, Consumer<Replica> recovery) {
        if (!enabled) {
            replicas.forEach(recovery);
            return;
        }
        Map<File, List<Runnable>> directoryTasks = new HashMap<>();
        for (Replica replica : replicas) {
            directoryTasks
                    .computeIfAbsent(
                            replica.getLogTablet().getDataDir(), ignored -> new ArrayList<>())
                    .add(() -> recovery.accept(replica));
        }
        recoverDirectories(directoryTasks);
    }

    /**
     * Retains failures in directory and replica input order and waits for actual pool termination.
     */
    @VisibleForTesting
    void recoverDirectories(Map<File, List<Runnable>> directoryTasks) {
        Map<File, List<Runnable>> ordered =
                new TreeMap<>(Comparator.comparing(File::getAbsolutePath));
        ordered.putAll(directoryTasks);
        List<ExecutorService> pools = new ArrayList<>();
        List<Future<?>> tasks = new ArrayList<>();
        Throwable failure = null;
        boolean interrupted = Thread.interrupted();
        try {
            for (Map.Entry<File, List<Runnable>> directory : ordered.entrySet()) {
                ExecutorService pool =
                        executorFactory.apply(directory.getKey(), directory.getValue().size());
                pools.add(pool);
                for (Runnable task : directory.getValue()) {
                    tasks.add(pool.submit(task));
                }
            }
        } catch (Throwable submissionFailure) {
            tasks.add(FutureUtils.completedExceptionally(submissionFailure));
        } finally {
            for (ExecutorService pool : pools) {
                try {
                    pool.shutdown();
                } catch (Throwable shutdownFailure) {
                    tasks.add(FutureUtils.completedExceptionally(shutdownFailure));
                }
            }
            for (Future<?> task : tasks) {
                boolean completed = false;
                while (!completed) {
                    try {
                        task.get();
                        completed = true;
                    } catch (InterruptedException e) {
                        interrupted = true;
                    } catch (ExecutionException e) {
                        failure = ExceptionUtils.firstOrSuppressed(e.getCause(), failure);
                        completed = true;
                    } catch (Throwable taskFailure) {
                        failure = ExceptionUtils.firstOrSuppressed(taskFailure, failure);
                        completed = true;
                    }
                }
            }
            for (ExecutorService pool : pools) {
                while (!pool.isTerminated()) {
                    try {
                        pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
            }
            interrupted |= Thread.interrupted();
            if (interrupted) {
                failure =
                        ExceptionUtils.firstOrSuppressed(
                                new InterruptedException("Interrupted during follower recovery."),
                                failure);
                Thread.currentThread().interrupt();
            }
        }
        if (failure != null) {
            ExceptionUtils.rethrow(failure);
        }
    }
}

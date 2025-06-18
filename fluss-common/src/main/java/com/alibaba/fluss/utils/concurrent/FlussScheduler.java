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

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.annotation.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A scheduler based on {@link ScheduledThreadPoolExecutor}.
 *
 * <p>It has a pool of fluss-scheduler- threads that do the actual work.
 */
@Internal
public class FlussScheduler implements Scheduler {

    private static final Logger LOG = LoggerFactory.getLogger(FlussScheduler.class);

    private final AtomicInteger schedulerThreadId = new AtomicInteger(0);
    private final int threads;
    private final boolean daemon;
    private final String threadNamePrefix;

    // TODO there is no need to use volatile and synchronized together. We can use volatile only.
    private volatile ScheduledThreadPoolExecutor executor;

    public FlussScheduler(int threads) {
        this(threads, true);
    }

    public FlussScheduler(int threads, boolean daemon) {
        this(threads, daemon, "fluss-scheduler-");
    }

    public FlussScheduler(int threads, boolean daemon, String threadNamePrefix) {
        this.threads = threads;
        this.daemon = daemon;
        this.threadNamePrefix = threadNamePrefix;
    }

    @Override
    public void startup() {
        LOG.debug("Initializing task scheduler.");
        synchronized (this) {
            if (isStarted()) {
                throw new IllegalStateException("This scheduler has already been started.");
            }
            ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(threads);
            executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            executor.setRemoveOnCancelPolicy(true);
            executor.setThreadFactory(
                    new ExecutorThreadFactory(
                            threadNamePrefix + schedulerThreadId.getAndIncrement()));
            this.executor = executor;
        }
    }

    @Override
    public void shutdown() throws InterruptedException {
        LOG.debug("Shutting down task scheduler.");
        // We use the local variable to avoid NullPointerException if another thread shuts down
        // scheduler at same time.
        ScheduledThreadPoolExecutor maybeExecutor = null;
        synchronized (this) {
            if (isStarted()) {
                maybeExecutor = executor;
                maybeExecutor.shutdown();
                this.executor = null;
            }
        }
        if (maybeExecutor != null) {
            maybeExecutor.awaitTermination(1, TimeUnit.DAYS);
        }
    }

    @Override
    public ScheduledFuture<?> schedule(String name, Runnable task, long delayMs, long periodMs) {
        LOG.debug(
                "Scheduling task {} with initial delay {} ms and period {} ms.",
                name,
                delayMs,
                periodMs);
        synchronized (this) {
            if (isStarted()) {
                Runnable runnable =
                        () -> {
                            try {
                                LOG.trace("Beginning execution of scheduled task '{}'.", name);
                                task.run();
                            } catch (Throwable t) {
                                LOG.error("Uncaught exception in scheduled task '{}'", name, t);
                            } finally {
                                LOG.trace("Completed execution of scheduled task '{}'.", name);
                            }
                        };
                if (periodMs > 0) {
                    return executor.scheduleAtFixedRate(
                            runnable, delayMs, periodMs, TimeUnit.MILLISECONDS);
                } else {
                    return executor.schedule(runnable, delayMs, TimeUnit.MILLISECONDS);
                }
            } else {
                LOG.info(
                        "Fluss scheduler is not running at the time task '{}' is scheduled. The task is ignored.",
                        name);
                return new NoOpScheduledFutureTask();
            }
        }
    }

    public final boolean isStarted() {
        return executor != null;
    }

    private static class NoOpScheduledFutureTask implements ScheduledFuture<Void> {

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return true;
        }

        @Override
        public boolean isCancelled() {
            return true;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public Void get() {
            return null;
        }

        @Override
        public Void get(long timeout, TimeUnit unit) {
            return null;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return 0L;
        }

        @Override
        public int compareTo(Delayed o) {
            long diff = getDelay(TimeUnit.NANOSECONDS) - o.getDelay(TimeUnit.NANOSECONDS);
            if (diff < 0) {
                return -1;
            } else if (diff > 0) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    @VisibleForTesting
    public boolean taskRunning(ScheduledFuture<?> task) {
        ScheduledThreadPoolExecutor e = executor;
        return e != null && e.getQueue().contains(task);
    }
}

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

package org.apache.fluss.server.kv;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.utils.timer.DefaultTimer;
import org.apache.fluss.server.utils.timer.Timer;
import org.apache.fluss.server.utils.timer.TimerTask;
import org.apache.fluss.utils.ExecutorUtils;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;
import org.apache.fluss.utils.concurrent.ShutdownableThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Shared scheduler for asynchronous KV flushes on one tablet server. */
public class KvFlushScheduler implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KvFlushScheduler.class);

    private static final long DEFAULT_RETRY_DELAY_MS = 100L;

    private static final String FLUSH_THREAD_POOL_NAME = "kv-flush";

    private static final String FLUSH_THREAD_NAME_PREFIX = FLUSH_THREAD_POOL_NAME + "-thread-";

    private final ThreadPoolExecutor flushExecutor;
    private final Timer retryTimer;
    private final ShutdownableThread retryReaper;
    private final Object lifecycleLock = new Object();

    private volatile boolean closed;

    @VisibleForTesting
    KvFlushScheduler() {
        this.flushExecutor = null;
        this.retryTimer = null;
        this.retryReaper = null;
    }

    public KvFlushScheduler(Configuration conf) {
        int flushThreads = Math.max(1, conf.get(ConfigOptions.KV_FLUSH_THREAD_NUM));
        this.flushExecutor = newExecutor(flushThreads, FLUSH_THREAD_POOL_NAME);
        this.retryTimer = new DefaultTimer("kv-flush-retry");
        this.retryReaper = new FlushRetryReaper();
        this.retryReaper.start();
        LOG.info("Created KV flush scheduler with {} flush threads.", flushThreads);
    }

    public void enqueue(KvTablet tablet) {
        FlushTask task = new FlushTask(tablet);
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            flushExecutor.execute(task);
        }
    }

    public void retryLater(KvTablet tablet) {
        retryLater(tablet, DEFAULT_RETRY_DELAY_MS);
    }

    public void retryLater(KvTablet tablet, long delayMs) {
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            retryTimer.add(
                    new TimerTask(delayMs) {
                        @Override
                        public void run() {
                            if (!closed) {
                                tablet.requestFlushRetry();
                            }
                        }
                    });
        }
    }

    @Override
    public void close() {
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            closed = true;
        }
        if (flushExecutor != null) {
            if (isFlushExecutorThread()) {
                flushExecutor.shutdown();
            } else {
                ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, flushExecutor);
            }
        }
        if (retryReaper != null && retryTimer != null) {
            retryReaper.initiateShutdown();
            retryTimer.add(
                    new TimerTask(0) {
                        @Override
                        public void run() {}
                    });
            try {
                retryReaper.awaitShutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted while shutting down KV flush scheduler retry reaper.", e);
            }
            retryTimer.shutdown();
        }
    }

    private boolean isFlushExecutorThread() {
        return Thread.currentThread().getName().startsWith(FLUSH_THREAD_NAME_PREFIX);
    }

    private static ThreadPoolExecutor newExecutor(int threads, String threadName) {
        return new ThreadPoolExecutor(
                threads,
                threads,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new ExecutorThreadFactory(threadName));
    }

    private static final class FlushTask implements Runnable {
        private final KvTablet tablet;

        private FlushTask(KvTablet tablet) {
            this.tablet = tablet;
        }

        @Override
        public void run() {
            tablet.runScheduledFlush();
        }
    }

    private class FlushRetryReaper extends ShutdownableThread {
        private FlushRetryReaper() {
            super("kv-flush-retry-reaper", false);
        }

        @Override
        public void doWork() throws InterruptedException {
            retryTimer.advanceClock(200L);
        }
    }
}

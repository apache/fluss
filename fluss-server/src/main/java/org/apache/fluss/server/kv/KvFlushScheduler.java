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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.utils.ExecutorUtils;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Shared scheduler for asynchronous KV flushes on one tablet server. */
public class KvFlushScheduler implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KvFlushScheduler.class);

    private static final long RETRY_DELAY_MS = 100L;

    private final ThreadPoolExecutor normalExecutor;
    private final ThreadPoolExecutor pressureExecutor;
    private final ScheduledExecutorService retryExecutor;
    private final AtomicLong sequence = new AtomicLong();

    private volatile boolean closed;

    public KvFlushScheduler(Configuration conf) {
        int totalThreads = Math.max(2, conf.get(ConfigOptions.SERVER_IO_POOL_SIZE));
        int pressureThreads = Math.max(1, totalThreads / 4);
        int normalThreads = Math.max(1, totalThreads - pressureThreads);
        this.normalExecutor =
                newExecutor(normalThreads, "kv-flush-normal", new PriorityBlockingQueue<>());
        this.pressureExecutor =
                newExecutor(pressureThreads, "kv-flush-pressure", new PriorityBlockingQueue<>());
        this.retryExecutor =
                new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory("kv-flush-retry"));
        LOG.info(
                "Created KV flush scheduler with {} normal threads and {} pressure threads.",
                normalThreads,
                pressureThreads);
    }

    public void enqueue(KvTablet tablet) {
        if (closed) {
            return;
        }
        FlushTask task = new FlushTask(tablet, sequence.incrementAndGet());
        if (tablet.usePressureFlushLane()) {
            pressureExecutor.execute(task);
        } else {
            normalExecutor.execute(task);
        }
    }

    public void retryLater(KvTablet tablet) {
        if (closed) {
            return;
        }
        retryExecutor.schedule(
                () -> tablet.requestFlushRetry(), RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        closed = true;
        ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, normalExecutor);
        ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, pressureExecutor);
        ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, retryExecutor);
    }

    private static ThreadPoolExecutor newExecutor(
            int threads, String threadName, PriorityBlockingQueue<Runnable> queue) {
        return new ThreadPoolExecutor(
                threads,
                threads,
                0L,
                TimeUnit.MILLISECONDS,
                queue,
                new ExecutorThreadFactory(threadName));
    }

    private static final class FlushTask implements Runnable, Comparable<FlushTask> {
        private final KvTablet tablet;
        private final long sequence;
        private final long score;

        private FlushTask(KvTablet tablet, long sequence) {
            this.tablet = tablet;
            this.sequence = sequence;
            this.score = tablet.flushScore();
        }

        @Override
        public void run() {
            tablet.runScheduledFlush();
        }

        @Override
        public int compareTo(FlushTask that) {
            int scoreCompare = Long.compare(that.score, this.score);
            if (scoreCompare != 0) {
                return scoreCompare;
            }
            return Long.compare(this.sequence, that.sequence);
        }
    }
}

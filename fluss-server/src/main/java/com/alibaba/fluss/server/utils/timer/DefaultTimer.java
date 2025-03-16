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

package com.alibaba.fluss.server.utils.timer;

import com.alibaba.fluss.utils.concurrent.ExecutorThreadFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.alibaba.fluss.utils.concurrent.LockUtils.inReadLock;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Default implementation of Timer interface that uses a hierarchical timing wheel. This class
 * provides a scalable and high-performance timing mechanism for handling a large number of timed
 * tasks.
 */
@ThreadSafe
public class DefaultTimer implements Timer {
    private final ExecutorService taskExecutor;
    private final DelayQueue<TimerTaskList> delayQueue;
    private final AtomicInteger taskCounter;
    private final TimingWheel timingWheel;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public DefaultTimer(String executorName, long tickMs, int wheelSize, long startMs) {
        this.taskExecutor =
                Executors.newFixedThreadPool(1, new ExecutorThreadFactory(executorName));
        this.delayQueue = new DelayQueue<>();
        this.taskCounter = new AtomicInteger(0);
        this.timingWheel = new TimingWheel(tickMs, wheelSize, startMs, taskCounter, delayQueue);
    }

    public DefaultTimer(String executorName) {
        this(executorName, 1, 20, TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
    }

    @Override
    public void add(TimerTask timerTask) {
        inReadLock(
                readWriteLock,
                () ->
                        addTimerTaskEntry(
                                new TimerTaskEntry(
                                        timerTask,
                                        timerTask.getDelayMs()
                                                + TimeUnit.NANOSECONDS.toMillis(
                                                        System.nanoTime()))));
    }

    @Override
    public boolean advanceClock(long waitMs) throws InterruptedException {
        TimerTaskList bucket = delayQueue.poll(waitMs, TimeUnit.MILLISECONDS);
        if (bucket != null) {
            readWriteLock.writeLock().lock();
            try {
                while (bucket != null) {
                    timingWheel.advanceClock(bucket.getExpiration());
                    bucket.flush(this::addTimerTaskEntry);
                    bucket = delayQueue.poll();
                }
            } finally {
                readWriteLock.writeLock().unlock();
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int numOfTimerTasks() {
        return taskCounter.get();
    }

    @Override
    public void shutdown() {
        taskExecutor.shutdown();
    }

    private void addTimerTaskEntry(TimerTaskEntry timerTaskEntry) {
        if (!timingWheel.add(timerTaskEntry)) {
            // Already expired or cancelled.
            if (!timerTaskEntry.isCancelled()) {
                taskExecutor.submit(timerTaskEntry.getTimerTask());
            }
        }
    }
}

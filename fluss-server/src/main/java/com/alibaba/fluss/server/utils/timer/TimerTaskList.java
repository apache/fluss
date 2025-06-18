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

package com.alibaba.fluss.server.utils.timer;

import com.alibaba.fluss.utils.clock.Clock;
import com.alibaba.fluss.utils.clock.SystemClock;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** A list of {@link TimerTask} which use link entry to store task. */
@ThreadSafe
class TimerTaskList implements Delayed {
    private final AtomicInteger taskCounter;
    private final Clock clock;
    private final TimerTaskEntry root = new TimerTaskEntry(null, -1);
    private final AtomicLong expiration = new AtomicLong(-1L);

    TimerTaskList(AtomicInteger taskCounter) {
        this(taskCounter, SystemClock.getInstance());
    }

    TimerTaskList(AtomicInteger taskCounter, Clock clock) {
        this.taskCounter = taskCounter;
        this.root.next = root;
        this.root.prev = root;
        this.clock = clock;
    }

    /**
     * Set the bucket's expiration time.
     *
     * @param expirationMs expiration time in milliseconds
     * @return return true if the expiration time is changed
     */
    boolean setExpiration(long expirationMs) {
        return expiration.getAndSet(expirationMs) != expirationMs;
    }

    /** Get the bucket's expiration time. */
    long getExpiration() {
        return expiration.get();
    }

    /** Apply the supplied function to each of tasks in this list. */
    synchronized void forEach(Consumer<TimerTask> f) {
        TimerTaskEntry entry = root.next;
        while (entry != root) {
            TimerTaskEntry nextEntry = entry.next;
            if (!entry.isCancelled()) {
                f.accept(entry.getTimerTask());
            }
            entry = nextEntry;
        }
    }

    /** Add a timer task entry to this list. */
    void add(TimerTaskEntry timerTaskEntry) {
        boolean done = false;
        while (!done) {
            // Remove the timer task entry if it is already in any other list. We do this outside of
            // the sync block below to avoid deadlocking. We may retry until timerTaskEntry.list
            // becomes null.
            timerTaskEntry.remove();
            synchronized (this) {
                synchronized (timerTaskEntry) {
                    if (timerTaskEntry.list == null) {
                        TimerTaskEntry tail = root.prev;
                        timerTaskEntry.next = root;
                        timerTaskEntry.prev = tail;
                        timerTaskEntry.list = this;
                        tail.next = timerTaskEntry;
                        root.prev = timerTaskEntry;
                        taskCounter.incrementAndGet();
                        done = true;
                    }
                }
            }
        }
    }

    /** Remove the specified timer task entry from this list. */
    synchronized void remove(TimerTaskEntry timerTaskEntry) {
        synchronized (timerTaskEntry) {
            if (timerTaskEntry.list == this) {
                timerTaskEntry.next.prev = timerTaskEntry.prev;
                timerTaskEntry.prev.next = timerTaskEntry.next;
                timerTaskEntry.next = null;
                timerTaskEntry.prev = null;
                timerTaskEntry.list = null;
                taskCounter.decrementAndGet();
            }
        }
    }

    /** Remove all task entries and apply the supplied function to each of them. */
    synchronized void flush(Consumer<TimerTaskEntry> f) {
        TimerTaskEntry head = root.next;
        while (head != root) {
            remove(head);
            f.accept(head);
            head = root.next;
        }
        expiration.set(-1L);
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(
                Math.max(getExpiration() - TimeUnit.NANOSECONDS.toMillis(clock.nanoseconds()), 0),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed d) {
        long dExp = ((TimerTaskList) d).getExpiration();
        return Long.compare(getExpiration(), dExp);
    }
}

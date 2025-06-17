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

import javax.annotation.concurrent.ThreadSafe;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Abstract class representing a delayed task that can be scheduled to run after a specified delay.
 *
 * <p>A TimerTask is meant to be used in conjunction with a Timer, which manages the scheduling and
 * execution of the task. The Timer ensures that the task is executed no sooner than the specified
 * delay after it was scheduled.
 */
@ThreadSafe
public abstract class TimerTask implements Runnable {
    protected final long delayMs;
    private volatile TimerTaskEntry timerTaskEntry = null;

    public TimerTask(long delayMs) {
        this.delayMs = delayMs;
    }

    public synchronized void cancel() {
        if (timerTaskEntry != null) {
            timerTaskEntry.remove();
        }

        timerTaskEntry = null;
    }

    synchronized void setTimerTaskEntry(TimerTaskEntry entry) {
        // if this timerTask is already held by an existing timer task entry, we will remove such an
        // entry first.
        if (timerTaskEntry != null && timerTaskEntry != entry) {
            timerTaskEntry.remove();
        }
        timerTaskEntry = entry;
    }

    TimerTaskEntry getTimerTaskEntry() {
        return timerTaskEntry;
    }

    @Override
    public abstract void run();

    public long getDelayMs() {
        return delayMs;
    }
}

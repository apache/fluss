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

package com.alibaba.fluss.utils.clock;

import com.alibaba.fluss.annotation.PublicEvolving;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link Clock} implementation which allows to advance time manually.
 *
 * @since 0.2
 */
@PublicEvolving
public final class ManualClock implements Clock {

    private final AtomicLong currentTimeNs;

    public ManualClock() {
        this(0);
    }

    public ManualClock(long startTimeInMs) {
        this.currentTimeNs = new AtomicLong(startTimeInMs * 1_000_000L);
    }

    @Override
    public long milliseconds() {
        return currentTimeNs.get() / 1_000_000L;
    }

    @Override
    public long nanoseconds() {
        return currentTimeNs.get();
    }

    /**
     * Advances the time by the given duration. Time can also move backwards by supplying a negative
     * value. This method performs no overflow check.
     */
    public void advanceTime(long duration, TimeUnit timeUnit) {
        currentTimeNs.addAndGet(timeUnit.toNanos(duration));
    }

    /**
     * Advances the time by the given duration. Time can also move backwards by supplying a negative
     * value. This method performs no overflow check.
     */
    public void advanceTime(Duration duration) {
        currentTimeNs.addAndGet(duration.toNanos());
    }
}

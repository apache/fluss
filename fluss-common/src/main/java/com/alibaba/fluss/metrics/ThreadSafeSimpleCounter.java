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

package com.alibaba.fluss.metrics;

import com.alibaba.fluss.annotation.Internal;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.LongAdder;

/** A simple low-overhead {@link Counter} that is thread-safe. */
@ThreadSafe
@Internal
public class ThreadSafeSimpleCounter implements Counter {

    /** the current count. */
    private final LongAdder longAdder = new LongAdder();

    /** Increment the current count by 1. */
    @Override
    public void inc() {
        longAdder.increment();
    }

    /**
     * Increment the current count by the given value.
     *
     * @param n value to increment the current count by
     */
    @Override
    public void inc(long n) {
        longAdder.add(n);
    }

    /** Decrement the current count by 1. */
    @Override
    public void dec() {
        longAdder.decrement();
    }

    /**
     * Decrement the current count by the given value.
     *
     * @param n value to decrement the current count by
     */
    @Override
    public void dec(long n) {
        longAdder.add(-n);
    }

    /**
     * Returns the current count.
     *
     * @return current count
     */
    @Override
    public long getCount() {
        return longAdder.longValue();
    }
}

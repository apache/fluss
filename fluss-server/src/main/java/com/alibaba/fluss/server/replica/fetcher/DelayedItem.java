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

package com.alibaba.fluss.server.replica.fetcher;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/** The item use to control the delay of the fetch operation. */
public class DelayedItem implements Delayed {

    private final long delayMs;
    private final long dueMs;

    public DelayedItem(long delayMs) {
        this.delayMs = delayMs;
        this.dueMs = System.currentTimeMillis() + delayMs;
    }

    public long getDelayMs() {
        return delayMs;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        long delayMs = Math.max(dueMs - System.currentTimeMillis(), 0);
        return unit.convert(delayMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        if (o instanceof DelayedItem) {
            DelayedItem other = (DelayedItem) o;
            return Long.compare(this.dueMs, other.dueMs);
        }
        throw new IllegalArgumentException("Cannot compare DelayedItem with " + o.getClass());
    }
}

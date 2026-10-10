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

package org.apache.fluss.microbench.stats;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/** Bounded, thread-safe latency sample reservoir. */
public class LatencyRecorder {

    private static final int CAPACITY = 100_000;

    private final AtomicLongArray samples = new AtomicLongArray(CAPACITY);
    private final AtomicLong count = new AtomicLong();
    private final AtomicLong max = new AtomicLong();

    public void record(long latencyNanos) {
        long position = count.getAndIncrement();
        max.accumulateAndGet(latencyNanos, Math::max);
        if (position < CAPACITY) {
            samples.set((int) position, latencyNanos);
        } else {
            long replacement = ThreadLocalRandom.current().nextLong(position + 1);
            if (replacement < CAPACITY) {
                samples.set((int) replacement, latencyNanos);
            }
        }
    }

    public long p50Nanos() {
        return percentile(0.50);
    }

    public long p95Nanos() {
        return percentile(0.95);
    }

    public long p99Nanos() {
        return percentile(0.99);
    }

    public long maxNanos() {
        return max.get();
    }

    private long percentile(double rank) {
        int size = (int) Math.min(count.get(), CAPACITY);
        if (size == 0) {
            return 0;
        }
        long[] sorted = new long[size];
        for (int i = 0; i < size; i++) {
            sorted[i] = samples.get(i);
        }
        Arrays.sort(sorted);
        return sorted[Math.max(0, (int) Math.ceil(rank * size) - 1)];
    }
}

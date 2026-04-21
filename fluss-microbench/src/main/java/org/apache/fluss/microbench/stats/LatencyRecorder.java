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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thread-safe latency recorder using reservoir sampling to bound memory usage. Stores at most
 * {@code RESERVOIR_SIZE} samples regardless of total record count. Uses {@link AtomicLongArray} for
 * memory visibility across threads.
 *
 * <p>Percentile queries cache the sorted snapshot to avoid redundant sorts when multiple
 * percentiles are queried in quick succession (e.g. during progress reporting). The cache uses
 * CAS-based updates to prevent redundant sorting under concurrent access.
 */
public class LatencyRecorder {
    // 100K samples balances memory (~800KB per recorder) vs. percentile accuracy (<0.1% error).
    private static final int RESERVOIR_SIZE = 100_000;

    private final AtomicLongArray reservoir = new AtomicLongArray(RESERVOIR_SIZE);
    private final AtomicLong maxValue = new AtomicLong(0);
    private final AtomicLong totalCount = new AtomicLong(0);

    // Cached sorted snapshot for percentile queries using atomic reference for thread safety
    private final AtomicReference<SortedSnapshot> cached = new AtomicReference<>();

    private static final class SortedSnapshot {
        final long[] data;
        final long count;

        SortedSnapshot(long[] data, long count) {
            this.data = data;
            this.count = count;
        }
    }

    public void record(long latencyNanos) {
        long n = totalCount.incrementAndGet();

        // Update max
        long currentMax;
        do {
            currentMax = maxValue.get();
            if (latencyNanos <= currentMax) {
                break;
            }
        } while (!maxValue.compareAndSet(currentMax, latencyNanos));

        // Reservoir sampling (Algorithm R)
        if (n <= RESERVOIR_SIZE) {
            reservoir.set((int) (n - 1), latencyNanos);
        } else {
            long j = java.util.concurrent.ThreadLocalRandom.current().nextLong(n);
            if (j < RESERVOIR_SIZE) {
                reservoir.set((int) j, latencyNanos);
            }
        }
    }

    public long count() {
        return totalCount.get();
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

    public long p999Nanos() {
        return percentile(0.999);
    }

    public long p9999Nanos() {
        return percentile(0.9999);
    }

    public long maxNanos() {
        return maxValue.get();
    }

    private long percentile(double p) {
        int size = (int) Math.min(totalCount.get(), RESERVOIR_SIZE);
        if (size == 0) {
            return 0;
        }
        long[] sorted = getSortedSnapshot(size);
        int index = (int) Math.ceil(p * sorted.length) - 1;
        return sorted[Math.max(0, index)];
    }

    /**
     * Returns a cached sorted snapshot of the reservoir. The cache uses CAS-based updates to
     * prevent redundant sorting under concurrent access, so multiple percentile queries within the
     * same progress tick share one sort.
     */
    private long[] getSortedSnapshot(int size) {
        for (int attempt = 0; attempt < 3; attempt++) {
            SortedSnapshot snap = cached.get();
            long currentCount = totalCount.get();

            // Check if we have a valid cached snapshot
            if (snap != null && snap.count == currentCount) {
                return snap.data;
            }

            // Build new sorted snapshot
            long[] sorted = new long[size];
            for (int i = 0; i < size; i++) {
                sorted[i] = reservoir.get(i);
            }
            java.util.Arrays.sort(sorted);
            SortedSnapshot newSnap = new SortedSnapshot(sorted, currentCount);

            // Try to update cache atomically
            if (cached.compareAndSet(snap, newSnap)) {
                return sorted;
            }
            // CAS failed: another thread updated the cache, retry to use it
        }
        // Fallback after max retries: return uncached sorted copy
        long[] sorted = new long[size];
        for (int i = 0; i < size; i++) {
            sorted[i] = reservoir.get(i);
        }
        java.util.Arrays.sort(sorted);
        return sorted;
    }

    /**
     * Returns a log-scale histogram of latency samples. Each bucket covers a power-of-2 range in
     * microseconds. Returns an array of [bucketLabelMicros, count] pairs suitable for charting.
     */
    public long[][] histogram(int maxBuckets) {
        int size = (int) Math.min(totalCount.get(), RESERVOIR_SIZE);
        if (size == 0) {
            return new long[0][];
        }
        long[] sorted = getSortedSnapshot(size);

        // Convert to microseconds for bucketing
        long minUs = sorted[0] / 1000;
        long maxUs = sorted[size - 1] / 1000;
        if (maxUs <= 0) {
            maxUs = 1;
        }

        // Handle degenerate case where all values have the same latency
        if (maxUs - minUs < 1) {
            long[][] result = new long[1][2];
            result[0][0] = Math.max(1, minUs);
            result[0][1] = size;
            return result;
        }

        // Create log-scale buckets
        int numBuckets = Math.min(maxBuckets, 30);
        double logMin = Math.log10(Math.max(1, minUs));
        double logMax = Math.log10(Math.max(2, maxUs));
        double step = (logMax - logMin) / numBuckets;
        if (step <= 0) {
            step = 1;
        }

        long[] boundaries = new long[numBuckets + 1];
        long[] counts = new long[numBuckets];
        for (int b = 0; b <= numBuckets; b++) {
            boundaries[b] = (long) Math.pow(10, logMin + b * step);
        }

        int bi = 0;
        for (long val : sorted) {
            long us = val / 1000;
            while (bi < numBuckets - 1 && us >= boundaries[bi + 1]) {
                bi++;
            }
            counts[bi]++;
        }

        long[][] result = new long[numBuckets][2];
        for (int b = 0; b < numBuckets; b++) {
            result[b][0] = boundaries[b];
            result[b][1] = counts[b];
        }
        return result;
    }
}

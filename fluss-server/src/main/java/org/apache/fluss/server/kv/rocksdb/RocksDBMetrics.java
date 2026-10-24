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

package org.apache.fluss.server.kv.rocksdb;

import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.SimpleCounter;
import org.apache.fluss.metrics.ThreadSafeSimpleCounter;
import org.apache.fluss.metrics.groups.MetricGroup;

import io.github.fluss_contrib.rocksdb.Statistics;
import io.github.fluss_contrib.rocksdb.TickerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** TabletServer-scoped read execution and shared block cache counters. */
public class RocksDBMetrics {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBMetrics.class);
    private static final String[] BLOCK_TYPES = {"data", "index", "filter"};
    private static final TickerType[] CACHE_TICKERS = {
        TickerType.BLOCK_CACHE_DATA_HIT, TickerType.BLOCK_CACHE_DATA_MISS,
        TickerType.BLOCK_CACHE_INDEX_HIT, TickerType.BLOCK_CACHE_INDEX_MISS,
        TickerType.BLOCK_CACHE_FILTER_HIT, TickerType.BLOCK_CACHE_FILTER_MISS
    };

    /** Read execution segments; each batch or complete prefix traversal counts once. */
    public enum ReadOperation {
        GET("get"),
        MULTI_GET("multiGet"),
        PREFIX_SCAN("prefixScan");

        private final String label;

        ReadOperation(String label) {
            this.label = label;
        }
    }

    private final ThreadSafeSimpleCounter[] readCounts =
            new ThreadSafeSimpleCounter[ReadOperation.values().length];
    private final ThreadSafeSimpleCounter[] readTimes =
            new ThreadSafeSimpleCounter[ReadOperation.values().length];
    private final Object cacheLock = new Object();
    // These are private collection cursors, never bucket metrics or exported labels.
    private final Map<Statistics, long[]> cacheSamples = new IdentityHashMap<>();
    private final long[] cacheTotals = new long[CACHE_TICKERS.length];

    /** Registers exactly twelve counters under the server's existing identity. */
    public RocksDBMetrics(MetricGroup serverGroup) {
        for (ReadOperation operation : ReadOperation.values()) {
            int index = operation.ordinal();
            MetricGroup group = serverGroup.addGroup("operation", operation.label);
            readCounts[index] = new ThreadSafeSimpleCounter();
            readTimes[index] =
                    new ThreadSafeSimpleCounter() {
                        @Override
                        public long getCount() {
                            // Convert only after accumulation so sub-microsecond calls are not
                            // lost.
                            return TimeUnit.NANOSECONDS.toMicros(super.getCount());
                        }
                    };
            group.counter(MetricNames.ROCKSDB_READ_EXECUTION_COUNT_TOTAL, readCounts[index]);
            group.counter(MetricNames.ROCKSDB_READ_EXECUTION_TIME_US_TOTAL, readTimes[index]);
        }
        for (int i = 0; i < CACHE_TICKERS.length; i++) {
            final int index = i;
            MetricGroup group = serverGroup.addGroup("block_type", BLOCK_TYPES[i / 2]);
            group.counter(
                    i % 2 == 0
                            ? MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_HITS_TOTAL
                            : MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_MISSES_TOTAL,
                    new SimpleCounter() {
                        @Override
                        public long getCount() {
                            return sampleCache(index);
                        }
                    });
        }
    }

    /** Records a completed attempt, including exceptional completion, in nanoseconds. */
    public void recordRead(ReadOperation operation, long elapsedNanos) {
        int index = operation.ordinal();
        readTimes[index].inc(elapsedNanos);
        readCounts[index].inc();
    }

    /**
     * Registers statistics only for a DB using this server's shared block cache. The returned
     * callback must run after the DB closes and before its Statistics native handle is released.
     */
    public Runnable registerSharedCache(Statistics statistics) {
        synchronized (cacheLock) {
            cacheSamples.put(statistics, new long[CACHE_TICKERS.length]);
        }
        return () -> {
            synchronized (cacheLock) {
                long[] previous = cacheSamples.get(statistics);
                if (previous != null) {
                    for (int i = 0; i < CACHE_TICKERS.length; i++) {
                        sampleCache(statistics, previous, i);
                    }
                    cacheSamples.remove(statistics);
                }
            }
        };
    }

    private long sampleCache(int index) {
        synchronized (cacheLock) {
            cacheSamples.forEach(
                    (statistics, previous) -> sampleCache(statistics, previous, index));
            return cacheTotals[index];
        }
    }

    private void sampleCache(Statistics statistics, long[] previous, int index) {
        try {
            long current = statistics.getTickerCount(CACHE_TICKERS[index]);
            // A reset starts a new native counter epoch, without decreasing the server total.
            cacheTotals[index] += current >= previous[index] ? current - previous[index] : current;
            previous[index] = current;
        } catch (Exception e) {
            // Preserve the cursor so the next successful sample still includes this interval.
            LOG.debug("Failed to sample shared block cache ticker {}", CACHE_TICKERS[index], e);
        }
    }
}

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

import org.apache.fluss.annotation.VisibleForTesting;

import io.github.fluss_contrib.rocksdb.HistogramData;
import io.github.fluss_contrib.rocksdb.HistogramType;
import io.github.fluss_contrib.rocksdb.Statistics;
import io.github.fluss_contrib.rocksdb.TickerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/** Shares native statistics by table ID and retains native metric totals per TabletServer. */
public final class RocksDBStatisticsRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBStatisticsRegistry.class);
    private static final TickerType[] CACHE_TICKERS = {
        TickerType.BLOCK_CACHE_DATA_HIT, TickerType.BLOCK_CACHE_DATA_MISS,
        TickerType.BLOCK_CACHE_INDEX_HIT, TickerType.BLOCK_CACHE_INDEX_MISS,
        TickerType.BLOCK_CACHE_FILTER_HIT, TickerType.BLOCK_CACHE_FILTER_MISS
    };

    private static final HistogramType[] READ_HISTOGRAMS = {
        HistogramType.DB_GET, HistogramType.DB_MULTIGET
    };

    private final Map<Long, Entry> entries = new HashMap<>();
    private final boolean sharedBlockCache;
    private final Supplier<Statistics> statisticsFactory;
    private final long[] cacheTotals = new long[CACHE_TICKERS.length];
    private final long[] readCounts = new long[READ_HISTOGRAMS.length];
    private final long[] readTimesMicros = new long[READ_HISTOGRAMS.length];

    /** Creates a statistics registry without shared-cache collection. */
    public RocksDBStatisticsRegistry() {
        this(false);
    }

    /** Creates a registry that collects cache tickers only when the shared cache is enabled. */
    public RocksDBStatisticsRegistry(boolean sharedBlockCache) {
        this(sharedBlockCache, Statistics::new);
    }

    @VisibleForTesting
    RocksDBStatisticsRegistry(boolean sharedBlockCache, Supplier<Statistics> statisticsFactory) {
        this.sharedBlockCache = sharedBlockCache;
        this.statisticsFactory = statisticsFactory;
    }

    /** Returns a shared-cache ticker total, including statistics whose last lease was released. */
    public synchronized long getSharedBlockCacheTickerCount(TickerType ticker) {
        for (int i = 0; i < CACHE_TICKERS.length; i++) {
            if (CACHE_TICKERS[i] == ticker) {
                if (sharedBlockCache) {
                    for (Entry entry : entries.values()) {
                        sampleCache(entry, i);
                    }
                }
                return cacheTotals[i];
            }
        }
        throw new IllegalArgumentException("Not a shared block cache ticker: " + ticker);
    }

    /** Returns the native read histogram count, retaining samples from closed DBs. */
    public synchronized long getNativeReadCount(HistogramType histogram) {
        return readCounts[sampleNativeReads(histogram)];
    }

    /** Returns the native read histogram sum in microseconds, retaining samples from closed DBs. */
    public synchronized long getNativeReadTimeMicros(HistogramType histogram) {
        return readTimesMicros[sampleNativeReads(histogram)];
    }

    /**
     * Acquires statistics that remain alive until the last RocksDB instance of the table releases
     * its lease.
     */
    public synchronized Lease acquire(long tableId) {
        Entry entry = entries.get(tableId);
        if (entry == null) {
            entry = new Entry(statisticsFactory.get());
            entries.put(tableId, entry);
        }
        entry.references++;
        return new Lease(tableId, entry);
    }

    private void sampleCache(Entry entry, int index) {
        try {
            long current = entry.statistics.getTickerCount(CACHE_TICKERS[index]);
            long previous = entry.previousCacheCounts[index];
            // A native counter reset starts a new epoch without reducing the server total.
            cacheTotals[index] += current >= previous ? current - previous : current;
            entry.previousCacheCounts[index] = current;
        } catch (Exception e) {
            LOG.debug("Failed to sample shared block cache ticker {}", CACHE_TICKERS[index], e);
        }
    }

    private int sampleNativeReads(HistogramType histogram) {
        for (int i = 0; i < READ_HISTOGRAMS.length; i++) {
            if (READ_HISTOGRAMS[i] == histogram) {
                for (Entry entry : entries.values()) {
                    sampleNativeRead(entry, i);
                }
                return i;
            }
        }
        throw new IllegalArgumentException("Not a native read histogram: " + histogram);
    }

    private void sampleNativeRead(Entry entry, int index) {
        try {
            HistogramData data = entry.statistics.getHistogramData(READ_HISTOGRAMS[index]);
            long count = data.getCount();
            long time = data.getSum();
            long previousCount = entry.previousReadCounts[index];
            long previousTime = entry.previousReadTimesMicros[index];
            // Count and sum belong to the same histogram reset epoch.
            boolean reset = count < previousCount || time < previousTime;
            readCounts[index] += reset ? count : count - previousCount;
            readTimesMicros[index] += reset ? time : time - previousTime;
            entry.previousReadCounts[index] = count;
            entry.previousReadTimesMicros[index] = time;
        } catch (Exception e) {
            LOG.debug("Failed to sample native read histogram {}", READ_HISTOGRAMS[index], e);
        }
    }

    /** An independently closeable reference to a table's shared statistics. */
    public final class Lease implements AutoCloseable {

        private final long tableId;
        private final Entry entry;
        private boolean closed;

        private Lease(long tableId, Entry entry) {
            this.tableId = tableId;
            this.entry = entry;
        }

        /** Returns the statistics borrowed for the lifetime of this lease. */
        public Statistics getStatistics() {
            return entry.statistics;
        }

        @Override
        public void close() {
            synchronized (RocksDBStatisticsRegistry.this) {
                if (!closed) {
                    closed = true;
                    if (--entry.references == 0) {
                        entries.remove(tableId);
                        if (sharedBlockCache) {
                            for (int i = 0; i < CACHE_TICKERS.length; i++) {
                                sampleCache(entry, i);
                            }
                        }
                        for (int i = 0; i < READ_HISTOGRAMS.length; i++) {
                            sampleNativeRead(entry, i);
                        }
                        entry.statistics.close();
                    }
                }
            }
        }
    }

    private static final class Entry {
        private final Statistics statistics;
        private final long[] previousCacheCounts = new long[CACHE_TICKERS.length];
        private final long[] previousReadCounts = new long[READ_HISTOGRAMS.length];
        private final long[] previousReadTimesMicros = new long[READ_HISTOGRAMS.length];
        private int references;

        private Entry(Statistics statistics) {
            this.statistics = statistics;
        }
    }
}

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

package com.alibaba.fluss.server.kv.rocksdb;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.metrics.Gauge;
import com.alibaba.fluss.metrics.MetricNames;
import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.server.metrics.group.BucketMetricGroup;

import org.rocksdb.Cache;
import org.rocksdb.HistogramData;
import org.rocksdb.HistogramType;
import org.rocksdb.MemoryUsageType;
import org.rocksdb.MemoryUtil;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * RocksDB metrics collector that collects and reports RocksDB statistics including cache,
 * compaction, memory, and I/O metrics with table bucket and server information tags.
 */
public class RocksDBMetricsCollector implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBMetricsCollector.class);

    private final RocksDB rocksDB;
    private final Statistics statistics;
    private final MetricGroup bucketMetricGroup;
    private final TableBucket tableBucket;
    private final TablePath tablePath;
    private final String partitionName;
    private final RocksDBResourceContainer resourceContainer;

    private volatile boolean registered = false;

    private volatile boolean closed = false;

    /** Latch to ensure safe cleanup completion. */
    private final CountDownLatch cleanupLatch = new CountDownLatch(1);

    /** Flag to track if cleanup is in progress. */
    private volatile boolean cleanupInProgress = false;

    // Cached values for efficient metric access
    private volatile long blockCacheHitCount = 0;
    private volatile long blockCacheMissCount = 0;

    /**
     * Wait for cleanup completion with timeout.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @return true if cleanup completed within timeout, false otherwise
     */
    public boolean waitForCleanup(long timeout, TimeUnit unit) {
        try {
            return cleanupLatch.await(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Check if the collector and its resources are in a valid state for metrics collection. This
     * method performs comprehensive validation to prevent accessing closed or invalid resources.
     *
     * @return true if the collector is valid and ready for metrics collection, false otherwise
     */
    private boolean isCollectorValid() {
        if (closed) {
            return false;
        }

        if (rocksDB == null || statistics == null || resourceContainer == null) {
            return false;
        }

        if (resourceContainer.isClosed()) {
            return false;
        }

        return true;
    }

    private volatile long compactionBytesRead = 0;
    private volatile long compactionBytesWritten = 0;
    private volatile long flushBytesWritten = 0;
    private volatile long bytesRead = 0;
    private volatile long bytesWritten = 0;
    private volatile long totalSstFilesSize = 0;
    private volatile long blockCacheAddCount = 0;
    private volatile long memtableMemoryUsage = 0;
    private volatile long blockCacheMemoryUsage = 0;
    private volatile long tableReadersMemoryUsage = 0;
    private volatile long totalMemoryUsage = 0;
    private volatile long blockCacheUsage = 0;
    private volatile long blockCachePinnedUsage = 0;
    private volatile long stallTimeMicros = 0;
    private volatile long numFilesAtLevel0 = 0;
    private volatile long compactionPending = 0;
    private volatile long flushPending = 0;
    private volatile long dbGetLatencyMicros = 0;
    private volatile long dbWriteLatencyMicros = 0;
    private volatile long compactionTimeMicros = 0;

    public RocksDBMetricsCollector(
            RocksDB rocksDB,
            Statistics statistics,
            BucketMetricGroup bucketMetricGroup,
            TableBucket tableBucket,
            TablePath tablePath,
            @Nullable String partitionName,
            RocksDBResourceContainer resourceContainer) {
        this.rocksDB = rocksDB;
        this.statistics = statistics;
        this.tableBucket = tableBucket;
        this.tablePath = tablePath;
        this.partitionName = partitionName;
        this.resourceContainer = resourceContainer;
        this.bucketMetricGroup = bucketMetricGroup;
        registerMetrics();
        RocksDBMetricsManager.getInstance().registerCollector(this);
        this.registered = true;
        LOG.info(
                "RocksDB metrics collector started for table {} bucket {}",
                tablePath,
                tableBucket.getBucket());
    }

    private void registerMetrics() {
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_BLOCK_CACHE_HIT_COUNT, (Gauge<Long>) () -> blockCacheHitCount);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_BLOCK_CACHE_MISS_COUNT,
                (Gauge<Long>) () -> blockCacheMissCount);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_COMPACTION_BYTES_READ, (Gauge<Long>) () -> compactionBytesRead);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_COMPACTION_BYTES_WRITTEN,
                (Gauge<Long>) () -> compactionBytesWritten);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_FLUSH_BYTES_WRITTEN, (Gauge<Long>) () -> flushBytesWritten);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_MEMTABLE_MEMORY_USAGE, (Gauge<Long>) () -> memtableMemoryUsage);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_BLOCK_CACHE_MEMORY_USAGE,
                (Gauge<Long>) () -> blockCacheMemoryUsage);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_TABLE_READERS_MEMORY_USAGE,
                (Gauge<Long>) () -> tableReadersMemoryUsage);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_TOTAL_MEMORY_USAGE, (Gauge<Long>) () -> totalMemoryUsage);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_BLOCK_CACHE_USAGE, (Gauge<Long>) () -> blockCacheUsage);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_BLOCK_CACHE_PINNED_USAGE,
                (Gauge<Long>) () -> blockCachePinnedUsage);
        bucketMetricGroup.gauge(MetricNames.ROCKSDB_BYTES_READ, (Gauge<Long>) () -> bytesRead);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_BYTES_WRITTEN, (Gauge<Long>) () -> bytesWritten);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_TOTAL_SST_FILES_SIZE, (Gauge<Long>) () -> totalSstFilesSize);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_BLOCK_CACHE_ADD_COUNT, (Gauge<Long>) () -> blockCacheAddCount);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_NUM_FILES_AT_LEVEL_0, (Gauge<Long>) () -> numFilesAtLevel0);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_WRITE_STALL_MICROS, (Gauge<Long>) () -> stallTimeMicros);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_COMPACTION_PENDING, (Gauge<Long>) () -> compactionPending);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_FLUSH_PENDING, (Gauge<Long>) () -> flushPending);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_DB_GET_LATENCY_MICROS, (Gauge<Long>) () -> dbGetLatencyMicros);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_DB_WRITE_LATENCY_MICROS,
                (Gauge<Long>) () -> dbWriteLatencyMicros);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_COMPACTION_TIME_MICROS,
                (Gauge<Long>) () -> compactionTimeMicros);
    }

    public void updateMetrics() {
        // Check if cleanup is in progress or collector is closed
        if (cleanupInProgress || !isCollectorValid()) {
            return;
        }

        try {
            blockCacheHitCount = getTickerValue(TickerType.BLOCK_CACHE_HIT);
            blockCacheMissCount = getTickerValue(TickerType.BLOCK_CACHE_MISS);
            compactionBytesRead = getTickerValue(TickerType.COMPACT_READ_BYTES);
            compactionBytesWritten = getTickerValue(TickerType.COMPACT_WRITE_BYTES);
            flushBytesWritten = getTickerValue(TickerType.FLUSH_WRITE_BYTES);
            bytesRead = getTickerValue(TickerType.BYTES_READ);
            bytesWritten = getTickerValue(TickerType.BYTES_WRITTEN);
            stallTimeMicros = getTickerValue(TickerType.STALL_MICROS);
            totalSstFilesSize = getPropertyValueAsLong("rocksdb.total-sst-files-size");
            blockCacheAddCount = getTickerValue(TickerType.BLOCK_CACHE_ADD);
            memtableMemoryUsage = getMemoryUsage(MemoryUsageType.kMemTableTotal);
            blockCacheMemoryUsage = getMemoryUsage(MemoryUsageType.kCacheTotal);
            tableReadersMemoryUsage = getMemoryUsage(MemoryUsageType.kTableReadersTotal);
            totalMemoryUsage = getTotalMemoryUsage();
            blockCacheUsage = getBlockCacheUsage();
            blockCachePinnedUsage = getBlockCachePinnedUsage();
            numFilesAtLevel0 = getPropertyValueAsLong("rocksdb.num-files-at-level0");
            compactionPending = getPropertyValueAsLong("rocksdb.compaction-pending");
            flushPending = getPropertyValueAsLong("rocksdb.flush-pending");
            dbGetLatencyMicros = getHistogramValue(HistogramType.DB_GET);
            dbWriteLatencyMicros = getHistogramValue(HistogramType.DB_WRITE);
            compactionTimeMicros = getHistogramValue(HistogramType.COMPACTION_TIME);
        } catch (Exception e) {
            LOG.warn(
                    "Error updating RocksDB metrics for table {} bucket {}",
                    tablePath,
                    tableBucket.getBucket(),
                    e);
        }
    }

    private long getTickerValue(TickerType tickerType) {
        try {
            // Check if collector is still valid before accessing statistics
            if (!isCollectorValid() || statistics == null) {
                return 0;
            }

            return statistics.getTickerCount(tickerType);
        } catch (Exception e) {
            LOG.debug("Error getting ticker value for {}: {}", tickerType, e.getMessage());
            return 0;
        }
    }

    private long getHistogramValue(HistogramType histogramType) {
        try {
            // Check if collector is still valid before accessing statistics
            if (!isCollectorValid() || statistics == null) {
                return 0;
            }

            HistogramData histogramData = statistics.getHistogramData(histogramType);
            return histogramData != null ? (long) histogramData.getAverage() : 0;
        } catch (Exception e) {
            LOG.debug("Error getting histogram value for {}: {}", histogramType, e.getMessage());
            return 0;
        }
    }

    private long getPropertyValueAsLong(String property) {
        try {
            // Check if collector is still valid before accessing rocksDB
            if (!isCollectorValid() || rocksDB == null) {
                return 0;
            }

            String value = rocksDB.getProperty(property);
            return value != null ? Long.parseLong(value) : 0;
        } catch (NumberFormatException | RocksDBException e) {
            LOG.debug("Error getting property value for {}: {}", property, e.getMessage());
            return 0;
        }
    }

    private long getMemoryUsage(MemoryUsageType memoryUsageType) {
        try {
            // Check if collector is still valid before accessing resources
            if (!isCollectorValid()) {
                return 0;
            }

            Set<Cache> caches = new HashSet<>();
            Cache blockCache = resourceContainer.getBlockCache();
            if (blockCache != null) {
                caches.add(blockCache);
            }

            Map<MemoryUsageType, Long> memoryUsage =
                    MemoryUtil.getApproximateMemoryUsageByType(
                            Collections.singletonList(rocksDB), caches);
            return memoryUsage.getOrDefault(memoryUsageType, 0L);
        } catch (Exception e) {
            LOG.debug("Error getting memory usage for {}: {}", memoryUsageType, e.getMessage());
            return 0;
        }
    }

    private long getTotalMemoryUsage() {
        try {
            // Check if collector is still valid before accessing resources
            if (!isCollectorValid()) {
                return 0;
            }

            Set<Cache> caches = new HashSet<>();
            Cache blockCache = resourceContainer.getBlockCache();
            if (blockCache != null) {
                caches.add(blockCache);
            }

            Map<MemoryUsageType, Long> memoryUsage =
                    MemoryUtil.getApproximateMemoryUsageByType(
                            Collections.singletonList(rocksDB), caches);
            return memoryUsage.values().stream().mapToLong(Long::longValue).sum();
        } catch (Exception e) {
            LOG.debug("Error getting total memory usage: {}", e.getMessage());
            return 0;
        }
    }

    private long getBlockCacheUsage() {
        try {
            // Check if collector is still valid before accessing cache
            if (!isCollectorValid()) {
                return 0;
            }

            Cache cache = resourceContainer.getBlockCache();
            if (cache == null) {
                return 0L;
            }

            // Check if cache is still valid before casting
            if (cache instanceof org.rocksdb.LRUCache) {
                return ((org.rocksdb.LRUCache) cache).getUsage();
            }

            return 0L;
        } catch (Exception e) {
            LOG.debug("Error getting block cache usage: {}", e.getMessage());
            return 0;
        }
    }

    private long getBlockCachePinnedUsage() {
        try {
            // Check if collector is still valid before accessing cache
            if (!isCollectorValid()) {
                return 0;
            }

            Cache cache = resourceContainer.getBlockCache();
            if (cache == null) {
                return 0L;
            }

            // Check if cache is still valid before casting
            if (cache instanceof org.rocksdb.LRUCache) {
                return ((org.rocksdb.LRUCache) cache).getPinnedUsage();
            }

            return 0L;
        } catch (Exception e) {
            LOG.debug("Error getting block cache pinned usage: {}", e.getMessage());
            return 0;
        }
    }

    @Override
    public void close() {
        LOG.info(
                "Closing RocksDB metrics collector for table {} bucket {}.",
                tablePath,
                tableBucket.getBucket());

        // Set cleanup in progress flag to prevent new operations
        cleanupInProgress = true;

        // Set closed flag to prevent new operations
        closed = true;

        // Unregister from global collector
        if (registered) {
            try {
                RocksDBMetricsManager.getInstance().unregisterCollector(this);
            } catch (Exception e) {
                LOG.warn("Error unregistering collector from manager: {}", e.getMessage());
            } finally {
                registered = false;
            }
        }

        // Signal cleanup completion
        cleanupLatch.countDown();
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public String getPartitionName() {
        return partitionName;
    }
}

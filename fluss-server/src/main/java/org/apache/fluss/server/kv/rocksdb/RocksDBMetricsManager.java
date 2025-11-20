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
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.concurrent.FlussScheduler;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Collector for RocksDB metrics that efficiently manages metric collection for thousands of RocksDB
 * instances using a shared thread pool and batch processing.
 */
public class RocksDBMetricsManager {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBMetricsManager.class);

    /** Update interval for metrics collection (in seconds). */
    private static final long METRICS_UPDATE_INTERVAL_SECONDS = 5;

    /** Update interval for metrics collection (in milliseconds). */
    private static final long METRICS_UPDATE_INTERVAL_MS = METRICS_UPDATE_INTERVAL_SECONDS * 1000;

    /** Scheduler for metrics collection. */
    private final Scheduler scheduler;

    /** Scheduled future for the metrics collection task. */
    private volatile ScheduledFuture<?> scheduledTask;

    /** Map of registered metrics collectors. */
    private final ConcurrentMap<String, RocksDBMetricsCollector> collectors =
            MapUtils.newConcurrentHashMap();

    /** Flag to track if manager is running. */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /** Flag to track if manager is shutdown. */
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    /** Lock for synchronizing collector registration/unregistration. */
    private final Object collectorLock = new Object();

    /** Server metric group for aggregating metrics. */
    private final TabletServerMetricGroup serverMetricGroup;

    /** Previous values for calculating deltas (for Counter metrics). */
    private long previousBlockCacheMissCount = 0;

    private long previousBlockCacheHitCount = 0;
    private long previousBlockCacheAddCount = 0;
    private long previousCompactionBytesRead = 0;
    private long previousCompactionBytesWritten = 0;
    private long previousFlushBytesWritten = 0;
    private long previousBytesRead = 0;
    private long previousBytesWritten = 0;

    /**
     * Constructor for production use.
     *
     * @param serverMetricGroup the server metric group for aggregating metrics
     */
    public RocksDBMetricsManager(TabletServerMetricGroup serverMetricGroup) {
        this.serverMetricGroup = serverMetricGroup;
        FlussScheduler flussScheduler = new FlussScheduler(1, true, "rocksdb-metrics-collector-");
        flussScheduler.startup();
        this.scheduler = flussScheduler;
    }

    /**
     * Constructor for testing purposes.
     *
     * @param scheduler the scheduler to use for metrics collection
     * @param serverMetricGroup the server metric group for aggregating metrics
     */
    @VisibleForTesting
    public RocksDBMetricsManager(Scheduler scheduler, TabletServerMetricGroup serverMetricGroup) {
        this.scheduler = scheduler;
        this.serverMetricGroup = serverMetricGroup;
    }

    /**
     * Register a metrics collector for collection.
     *
     * @param collector the collector to register
     */
    public void registerCollector(RocksDBMetricsCollector collector) {
        if (shutdown.get()) {
            LOG.warn("Cannot register collector after collector is shutdown");
            return;
        }

        if (collector == null) {
            LOG.warn("Cannot register null collector");
            return;
        }

        synchronized (collectorLock) {
            String key = createCollectorKey(collector);
            collectors.put(key, collector);
            // Start the collection if this is the first collector
            if (collectors.size() == 1 && !running.get()) {
                startCollection();
            }

            LOG.debug(
                    "Registered RocksDB metrics collector for {}, total collectors: {}",
                    key,
                    collectors.size());
        }
    }

    /**
     * Unregister a metrics collector.
     *
     * @param collector the collector to unregister
     */
    public void unregisterCollector(RocksDBMetricsCollector collector) {
        if (collector == null) {
            LOG.warn("Cannot unregister null collector");
            return;
        }

        synchronized (collectorLock) {
            String key = createCollectorKey(collector);
            RocksDBMetricsCollector removed = collectors.remove(key);
            if (removed != null) {
                LOG.debug(
                        "Unregistered RocksDB metrics collector for {}, remaining collectors: {}",
                        key,
                        collectors.size());
            } else {
                LOG.debug("Collector {} was not found in registry", key);
            }
        }
    }

    /** Start the metrics collection if not already running. */
    private void startCollection() {
        if (running.compareAndSet(false, true)) {
            LOG.info("Starting RocksDB metrics collection.");

            // Ensure scheduler is started (for FlussScheduler)
            if (scheduler instanceof FlussScheduler) {
                FlussScheduler flussScheduler = (FlussScheduler) scheduler;
                if (!flussScheduler.isStarted()) {
                    flussScheduler.startup();
                }
            }

            scheduledTask =
                    scheduler.schedule(
                            "rocksdb-metrics-collection",
                            this::gatherMetrics,
                            METRICS_UPDATE_INTERVAL_MS,
                            METRICS_UPDATE_INTERVAL_MS);
        }
    }

    /** Collect and aggregate metrics from all registered collectors. */
    private void gatherMetrics() {
        if (serverMetricGroup == null) {
            return;
        }

        // Create a defensive copy of collectors to avoid race conditions
        List<RocksDBMetricsCollector> collectorList;
        synchronized (collectorLock) {
            collectorList = new ArrayList<>(collectors.values());
        }

        if (collectorList.isEmpty()) {
            resetMetrics(serverMetricGroup);
            return;
        }

        // Initialize aggregation variables
        long blockCacheMissCount = 0;
        long blockCacheHitCount = 0;
        long blockCacheAddCount = 0;
        long compactionBytesRead = 0;
        long compactionBytesWritten = 0;
        long flushBytesWritten = 0;
        long bytesRead = 0;
        long bytesWritten = 0;

        long blockCacheUsage = 0;
        long blockCachePinnedUsage = 0;
        long memtableMemoryUsage = 0;
        long blockCacheMemoryUsage = 0;
        long tableReadersMemoryUsage = 0;
        long totalMemoryUsage = 0;
        long totalSstFilesSize = 0;

        long compactionPendingSum = 0;
        long flushPendingSum = 0;
        long numFilesAtLevel0Sum = 0;

        long writeStallMicrosMax = 0;

        long dbGetLatencyMicrosSum = 0;
        long dbWriteLatencyMicrosSum = 0;
        long compactionTimeMicrosSum = 0;
        int latencyCollectorCount = 0;
        int validCollectorCount = 0;

        // Update and aggregate metrics in a single pass
        for (RocksDBMetricsCollector collector : collectorList) {
            if (!collector.isCollectorValidForAggregation()) {
                continue;
            }

            // Update metrics first, then aggregate
            collector.updateMetrics();

            // Counter type (SUM)
            blockCacheMissCount += collector.getBlockCacheMissCount();
            blockCacheHitCount += collector.getBlockCacheHitCount();
            blockCacheAddCount += collector.getBlockCacheAddCount();
            compactionBytesRead += collector.getCompactionBytesRead();
            compactionBytesWritten += collector.getCompactionBytesWritten();
            flushBytesWritten += collector.getFlushBytesWritten();
            bytesRead += collector.getBytesRead();
            bytesWritten += collector.getBytesWritten();

            // Gauge type SUM (resource usage)
            blockCacheUsage += collector.getBlockCacheUsage();
            blockCachePinnedUsage += collector.getBlockCachePinnedUsage();
            memtableMemoryUsage += collector.getMemtableMemoryUsage();
            blockCacheMemoryUsage += collector.getBlockCacheMemoryUsage();
            tableReadersMemoryUsage += collector.getTableReadersMemoryUsage();
            totalMemoryUsage += collector.getTotalMemoryUsage();
            totalSstFilesSize += collector.getTotalSstFilesSize();

            // Gauge type AVG (count metrics)
            compactionPendingSum += collector.getCompactionPending();
            flushPendingSum += collector.getFlushPending();
            numFilesAtLevel0Sum += collector.getNumFilesAtLevel0();

            // Gauge type MAX (time metrics)
            long stallTime = collector.getStallTimeMicros();
            if (stallTime > writeStallMicrosMax) {
                writeStallMicrosMax = stallTime;
            }

            // Histogram type AVG (latency metrics)
            long dbGetLatency = collector.getDbGetLatencyMicros();
            long dbWriteLatency = collector.getDbWriteLatencyMicros();
            long compactionTime = collector.getCompactionTimeMicros();
            if (dbGetLatency > 0 || dbWriteLatency > 0 || compactionTime > 0) {
                dbGetLatencyMicrosSum += dbGetLatency;
                dbWriteLatencyMicrosSum += dbWriteLatency;
                compactionTimeMicrosSum += compactionTime;
                latencyCollectorCount++;
            }

            validCollectorCount++;
        }

        int collectorCount = validCollectorCount;

        // Update server-level Counter metrics (calculate deltas and increment)
        long deltaBlockCacheMissCount = blockCacheMissCount - previousBlockCacheMissCount;
        long deltaBlockCacheHitCount = blockCacheHitCount - previousBlockCacheHitCount;
        long deltaBlockCacheAddCount = blockCacheAddCount - previousBlockCacheAddCount;
        long deltaCompactionBytesRead = compactionBytesRead - previousCompactionBytesRead;
        long deltaCompactionBytesWritten = compactionBytesWritten - previousCompactionBytesWritten;
        long deltaFlushBytesWritten = flushBytesWritten - previousFlushBytesWritten;
        long deltaBytesRead = bytesRead - previousBytesRead;
        long deltaBytesWritten = bytesWritten - previousBytesWritten;

        // Only increment if delta is positive (handle counter reset or decrease)
        if (deltaBlockCacheMissCount > 0) {
            serverMetricGroup.rocksdbBlockCacheMissCount().inc(deltaBlockCacheMissCount);
        }
        if (deltaBlockCacheHitCount > 0) {
            serverMetricGroup.rocksdbBlockCacheHitCount().inc(deltaBlockCacheHitCount);
        }
        if (deltaBlockCacheAddCount > 0) {
            serverMetricGroup.rocksdbBlockCacheAddCount().inc(deltaBlockCacheAddCount);
        }
        if (deltaCompactionBytesRead > 0) {
            serverMetricGroup.rocksdbCompactionBytesRead().inc(deltaCompactionBytesRead);
        }
        if (deltaCompactionBytesWritten > 0) {
            serverMetricGroup.rocksdbCompactionBytesWritten().inc(deltaCompactionBytesWritten);
        }
        if (deltaFlushBytesWritten > 0) {
            serverMetricGroup.rocksdbFlushBytesWritten().inc(deltaFlushBytesWritten);
        }
        if (deltaBytesRead > 0) {
            serverMetricGroup.rocksdbBytesRead().inc(deltaBytesRead);
        }
        if (deltaBytesWritten > 0) {
            serverMetricGroup.rocksdbBytesWritten().inc(deltaBytesWritten);
        }

        // Update previous values for next iteration
        previousBlockCacheMissCount = blockCacheMissCount;
        previousBlockCacheHitCount = blockCacheHitCount;
        previousBlockCacheAddCount = blockCacheAddCount;
        previousCompactionBytesRead = compactionBytesRead;
        previousCompactionBytesWritten = compactionBytesWritten;
        previousFlushBytesWritten = flushBytesWritten;
        previousBytesRead = bytesRead;
        previousBytesWritten = bytesWritten;

        serverMetricGroup.rocksdbBlockCacheUsage().set(blockCacheUsage);
        serverMetricGroup.rocksdbBlockCachePinnedUsage().set(blockCachePinnedUsage);
        serverMetricGroup.rocksdbMemtableMemoryUsage().set(memtableMemoryUsage);
        serverMetricGroup.rocksdbBlockCacheMemoryUsage().set(blockCacheMemoryUsage);
        serverMetricGroup.rocksdbTableReadersMemoryUsage().set(tableReadersMemoryUsage);
        serverMetricGroup.rocksdbTotalMemoryUsage().set(totalMemoryUsage);
        serverMetricGroup.rocksdbTotalSstFilesSize().set(totalSstFilesSize);

        serverMetricGroup.rocksdbCompactionPendingSum().set(compactionPendingSum);
        serverMetricGroup.rocksdbFlushPendingSum().set(flushPendingSum);
        serverMetricGroup.rocksdbNumFilesAtLevel0Sum().set(numFilesAtLevel0Sum);
        serverMetricGroup.rocksdbCollectorCount().set(collectorCount);

        serverMetricGroup.rocksdbWriteStallMicros().set(writeStallMicrosMax);

        // Update Histogram metrics (update with average latency values)
        if (latencyCollectorCount > 0) {
            long avgDbGetLatency = dbGetLatencyMicrosSum / latencyCollectorCount;
            long avgDbWriteLatency = dbWriteLatencyMicrosSum / latencyCollectorCount;
            long avgCompactionTime = compactionTimeMicrosSum / latencyCollectorCount;
            if (avgDbGetLatency > 0) {
                serverMetricGroup.rocksdbDbGetLatencyHistogram().update(avgDbGetLatency);
            }
            if (avgDbWriteLatency > 0) {
                serverMetricGroup.rocksdbDbWriteLatencyHistogram().update(avgDbWriteLatency);
            }
            if (avgCompactionTime > 0) {
                serverMetricGroup.rocksdbCompactionTimeHistogram().update(avgCompactionTime);
            }
        }
        serverMetricGroup.rocksdbLatencyCollectorCount().set(latencyCollectorCount);
    }

    /** Reset all metrics to 0. */
    private void resetMetrics(TabletServerMetricGroup serverMetrics) {
        // Reset previous values for Counter metrics
        previousBlockCacheMissCount = 0;
        previousBlockCacheHitCount = 0;
        previousBlockCacheAddCount = 0;
        previousCompactionBytesRead = 0;
        previousCompactionBytesWritten = 0;
        previousFlushBytesWritten = 0;
        previousBytesRead = 0;
        previousBytesWritten = 0;

        serverMetrics.rocksdbBlockCacheUsage().set(0);
        serverMetrics.rocksdbBlockCachePinnedUsage().set(0);
        serverMetrics.rocksdbMemtableMemoryUsage().set(0);
        serverMetrics.rocksdbBlockCacheMemoryUsage().set(0);
        serverMetrics.rocksdbTableReadersMemoryUsage().set(0);
        serverMetrics.rocksdbTotalMemoryUsage().set(0);
        serverMetrics.rocksdbTotalSstFilesSize().set(0);

        serverMetrics.rocksdbCompactionPendingSum().set(0);
        serverMetrics.rocksdbFlushPendingSum().set(0);
        serverMetrics.rocksdbNumFilesAtLevel0Sum().set(0);
        serverMetrics.rocksdbCollectorCount().set(0);

        serverMetrics.rocksdbWriteStallMicros().set(0);

        serverMetrics.rocksdbLatencyCollectorCount().set(0);
    }

    /** Create a unique key for a collector. */
    private String createCollectorKey(RocksDBMetricsCollector collector) {
        return String.format(
                "rocks-metric-collector-%s-%s-%s-%s",
                collector.getTablePath().getDatabaseName(),
                collector.getTablePath().getTableName(),
                collector.getPartitionName() != null ? collector.getPartitionName() : "none",
                collector.getTableBucket().getBucket());
    }

    /** Get the number of registered collectors. */
    public int getCollectorCount() {
        return collectors.size();
    }

    /** Shutdown the metrics manager. */
    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            LOG.info("Shutting down RocksDB metrics collector");

            running.set(false);

            // Cancel scheduled task
            if (scheduledTask != null) {
                scheduledTask.cancel(false);
                scheduledTask = null;
            }

            synchronized (collectorLock) {
                collectors.clear();
            }

            // Shutdown scheduler if it's a FlussScheduler
            if (scheduler instanceof FlussScheduler) {
                try {
                    scheduler.shutdown();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /** Check if the manager is running. */
    public boolean isRunning() {
        return running.get();
    }

    /** Check if the manager is shutdown. */
    public boolean isShutdown() {
        return shutdown.get();
    }
}

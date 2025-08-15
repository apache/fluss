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

import com.alibaba.fluss.utils.MapUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Global collector for RocksDB metrics that efficiently manages metric collection for thousands of
 * RocksDB instances using a shared thread pool and batch processing.
 */
public class RocksDBMetricsManager {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBMetricsManager.class);

    /** Singleton instance. */
    private static volatile RocksDBMetricsManager instance;

    /** Update interval for metrics collection (in seconds). */
    private static final long METRICS_UPDATE_INTERVAL_SECONDS = 5;

    /** Batch size for processing metrics collectors. */
    private static final int BATCH_SIZE = 100;

    /** Shared thread pool for metrics collection. */
    private final ScheduledExecutorService metricsCollectionExecutor;

    /** Map of registered metrics collectors. */
    private final ConcurrentMap<String, RocksDBMetricsCollector> collectors =
            MapUtils.newConcurrentHashMap();

    /** Flag to track if collector is running. */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /** Flag to track if collector is shutdown. */
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    /** Lock for synchronizing collector registration/unregistration. */
    private final Object collectorLock = new Object();

    private RocksDBMetricsManager() {
        this.metricsCollectionExecutor =
                Executors.newScheduledThreadPool(
                        1,
                        r -> {
                            Thread thread = new Thread(r, "rocksdb-metrics-collector");
                            thread.setDaemon(true);
                            return thread;
                        });
    }

    /** Get the singleton instance of RocksDBMetricsCollector. */
    public static RocksDBMetricsManager getInstance() {
        if (instance == null) {
            synchronized (RocksDBMetricsManager.class) {
                if (instance == null) {
                    instance = new RocksDBMetricsManager();
                }
            }
        }
        return instance;
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
            synchronized (key.intern()) {
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
    }

    /** Start the metrics collection if not already running. */
    private void startCollection() {
        if (running.compareAndSet(false, true)) {
            LOG.info("Starting RocksDB metrics collection.");

            metricsCollectionExecutor.scheduleAtFixedRate(
                    this::gatherMetrics,
                    METRICS_UPDATE_INTERVAL_SECONDS,
                    METRICS_UPDATE_INTERVAL_SECONDS,
                    TimeUnit.SECONDS);
        }
    }

    /** Collect metrics from all registered collectors in batches. */
    private void gatherMetrics() {
        if (collectors.isEmpty()) {
            return;
        }

        // Create a defensive copy of collectors to avoid race conditions
        List<RocksDBMetricsCollector> collectorList;
        synchronized (collectorLock) {
            collectorList = new ArrayList<>(collectors.values());
        }

        if (collectorList.isEmpty()) {
            return;
        }

        // Process collectors in batches to avoid overwhelming the system
        for (int i = 0; i < collectorList.size(); i += BATCH_SIZE) {
            int endIndex = Math.min(i + BATCH_SIZE, collectorList.size());
            List<RocksDBMetricsCollector> batch = collectorList.subList(i, endIndex);

            // Submit batch for processing
            metricsCollectionExecutor.submit(() -> processBatch(batch));
        }
    }

    /** Process a batch of collectors. */
    private void processBatch(List<RocksDBMetricsCollector> batch) {
        for (RocksDBMetricsCollector collector : batch) {
            try {
                String key = createCollectorKey(collector);
                synchronized (key.intern()) {
                    collector.updateMetrics();
                }
            } catch (Exception e) {
                LOG.warn(
                        "Error updating metrics for collector {}",
                        createCollectorKey(collector),
                        e);
            }
        }
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

    /** Shutdown the metrics collector. */
    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            LOG.info("Shutting down RocksDB metrics collector");

            running.set(false);

            synchronized (collectorLock) {
                collectors.clear();
            }

            if (metricsCollectionExecutor != null && !metricsCollectionExecutor.isShutdown()) {
                metricsCollectionExecutor.shutdown();
                try {
                    if (!metricsCollectionExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                        metricsCollectionExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    metricsCollectionExecutor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /** Check if the collector is running. */
    public boolean isRunning() {
        return running.get();
    }

    /** Check if the collector is shutdown. */
    public boolean isShutdown() {
        return shutdown.get();
    }
}

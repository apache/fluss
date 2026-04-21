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

package org.apache.fluss.microbench.sampling;

import org.apache.fluss.microbench.config.SamplingConfig;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Orchestrates all sub-samplers and collects {@link ResourceSnapshot}s on a schedule. */
public class ResourceSampler {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceSampler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final SamplingConfig config;
    private final List<ResourceSnapshot> snapshots =
            Collections.synchronizedList(new ArrayList<>());

    private ScheduledExecutorService scheduler;
    private ScheduledExecutorService nmtScheduler; // Separate scheduler for low-frequency NMT
    private boolean hasSeparateNmtSampling; // True when NMT has different interval
    private OshiSampler clientOshi;
    private OshiSampler serverOshi;
    private JvmSampler jvmSampler;
    private ClientMetricSampler clientMetricSampler;
    private NmtSampler nmtSampler;
    private JfrManager jfrManager;
    private Path metricsFile;
    private ResourceSnapshot prevSnapshot;

    public ResourceSampler(SamplingConfig config) {
        this.config = config;
    }

    /**
     * Starts sampling for embedded (same-JVM) mode. Only samples the current process via OSHI + JVM
     * MXBean. No server-side sampling (NMT/JFR/FlussMetrics) since everything is in one JVM.
     */
    public void startSelfOnly() {
        this.clientOshi = OshiSampler.forSelf();
        this.jvmSampler = new JvmSampler();
        this.clientMetricSampler = ClientMetricSampler.instance();

        long intervalMs = parseIntervalMs(config.interval());
        scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "resource-sampler");
                            t.setDaemon(true);
                            return t;
                        });
        scheduler.scheduleAtFixedRate(this::sampleSelfOnly, 0, intervalMs, TimeUnit.MILLISECONDS);
        LOG.info("ResourceSampler started (self-only mode): interval={}ms", intervalMs);
    }

    /**
     * Initializes all sub-samplers and starts scheduled sampling. Uses separate schedulers for
     * high-frequency (OSHI/JVM/Metrics) and low-frequency (NMT) sampling to reduce process fork
     * overhead.
     *
     * @param serverPid PID of the server (MiniCluster) process
     * @param metricsFile path to the JSON metrics file written by FlussMetricSampler
     */
    public void start(long serverPid, Path metricsFile) {
        this.metricsFile = metricsFile;
        this.clientOshi = OshiSampler.forSelf();
        this.serverOshi = OshiSampler.forPid(serverPid);
        this.jvmSampler = new JvmSampler();
        this.clientMetricSampler = ClientMetricSampler.instance();

        boolean nmtEnabled = config.isNmtEnabled() != null && config.isNmtEnabled();
        if (nmtEnabled) {
            this.nmtSampler = new NmtSampler(serverPid);
        }

        boolean jfrEnabled = config.isJfrEnabled() != null && config.isJfrEnabled();
        if (jfrEnabled) {
            this.jfrManager = new JfrManager(serverPid);
            jfrManager.start(config.jfrConfig());
        }

        long intervalMs = parseIntervalMs(config.interval());
        long nmtIntervalMs = parseIntervalMs(config.nmtInterval());

        // Single-threaded scheduler for high-frequency samplers
        scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "resource-sampler");
                            t.setDaemon(true);
                            return t;
                        });
        scheduler.scheduleAtFixedRate(this::sampleOnce, 0, intervalMs, TimeUnit.MILLISECONDS);

        // Separate scheduler for NMT if enabled and different interval
        if (nmtEnabled && nmtIntervalMs != intervalMs) {
            this.hasSeparateNmtSampling = true;
            nmtScheduler =
                    Executors.newSingleThreadScheduledExecutor(
                            r -> {
                                Thread t = new Thread(r, "nmt-sampler");
                                t.setDaemon(true);
                                return t;
                            });
            nmtScheduler.scheduleAtFixedRate(
                    this::sampleNmtOnly, 0, nmtIntervalMs, TimeUnit.MILLISECONDS);
            LOG.info(
                    "ResourceSampler started: interval={}ms, nmt-interval={}ms, jfr={}",
                    intervalMs,
                    nmtIntervalMs,
                    jfrEnabled);
        } else {
            LOG.info(
                    "ResourceSampler started: interval={}ms, nmt={}, jfr={}",
                    intervalMs,
                    nmtEnabled,
                    jfrEnabled);
        }
    }

    /** Stops the sampling scheduler and optionally stops JFR. */
    public void stop() {
        stop(null);
    }

    /**
     * Stops the sampling scheduler and optionally saves JFR output.
     *
     * @param jfrOutputPath if non-null and JFR is active, saves the recording here
     */
    public void stop(Path jfrOutputPath) {
        // Stop high-frequency scheduler
        if (scheduler != null) {
            scheduler.shutdownNow();
            try {
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // Stop NMT scheduler if present
        if (nmtScheduler != null) {
            nmtScheduler.shutdownNow();
            try {
                nmtScheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (jfrManager != null && jfrOutputPath != null) {
            jfrManager.stop(jfrOutputPath);
        }
    }

    /** Returns an unmodifiable view of collected snapshots. */
    public List<ResourceSnapshot> snapshots() {
        return Collections.unmodifiableList(new ArrayList<>(snapshots));
    }

    private void sampleSelfOnly() {
        try {
            ResourceSnapshot snap = new ResourceSnapshot();
            snap.timestamp = System.currentTimeMillis();
            snap.client = clientOshi.sample();
            jvmSampler.sample(snap);
            if (clientMetricSampler != null) {
                clientMetricSampler.sample(snap);
            }
            // Compute disk I/O rates from previous snapshot
            computeDiskIoRates(snap, prevSnapshot);
            prevSnapshot = snap;
            snapshots.add(snap);
        } catch (Exception e) {
            LOG.warn("Sampling iteration failed", e);
        }
    }

    private void sampleOnce() {
        try {
            ResourceSnapshot snap = new ResourceSnapshot();
            snap.timestamp = System.currentTimeMillis();

            // Client: OSHI + JVM + client metrics
            snap.client = clientOshi.sample();
            jvmSampler.sample(snap);
            clientMetricSampler.sample(snap);

            // Server: OSHI
            snap.server = serverOshi.sample();

            // Server: NMT (only when using unified sampling interval)
            if (nmtSampler != null && !hasSeparateNmtSampling) {
                nmtSampler.sample(snap);
            }

            // Server: read metrics file (written by FlussMetricSampler in server JVM)
            readServerMetricsFile(snap);

            // Compute native untracked: RSS - (heap + nonheap + thread + code + gc + internal)
            computeNativeUntracked(snap);

            // Compute disk I/O rates from previous snapshot
            computeDiskIoRates(snap, prevSnapshot);
            prevSnapshot = snap;

            snapshots.add(snap);
        } catch (Exception e) {
            LOG.warn("Sampling iteration failed", e);
        }
    }

    private void computeDiskIoRates(ResourceSnapshot snap, ResourceSnapshot prev) {
        if (prev == null || snap.timestamp <= prev.timestamp) {
            return;
        }
        double intervalSec = (snap.timestamp - prev.timestamp) / 1000.0;
        if (intervalSec <= 0) {
            return;
        }
        if (snap.client != null && prev.client != null) {
            snap.clientDiskReadBytesPerSec =
                    (long) ((snap.client.diskReadBytes - prev.client.diskReadBytes) / intervalSec);
            snap.clientDiskWriteBytesPerSec =
                    (long)
                            ((snap.client.diskWriteBytes - prev.client.diskWriteBytes)
                                    / intervalSec);
        }
        if (snap.server != null && prev.server != null) {
            snap.serverDiskReadBytesPerSec =
                    (long) ((snap.server.diskReadBytes - prev.server.diskReadBytes) / intervalSec);
            snap.serverDiskWriteBytesPerSec =
                    (long)
                            ((snap.server.diskWriteBytes - prev.server.diskWriteBytes)
                                    / intervalSec);
        }
    }

    /** Samples NMT only (used when NMT has a different interval than other samplers). */
    private void sampleNmtOnly() {
        try {
            if (nmtSampler == null) {
                return;
            }
            // Find or create the latest snapshot to attach NMT data
            ResourceSnapshot snap;
            synchronized (snapshots) {
                if (snapshots.isEmpty()) {
                    snap = new ResourceSnapshot();
                    snap.timestamp = System.currentTimeMillis();
                    snapshots.add(snap);
                } else {
                    snap = snapshots.get(snapshots.size() - 1);
                }
            }
            // Synchronize on the snapshot to avoid racing with the main sampler thread
            // that may read NMT fields (e.g. in computeNativeUntracked).
            synchronized (snap) {
                nmtSampler.sample(snap);
            }
        } catch (Exception e) {
            LOG.warn("NMT sampling iteration failed", e);
        }
    }

    @SuppressWarnings("unchecked")
    private void readServerMetricsFile(ResourceSnapshot snap) {
        if (metricsFile == null || !Files.exists(metricsFile)) {
            return;
        }
        try {
            Map<String, Object> data = MAPPER.readValue(metricsFile.toFile(), Map.class);

            // Store full metrics dump for report rendering
            data.remove("timestamp"); // exclude timestamp from metrics map
            snap.serverMetrics.putAll(data);

            // Extract well-known fields into typed snapshot fields
            snap.serverRocksdbMemtable =
                    findMetricBySuffix(data, "rocksdbMemTableMemoryUsageTotal");
            snap.serverRocksdbBlockCache =
                    findMetricBySuffix(data, "rocksdbBlockCacheMemoryUsageTotal");
            snap.serverRocksdbTableReaders =
                    findMetricBySuffix(data, "rocksdbTableReadersMemoryUsageTotal");
            snap.serverNettyDirectMemory = findMetricBySuffix(data, "netty.usedDirectMemory");
        } catch (Exception e) {
            LOG.debug("Failed to read server metrics file {}", metricsFile, e);
        }
    }

    /** Finds a metric value by key suffix, since keys have variable scope prefixes. */
    private static long findMetricBySuffix(Map<String, Object> data, String suffix) {
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            if (entry.getKey().endsWith(suffix)) {
                return toLong(entry.getValue());
            }
        }
        return 0;
    }

    private void computeNativeUntracked(ResourceSnapshot snap) {
        if (snap.server == null) {
            return;
        }
        long rss = snap.server.rssBytes;
        if (rss <= 0) {
            return;
        }
        long tracked = 0;
        // NMT categories (includes heap, metaspace/class, thread, code, GC, internal)
        if (snap.serverNmtHeap > 0) {
            tracked += snap.serverNmtHeap;
        }
        if (snap.serverNmtClass > 0) {
            tracked += snap.serverNmtClass;
        }
        if (snap.serverNmtThreadStacks > 0) {
            tracked += snap.serverNmtThreadStacks;
        }
        if (snap.serverNmtCodeCache > 0) {
            tracked += snap.serverNmtCodeCache;
        }
        if (snap.serverNmtGc > 0) {
            tracked += snap.serverNmtGc;
        }
        if (snap.serverNmtInternal > 0) {
            tracked += snap.serverNmtInternal;
        }
        // Application-level native allocations
        tracked += snap.serverRocksdbMemtable;
        tracked += snap.serverRocksdbBlockCache;
        tracked += snap.serverRocksdbTableReaders;
        tracked += snap.serverNettyDirectMemory;
        snap.serverNativeUntracked = Math.max(0, rss - tracked);
    }

    private static long toLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return 0;
    }

    static long parseIntervalMs(String interval) {
        if (interval == null || interval.isEmpty()) {
            return 1000; // default 1s
        }
        String trimmed = interval.trim().toLowerCase();
        if (trimmed.endsWith("ms")) {
            return Long.parseLong(trimmed.substring(0, trimmed.length() - 2).trim());
        } else if (trimmed.endsWith("s")) {
            return (long)
                    (Double.parseDouble(trimmed.substring(0, trimmed.length() - 1).trim()) * 1000);
        } else if (trimmed.endsWith("m")) {
            return (long)
                    (Double.parseDouble(trimmed.substring(0, trimmed.length() - 1).trim())
                            * 60_000);
        }
        return Long.parseLong(trimmed);
    }
}

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

package org.apache.fluss.server.metrics;

import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.rpc.messages.GetServerInfoResponse;
import org.apache.fluss.server.metadata.TabletServerResource;
import org.apache.fluss.server.storage.DiskUsageCollector;
import org.apache.fluss.server.tablet.TabletServerResourceProbe;
import org.apache.fluss.utils.concurrent.Scheduler;

import javax.annotation.Nullable;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;

/** Collects and exposes one cached node metrics snapshot for a Fluss server process. */
public final class ServerNodeMetrics implements AutoCloseable {

    private static final long SAMPLE_INTERVAL_MS = 10_000L;

    private final TabletServerResourceProbe resourceProbe;
    private final String serverId;
    private final ServerType serverType;
    private final DiskUsageCollector diskUsageCollector;
    private final AtomicReference<Snapshot> latestSnapshot;
    private final ScheduledFuture<?> samplingTask;
    private final ServerNodeMetricGroup metricGroup;

    /** Creates and starts a periodic node metrics collector. */
    public ServerNodeMetrics(
            Configuration conf,
            MetricRegistry registry,
            String clusterId,
            String hostname,
            String serverId,
            ServerType serverType,
            @Nullable List<File> dataDirs,
            Scheduler scheduler) {
        this.resourceProbe = new TabletServerResourceProbe(conf);
        this.serverId = serverId;
        this.serverType = serverType;
        this.diskUsageCollector = dataDirs == null ? null : new DiskUsageCollector(dataDirs);
        this.latestSnapshot = new AtomicReference<>(collectSnapshot());
        this.metricGroup =
                new ServerNodeMetricGroup(registry, clusterId, hostname, serverId, serverType);
        registerMetrics(metricGroup);
        this.samplingTask =
                scheduler.schedule("server-node-metrics", this::refresh, 0L, SAMPLE_INTERVAL_MS);
    }

    /** Returns the latest cached node metrics snapshot. */
    public Snapshot snapshot() {
        return latestSnapshot.get();
    }

    /** Converts the latest cached snapshot to the public RPC response. */
    public GetServerInfoResponse toResponse() {
        Snapshot snapshot = snapshot();
        GetServerInfoResponse response = new GetServerInfoResponse();
        response.setServerId(serverId)
                .setServerType(serverType.toTypeId())
                .setCpuCores(snapshot.getCpuCores())
                .setMemoryTotalBytes(snapshot.getMemoryTotalBytes())
                .setCpuUsageRatio(snapshot.getCpuUsageRatio())
                .setMemoryUsedBytes(snapshot.getMemoryUsedBytes())
                .setCollectedAtMs(snapshot.getCollectedAtMs());
        if (snapshot.hasDataDisk()) {
            response.setDataDiskTotalBytes(snapshot.getDataDiskTotalBytes());
            response.setDataDiskUsedBytes(snapshot.getDataDiskUsedBytes());
        }
        return response;
    }

    /** Stops sampling and unregisters all node metrics. */
    @Override
    public void close() {
        samplingTask.cancel(false);
        metricGroup.close();
    }

    private void refresh() {
        latestSnapshot.set(collectSnapshot());
    }

    private Snapshot collectSnapshot() {
        TabletServerResource resource = resourceProbe.probe();
        double cpuCores =
                resource.getCpuCores() == null
                        ? Runtime.getRuntime().availableProcessors()
                        : resource.getCpuCores();
        long memoryTotalBytes =
                resource.getMemoryBytes() == null
                        ? getOperatingSystemBean().getTotalPhysicalMemorySize()
                        : resource.getMemoryBytes();
        com.sun.management.OperatingSystemMXBean operatingSystemBean = getOperatingSystemBean();
        double cpuUsageRatio = normalizeCpuUsage(operatingSystemBean.getSystemCpuLoad());
        long memoryUsedBytes =
                resourceProbe
                        .probeMemoryUsedBytes()
                        .orElse(
                                Math.max(
                                        0L,
                                        operatingSystemBean.getTotalPhysicalMemorySize()
                                                - operatingSystemBean.getFreePhysicalMemorySize()));

        Long dataDiskTotalBytes = null;
        Long dataDiskUsedBytes = null;
        if (diskUsageCollector != null) {
            try {
                DiskUsageCollector.DiskUsage diskUsage = diskUsageCollector.collectUsage();
                dataDiskTotalBytes = diskUsage.getTotalBytes();
                dataDiskUsedBytes = diskUsage.getUsedBytes();
            } catch (Exception ignored) {
                // The previous disk values remain available through the previous snapshot.
                Snapshot previous = latestSnapshot == null ? null : latestSnapshot.get();
                if (previous != null && previous.hasDataDisk()) {
                    dataDiskTotalBytes = previous.getDataDiskTotalBytes();
                    dataDiskUsedBytes = previous.getDataDiskUsedBytes();
                }
            }
        }
        return new Snapshot(
                cpuCores,
                memoryTotalBytes,
                cpuUsageRatio,
                memoryUsedBytes,
                dataDiskTotalBytes,
                dataDiskUsedBytes,
                System.currentTimeMillis());
    }

    private com.sun.management.OperatingSystemMXBean getOperatingSystemBean() {
        return (com.sun.management.OperatingSystemMXBean)
                ManagementFactory.getOperatingSystemMXBean();
    }

    private double normalizeCpuUsage(double cpuUsageRatio) {
        if (Double.isNaN(cpuUsageRatio) || cpuUsageRatio < 0.0) {
            return 0.0;
        }
        return Math.min(cpuUsageRatio, 1.0);
    }

    private void registerMetrics(MetricGroup group) {
        group.<Double, Gauge<Double>>gauge(
                MetricNames.NODE_CPU_CORES, () -> latestSnapshot.get().getCpuCores());
        group.<Long, Gauge<Long>>gauge(
                MetricNames.NODE_MEMORY_TOTAL_BYTES,
                () -> latestSnapshot.get().getMemoryTotalBytes());
        group.<Double, Gauge<Double>>gauge(
                MetricNames.NODE_CPU_USAGE_RATIO, () -> latestSnapshot.get().getCpuUsageRatio());
        group.<Long, Gauge<Long>>gauge(
                MetricNames.NODE_MEMORY_USED_BYTES,
                () -> latestSnapshot.get().getMemoryUsedBytes());
        if (diskUsageCollector != null) {
            group.<Long, Gauge<Long>>gauge(
                    MetricNames.DATA_DISK_TOTAL_BYTES,
                    () -> latestSnapshot.get().getDataDiskTotalBytes());
            group.<Long, Gauge<Long>>gauge(
                    MetricNames.DATA_DISK_USED_BYTES,
                    () -> latestSnapshot.get().getDataDiskUsedBytes());
        }
    }

    /** Immutable node metrics snapshot. */
    public static final class Snapshot {
        private final double cpuCores;
        private final long memoryTotalBytes;
        private final double cpuUsageRatio;
        private final long memoryUsedBytes;
        private final @Nullable Long dataDiskTotalBytes;
        private final @Nullable Long dataDiskUsedBytes;
        private final long collectedAtMs;

        private Snapshot(
                double cpuCores,
                long memoryTotalBytes,
                double cpuUsageRatio,
                long memoryUsedBytes,
                @Nullable Long dataDiskTotalBytes,
                @Nullable Long dataDiskUsedBytes,
                long collectedAtMs) {
            this.cpuCores = cpuCores;
            this.memoryTotalBytes = memoryTotalBytes;
            this.cpuUsageRatio = cpuUsageRatio;
            this.memoryUsedBytes = memoryUsedBytes;
            this.dataDiskTotalBytes = dataDiskTotalBytes;
            this.dataDiskUsedBytes = dataDiskUsedBytes;
            this.collectedAtMs = collectedAtMs;
        }

        /** Returns the effective CPU capacity. */
        public double getCpuCores() {
            return cpuCores;
        }

        /** Returns the effective memory capacity in bytes. */
        public long getMemoryTotalBytes() {
            return memoryTotalBytes;
        }

        /** Returns the machine CPU usage ratio. */
        public double getCpuUsageRatio() {
            return cpuUsageRatio;
        }

        /** Returns the machine memory usage in bytes. */
        public long getMemoryUsedBytes() {
            return memoryUsedBytes;
        }

        /** Returns whether this snapshot contains Fluss data disk metrics. */
        public boolean hasDataDisk() {
            return dataDiskTotalBytes != null && dataDiskUsedBytes != null;
        }

        /** Returns the total Fluss data disk capacity in bytes. */
        public long getDataDiskTotalBytes() {
            return dataDiskTotalBytes == null ? 0L : dataDiskTotalBytes;
        }

        /** Returns the used Fluss data disk capacity in bytes. */
        public long getDataDiskUsedBytes() {
            return dataDiskUsedBytes == null ? 0L : dataDiskUsedBytes;
        }

        /** Returns the collection timestamp in epoch milliseconds. */
        public long getCollectedAtMs() {
            return collectedAtMs;
        }
    }
}

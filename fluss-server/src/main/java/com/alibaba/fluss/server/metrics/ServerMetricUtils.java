/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.server.metrics;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metrics.Gauge;
import com.alibaba.fluss.metrics.MeterView;
import com.alibaba.fluss.metrics.groups.AbstractMetricGroup;
import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.metrics.registry.MetricRegistry;
import com.alibaba.fluss.server.metrics.group.CoordinatorMetricGroup;
import com.alibaba.fluss.server.metrics.group.TabletServerMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import java.lang.management.ClassLoadingMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Utility class for server to register pre-defined metric sets. */
public class ServerMetricUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ServerMetricUtils.class);

    public static final String METRIC_GROUP_STATUS_NAME = "status";
    static final String METRIC_GROUP_HEAP_NAME = "heap";
    static final String METRIC_GROUP_NONHEAP_NAME = "nonHeap";
    static final String METRIC_GROUP_METASPACE_NAME = "metaspace";
    static final String MEMORY_USED = "used";
    static final String MEMORY_COMMITTED = "committed";
    static final String MEMORY_MAX = "max";

    @VisibleForTesting static final String METRIC_GROUP_MEMORY = "memory";

    public static CoordinatorMetricGroup createCoordinatorGroup(
            MetricRegistry registry, String clusterId, String hostname, String serverId) {
        CoordinatorMetricGroup coordinatorMetricGroup =
                new CoordinatorMetricGroup(registry, clusterId, hostname, serverId);
        createAndInitializeStatusMetricGroup(coordinatorMetricGroup);
        return coordinatorMetricGroup;
    }

    public static TabletServerMetricGroup createTabletServerGroup(
            MetricRegistry registry, String clusterId, String hostname, int serverId) {
        TabletServerMetricGroup tabletServerMetricGroup =
                new TabletServerMetricGroup(registry, clusterId, hostname, serverId);
        createAndInitializeStatusMetricGroup(tabletServerMetricGroup);
        return tabletServerMetricGroup;
    }

    public static String validateAndGetClusterId(Configuration conf) {
        String zkRoot = conf.get(ConfigOptions.ZOOKEEPER_ROOT);
        if (!zkRoot.startsWith("/")) {
            throw new IllegalArgumentException("zkRoot must start with '/'");
        }
        return zkRoot.substring(1);
    }

    private static void createAndInitializeStatusMetricGroup(
            AbstractMetricGroup parentMetricGroup) {
        MetricGroup statusGroup = parentMetricGroup.addGroup(METRIC_GROUP_STATUS_NAME);
        instantiateStatusMetrics(statusGroup);
    }

    private static void instantiateStatusMetrics(MetricGroup metricGroup) {
        MetricGroup jvm = metricGroup.addGroup("JVM");
        instantiateClassLoaderMetrics(jvm.addGroup("classLoader"));
        instantiateGarbageCollectorMetrics(
                jvm.addGroup("GC"), ManagementFactory.getGarbageCollectorMXBeans());
        instantiateMemoryMetrics(jvm.addGroup(METRIC_GROUP_MEMORY));
        instantiateThreadMetrics(jvm.addGroup("threads"));
        instantiateCPUMetrics(jvm.addGroup("CPU"));
    }

    private static void instantiateClassLoaderMetrics(MetricGroup metrics) {
        final ClassLoadingMXBean mxBean = ManagementFactory.getClassLoadingMXBean();
        metrics.<Long, Gauge<Long>>gauge("classesLoaded", mxBean::getTotalLoadedClassCount);
        metrics.<Long, Gauge<Long>>gauge("classesUnloaded", mxBean::getUnloadedClassCount);
    }

    static void instantiateGarbageCollectorMetrics(
            MetricGroup metrics, List<GarbageCollectorMXBean> garbageCollectors) {
        for (final GarbageCollectorMXBean garbageCollector : garbageCollectors) {
            MetricGroup gcGroup = metrics.addGroup(garbageCollector.getName());
            gcGroup.gauge("count", garbageCollector::getCollectionCount);
            Gauge<Long> timeGauge = gcGroup.gauge("time", garbageCollector::getCollectionTime);
            gcGroup.meter("timeMsPerSecond", new MeterView(timeGauge));
        }
        Gauge<Long> totalGcTime =
                () ->
                        garbageCollectors.stream()
                                .mapToLong(GarbageCollectorMXBean::getCollectionTime)
                                .sum();
        Gauge<Long> totalGcCount =
                () ->
                        garbageCollectors.stream()
                                .mapToLong(GarbageCollectorMXBean::getCollectionCount)
                                .sum();
        MetricGroup allGroup = metrics.addGroup("all");
        allGroup.gauge("count", totalGcCount);
        Gauge<Long> totalTime = allGroup.gauge("time", totalGcTime);
        allGroup.meter("timeMsPerSecond", new MeterView(totalTime));
    }

    private static void instantiateMemoryMetrics(MetricGroup metrics) {
        instantiateHeapMemoryMetrics(metrics.addGroup(METRIC_GROUP_HEAP_NAME));
        instantiateNonHeapMemoryMetrics(metrics.addGroup(METRIC_GROUP_NONHEAP_NAME));
        instantiateMetaspaceMemoryMetrics(metrics);
        final MBeanServer con = ManagementFactory.getPlatformMBeanServer();
        final String directBufferPoolName = "java.nio:type=BufferPool,name=direct";
        try {
            final ObjectName directObjectName = new ObjectName(directBufferPoolName);
            MetricGroup direct = metrics.addGroup("direct");
            direct.<Long, Gauge<Long>>gauge(
                    "count", new AttributeGauge<>(con, directObjectName, "Count", -1L));
            direct.<Long, Gauge<Long>>gauge(
                    "memoryUsed", new AttributeGauge<>(con, directObjectName, "MemoryUsed", -1L));
            direct.<Long, Gauge<Long>>gauge(
                    "totalCapacity",
                    new AttributeGauge<>(con, directObjectName, "TotalCapacity", -1L));
        } catch (MalformedObjectNameException e) {
            LOG.warn("Could not create object name {}.", directBufferPoolName, e);
        }
        final String mappedBufferPoolName = "java.nio:type=BufferPool,name=mapped";
        try {
            final ObjectName mappedObjectName = new ObjectName(mappedBufferPoolName);
            MetricGroup mapped = metrics.addGroup("mapped");
            mapped.<Long, Gauge<Long>>gauge(
                    "count", new AttributeGauge<>(con, mappedObjectName, "Count", -1L));
            mapped.<Long, Gauge<Long>>gauge(
                    "memoryUsed", new AttributeGauge<>(con, mappedObjectName, "MemoryUsed", -1L));
            mapped.<Long, Gauge<Long>>gauge(
                    "totalCapacity",
                    new AttributeGauge<>(con, mappedObjectName, "TotalCapacity", -1L));
        } catch (MalformedObjectNameException e) {
            LOG.warn("Could not create object name {}.", mappedBufferPoolName, e);
        }
    }

    @VisibleForTesting
    static void instantiateHeapMemoryMetrics(final MetricGroup metricGroup) {
        instantiateMemoryUsageMetrics(
                metricGroup, () -> ManagementFactory.getMemoryMXBean().getHeapMemoryUsage());
    }

    @VisibleForTesting
    static void instantiateNonHeapMemoryMetrics(final MetricGroup metricGroup) {
        instantiateMemoryUsageMetrics(
                metricGroup, () -> ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage());
    }

    @VisibleForTesting
    static void instantiateMetaspaceMemoryMetrics(final MetricGroup parentMetricGroup) {
        final List<MemoryPoolMXBean> memoryPoolMXBeans =
                ManagementFactory.getMemoryPoolMXBeans().stream()
                        .filter(bean -> "Metaspace".equals(bean.getName()))
                        .collect(Collectors.toList());
        if (memoryPoolMXBeans.isEmpty()) {
            LOG.info(
                    "The '{}' metrics will not be exposed because no pool named 'Metaspace' could be found. This might be caused by the used JVM.",
                    METRIC_GROUP_METASPACE_NAME);
            return;
        }
        final MetricGroup metricGroup = parentMetricGroup.addGroup(METRIC_GROUP_METASPACE_NAME);
        final Iterator<MemoryPoolMXBean> beanIterator = memoryPoolMXBeans.iterator();
        final MemoryPoolMXBean firstPool = beanIterator.next();
        instantiateMemoryUsageMetrics(metricGroup, firstPool::getUsage);
        if (beanIterator.hasNext()) {
            LOG.debug(
                    "More than one memory pool named 'Metaspace' is present. Only the first pool was used for instantiating the '{}' metrics.",
                    METRIC_GROUP_METASPACE_NAME);
        }
    }

    private static void instantiateMemoryUsageMetrics(
            final MetricGroup metricGroup, final Supplier<MemoryUsage> memoryUsageSupplier) {
        metricGroup.<Long, Gauge<Long>>gauge(
                MEMORY_USED, () -> memoryUsageSupplier.get().getUsed());
        metricGroup.<Long, Gauge<Long>>gauge(
                MEMORY_COMMITTED, () -> memoryUsageSupplier.get().getCommitted());
        metricGroup.<Long, Gauge<Long>>gauge(MEMORY_MAX, () -> memoryUsageSupplier.get().getMax());
    }

    private static void instantiateThreadMetrics(MetricGroup metrics) {
        final ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
        metrics.<Integer, Gauge<Integer>>gauge("count", mxBean::getThreadCount);
    }

    private static void instantiateCPUMetrics(MetricGroup metrics) {
        try {
            final com.sun.management.OperatingSystemMXBean mxBean =
                    (com.sun.management.OperatingSystemMXBean)
                            ManagementFactory.getOperatingSystemMXBean();
            metrics.<Double, Gauge<Double>>gauge("load", mxBean::getProcessCpuLoad);
            metrics.<Long, Gauge<Long>>gauge("time", mxBean::getProcessCpuTime);
        } catch (Exception e) {
            LOG.warn(
                    "Cannot access com.sun.management.OperatingSystemMXBean.getProcessCpuLoad()"
                            + " - CPU load metrics will not be available.",
                    e);
        }
    }

    private static final class AttributeGauge<T> implements Gauge<T> {
        private final MBeanServer server;
        private final ObjectName objectName;
        private final String attributeName;
        private final T errorValue;

        private AttributeGauge(
                MBeanServer server, ObjectName objectName, String attributeName, T errorValue) {
            this.server = checkNotNull(server);
            this.objectName = checkNotNull(objectName);
            this.attributeName = checkNotNull(attributeName);
            this.errorValue = errorValue;
        }

        @SuppressWarnings("unchecked")
        @Override
        public T getValue() {
            try {
                return (T) server.getAttribute(objectName, attributeName);
            } catch (MBeanException
                    | AttributeNotFoundException
                    | InstanceNotFoundException
                    | ReflectionException e) {
                LOG.warn("Could not read attribute {}.", attributeName, e);
                return errorValue;
            }
        }
    }
}

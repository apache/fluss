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

import org.apache.fluss.classloading.ComponentClassLoader;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.MeterView;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.groups.GenericMetricGroup;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;
import org.apache.fluss.testutils.common.ClassLoaderUtils;
import org.apache.fluss.utils.function.CheckedSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.management.ObjectName;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.server.metrics.ServerMetricUtils.MEMORY_COMMITTED;
import static org.apache.fluss.server.metrics.ServerMetricUtils.MEMORY_MAX;
import static org.apache.fluss.server.metrics.ServerMetricUtils.MEMORY_USED;
import static org.apache.fluss.server.metrics.ServerMetricUtils.instantiateGarbageCollectorMetrics;
import static org.apache.fluss.server.metrics.ServerMetricUtils.instantiateHeapMemoryMetrics;
import static org.apache.fluss.server.metrics.ServerMetricUtils.instantiateMetaspaceMemoryMetrics;
import static org.apache.fluss.server.metrics.ServerMetricUtils.instantiateNonHeapMemoryMetrics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Test for {@link ServerMetricUtils}. */
public class ServerMetricUtilsTest {
    /** Container for local objects to keep them from gc runs. */
    private final List<Object> referencedObjects = new ArrayList<>();

    @AfterEach
    void cleanupReferencedObjects() {
        referencedObjects.clear();
    }

    @Test
    void testNonHeapMetricsCompleteness() {
        final InterceptingMetricGroup nonHeapMetrics = new InterceptingMetricGroup();

        instantiateNonHeapMemoryMetrics(nonHeapMetrics);

        assertThat(nonHeapMetrics.get(MEMORY_USED)).isNotNull();
        assertThat(nonHeapMetrics.get(MEMORY_COMMITTED)).isNotNull();
        assertThat(nonHeapMetrics.get(MEMORY_MAX)).isNotNull();
    }

    @Test
    void testMetaspaceCompleteness() {
        assertThat(hasMetaspaceMemoryPool())
                .withFailMessage("Requires JVM with Metaspace memory pool")
                .isTrue();
        final InterceptingMetricGroup metaspaceMetrics =
                new InterceptingMetricGroup() {
                    @Override
                    public MetricGroup addGroup(String name) {
                        return this;
                    }
                };

        instantiateMetaspaceMemoryMetrics(metaspaceMetrics);

        assertThat(metaspaceMetrics.get(MEMORY_USED)).isNotNull();
        assertThat(metaspaceMetrics.get(MEMORY_COMMITTED)).isNotNull();
        assertThat(metaspaceMetrics.get(MEMORY_MAX)).isNotNull();
    }

    @Test
    public void testGcMetricCompleteness() {
        Map<String, InterceptingMetricGroup> addedGroups = new HashMap<>();
        InterceptingMetricGroup gcGroup =
                new InterceptingMetricGroup() {
                    @Override
                    public MetricGroup addGroup(String name) {
                        return addedGroups.computeIfAbsent(
                                name, k -> new InterceptingMetricGroup());
                    }
                };

        List<GarbageCollectorMXBean> garbageCollectors = new ArrayList<>();
        garbageCollectors.add(new TestGcBean("gc1", 100, 500));
        garbageCollectors.add(new TestGcBean("gc2", 50, 250));

        instantiateGarbageCollectorMetrics(gcGroup, garbageCollectors);
        assertThat(addedGroups).containsOnlyKeys("gc1", "gc2", "all");

        // Make sure individual collector metrics are correct
        validateCollectorMetric(addedGroups.get("gc1"), 100, 500L);
        validateCollectorMetric(addedGroups.get("gc2"), 50L, 250L);

        // Make sure all/total collector metrics are correct
        validateCollectorMetric(addedGroups.get("all"), 150L, 750L);
    }

    private static void validateCollectorMetric(
            InterceptingMetricGroup group, long count, long time) {
        assertThat(((Gauge) group.get("count")).getValue()).isEqualTo(count);
        assertThat(((Gauge) group.get("time")).getValue()).isEqualTo(time);
        MeterView perSecond = ((MeterView) group.get("timeMsPerSecond"));
        perSecond.update();
        assertThat(perSecond.getRate()).isGreaterThan(0.);
    }

    private static boolean hasMetaspaceMemoryPool() {
        return ManagementFactory.getMemoryPoolMXBeans().stream()
                .anyMatch(bean -> "Metaspace".equals(bean.getName()));
    }

    @Test
    void testHeapMetricsCompleteness() {
        final InterceptingMetricGroup heapMetrics = new InterceptingMetricGroup();

        instantiateHeapMemoryMetrics(heapMetrics);

        assertThat(heapMetrics.get(MEMORY_USED)).isNotNull();
        assertThat(heapMetrics.get(MEMORY_COMMITTED)).isNotNull();
        assertThat(heapMetrics.get(MEMORY_MAX)).isNotNull();
    }

    /**
     * Tests that heap/non-heap metrics do not rely on a static MemoryUsage instance.
     *
     * <p>We can only check this easily for the currently used heap memory, so we use it this as a
     * proxy for testing the functionality in general.
     */
    @Test
    void testHeapMetricUsageNotStatic() throws Exception {
        final InterceptingMetricGroup heapMetrics = new InterceptingMetricGroup();

        instantiateHeapMemoryMetrics(heapMetrics);

        @SuppressWarnings("unchecked")
        final Gauge<Long> used = (Gauge<Long>) heapMetrics.get(MEMORY_USED);

        runUntilMetricChanged("Heap", 10, () -> new byte[1024 * 1024 * 8], used);
    }

    @Test
    void testMetaspaceMetricUsageNotStatic() throws Exception {
        assertThat(hasMetaspaceMemoryPool())
                .withFailMessage("Requires JVM with Metaspace memory pool")
                .isTrue();

        final InterceptingMetricGroup metaspaceMetrics =
                new InterceptingMetricGroup() {
                    @Override
                    public MetricGroup addGroup(String name) {
                        return this;
                    }
                };

        instantiateMetaspaceMemoryMetrics(metaspaceMetrics);

        @SuppressWarnings("unchecked")
        final Gauge<Long> used = (Gauge<Long>) metaspaceMetrics.get(MEMORY_USED);

        runUntilMetricChanged("Metaspace", 10, ServerMetricUtilsTest::redefineDummyClass, used);
    }

    @Test
    void testNonHeapMetricUsageNotStatic() throws Exception {
        final InterceptingMetricGroup nonHeapMetrics = new InterceptingMetricGroup();

        instantiateNonHeapMemoryMetrics(nonHeapMetrics);

        @SuppressWarnings("unchecked")
        final Gauge<Long> used = (Gauge<Long>) nonHeapMetrics.get(MEMORY_USED);

        runUntilMetricChanged("Non-heap", 10, ServerMetricUtilsTest::redefineDummyClass, used);
    }

    // --------------- utility methods and classes ---------------

    /**
     * An extreme simple class with no dependencies outside "java" package to ease re-defining from
     * its bytecode.
     */
    private static class Dummy {}

    /**
     * Define a new class using {@link Dummy} class's name and bytecode to consume Metaspace and
     * NonHeap memory.
     */
    private static Class<?> redefineDummyClass() throws ClassNotFoundException {
        Class<?> clazz = Dummy.class;
        ComponentClassLoader classLoader =
                new ComponentClassLoader(
                        ClassLoaderUtils.getClasspathURLs(),
                        clazz.getClassLoader(),
                        new String[] {"java."},
                        new String[] {"org.apache."},
                        Collections.emptyMap());

        Class<?> newClass = classLoader.loadClass(clazz.getName());

        assertThat(newClass).isNotSameAs(clazz);
        assertThat(newClass.getName()).isEqualTo(clazz.getName());
        return newClass;
    }

    /** Caller may choose to run multiple times for possible interference with other tests. */
    private void runUntilMetricChanged(
            String name, int maxRuns, CheckedSupplier<Object> objectCreator, Gauge<Long> metric)
            throws Exception {
        maxRuns = Math.max(1, maxRuns);
        long initialValue = metric.getValue();
        for (int i = 0; i < maxRuns; i++) {
            Object object = objectCreator.get();
            long currentValue = metric.getValue();
            if (currentValue != initialValue) {
                return;
            }
            referencedObjects.add(object);
            Thread.sleep(50);
        }
        String msg =
                String.format(
                        "%s usage metric never changed its value after %d runs.", name, maxRuns);
        fail(msg);
    }

    static class TestGcBean implements GarbageCollectorMXBean {

        final String name;
        final long collectionCount;
        final long collectionTime;

        public TestGcBean(String name, long collectionCount, long collectionTime) {
            this.name = name;
            this.collectionCount = collectionCount;
            this.collectionTime = collectionTime;
        }

        @Override
        public long getCollectionCount() {
            return collectionCount;
        }

        @Override
        public long getCollectionTime() {
            return collectionTime;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public boolean isValid() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String[] getMemoryPoolNames() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ObjectName getObjectName() {
            throw new UnsupportedOperationException();
        }
    }

    private static class InterceptingMetricGroup extends GenericMetricGroup {

        private Map<String, Metric> intercepted;

        public InterceptingMetricGroup() {
            super(NOPMetricRegistry.INSTANCE, null, "intercepting");
        }

        public Metric get(String name) {
            return intercepted.get(name);
        }

        @Override
        protected void addMetric(String name, Metric metric) {
            if (intercepted == null) {
                intercepted = new HashMap<>();
            }
            intercepted.put(name, metric);
            super.addMetric(name, metric);
        }
    }
}

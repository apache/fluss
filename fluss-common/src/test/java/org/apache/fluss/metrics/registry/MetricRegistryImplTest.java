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

package org.apache.fluss.metrics.registry;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.SimpleCounter;
import org.apache.fluss.metrics.groups.GenericMetricGroup;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.reporter.MetricReporterPlugin;
import org.apache.fluss.metrics.reporter.ScheduledMetricReporter;
import org.apache.fluss.metrics.util.TestReporter;
import org.apache.fluss.plugin.PluginManager;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for the {@link MetricRegistryImpl}. */
class MetricRegistryImplTest {

    @Test
    void testIsShutdown() throws Exception {
        MetricRegistryImpl metricRegistry =
                new MetricRegistryImpl(Collections.singletonList(new TestReporter("test")));

        assertThat(metricRegistry.isShutdown()).isFalse();

        metricRegistry.closeAsync().get();

        assertThat(metricRegistry.isShutdown()).isTrue();
    }

    /** Verifies that reporters are notified of added/removed metrics. */
    @Test
    void testReporterNotifications() throws Exception {
        final NotificationCapturingReporter reporter1 = new NotificationCapturingReporter();
        final NotificationCapturingReporter reporter2 = new NotificationCapturingReporter();

        MetricRegistryImpl registry = new MetricRegistryImpl(Arrays.asList(reporter1, reporter2));

        GenericMetricGroup root = new GenericMetricGroup(registry, null, "test");

        root.counter("rootCounter");

        assertThat(reporter1.getLastAddedMetric()).containsInstanceOf(Counter.class);
        assertThat(reporter1.getLastAddedMetricName()).hasValue("rootCounter");

        assertThat(reporter2.getLastAddedMetric()).containsInstanceOf(Counter.class);
        assertThat(reporter2.getLastAddedMetricName()).hasValue("rootCounter");

        root.close();

        assertThat(reporter1.getLastRemovedMetric()).containsInstanceOf(Counter.class);
        assertThat(reporter1.getLastRemovedMetricName()).hasValue("rootCounter");

        assertThat(reporter2.getLastRemovedMetric()).containsInstanceOf(Counter.class);
        assertThat(reporter2.getLastRemovedMetricName()).hasValue("rootCounter");

        registry.closeAsync().get();
    }

    @Test
    void testReporterFiltersApplyToRegistrationAndRemoval() throws Exception {
        TestReporter filtered = spy(new TestReporter("filtered"));
        TestReporter unfiltered = spy(new TestReporter("unfiltered"));
        PluginManager pluginManager = mock(PluginManager.class);
        when(pluginManager.load(MetricReporterPlugin.class))
                .thenReturn(Arrays.<MetricReporterPlugin>asList(filtered, unfiltered).iterator());

        Configuration config = new Configuration();
        config.set(ConfigOptions.METRICS_REPORTERS, Arrays.asList("filtered", "unfiltered"));
        config.setString("metrics.reporter.filtered.filter.includes", "test.*:keep*");
        config.setString("metrics.reporter.filtered.filter.excludes", "*:keepDebug;*.bucket");

        try (MetricRegistry registry = MetricRegistry.create(config, pluginManager)) {
            GenericMetricGroup root = new GenericMetricGroup(registry, null, "test");
            MetricGroup table = root.addGroup("table", "orders");
            Counter kept = table.counter("keep");
            table.counter("keepDebug");
            table.counter("other");
            table.addGroup("bucket", "1").counter("keep");

            ArgumentCaptor<MetricGroup> group = ArgumentCaptor.forClass(MetricGroup.class);
            verify(filtered).notifyOfAddedMetric(eq(kept), eq("keep"), group.capture());
            assertThat(group.getValue().getLogicalScope(input -> input, '_'))
                    .isEqualTo("test_table");
            verify(filtered, times(1)).notifyOfAddedMetric(any(), anyString(), any());
            verify(unfiltered, times(4)).notifyOfAddedMetric(any(), anyString(), any());

            root.close();

            verify(filtered).notifyOfRemovedMetric(eq(kept), eq("keep"), any());
            verify(filtered, times(1)).notifyOfRemovedMetric(any(), anyString(), any());
            verify(unfiltered, times(4)).notifyOfRemovedMetric(any(), anyString(), any());
        }
        verify(filtered).close();
        verify(unfiltered).close();
    }

    /**
     * Reporter that exposes the name and metric instance of the last metric that was added or
     * removed.
     */
    private static class NotificationCapturingReporter extends TestReporter {
        private static final String NAME = "notificationReporter";
        @Nullable private Metric addedMetric;
        @Nullable private String addedMetricName;

        @Nullable private Metric removedMetric;
        @Nullable private String removedMetricName;

        public NotificationCapturingReporter() {
            super(NAME);
        }

        @Override
        public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
            addedMetric = metric;
            addedMetricName = metricName;
        }

        @Override
        public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
            removedMetric = metric;
            removedMetricName = metricName;
        }

        public Optional<Metric> getLastAddedMetric() {
            return Optional.ofNullable(addedMetric);
        }

        public Optional<String> getLastAddedMetricName() {
            return Optional.ofNullable(addedMetricName);
        }

        public Optional<Metric> getLastRemovedMetric() {
            return Optional.ofNullable(removedMetric);
        }

        public Optional<String> getLastRemovedMetricName() {
            return Optional.ofNullable(removedMetricName);
        }
    }

    @Test
    void testExceptionIsolation() throws Exception {
        final NotificationCapturingReporter reporter1 = new NotificationCapturingReporter();

        MetricRegistryImpl registry =
                new MetricRegistryImpl(Arrays.asList(new FailingReporter(), reporter1));

        Counter metric = new SimpleCounter();

        GenericMetricGroup dummyGroup = new GenericMetricGroup(registry, null, "test");

        registry.register(metric, "counter", dummyGroup);

        assertThat(reporter1.getLastAddedMetric()).hasValue(metric);
        assertThat(reporter1.getLastAddedMetricName()).hasValue("counter");

        registry.unregister(metric, "counter", dummyGroup);

        assertThat(reporter1.getLastRemovedMetric()).hasValue(metric);
        assertThat(reporter1.getLastRemovedMetricName()).hasValue("counter");

        registry.closeAsync().get();
    }

    /**
     * Verifies that reporters implementing the Scheduled interface are regularly called to report
     * the metrics.
     */
    @Test
    void testReporterScheduling() throws Exception {
        final ReportCountingReporter reporter = new ReportCountingReporter();
        ManuallyTriggeredScheduledExecutorService scheduledReportExecutorService =
                new ManuallyTriggeredScheduledExecutorService();

        try (MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        Collections.singletonList(reporter), scheduledReportExecutorService)) {

            // only start counting from now on
            reporter.resetCount();

            for (int x = 0; x < 10; x++) {
                scheduledReportExecutorService.triggerPeriodicScheduledTasks();
                assertThat(reporter.getReportCount()).isEqualTo(x + 1);
            }
        }
    }

    /** Reporter that throws an exception when it is notified of an added or removed metric. */
    private static class FailingReporter extends TestReporter {

        private static final String NAME = "failingReporter";

        public FailingReporter() {
            super(NAME);
        }

        @Override
        public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
            throw new RuntimeException();
        }

        @Override
        public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
            throw new RuntimeException();
        }
    }

    /** Reporter that exposes how often report() was called. */
    private static class ReportCountingReporter extends TestReporter
            implements ScheduledMetricReporter {

        private static final String NAME = "reportCountingReporter";

        private int reportCount = 0;

        public ReportCountingReporter() {
            super(NAME);
        }

        @Override
        public void report() {
            reportCount++;
        }

        @Override
        public Duration scheduleInterval() {
            return Duration.ofMillis(1);
        }

        public int getReportCount() {
            return reportCount;
        }

        public void resetCount() {
            reportCount = 0;
        }
    }
}

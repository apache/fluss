/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.metrics.registry;

import com.alibaba.fluss.metrics.Counter;
import com.alibaba.fluss.metrics.Metric;
import com.alibaba.fluss.metrics.SimpleCounter;
import com.alibaba.fluss.metrics.groups.GenericMetricGroup;
import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.metrics.reporter.ScheduledMetricReporter;
import com.alibaba.fluss.metrics.util.TestReporter;
import com.alibaba.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

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

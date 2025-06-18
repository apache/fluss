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

package com.alibaba.fluss.metrics.registry;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.exception.FlussException;
import com.alibaba.fluss.metrics.Metric;
import com.alibaba.fluss.metrics.MetricView;
import com.alibaba.fluss.metrics.MetricViewUpdater;
import com.alibaba.fluss.metrics.groups.AbstractMetricGroup;
import com.alibaba.fluss.metrics.groups.FrontMetricGroup;
import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.metrics.groups.ReporterScopedSettings;
import com.alibaba.fluss.metrics.reporter.MetricReporter;
import com.alibaba.fluss.metrics.reporter.ScheduledMetricReporter;
import com.alibaba.fluss.utils.ExceptionUtils;
import com.alibaba.fluss.utils.ExecutorUtils;
import com.alibaba.fluss.utils.TimeUtils;
import com.alibaba.fluss.utils.concurrent.ExecutorThreadFactory;
import com.alibaba.fluss.utils.concurrent.FutureUtils;
import com.alibaba.fluss.utils.function.QuadConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A MetricRegistry keeps track of all registered {@link Metric Metrics}. It serves as the
 * connection between {@link MetricGroup MetricGroups} and {@link MetricReporter MetricReporters}.
 */
public class MetricRegistryImpl implements MetricRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(MetricRegistryImpl.class);

    private final Object lock = new Object();

    private final List<ReporterAndSettings> reporters;

    private final CompletableFuture<Void> terminationFuture;

    private final ScheduledExecutorService viewUpdaterScheduledExecutor;
    private final ScheduledExecutorService reporterScheduledExecutor;

    private MetricViewUpdater metricViewUpdater;

    private boolean isShutdown;

    public MetricRegistryImpl(List<MetricReporter> reporters) {
        this(
                reporters,
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("fluss-metric-reporter")));
    }

    @VisibleForTesting
    protected MetricRegistryImpl(
            List<MetricReporter> reporters, ScheduledExecutorService reporterScheduledExecutor) {
        this.terminationFuture = new CompletableFuture<>();
        this.isShutdown = false;
        this.reporters = new ArrayList<>(4);
        this.viewUpdaterScheduledExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("fluss-metric-view-updater"));
        this.reporterScheduledExecutor = reporterScheduledExecutor;
        initMetricReporters(reporters);
    }

    private void initMetricReporters(Collection<MetricReporter> reporterInstances) {
        if (reporterInstances.isEmpty()) {
            // no reporters defined by default, don't report anything
            LOG.info("No metrics reporter configured, no metrics will be exposed/reported.");
            return;
        }
        for (MetricReporter reporter : reporterInstances) {
            final String className = reporter.getClass().getName();
            if (reporter instanceof ScheduledMetricReporter) {
                ScheduledMetricReporter scheduledMetricReporter =
                        (ScheduledMetricReporter) reporter;
                LOG.info(
                        "Periodically reporting metrics in intervals of {} for reporter of type {}.",
                        TimeUtils.formatWithHighestUnit(scheduledMetricReporter.scheduleInterval()),
                        className);
                reporterScheduledExecutor.scheduleWithFixedDelay(
                        new MetricRegistryImpl.ReporterTask(scheduledMetricReporter),
                        scheduledMetricReporter.scheduleInterval().toMillis(),
                        scheduledMetricReporter.scheduleInterval().toMillis(),
                        TimeUnit.MILLISECONDS);
            } else {
                LOG.info("Reporting metrics for reporter of type {}.", className);
            }
            reporters.add(
                    new ReporterAndSettings(
                            reporter, new ReporterScopedSettings(reporterInstances.size())));
        }
    }

    @Override
    public int getNumberReporters() {
        return reporters.size();
    }

    /**
     * Returns whether this registry has been shutdown.
     *
     * @return true, if this registry was shutdown, otherwise false
     */
    public boolean isShutdown() {
        synchronized (lock) {
            return isShutdown;
        }
    }

    /**
     * Shuts down this registry and the associated {@link MetricReporter}.
     *
     * <p>NOTE: This operation is asynchronous and returns a future which is completed once the
     * shutdown operation has been completed.
     *
     * @return Future which is completed once the {@link MetricRegistryImpl} is shut down.
     */
    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (lock) {
            if (!isShutdown) {
                isShutdown = true;
                final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);
                final Duration gracePeriod = Duration.ofSeconds(1L);

                Throwable throwable = null;
                for (ReporterAndSettings reporterAndSettings : reporters) {
                    try {
                        reporterAndSettings.reporter.close();
                    } catch (Throwable t) {
                        throwable = ExceptionUtils.firstOrSuppressed(t, throwable);
                    }
                }
                reporters.clear();

                if (throwable != null) {
                    terminationFutures.add(
                            FutureUtils.completedExceptionally(
                                    new FlussException(
                                            "Could not shut down the metric reporters properly.",
                                            throwable)));
                }

                final CompletableFuture<Void> reporterExecutorShutdownFuture =
                        ExecutorUtils.nonBlockingShutdown(
                                gracePeriod.toMillis(),
                                TimeUnit.MILLISECONDS,
                                reporterScheduledExecutor);
                terminationFutures.add(reporterExecutorShutdownFuture);

                final CompletableFuture<Void> viewUpdaterExecutorShutdownFuture =
                        ExecutorUtils.nonBlockingShutdown(
                                gracePeriod.toMillis(),
                                TimeUnit.MILLISECONDS,
                                viewUpdaterScheduledExecutor);

                terminationFutures.add(viewUpdaterExecutorShutdownFuture);

                FutureUtils.completeAll(terminationFutures)
                        .whenComplete(
                                (Void ignored, Throwable error) -> {
                                    if (error != null) {
                                        terminationFuture.completeExceptionally(error);
                                    } else {
                                        terminationFuture.complete(null);
                                    }
                                });
            }
            return terminationFuture;
        }
    }

    @Override
    public void register(Metric metric, String metricName, AbstractMetricGroup group) {
        synchronized (lock) {
            if (isShutdown()) {
                LOG.warn(
                        "Cannot register metric, because the MetricRegistry has already been shut down.");
            } else {
                if (reporters != null) {
                    forAllReporters(MetricReporter::notifyOfAddedMetric, metric, metricName, group);
                }

                try {
                    if (metric instanceof MetricView) {
                        if (metricViewUpdater == null) {
                            metricViewUpdater = new MetricViewUpdater(viewUpdaterScheduledExecutor);
                        }
                        metricViewUpdater.notifyOfAddedView((MetricView) metric);
                    }
                } catch (Exception e) {
                    LOG.warn("Error while registering metric: {}.", metricName, e);
                }
            }
        }
    }

    @Override
    public void unregister(Metric metric, String metricName, AbstractMetricGroup group) {
        synchronized (lock) {
            if (isShutdown()) {
                LOG.warn(
                        "Cannot unregister metric, because the MetricRegistry has already been shut down.");
            } else {
                if (reporters != null) {
                    forAllReporters(
                            MetricReporter::notifyOfRemovedMetric, metric, metricName, group);
                }

                try {
                    if (metric instanceof MetricView) {
                        if (metricViewUpdater != null) {
                            metricViewUpdater.notifyOfRemovedView((MetricView) metric);
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("Error while unregistering metric: {}", metricName, e);
                }
            }
        }
    }

    @GuardedBy("lock")
    private void forAllReporters(
            QuadConsumer<MetricReporter, Metric, String, MetricGroup> operation,
            Metric metric,
            String metricName,
            AbstractMetricGroup group) {
        for (ReporterAndSettings reporterAndSettings : reporters) {
            try {
                if (reporterAndSettings != null) {
                    FrontMetricGroup<?> front =
                            new FrontMetricGroup<>(reporterAndSettings.getSettings(), group);
                    operation.accept(reporterAndSettings.reporter, metric, metricName, front);
                }
            } catch (Exception e) {
                LOG.warn("Error while handling metric: {}.", metricName, e);
            }
        }
    }

    // ------------------------------------------------------------------------
    /**
     * This task is explicitly a static class, so that it does not hold any references to the
     * enclosing MetricsRegistry instance.
     *
     * <p>This is a subtle difference, but very important: With this static class, the enclosing
     * class instance may become garbage-collectible, whereas with an anonymous inner class, the
     * timer thread (which is a GC root) will hold a reference via the timer task and its enclosing
     * instance pointer. Making the MetricsRegistry garbage collectible makes the java.util.Timer
     * garbage collectible, which acts as a fail-safe to stop the timer thread and prevents resource
     * leaks.
     */
    private static final class ReporterTask extends TimerTask {

        private final ScheduledMetricReporter reporter;

        private ReporterTask(ScheduledMetricReporter reporter) {
            this.reporter = reporter;
        }

        @Override
        public void run() {
            try {
                reporter.report();
            } catch (Throwable t) {
                LOG.warn("Error while reporting metrics", t);
            }
        }
    }

    private static class ReporterAndSettings {

        private final MetricReporter reporter;
        private final ReporterScopedSettings settings;

        private ReporterAndSettings(MetricReporter reporter, ReporterScopedSettings settings) {
            this.reporter = checkNotNull(reporter);
            this.settings = checkNotNull(settings);
        }

        public ReporterScopedSettings getSettings() {
            return settings;
        }
    }
}

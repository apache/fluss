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

package org.apache.fluss.metrics.opentelemetry;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.CharacterFilter;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.Meter;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.reporter.MetricReporter;
import org.apache.fluss.metrics.reporter.ScheduledMetricReporter;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.semconv.ServiceAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** {@link MetricReporter} that exports {@link Metric Metrics} via OpenTelemetry. */
public class OpenTelemetryReporter implements MetricReporter, ScheduledMetricReporter {

    private static final Logger LOG = LoggerFactory.getLogger(OpenTelemetryReporter.class);

    private static final char SCOPE_SEPARATOR = '.';
    private static final String SCOPE_PREFIX = "fluss" + SCOPE_SEPARATOR;

    @VisibleForTesting final Resource resource;
    private final MetricExporter exporter;
    private final Duration interval;
    private final Clock clock;

    @VisibleForTesting final Map<Gauge<?>, MetricMetadata> gauges = new HashMap<>();
    @VisibleForTesting final Map<Counter, MetricMetadata> counters = new HashMap<>();
    @VisibleForTesting final Map<Histogram, MetricMetadata> histograms = new HashMap<>();
    @VisibleForTesting final Map<Meter, MetricMetadata> meters = new HashMap<>();

    private Map<Metric, Long> lastValueSnapshots = Collections.emptyMap();
    private long lastCollectTimeNanos = 0;
    private @Nullable CompletableResultCode lastResult;

    OpenTelemetryReporter(
            String endpoint,
            Duration interval,
            Duration timeout,
            String serviceName,
            String serviceVersion) {
        this.exporter = buildMetricExporter(endpoint, timeout);
        this.resource = buildResource(serviceName, serviceVersion);
        this.interval = interval;
        this.clock = Clock.systemUTC();
    }

    private static MetricExporter buildMetricExporter(String endpoint, Duration timeout) {
        OtlpGrpcMetricExporterBuilder grpcExporterBuilder = OtlpGrpcMetricExporter.builder();
        grpcExporterBuilder.setEndpoint(endpoint);
        grpcExporterBuilder.setTimeout(timeout);
        return grpcExporterBuilder.build();
    }

    private static Resource buildResource(String serviceName, String serviceVersion) {
        Resource resource = Resource.getDefault();

        if (serviceName != null) {
            resource =
                    resource.merge(
                            Resource.create(
                                    Attributes.of(ServiceAttributes.SERVICE_NAME, serviceName)));
        }

        if (serviceVersion != null) {
            resource =
                    resource.merge(
                            Resource.create(
                                    Attributes.of(
                                            ServiceAttributes.SERVICE_VERSION, serviceVersion)));
        }

        return resource;
    }

    @Override
    public void open(Configuration config) {
        // do nothing
    }

    @Override
    public void close() {
        exporter.flush();

        if (lastResult != null) {
            LOG.debug("Waiting for up to 1 minute to export pending metrics.");
            CompletableResultCode resultCode = lastResult.join(1, TimeUnit.MINUTES);
            if (!resultCode.isSuccess()) {
                LOG.warn(
                        "Failed to export pending metrics when closing reporter. Result code: {}. Details: {}",
                        resultCode,
                        resultCode.getFailureThrowable());
            }
        }

        exporter.close();
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        final String name = SCOPE_PREFIX + getLogicalScope(group) + SCOPE_SEPARATOR + metricName;

        Map<String, String> variables =
                group.getAllVariables().entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        e -> removeEnclosingAngleBrackets(e.getKey()),
                                        Map.Entry::getValue));
        LOG.debug("Adding metric {} with variables {}", metricName, variables);

        final MetricMetadata metricMetadata = new MetricMetadata(name, variables);

        synchronized (this) {
            switch (metric.getMetricType()) {
                case COUNTER:
                    this.counters.put((Counter) metric, metricMetadata);
                    break;
                case GAUGE:
                    this.gauges.put((Gauge<?>) metric, metricMetadata);
                    break;
                case HISTOGRAM:
                    this.histograms.put((Histogram) metric, metricMetadata);
                    break;
                case METER:
                    this.meters.put((Meter) metric, metricMetadata);
                    break;
                default:
                    LOG.warn(
                            "Cannot add unknown metric type {}. This indicates that the reporter does not "
                                    + "support this metric type.",
                            metric.getClass().getName());
            }
        }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        synchronized (this) {
            // Make sure we are not caching the metric object
            lastValueSnapshots.remove(metric);

            switch (metric.getMetricType()) {
                case COUNTER:
                    this.counters.remove((Counter) metric);
                    break;
                case GAUGE:
                    this.gauges.remove((Gauge<?>) metric);
                    break;
                case HISTOGRAM:
                    this.histograms.remove((Histogram) metric);
                    break;
                case METER:
                    this.meters.remove((Meter) metric);
                    break;
                default:
                    LOG.warn(
                            "Cannot remove unknown metric type {}. This indicates that the reporter does "
                                    + "not support this metric type.",
                            metric.getClass().getName());
            }
        }
    }

    @Override
    public void report() {
        synchronized (this) {
            Collection<MetricData> metricData = collectMetrics();
            try {
                lastResult = exporter.export(metricData);
                lastResult.whenComplete(
                        () -> {
                            if (lastResult.isSuccess()) {
                                LOG.debug(
                                        "Exported {} metrics using {}",
                                        metricData.size(),
                                        exporter.getClass().getName());
                            } else {
                                LOG.warn(
                                        "Failed to export {} metrics using {}",
                                        metricData.size(),
                                        exporter.getClass().getName());
                            }
                        });
            } catch (Exception e) {
                LOG.error(
                        "Failed to call export for {} metrics using {}",
                        metricData.size(),
                        exporter.getClass().getName());
            }
        }
    }

    @Override
    public Duration scheduleInterval() {
        return interval;
    }

    private static String getLogicalScope(MetricGroup group) {
        return group.getLogicalScope(CharacterFilter.NO_OP_FILTER, SCOPE_SEPARATOR);
    }

    @VisibleForTesting
    static String removeEnclosingAngleBrackets(String str) {
        if (str.length() >= 2 && str.charAt(0) == '<' && str.charAt(str.length() - 1) == '>') {
            return str.substring(1, str.length() - 1);
        }
        return str;
    }

    private long getCurrentTimeNanos() {
        Instant now = clock.instant();
        return TimeUnit.SECONDS.toNanos(now.getEpochSecond()) + now.getNano();
    }

    private Collection<MetricData> collectMetrics() {
        // This method is not thread-safe.
        // The callee must hold the monitor on 'this' before calling it.
        long currentTimeNanos = getCurrentTimeNanos();
        List<MetricData> data = new ArrayList<>();
        OpenTelemetryAdapter.CollectionMetadata collectionMetadata =
                new OpenTelemetryAdapter.CollectionMetadata(
                        resource, lastCollectTimeNanos, currentTimeNanos);

        Map<Metric, Long> currentValueSnapshots = takeLastValueSnapshots();

        for (Counter counter : counters.keySet()) {
            Long count = currentValueSnapshots.get(counter);
            Long lastCount = lastValueSnapshots.getOrDefault(counter, 0L);
            MetricMetadata metricMetadata = counters.get(counter);
            Optional<MetricData> metricData =
                    OpenTelemetryAdapter.convertCounter(
                            collectionMetadata, count, lastCount, metricMetadata);
            metricData.ifPresent(data::add);
        }

        for (Gauge<?> gauge : gauges.keySet()) {
            MetricMetadata metricMetadata = gauges.get(gauge);
            Optional<MetricData> metricData =
                    OpenTelemetryAdapter.convertGauge(collectionMetadata, gauge, metricMetadata);
            metricData.ifPresent(data::add);
        }

        for (Meter meter : meters.keySet()) {
            Long count = currentValueSnapshots.get(meter);
            Long lastCount = lastValueSnapshots.getOrDefault(meter, 0L);
            MetricMetadata metricMetadata = meters.get(meter);
            List<MetricData> metricData =
                    OpenTelemetryAdapter.convertMeter(
                            collectionMetadata, meter, count, lastCount, metricMetadata);
            data.addAll(metricData);
        }

        for (Histogram histogram : histograms.keySet()) {
            MetricMetadata metricMetadata = histograms.get(histogram);
            Optional<MetricData> metricData =
                    OpenTelemetryAdapter.convertHistogram(
                            collectionMetadata, histogram, metricMetadata);
            metricData.ifPresent(data::add);
        }

        lastValueSnapshots = currentValueSnapshots;
        lastCollectTimeNanos = currentTimeNanos;

        return data;
    }

    private Map<Metric, Long> takeLastValueSnapshots() {
        // This method is not thread-safe.
        // The callee must hold the monitor on 'this' before calling it.
        Map<Metric, Long> map = new HashMap<>();

        for (Counter counter : counters.keySet()) {
            map.put(counter, counter.getCount());
        }

        for (Meter meter : meters.keySet()) {
            map.put(meter, meter.getCount());
        }

        return map;
    }
}

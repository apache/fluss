/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.metrics.datadog;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.HistogramStatistics;
import org.apache.fluss.metrics.Meter;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.reporter.ScheduledMetricReporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * {@link ScheduledMetricReporter} that pushes {@link Metric Metrics} to Datadog over its HTTP API.
 *
 * <p>Counters are sent as Datadog counts holding the increase since the last report Datadog
 * accepted, so a failed request does not lose increments. Gauges and meters are sent as gauges, and
 * histograms as one gauge per aggregation, named {@code <metric>.<aggregation>}. Metric variables
 * are sent as {@code key:value} tags, and the {@code host} variable also names the host.
 */
public class DatadogReporter implements ScheduledMetricReporter {

    private static final Logger LOG = LoggerFactory.getLogger(DatadogReporter.class);

    private static final String METRIC_PREFIX = "fluss.";
    private static final char SCOPE_SEPARATOR = '.';
    private static final Pattern INVALID_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9_.]");
    private static final String HOST_VARIABLE = "host";

    private final DatadogHttpClient client;
    private final Duration pushInterval;
    private final int maxSeriesPerRequest;
    private final List<String> staticTags;

    @VisibleForTesting final Map<Metric, MetricInfo> metrics = new ConcurrentHashMap<>();

    /** Whether Datadog has answered the API key check, which only needs to happen once. */
    private boolean apiKeyChecked;

    /** The count of each counter that Datadog has accepted so far. */
    private final Map<Counter, Long> reportedCounts = new ConcurrentHashMap<>();

    DatadogReporter(
            DatadogHttpClient client,
            Duration pushInterval,
            int maxSeriesPerRequest,
            List<String> staticTags) {
        this.client = client;
        this.pushInterval = pushInterval;
        this.maxSeriesPerRequest = maxSeriesPerRequest;
        this.staticTags = staticTags;
    }

    @VisibleForTesting
    DatadogHttpClient client() {
        return client;
    }

    @Override
    public void open(Configuration config) {
        // the reporter is configured by DatadogReporterPlugin
    }

    @Override
    public void close() {
        // nothing to release: each report opens and closes its own connections
    }

    @Override
    public Duration scheduleInterval() {
        return pushInterval;
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        String name =
                METRIC_PREFIX
                        + group.getLogicalScope(DatadogReporter::filterCharacters, SCOPE_SEPARATOR)
                        + SCOPE_SEPARATOR
                        + filterCharacters(metricName);
        List<String> tags = new ArrayList<>(staticTags);
        String host = null;
        for (Map.Entry<String, String> variable : group.getAllVariables().entrySet()) {
            if (variable.getValue() == null || variable.getValue().isEmpty()) {
                continue;
            }
            tags.add(variable.getKey() + ":" + variable.getValue());
            if (HOST_VARIABLE.equals(variable.getKey())) {
                host = variable.getValue();
            }
        }
        metrics.put(metric, new MetricInfo(name, Collections.unmodifiableList(tags), host));
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        metrics.remove(metric);
        if (metric instanceof Counter) {
            reportedCounts.remove(metric);
        }
    }

    @Override
    public void report() {
        if (!apiKeyChecked) {
            checkApiKey();
        }
        long timestampSeconds = System.currentTimeMillis() / 1000;
        List<PendingSeries> pending = new ArrayList<>();
        for (Map.Entry<Metric, MetricInfo> entry : metrics.entrySet()) {
            try {
                addSeries(entry.getKey(), entry.getValue(), pending);
            } catch (RuntimeException e) {
                LOG.debug("Failed to read metric {}", entry.getValue().name, e);
            }
        }
        for (int from = 0; from < pending.size(); from += maxSeriesPerRequest) {
            send(
                    pending.subList(from, Math.min(from + maxSeriesPerRequest, pending.size())),
                    timestampSeconds);
        }
    }

    /**
     * Logs an error if Datadog rejects the API key. Datadog drops metrics sent with an unknown key
     * without reporting an error, so this is the only way such a key shows up.
     */
    private void checkApiKey() {
        try {
            if (!client.isApiKeyValid()) {
                LOG.error(
                        "Datadog rejected the configured API key, so it drops every metric this "
                                + "reporter sends. Check 'metrics.reporter.datadog.api-key' and "
                                + "'metrics.reporter.datadog.site'.");
            }
            apiKeyChecked = true;
        } catch (IOException e) {
            LOG.warn(
                    "Could not check the Datadog API key, will check again on the next report.", e);
        }
    }

    private void send(List<PendingSeries> chunk, long timestampSeconds) {
        List<DatadogSeries> series = new ArrayList<>(chunk.size());
        for (PendingSeries p : chunk) {
            series.add(p.series);
        }
        try {
            client.send(series, timestampSeconds);
        } catch (Exception e) {
            LOG.warn("Failed to send {} metric series to Datadog", series.size(), e);
            return;
        }
        for (PendingSeries p : chunk) {
            if (p.counter != null) {
                reportedCounts.put(p.counter, p.count);
            }
        }
    }

    private void addSeries(Metric metric, MetricInfo info, List<PendingSeries> pending) {
        if (metric instanceof Counter) {
            Counter counter = (Counter) metric;
            long count = counter.getCount();
            long increase = count - reportedCounts.getOrDefault(counter, 0L);
            // A counter that went down was reset, so all of its current count is new.
            DatadogSeries series =
                    info.series(DatadogSeries.Type.COUNT, increase < 0 ? count : increase);
            pending.add(new PendingSeries(series, counter, count));
        } else if (metric instanceof Gauge) {
            Object value = ((Gauge<?>) metric).getValue();
            if (value instanceof Number) {
                pending.add(
                        new PendingSeries(
                                info.series(
                                        DatadogSeries.Type.GAUGE, ((Number) value).doubleValue())));
            } else if (value instanceof Boolean) {
                pending.add(
                        new PendingSeries(
                                info.series(DatadogSeries.Type.GAUGE, (Boolean) value ? 1 : 0)));
            }
            // Datadog only stores numbers, so other gauges are not reported.
        } else if (metric instanceof Meter) {
            pending.add(
                    new PendingSeries(
                            info.series(DatadogSeries.Type.GAUGE, ((Meter) metric).getRate())));
        } else if (metric instanceof Histogram) {
            Histogram histogram = (Histogram) metric;
            HistogramStatistics stats = histogram.getStatistics();
            pending.add(new PendingSeries(info.series("count", histogram.getCount())));
            pending.add(new PendingSeries(info.series("avg", stats.getMean())));
            pending.add(new PendingSeries(info.series("stddev", stats.getStdDev())));
            pending.add(new PendingSeries(info.series("min", stats.getMin())));
            pending.add(new PendingSeries(info.series("max", stats.getMax())));
            pending.add(new PendingSeries(info.series("median", stats.getQuantile(0.5))));
            pending.add(new PendingSeries(info.series("75percentile", stats.getQuantile(0.75))));
            pending.add(new PendingSeries(info.series("95percentile", stats.getQuantile(0.95))));
            pending.add(new PendingSeries(info.series("98percentile", stats.getQuantile(0.98))));
            pending.add(new PendingSeries(info.series("99percentile", stats.getQuantile(0.99))));
            pending.add(new PendingSeries(info.series("999percentile", stats.getQuantile(0.999))));
        }
    }

    private static String filterCharacters(String input) {
        return INVALID_CHAR_PATTERN.matcher(input).replaceAll("_");
    }

    /** A series to send and, for a counter, the count it reports once Datadog accepts it. */
    private static final class PendingSeries {
        final DatadogSeries series;
        @Nullable final Counter counter;
        final long count;

        PendingSeries(DatadogSeries series) {
            this(series, null, 0);
        }

        PendingSeries(DatadogSeries series, @Nullable Counter counter, long count) {
            this.series = series;
            this.counter = counter;
            this.count = count;
        }
    }

    /** The Datadog name, tags and host of a registered metric. */
    @VisibleForTesting
    static final class MetricInfo {
        final String name;
        final List<String> tags;
        @Nullable final String host;

        MetricInfo(String name, List<String> tags, @Nullable String host) {
            this.name = name;
            this.tags = tags;
            this.host = host;
        }

        DatadogSeries series(DatadogSeries.Type type, double value) {
            return new DatadogSeries(name, type, value, tags, host);
        }

        DatadogSeries series(String aggregation, double value) {
            return new DatadogSeries(
                    name + SCOPE_SEPARATOR + aggregation,
                    DatadogSeries.Type.GAUGE,
                    value,
                    tags,
                    host);
        }
    }
}

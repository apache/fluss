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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.HistogramStatistics;
import org.apache.fluss.metrics.Meter;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.reporter.MetricReporter;
import org.apache.fluss.metrics.reporter.MetricReporterPlugin;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Server-side metric reporter loaded via SPI. Implements both {@link MetricReporterPlugin} and
 * {@link MetricReporter}. Multiple instances (Coordinator + TabletServer) share a static metrics
 * map so all metrics are merged into a single JSON dump. Only the first instance starts the
 * scheduler.
 */
public class FlussMetricSampler implements MetricReporterPlugin, MetricReporter {

    private static final Logger LOG = LoggerFactory.getLogger(FlussMetricSampler.class);
    private static final String CONFIG_KEY = "metrics.reporter.perf.file";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Shared across all instances (Coordinator + TabletServer) in the same JVM
    private static final ConcurrentMap<String, Metric> METRICS =
            org.apache.fluss.utils.MapUtils.newConcurrentHashMap();

    private static volatile Path outputPath;
    private static volatile ScheduledExecutorService scheduler;
    private static int instanceCount;

    /** Tracks whether this instance successfully registered in open(). */
    private boolean isRegistered;

    @Override
    public String identifier() {
        return "perf";
    }

    @Override
    public MetricReporter createMetricReporter(Configuration configuration) {
        return this;
    }

    @Override
    public void open(Configuration config) {
        String filePath = config.getRawValue(CONFIG_KEY).map(Object::toString).orElse(null);
        if (filePath == null || filePath.isEmpty()) {
            LOG.warn("No output file configured for perf metric reporter (key={})", CONFIG_KEY);
            return;
        }
        synchronized (FlussMetricSampler.class) {
            isRegistered = true;
            outputPath = Paths.get(filePath);
            instanceCount++;
            if (scheduler == null) {
                scheduler =
                        Executors.newSingleThreadScheduledExecutor(
                                r -> {
                                    Thread t = new Thread(r, "fluss-microbench-metric-dumper");
                                    t.setDaemon(true);
                                    return t;
                                });
                scheduler.scheduleAtFixedRate(
                        FlussMetricSampler::dumpMetrics, 500, 500, TimeUnit.MILLISECONDS);
                LOG.info("FlussMetricSampler scheduler started, writing to {}", outputPath);
            } else {
                LOG.info(
                        "FlussMetricSampler instance joined shared metrics (writing to {})",
                        outputPath);
            }
        }
    }

    @Override
    public void close() {
        synchronized (FlussMetricSampler.class) {
            if (!isRegistered) {
                return;
            }
            isRegistered = false;
            instanceCount--;
            if (instanceCount <= 0 && scheduler != null) {
                scheduler.shutdownNow();
                try {
                    scheduler.awaitTermination(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                scheduler = null;
                METRICS.clear();
                LOG.info("FlussMetricSampler scheduler shut down (last instance closed)");
            }
        }
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        String qualifiedName = group.getLogicalScope(s -> s, '.') + "." + metricName;
        METRICS.put(qualifiedName, metric);
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        String qualifiedName = group.getLogicalScope(s -> s, '.') + "." + metricName;
        METRICS.remove(qualifiedName);
    }

    private static void dumpMetrics() {
        if (outputPath == null) {
            return;
        }
        Path tmpFile = outputPath.resolveSibling(outputPath.getFileName() + ".tmp");
        try {
            Map<String, Object> snapshot = new LinkedHashMap<>();
            snapshot.put("timestamp", System.currentTimeMillis());
            for (Map.Entry<String, Metric> entry : METRICS.entrySet()) {
                Metric m = entry.getValue();
                if (m instanceof Counter) {
                    snapshot.put(entry.getKey(), ((Counter) m).getCount());
                } else if (m instanceof Gauge) {
                    Object val = ((Gauge<?>) m).getValue();
                    if (val instanceof Number) {
                        snapshot.put(entry.getKey(), val);
                    } else if (val != null) {
                        snapshot.put(entry.getKey(), val.toString());
                    }
                } else if (m instanceof Meter) {
                    Meter meter = (Meter) m;
                    snapshot.put(entry.getKey() + ".count", meter.getCount());
                    snapshot.put(entry.getKey() + ".rate", meter.getRate());
                } else if (m instanceof Histogram) {
                    Histogram hist = (Histogram) m;
                    snapshot.put(entry.getKey() + ".count", hist.getCount());
                    try {
                        HistogramStatistics stats = hist.getStatistics();
                        snapshot.put(entry.getKey() + ".p50", stats.getQuantile(0.5));
                        snapshot.put(entry.getKey() + ".p95", stats.getQuantile(0.95));
                        snapshot.put(entry.getKey() + ".p99", stats.getQuantile(0.99));
                        snapshot.put(entry.getKey() + ".mean", stats.getMean());
                        snapshot.put(entry.getKey() + ".max", stats.getMax());
                    } catch (Exception e) {
                        // stats may not be available yet
                    }
                }
            }
            MAPPER.writeValue(tmpFile.toFile(), snapshot);
            Files.move(
                    tmpFile,
                    outputPath,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            LOG.warn("Failed to dump metrics to {}", outputPath, e);
        } finally {
            try {
                Files.deleteIfExists(tmpFile);
            } catch (IOException e) {
                // best-effort cleanup
            }
        }
    }
}

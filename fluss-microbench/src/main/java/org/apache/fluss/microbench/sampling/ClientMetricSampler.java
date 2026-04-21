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
import org.apache.fluss.metrics.Meter;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.reporter.MetricReporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentMap;

import static org.apache.fluss.utils.MapUtils.newConcurrentHashMap;

/**
 * In-process metric reporter for the client (perf harness) JVM. Singleton pattern — injected via
 * MetricRegistry, not SPI.
 */
public class ClientMetricSampler implements MetricReporter {

    private static final Logger LOG = LoggerFactory.getLogger(ClientMetricSampler.class);

    private static final ClientMetricSampler INSTANCE = new ClientMetricSampler();

    private final ConcurrentMap<String, Metric> metrics = newConcurrentHashMap();

    private ClientMetricSampler() {}

    public static ClientMetricSampler instance() {
        return INSTANCE;
    }

    @Override
    public void open(Configuration config) {
        // no-op
    }

    @Override
    public void close() {
        metrics.clear();
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        String qualifiedName = group.getLogicalScope(s -> s, '.') + "." + metricName;
        metrics.put(qualifiedName, metric);
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        String qualifiedName = group.getLogicalScope(s -> s, '.') + "." + metricName;
        metrics.remove(qualifiedName);
    }

    /** Reads collected client metrics into the given snapshot. */
    @SuppressWarnings("unchecked")
    public void sample(ResourceSnapshot snap) {
        try {
            // Writer throughput
            snap.clientBytesSendTotal = findMeterCountBySuffix("bytesSendPerSecond");
            snap.clientRecordsSendTotal = findMeterCountBySuffix("recordSendPerSecond");
            snap.clientSendLatencyMs = findGaugeBySuffix("sendLatencyMs");
            snap.clientBatchQueueTimeMs = findGaugeBySuffix("batchQueueTimeMs");

            // Writer retry & batch stats
            snap.clientRecordsRetryTotal = findMeterCountBySuffix("recordsRetryPerSecond");
            snap.clientBytesPerBatchMean = findHistogramMean("bytesPerBatch");
            snap.clientRecordsPerBatchMean = findHistogramMean("recordsPerBatch");

            // RPC metrics
            snap.clientRpcRequestRate = findGaugeBySuffix("requestsPerSecond_total");
            snap.clientRpcResponseRate = findGaugeBySuffix("responsesPerSecond_total");
            snap.clientRpcBytesInRate = findGaugeBySuffix("bytesInPerSecond_total");
            snap.clientRpcBytesOutRate = findGaugeBySuffix("bytesOutPerSecond_total");
            snap.clientRpcRequestLatencyAvg = findGaugeBySuffix("requestLatencyMs_avg");
            snap.clientRpcRequestLatencyMax = findGaugeBySuffix("requestLatencyMs_max");
            snap.clientRpcInflight = findGaugeBySuffix("requestsInFlight_total");
        } catch (Exception e) {
            LOG.warn("Failed to sample client metrics", e);
        }
    }

    private long findMeterCountBySuffix(String suffix) {
        for (java.util.Map.Entry<String, Metric> entry : metrics.entrySet()) {
            if (entry.getKey().endsWith(suffix) && entry.getValue() instanceof Meter) {
                return ((Meter) entry.getValue()).getCount();
            }
        }
        // Fallback: try Counter
        return findCounterBySuffix(suffix);
    }

    private long findCounterBySuffix(String suffix) {
        for (java.util.Map.Entry<String, Metric> entry : metrics.entrySet()) {
            if (entry.getKey().endsWith(suffix) && entry.getValue() instanceof Counter) {
                return ((Counter) entry.getValue()).getCount();
            }
        }
        return 0;
    }

    private double findGaugeBySuffix(String suffix) {
        for (java.util.Map.Entry<String, Metric> entry : metrics.entrySet()) {
            if (entry.getKey().endsWith(suffix) && entry.getValue() instanceof Gauge) {
                Object val = ((Gauge<?>) entry.getValue()).getValue();
                if (val instanceof Number) {
                    return ((Number) val).doubleValue();
                }
            }
        }
        return 0.0;
    }

    private double findHistogramMean(String suffix) {
        for (java.util.Map.Entry<String, Metric> entry : metrics.entrySet()) {
            if (entry.getKey().endsWith(suffix) && entry.getValue() instanceof Histogram) {
                try {
                    return ((Histogram) entry.getValue()).getStatistics().getMean();
                } catch (Exception e) {
                    return 0.0;
                }
            }
        }
        return 0.0;
    }
}

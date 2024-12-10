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

package com.alibaba.fluss.client.metrics;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.client.scanner.log.LogScanner;
import com.alibaba.fluss.client.scanner.snapshot.SnapshotScanner;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.metrics.CharacterFilter;
import com.alibaba.fluss.metrics.Counter;
import com.alibaba.fluss.metrics.DescriptiveStatisticsHistogram;
import com.alibaba.fluss.metrics.Gauge;
import com.alibaba.fluss.metrics.Histogram;
import com.alibaba.fluss.metrics.MeterView;
import com.alibaba.fluss.metrics.Metric;
import com.alibaba.fluss.metrics.MetricNames;
import com.alibaba.fluss.metrics.ThreadSafeSimpleCounter;
import com.alibaba.fluss.metrics.groups.AbstractMetricGroup;
import com.alibaba.fluss.rpc.metrics.ClientMetricGroup;
import com.alibaba.fluss.server.metrics.group.BucketMetricGroup;

import java.util.HashMap;
import java.util.Map;

import static com.alibaba.fluss.metrics.utils.MetricGroupUtils.makeScope;

/** The metric group for scanner, including {@link LogScanner} and {@link SnapshotScanner}. */
@Internal
public class ScannerMetricGroup extends AbstractMetricGroup {
    private static final String name = "scanner";
    private static final int WINDOW_SIZE = 1024;

    private final TablePath tablePath;

    private final Counter fetchRequestCount;
    private final Histogram bytesPerRequest;

    // remote log
    private final Counter remoteFetchBytesCount;
    private final Counter remoteFetchRequestCount;
    private final Counter remoteFetchErrorCount;

    private volatile long fetchLatencyInMs;
    private volatile long timeMsBetweenPoll;
    private volatile double pollIdleRatio;
    private volatile long lastPollMs;
    private volatile long pollStartMs;
    private final Map<Integer, BucketMetricGroup> buckets = new HashMap<>();

    public ScannerMetricGroup(ClientMetricGroup parent, TablePath tablePath) {
        super(parent.getMetricRegistry(), makeScope(parent, name), parent);
        this.tablePath = tablePath;

        fetchRequestCount = new ThreadSafeSimpleCounter();
        meter(MetricNames.SCANNER_FETCH_RATE, new MeterView(fetchRequestCount));

        remoteFetchBytesCount = new ThreadSafeSimpleCounter();
        meter(MetricNames.SCANNER_REMOTE_FETCH_BYTES_RATE, new MeterView(remoteFetchBytesCount));
        remoteFetchRequestCount = new ThreadSafeSimpleCounter();
        meter(MetricNames.SCANNER_REMOTE_FETCH_RATE, new MeterView(remoteFetchRequestCount));
        remoteFetchErrorCount = new ThreadSafeSimpleCounter();
        meter(MetricNames.SCANNER_REMOTE_FETCH_ERROR_RATE, new MeterView(remoteFetchErrorCount));

        bytesPerRequest =
                histogram(
                        MetricNames.SCANNER_BYTES_PER_REQUEST,
                        new DescriptiveStatisticsHistogram(WINDOW_SIZE));

        gauge(MetricNames.SCANNER_TIME_MS_BETWEEN_POLL, () -> timeMsBetweenPoll);
        gauge(MetricNames.SCANNER_LAST_POLL_SECONDS_AGO, this::lastPollSecondsAgo);
        gauge(MetricNames.SCANNER_FETCH_LATENCY_MS, () -> fetchLatencyInMs);
        gauge(MetricNames.SCANNER_POLL_IDLE_RATIO, () -> pollIdleRatio);
    }

    public Counter fetchRequestCount() {
        return fetchRequestCount;
    }

    public Histogram bytesPerRequest() {
        return bytesPerRequest;
    }

    public Counter remoteFetchBytesCount() {
        return remoteFetchBytesCount;
    }

    public Counter remoteFetchRequestCount() {
        return remoteFetchRequestCount;
    }

    public Counter remoteFetchErrorCount() {
        return remoteFetchErrorCount;
    }

    public void recordPollStart(long pollStartMs) {
        this.pollStartMs = pollStartMs;
        this.timeMsBetweenPoll = lastPollMs != 0L ? pollStartMs - lastPollMs : 0L;
        this.lastPollMs = pollStartMs;
    }

    public void recordPollEnd(long pollEndMs) {
        long pollTimeMs = pollEndMs - pollStartMs;
        this.pollIdleRatio = pollTimeMs * 1.0 / (pollTimeMs + timeMsBetweenPoll);
    }

    public void updateFetchLatency(long latencyInMs) {
        fetchLatencyInMs = latencyInMs;
    }

    private long lastPollSecondsAgo() {
        return (System.currentTimeMillis() - lastPollMs) / 1000;
    }

    public void recordBucketLag(int bucketId, long lag) {
        buckets.computeIfAbsent(
                bucketId, (bucket) -> new BucketMetricGroup(registry, bucketId, this));
        Metric metric = buckets.get(bucketId).metrics().get(bucketRecordsLagMetricName(bucketId));
        if (metric == null) {
            SimpleGauge simpleGauge = new SimpleGauge(lag);
            buckets.get(bucketId).gauge(bucketRecordsLagMetricName(bucketId), simpleGauge);
        } else {
            ((SimpleGauge) metric).setValue(lag);
        }
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return name;
    }

    @Override
    protected final void putVariables(Map<String, String> variables) {
        variables.put("database", tablePath.getDatabaseName());
        variables.put("table", tablePath.getTableName());
    }

    private static String bucketRecordsLagMetricName(int bucketId) {
        return bucketId + ".records-lag";
    }

    @Override
    public Map<String, Metric> metrics() {
        Map<String, Metric> allMetric = new HashMap<>(super.metrics());
        buckets.forEach((key, value) -> allMetric.putAll(value.metrics()));
        return allMetric;
    }

    private static class SimpleGauge implements Gauge<Long> {
        private long value;

        public SimpleGauge(long value) {
            this.value = value;
        }

        @Override
        public Long getValue() {
            return value;
        }

        public void setValue(long value) {
            this.value = value;
        }
    }
}

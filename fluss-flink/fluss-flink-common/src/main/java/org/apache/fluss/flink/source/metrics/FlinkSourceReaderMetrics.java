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

package org.apache.fluss.flink.source.metrics;

import org.apache.fluss.flink.source.reader.FlinkSourceReader;
import org.apache.fluss.metrics.Gauge;

import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** A collection class for handling metrics in {@link FlinkSourceReader} of Fluss. */
public class FlinkSourceReaderMetrics {

    public static final long UNINITIALIZED = -1;

    // Source reader metric group
    private final SourceReaderMetricGroup sourceReaderMetricGroup;
    private final Set<Gauge<Long>> recordsLagMetrics = ConcurrentHashMap.newKeySet();

    // For currentFetchEventTimeLag metric
    private volatile long currentFetchEventTimeLag = UNINITIALIZED;

    private boolean pendingRecordsGaugeRegistered;

    public FlinkSourceReaderMetrics(SourceReaderMetricGroup sourceReaderMetricGroup) {
        this.sourceReaderMetricGroup = sourceReaderMetricGroup;
    }

    public void reportRecordEventTime(long lag) {
        if (currentFetchEventTimeLag == UNINITIALIZED) {
            // Lazily register the currentFetchEventTimeLag
            // Set the lag before registering the metric to avoid metric reporter getting
            // the uninitialized value
            currentFetchEventTimeLag = lag;
            sourceReaderMetricGroup.gauge(
                    MetricNames.CURRENT_FETCH_EVENT_TIME_LAG, () -> currentFetchEventTimeLag);
            return;
        }
        currentFetchEventTimeLag = lag;
    }

    /**
     * Adds a scanner's records-lag metric to the metrics tracked by the standard Flink
     * pendingRecords gauge. The Flink gauge itself is registered only once.
     */
    public synchronized void maybeAddRecordsLagMetric(Gauge<Long> recordsLagMetric) {
        recordsLagMetrics.add(recordsLagMetric);
        if (!pendingRecordsGaugeRegistered) {
            sourceReaderMetricGroup.setPendingRecordsGauge(this::getPendingRecords);
            pendingRecordsGaugeRegistered = true;
        }
    }

    /** Removes a scanner's records-lag metric from the tracked metrics. */
    public void removeRecordsLagMetric(Gauge<Long> recordsLagMetric) {
        recordsLagMetrics.remove(recordsLagMetric);
    }

    public SourceReaderMetricGroup getSourceReaderMetricGroup() {
        return sourceReaderMetricGroup;
    }

    private long getPendingRecords() {
        long pendingRecords = 0L;
        for (Gauge<Long> recordsLagMetric : recordsLagMetrics) {
            pendingRecords += recordsLagMetric.getValue();
        }
        return pendingRecords;
    }
}

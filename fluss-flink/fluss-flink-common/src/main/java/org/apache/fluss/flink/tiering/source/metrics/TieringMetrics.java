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

package org.apache.fluss.flink.tiering.source.metrics;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metrics.MetricNames;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;

/**
 * A collection class for handling metrics in Tiering source reader.
 *
 * <p>All metrics are registered under group "fluss.tieringService", which is a child group of
 * {@link org.apache.flink.metrics.groups.OperatorMetricGroup}.
 *
 * <p>The following metrics are available:
 *
 * <ul>
 *   <li>{@code fluss.tieringService.readBytes} - Counter: cumulative bytes read from Fluss records
 *       since the job started (actual Fluss records size).
 *   <li>{@code fluss.tieringService.readBytesPerSecond} - Meter: bytes-per-second rate derived from
 *       the counter using a 60-second sliding window.
 * </ul>
 */
@Internal
public class TieringMetrics {

    // Metric group names
    public static final String FLUSS_METRIC_GROUP = "fluss";
    public static final String TIERING_SERVICE_GROUP = "tieringService";

    private final Counter readBytesCounter;

    public TieringMetrics(SourceReaderMetricGroup sourceReaderMetricGroup) {
        MetricGroup tieringServiceGroup =
                sourceReaderMetricGroup
                        .addGroup(FLUSS_METRIC_GROUP)
                        .addGroup(TIERING_SERVICE_GROUP);

        this.readBytesCounter = tieringServiceGroup.counter(MetricNames.TIERING_SERVICE_READ_BYTES);
        tieringServiceGroup.meter(
                MetricNames.TIERING_SERVICE_READ_BYTES_RATE, new MeterView(readBytesCounter));
    }

    /** Records bytes read from Fluss records. Called per batch or record processing. */
    public void recordBytesRead(long bytes) {
        readBytesCounter.inc(bytes);
    }
}

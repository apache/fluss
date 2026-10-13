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
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;

/**
 * A collection class for handling metrics in {@link
 * org.apache.fluss.flink.tiering.source.enumerator.TieringSourceEnumerator}.
 *
 * <p>All metrics are registered under group "fluss.tieringService", which is a child group of
 * {@link SplitEnumeratorMetricGroup}.
 *
 * <p>The following metrics are available:
 *
 * <ul>
 *   <li>{@code fluss.tieringService.tiering.heartbeat.failure} - Counter: cumulative number of
 *       times the heartbeat request to the Fluss coordinator failed.
 *   <li>{@code fluss.tieringService.tiering.request-table.empty} - Counter: cumulative number of
 *       times the enumerator requested a new tiering table but the coordinator returned none.
 *   <li>{@code fluss.tieringService.tiering.request-table.failure} - Counter: cumulative number of
 *       times the periodic {@code requestTieringTableSplitsViaHeartBeat} call failed.
 * </ul>
 */
@Internal
public class TieringEnumeratorMetrics {

    public static final String FLUSS_METRIC_GROUP = "fluss";
    public static final String TIERING_SERVICE_GROUP = "tieringService";

    private final Counter heartbeatFailureCounter;
    private final Counter requestTableEmptyCounter;
    private final Counter requestTableFailureCounter;

    public TieringEnumeratorMetrics(SplitEnumeratorMetricGroup enumeratorMetricGroup) {
        MetricGroup tieringServiceGroup =
                enumeratorMetricGroup.addGroup(FLUSS_METRIC_GROUP).addGroup(TIERING_SERVICE_GROUP);

        this.heartbeatFailureCounter =
                tieringServiceGroup.counter(MetricNames.TIERING_HEARTBEAT_FAILURE);
        this.requestTableEmptyCounter =
                tieringServiceGroup.counter(MetricNames.TIERING_REQUEST_TABLE_EMPTY);
        this.requestTableFailureCounter =
                tieringServiceGroup.counter(MetricNames.TIERING_REQUEST_TABLE_FAILURE);
    }

    /** Increments when a heartbeat request to the coordinator fails. */
    public void incHeartbeatFailure() {
        heartbeatFailureCounter.inc();
    }

    /** Increments when the coordinator has no tiering table to assign. */
    public void incRequestTableEmpty() {
        requestTableEmptyCounter.inc();
    }

    /** Increments when the periodic request for tiering table splits fails. */
    public void incRequestTableFailure() {
        requestTableFailureCounter.inc();
    }

    public long getHeartbeatFailureCount() {
        return heartbeatFailureCounter.getCount();
    }

    public long getRequestTableEmptyCount() {
        return requestTableEmptyCounter.getCount();
    }

    public long getRequestTableFailureCount() {
        return requestTableFailureCounter.getCount();
    }
}

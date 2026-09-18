/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.coordinator.rebalance;

import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.ThreadSafeSimpleCounter;
import org.apache.fluss.metrics.groups.MetricGroup;

import javax.annotation.Nullable;

import java.util.function.ToLongFunction;

import static org.apache.fluss.cluster.rebalance.RebalanceStatus.COMPLETED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.FAILED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.TIMEOUT;

/** Rebalance metrics registered for the lifetime of a coordinator server. */
public class RebalanceMetrics {

    private final Counter rebalancesCompleted = new ThreadSafeSimpleCounter();
    private final Counter rebalancesFailed = new ThreadSafeSimpleCounter();
    private final Counter rebalancesCanceled = new ThreadSafeSimpleCounter();

    private volatile @Nullable RebalanceManager current;

    /** Registers the rebalance metrics on the coordinator metric group. */
    public RebalanceMetrics(MetricGroup metricGroup) {
        metricGroup.counter(MetricNames.REBALANCES_COMPLETED_TOTAL, rebalancesCompleted);
        metricGroup.counter(MetricNames.REBALANCES_FAILED_TOTAL, rebalancesFailed);
        metricGroup.counter(MetricNames.REBALANCES_CANCELED_TOTAL, rebalancesCanceled);
        metricGroup.gauge(
                MetricNames.REBALANCE_IN_PROGRESS,
                () -> readCurrent(RebalanceManager::rebalanceInProgress));
        metricGroup.gauge(
                MetricNames.REBALANCE_BUCKETS_PENDING,
                () -> readCurrent(RebalanceManager::pendingBucketCount));
        metricGroup.gauge(
                MetricNames.REBALANCE_BUCKETS_COMPLETED,
                () -> readCurrent(manager -> manager.finishedBucketCount(COMPLETED)));
        metricGroup.gauge(
                MetricNames.REBALANCE_BUCKETS_FAILED,
                () -> readCurrent(manager -> manager.finishedBucketCount(FAILED)));
        metricGroup.gauge(
                MetricNames.REBALANCE_BUCKETS_TIMED_OUT,
                () -> readCurrent(manager -> manager.finishedBucketCount(TIMEOUT)));
        metricGroup.gauge(
                MetricNames.REBALANCE_DURATION_MS,
                () -> readCurrent(RebalanceManager::rebalanceDurationMs));
        metricGroup.gauge(
                MetricNames.INFLIGHT_BUCKET_DURATION_MS,
                () -> readCurrent(RebalanceManager::inflightBucketDurationMs));
    }

    void bind(RebalanceManager manager) {
        current = manager;
    }

    void unbind(RebalanceManager manager) {
        if (current == manager) {
            current = null;
        }
    }

    void incRebalancesCompleted() {
        rebalancesCompleted.inc();
    }

    void incRebalancesFailed() {
        rebalancesFailed.inc();
    }

    void incRebalancesCanceled() {
        rebalancesCanceled.inc();
    }

    private long readCurrent(ToLongFunction<RebalanceManager> read) {
        RebalanceManager manager = current;
        return manager == null ? 0L : read.applyAsLong(manager);
    }
}

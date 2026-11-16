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

package org.apache.fluss.flink.tiering.source;

import org.apache.fluss.flink.tiering.source.metrics.TieringMetrics;
import org.apache.fluss.metrics.MetricNames;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TieringMetrics}. */
class TieringMetricsTest {

    @Test
    void testRecordBytesRead() {
        MetricListener metricListener = new MetricListener();
        TieringMetrics tieringMetrics =
                new TieringMetrics(
                        InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup()));

        tieringMetrics.recordBytesRead(100);
        tieringMetrics.recordBytesRead(200);

        Optional<Counter> counter =
                metricListener.getCounter(
                        TieringMetrics.FLUSS_METRIC_GROUP,
                        TieringMetrics.TIERING_SERVICE_GROUP,
                        MetricNames.TIERING_SERVICE_READ_BYTES);
        assertThat(counter).isPresent();
        assertThat(counter.get().getCount()).isEqualTo(300);
    }

    @Test
    void testPendingRecordsSumsSplitReaderAndEnumeratorBacklog() {
        MetricListener metricListener = new MetricListener();
        TieringMetrics tieringMetrics =
                new TieringMetrics(
                        InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup()));

        // registered up front, so an idle reader reports 0 rather than no metric
        Optional<Gauge<Long>> pendingRecords =
                metricListener.getGauge(
                        org.apache.flink.runtime.metrics.MetricNames.PENDING_RECORDS);
        assertThat(pendingRecords).isPresent();
        assertThat(pendingRecords.get().getValue()).isEqualTo(0L);

        tieringMetrics.setSplitReaderBacklog(70);
        tieringMetrics.setEnumeratorBacklog(30);
        assertThat(pendingRecords.get().getValue()).isEqualTo(100L);

        tieringMetrics.setSplitReaderBacklog(0);
        tieringMetrics.setEnumeratorBacklog(-5);
        assertThat(pendingRecords.get().getValue()).isEqualTo(0L);
    }

    @Test
    void testRecordsConsumedAreCountedOnlyWhenFlushed() {
        InternalSourceReaderMetricGroup metricGroup =
                InternalSourceReaderMetricGroup.wrap(
                        UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup());
        TieringMetrics tieringMetrics = new TieringMetrics(metricGroup);
        Counter numRecordsIn = metricGroup.getIOMetricGroup().getNumRecordsInCounter();

        // the split fetcher thread only accumulates
        tieringMetrics.recordsConsumed(40);
        tieringMetrics.recordsConsumed(2);
        tieringMetrics.recordsConsumed(0);
        assertThat(numRecordsIn.getCount()).isEqualTo(0L);

        // the task thread moves them into numRecordsIn, once
        tieringMetrics.flushRecordsConsumed();
        assertThat(numRecordsIn.getCount()).isEqualTo(42L);
        tieringMetrics.flushRecordsConsumed();
        assertThat(numRecordsIn.getCount()).isEqualTo(42L);

        tieringMetrics.recordsConsumed(8);
        tieringMetrics.flushRecordsConsumed();
        assertThat(numRecordsIn.getCount()).isEqualTo(50L);
    }

    @Test
    void testFetcherBusyTimeCarriesSubMillisecondFetches() {
        MetricListener metricListener = new MetricListener();
        TieringMetrics tieringMetrics =
                new TieringMetrics(
                        InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup()));
        Counter busyTime =
                metricListener
                        .getCounter(
                                TieringMetrics.FLUSS_METRIC_GROUP,
                                TieringMetrics.TIERING_SERVICE_GROUP,
                                MetricNames.TIERING_SERVICE_FETCHER_BUSY_TIME)
                        .get();
        assertThat(
                        metricListener.getMeter(
                                TieringMetrics.FLUSS_METRIC_GROUP,
                                TieringMetrics.TIERING_SERVICE_GROUP,
                                MetricNames.TIERING_SERVICE_FETCHER_BUSY_TIME_RATE))
                .isPresent();

        // three fetches of 0.4 ms add up to 1.2 ms, of which 1 ms is counted
        tieringMetrics.recordFetcherBusyTime(400_000L);
        tieringMetrics.recordFetcherBusyTime(400_000L);
        assertThat(busyTime.getCount()).isEqualTo(0L);
        tieringMetrics.recordFetcherBusyTime(400_000L);
        assertThat(busyTime.getCount()).isEqualTo(1L);

        // the 0.2 ms carried over completes the next 2.8 ms to 3 ms
        tieringMetrics.recordFetcherBusyTime(2_800_000L);
        assertThat(busyTime.getCount()).isEqualTo(4L);

        // durations that aren't positive are ignored
        tieringMetrics.recordFetcherBusyTime(-5_000_000L);
        tieringMetrics.recordFetcherBusyTime(0L);
        assertThat(busyTime.getCount()).isEqualTo(4L);
    }
}

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

import org.apache.fluss.metadata.TableBucket;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics.BUCKET_GROUP;
import static org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics.FLUSS_METRIC_GROUP;
import static org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics.PARTITION_GROUP;
import static org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics.READER_METRIC_GROUP;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics}. */
class FlinkSourceReaderMetricsTest {

    @Test
    void testCurrentOffsetTracking() {
        MetricListener metricListener = new MetricListener();

        final TableBucket t0 = new TableBucket(0, 0L, 1);
        final TableBucket t1 = new TableBucket(0, 0L, 2);
        final TableBucket t2 = new TableBucket(0, null, 1);
        final TableBucket t3 = new TableBucket(0, null, 2);

        final FlinkSourceReaderMetrics flinkSourceReaderMetrics =
                new FlinkSourceReaderMetrics(
                        InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup()));

        flinkSourceReaderMetrics.registerTableBucket(t0);
        flinkSourceReaderMetrics.registerTableBucket(t1);
        flinkSourceReaderMetrics.registerTableBucket(t2);
        flinkSourceReaderMetrics.registerTableBucket(t3);

        flinkSourceReaderMetrics.recordCurrentOffset(t0, 15213L);
        flinkSourceReaderMetrics.recordCurrentOffset(t1, 18213L);
        flinkSourceReaderMetrics.recordCurrentOffset(t2, 18613L);
        flinkSourceReaderMetrics.recordCurrentOffset(t3, 15513L);

        assertCurrentOffset(t0, 15213L, metricListener);
        assertCurrentOffset(t1, 18213L, metricListener);
        assertCurrentOffset(t2, 18613L, metricListener);
        assertCurrentOffset(t3, 15513L, metricListener);
    }

    @Test
    void testRecordsLagTracking() {
        MetricListener metricListener = new MetricListener();
        final TableBucket t0 = new TableBucket(0, 0L, 1);
        final TableBucket t1 = new TableBucket(0, null, 2);

        final FlinkSourceReaderMetrics flinkSourceReaderMetrics =
                new FlinkSourceReaderMetrics(
                        InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup()));

        flinkSourceReaderMetrics.registerTableBucket(t0, "dt=2026-05-19", 100L, null, true);
        flinkSourceReaderMetrics.recordLogEndOffset(t0, 130L);
        flinkSourceReaderMetrics.recordLogEndOffset(t0, 135L);
        flinkSourceReaderMetrics.recordLogEndOffset(t0, 120L);

        flinkSourceReaderMetrics.registerTableBucket(t1, null, 5L, 8L, false);
        flinkSourceReaderMetrics.recordLogEndOffset(t1, 99L);

        assertRecordsLag(t0, 35L, metricListener);
        assertLogEndOffset(t0, 135L, metricListener);
        assertRecordsLag(t1, 3L, metricListener);
        assertLogEndOffset(t1, 8L, metricListener);
        assertSourceGauge(
                FlinkSourceReaderMetrics.RECORDS_LAG_MAX_METRIC_GAUGE, 35L, metricListener);
        assertSourceGauge(
                FlinkSourceReaderMetrics.RECORDS_LAG_SUM_METRIC_GAUGE, 38L, metricListener);
        assertLaggingBuckets(2, metricListener);

        flinkSourceReaderMetrics.recordCurrentOffset(t0, 109L);
        flinkSourceReaderMetrics.recordCurrentOffset(t1, 7L);

        assertRecordsLag(t0, 25L, metricListener);
        assertRecordsLag(t1, 0L, metricListener);
        assertSourceGauge(
                FlinkSourceReaderMetrics.RECORDS_LAG_MAX_METRIC_GAUGE, 25L, metricListener);
        assertSourceGauge(
                FlinkSourceReaderMetrics.RECORDS_LAG_SUM_METRIC_GAUGE, 25L, metricListener);
        assertLaggingBuckets(1, metricListener);

        flinkSourceReaderMetrics.unregisterTableBucket(t0);

        assertSourceGauge(
                FlinkSourceReaderMetrics.RECORDS_LAG_MAX_METRIC_GAUGE, 0L, metricListener);
        assertSourceGauge(
                FlinkSourceReaderMetrics.RECORDS_LAG_SUM_METRIC_GAUGE, 0L, metricListener);
        assertLaggingBuckets(0, metricListener);
    }

    // ----------- Assertions --------------

    private void assertCurrentOffset(
            TableBucket tb, long expectedOffset, MetricListener metricListener) {
        assertBucketGauge(
                tb,
                FlinkSourceReaderMetrics.CURRENT_OFFSET_METRIC_GAUGE,
                expectedOffset,
                metricListener);
    }

    private void assertLogEndOffset(
            TableBucket tb, long expectedOffset, MetricListener metricListener) {
        assertBucketGauge(
                tb,
                FlinkSourceReaderMetrics.LOG_END_OFFSET_METRIC_GAUGE,
                expectedOffset,
                metricListener);
    }

    private void assertRecordsLag(TableBucket tb, long expectedLag, MetricListener metricListener) {
        assertBucketGauge(
                tb, FlinkSourceReaderMetrics.RECORDS_LAG_METRIC_GAUGE, expectedLag, metricListener);
    }

    private void assertBucketGauge(
            TableBucket tb, String metricName, long expectedValue, MetricListener metricListener) {
        final Optional<Gauge<Long>> currentOffsetGauge;
        if (tb.getPartitionId() == null) {
            currentOffsetGauge =
                    metricListener.getGauge(
                            FLUSS_METRIC_GROUP,
                            READER_METRIC_GROUP,
                            BUCKET_GROUP,
                            String.valueOf(tb.getBucket()),
                            metricName);
        } else {
            currentOffsetGauge =
                    metricListener.getGauge(
                            FLUSS_METRIC_GROUP,
                            READER_METRIC_GROUP,
                            PARTITION_GROUP,
                            String.valueOf(tb.getPartitionId()),
                            BUCKET_GROUP,
                            String.valueOf(tb.getBucket()),
                            metricName);
        }

        assertThat(currentOffsetGauge).isPresent();
        assertThat((long) currentOffsetGauge.get().getValue()).isEqualTo(expectedValue);
    }

    private void assertSourceGauge(
            String metricName, long expectedValue, MetricListener metricListener) {
        Optional<Gauge<Long>> gauge =
                metricListener.getGauge(FLUSS_METRIC_GROUP, READER_METRIC_GROUP, metricName);
        assertThat(gauge).isPresent();
        assertThat((long) gauge.get().getValue()).isEqualTo(expectedValue);
    }

    private void assertLaggingBuckets(int expectedValue, MetricListener metricListener) {
        Optional<Gauge<Integer>> gauge =
                metricListener.getGauge(
                        FLUSS_METRIC_GROUP,
                        READER_METRIC_GROUP,
                        FlinkSourceReaderMetrics.LAGGING_BUCKETS_METRIC_GAUGE);
        assertThat(gauge).isPresent();
        assertThat((int) gauge.get().getValue()).isEqualTo(expectedValue);
    }
}

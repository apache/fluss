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
    void testRecordsConsumedAreCountedOnlyWhenFlushed() {
        InternalSourceReaderMetricGroup metricGroup =
                InternalSourceReaderMetricGroup.wrap(
                        UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup());
        TieringMetrics tieringMetrics = new TieringMetrics(metricGroup);
        Counter numRecordsIn = metricGroup.getIOMetricGroup().getNumRecordsInCounter();

        tieringMetrics.recordsConsumed(40);
        tieringMetrics.recordsConsumed(2);
        tieringMetrics.recordsConsumed(0);
        assertThat(numRecordsIn.getCount()).isEqualTo(0L);

        tieringMetrics.flushRecordsConsumed();
        assertThat(numRecordsIn.getCount()).isEqualTo(42L);
        tieringMetrics.flushRecordsConsumed();
        assertThat(numRecordsIn.getCount()).isEqualTo(42L);

        tieringMetrics.recordsConsumed(8);
        tieringMetrics.flushRecordsConsumed();
        assertThat(numRecordsIn.getCount()).isEqualTo(50L);
    }
}

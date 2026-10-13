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

package org.apache.fluss.server.metrics.group;

import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.MeterView;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.ThreadSafeSimpleCounter;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/** Test for {@link TableMetricGroup}. */
class TableMetricGroupTest {

    @Test
    void testRemoteKvCopyRateMetric() {
        MetricRegistry registry = mock(MetricRegistry.class);
        TableMetricGroup group =
                new TableMetricGroup(
                        registry,
                        TablePath.of("db", "pk_table"),
                        true,
                        TestingMetricGroups.TABLET_SERVER_METRICS);
        try {
            Counter counter = group.remoteKvCopyBytes();
            assertThat(counter).isInstanceOf(ThreadSafeSimpleCounter.class);
            ArgumentCaptor<AbstractMetricGroup> metricGroup =
                    ArgumentCaptor.forClass(AbstractMetricGroup.class);
            ArgumentCaptor<MeterView> meter = ArgumentCaptor.forClass(MeterView.class);
            verify(registry)
                    .register(
                            meter.capture(),
                            eq(MetricNames.REMOTE_KV_COPY_BYTES_RATE),
                            metricGroup.capture());
            verify(registry, never()).register(eq(counter), any(), any());
            assertThat(metricGroup.getValue().getMetrics())
                    .containsEntry(MetricNames.REMOTE_KV_COPY_BYTES_RATE, meter.getValue())
                    .doesNotContainKey("remoteKvCopyBytes");
            assertThat(metricGroup.getValue().getAllVariables())
                    .containsEntry("database", "db")
                    .containsEntry("table", "pk_table");
            assertThat(metricGroup.getValue().getScopeComponents()).endsWith("kv");
            counter.inc(120L);
            assertThat(meter.getValue().getCount()).isEqualTo(120L);
            meter.getValue().update();
            assertThat(meter.getValue().getRate()).isEqualTo(2.0);
            group.close();
            verify(registry, never()).unregister(eq(counter), any(), any());
            verify(registry)
                    .unregister(
                            meter.getValue(),
                            MetricNames.REMOTE_KV_COPY_BYTES_RATE,
                            metricGroup.getValue());
        } finally {
            group.close();
        }
    }

    @Test
    void testLogTableDoesNotExposeRemoteKvCopyRateMetric() {
        MetricRegistry registry = mock(MetricRegistry.class);
        TableMetricGroup group =
                new TableMetricGroup(
                        registry,
                        TablePath.of("db", "log_table"),
                        false,
                        TestingMetricGroups.TABLET_SERVER_METRICS);
        try {
            group.remoteKvCopyBytes().inc(120L);
            assertThat(group.remoteKvCopyBytes().getCount()).isZero();
            verify(registry, never()).register(any(), eq("remoteKvCopyBytes"), any());
            verify(registry, never())
                    .register(any(), eq(MetricNames.REMOTE_KV_COPY_BYTES_RATE), any());
        } finally {
            group.close();
        }
    }

    @Test
    void testKvBackpressureMetricsExposeOnlyCoreSignals() {
        TableMetricGroup metricGroup =
                new TableMetricGroup(
                        NOPMetricRegistry.INSTANCE,
                        TablePath.of("db", "pk_table"),
                        true,
                        TestingMetricGroups.TABLET_SERVER_METRICS);

        assertThat(metricGroup.getMetrics())
                .containsKeys(
                        MetricNames.KV_BACKPRESSURE_MAX_PRESSURE,
                        MetricNames.KV_BACKPRESSURE_REJECTIONS_TOTAL)
                .doesNotContainKey("kvBackpressureAffectedBuckets");
    }
}

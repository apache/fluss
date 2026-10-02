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

package org.apache.fluss.client.metrics;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.CharacterFilter;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistryImpl;
import org.apache.fluss.metrics.reporter.MetricReporter;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link WriterMetricGroup} and {@link MultiTableWriterMetricGroup}. */
class WriterMetricGroupTest {

    private static final String WRITER_INSTANCE_ID = "writer_instance_id";

    @Test
    void testSingleTableWriters() throws Exception {
        try (MetricTestContext context = new MetricTestContext()) {
            WriterMetricGroup writer = new WriterMetricGroup(context.clientMetricGroup);

            assertThat(logicalScope(writer)).isEqualTo("client.writer");
            assertThat(writer.getAllVariables()).containsOnlyKeys("client_id");
        }
    }

    @Test
    void testMultiTableWriter() throws Exception {
        try (MetricTestContext context = new MetricTestContext()) {
            WriterMetricGroup sharedWriter = new WriterMetricGroup(context.clientMetricGroup);
            WriterMetricGroup multiWriter1 =
                    new MultiTableWriterMetricGroup(context.clientMetricGroup);
            WriterMetricGroup multiWriter2 =
                    new MultiTableWriterMetricGroup(context.clientMetricGroup);
            String instanceId1 = multiWriter1.getAllVariables().get(WRITER_INSTANCE_ID);
            String instanceId2 = multiWriter2.getAllVariables().get(WRITER_INSTANCE_ID);

            assertThat(logicalScope(multiWriter1)).isEqualTo("client.multi_table_writer");
            assertThat(sharedWriter.getAllVariables()).doesNotContainKey(WRITER_INSTANCE_ID);
            assertThat(instanceId1).isNotNull();
            assertThat(instanceId2).isNotNull().isNotEqualTo(instanceId1);
            assertThat(context.reporter.added).doesNotHaveDuplicates();

            multiWriter1.close();

            assertThat(context.reporter.removed).isNotEmpty();
            assertThat(context.reporter.removed)
                    .allSatisfy(
                            identity ->
                                    assertThat(identity)
                                            .contains(WRITER_INSTANCE_ID + "=" + instanceId1));
        }
    }

    private static String logicalScope(MetricGroup group) {
        return group.getLogicalScope(CharacterFilter.NO_OP_FILTER, '.');
    }

    private static final class MetricTestContext implements AutoCloseable {
        final RecordingReporter reporter = new RecordingReporter();
        final MetricRegistryImpl registry =
                new MetricRegistryImpl(Collections.singletonList(reporter));
        final ClientMetricGroup clientMetricGroup = new ClientMetricGroup(registry, "test-client");

        @Override
        public void close() throws Exception {
            registry.closeAsync().get();
        }
    }

    /** Records reporter-facing metric identities: scoped metric name plus variables. */
    private static final class RecordingReporter implements MetricReporter {

        final List<String> added = new ArrayList<>();
        final List<String> removed = new ArrayList<>();

        @Override
        public void open(Configuration config) {}

        @Override
        public void close() {}

        @Override
        public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
            added.add(identityOf(metricName, group));
        }

        @Override
        public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
            removed.add(identityOf(metricName, group));
        }

        private static String identityOf(String metricName, MetricGroup group) {
            return logicalScope(group) + '.' + metricName + new TreeMap<>(group.getAllVariables());
        }
    }
}

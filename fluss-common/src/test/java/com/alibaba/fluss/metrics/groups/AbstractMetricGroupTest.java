/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.metrics.groups;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metrics.CharacterFilter;
import com.alibaba.fluss.metrics.registry.MetricRegistry;
import com.alibaba.fluss.metrics.registry.MetricRegistryImpl;
import com.alibaba.fluss.metrics.util.CollectingMetricsReporter;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link AbstractMetricGroup}. */
class AbstractMetricGroupTest {

    /**
     * Verifies that no {@link NullPointerException} is thrown when {@link
     * AbstractMetricGroup#getAllVariables()} is called and the parent is null.
     */
    @Test
    void testGetAllVariables() throws Exception {
        MetricRegistry registry = MetricRegistry.create(new Configuration(), null);

        AbstractMetricGroup group =
                new AbstractMetricGroup(registry, new String[0], null) {

                    @Override
                    protected String getGroupName(CharacterFilter filter) {
                        return "test";
                    }
                };
        assertThat(group.getAllVariables()).isEmpty();

        group =
                new GenericMetricGroup(registry, null, "test") {
                    @Override
                    protected void putVariables(Map<String, String> variables) {
                        variables.put("k1", "v1");
                        variables.put("k2", "v2");
                    }
                };

        Map<String, String> expected = new HashMap<>();
        expected.put("k1", "v1");
        expected.put("k2", "v2");
        assertThat(group.getAllVariables()).containsAllEntriesOf(expected);

        group =
                new GenericMetricGroup(registry, group, "test2") {
                    @Override
                    protected void putVariables(Map<String, String> variables) {
                        variables.put("k3", "v3");
                        variables.put("k4", "v4");
                    }
                };
        expected.put("k3", "v3");
        expected.put("k4", "v4");
        assertThat(group.getAllVariables()).containsAllEntriesOf(expected);

        registry.closeAsync().get();
    }

    // ========================================================================
    // Scope Caching
    // ========================================================================

    private static final CharacterFilter FILTER_B = input -> input.replace("B", "X");
    private static final CharacterFilter FILTER_C = input -> input.replace("C", "X");

    @Test
    void testLogicalScopeCachingForMultipleReporters() throws Exception {
        String counterName = "1";
        CollectingMetricsReporter reporter1 = new CollectingMetricsReporter("reporter1", FILTER_B);
        CollectingMetricsReporter reporter2 = new CollectingMetricsReporter("reporter2", FILTER_C);
        MetricRegistryImpl testRegistry =
                new MetricRegistryImpl(Arrays.asList(reporter1, reporter2));
        try {
            MetricGroup testGroup =
                    new GenericMetricGroup(testRegistry, null, "test").addGroup("B").addGroup("C");
            testGroup.counter(counterName);
            assertThat(testRegistry.getNumberReporters())
                    .withFailMessage("Reporters were not properly instantiated")
                    .isEqualTo(2);
            assertThat(reporter1.findAdded(counterName).group.getLogicalScope(reporter1, '-'))
                    .isEqualTo("test-X-C");
            assertThat(reporter2.findAdded(counterName).group.getLogicalScope(reporter2, ','))
                    .isEqualTo("test,B,X");
        } finally {
            testRegistry.closeAsync().get();
        }
    }
}

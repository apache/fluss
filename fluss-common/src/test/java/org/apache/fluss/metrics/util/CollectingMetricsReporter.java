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

package org.apache.fluss.metrics.util;

import org.apache.fluss.metrics.CharacterFilter;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.groups.MetricGroup;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Reporter that collects added and removed metrics so that it can be verified in the test (e.g.
 * that the configured delimiter is applied correctly when generating the metric identifier).
 */
public class CollectingMetricsReporter extends TestReporter {

    @Nullable private final CharacterFilter characterFilter;
    private final List<MetricGroupAndName> addedMetrics = new ArrayList<>();
    private final List<MetricGroupAndName> removedMetrics = new ArrayList<>();

    public CollectingMetricsReporter(
            String reporterName, @Nullable CharacterFilter characterFilter) {
        super(reporterName);
        this.characterFilter = characterFilter;
    }

    @Override
    public String filterCharacters(String input) {
        return characterFilter == null ? input : characterFilter.filterCharacters(input);
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        addedMetrics.add(new MetricGroupAndName(metricName, group));
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        removedMetrics.add(new MetricGroupAndName(metricName, group));
    }

    public MetricGroupAndName findAdded(String name) {
        return getMetricGroupAndName(name, addedMetrics);
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    private MetricGroupAndName getMetricGroupAndName(
            String name, List<MetricGroupAndName> removedMetrics) {
        return removedMetrics.stream()
                .filter(groupAndName -> groupAndName.name.equals(name))
                .findAny()
                .get();
    }

    /** Metric group and name. */
    public static class MetricGroupAndName {
        public final String name;
        public final MetricGroup group;

        MetricGroupAndName(String name, MetricGroup group) {
            this.name = name;
            this.group = group;
        }
    }
}

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

package com.alibaba.fluss.metrics.util;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metrics.CharacterFilter;
import com.alibaba.fluss.metrics.Metric;
import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.metrics.reporter.MetricReporter;
import com.alibaba.fluss.metrics.reporter.MetricReporterPlugin;

/** No-op reporter implementation. */
public class TestReporter implements MetricReporter, CharacterFilter, MetricReporterPlugin {

    private Configuration config;
    private final String reporterName;

    public TestReporter(String reporterName) {
        this.reporterName = reporterName;
    }

    @Override
    public void open(Configuration config) {
        this.config = config;
    }

    @Override
    public void close() {}

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {}

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {}

    @Override
    public String filterCharacters(String input) {
        return input;
    }

    @Override
    public MetricReporter createMetricReporter(Configuration configuration) {
        return this;
    }

    @Override
    public String identifier() {
        return reporterName;
    }

    public Configuration getConfig() {
        return config;
    }
}

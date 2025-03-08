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

package com.alibaba.fluss.metrics.prometheus;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metrics.reporter.MetricReporter;
import com.alibaba.fluss.metrics.reporter.MetricReporterPlugin;
import com.alibaba.fluss.utils.NetUtils;

import java.util.Iterator;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** {@link MetricReporterPlugin} for {@link PrometheusReporter}. */
public class PrometheusReporterPlugin implements MetricReporterPlugin {

    private static final String PLUGIN_NAME = "prometheus";

    @Override
    public MetricReporter createMetricReporter(Configuration configuration) {
        String portsConfig =
                configuration.getString(ConfigOptions.METRICS_REPORTER_PROMETHEUS_PORT);
        Iterator<Integer> ports = NetUtils.getPortRangeFromString(portsConfig);
        return new PrometheusReporter(ports);
    }

    @Override
    public String identifier() {
        return PLUGIN_NAME;
    }
}

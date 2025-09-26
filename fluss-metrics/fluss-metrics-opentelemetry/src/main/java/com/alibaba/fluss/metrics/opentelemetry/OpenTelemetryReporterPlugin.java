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

package com.alibaba.fluss.metrics.opentelemetry;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.IllegalConfigurationException;
import com.alibaba.fluss.metrics.reporter.MetricReporter;
import com.alibaba.fluss.metrics.reporter.MetricReporterPlugin;

import java.time.Duration;

/** {@link MetricReporterPlugin} for {@link OpenTelemetryReporter}. */
public class OpenTelemetryReporterPlugin implements MetricReporterPlugin {

    private static final String PLUGIN_NAME = "opentelemetry";

    @Override
    public MetricReporter createMetricReporter(Configuration configuration) {
        String endpoint =
                configuration.getString(ConfigOptions.METRICS_REPORTER_OPENTELEMETRY_ENDPOINT);
        if (endpoint == null || endpoint.trim().isEmpty()) {
            throw new IllegalConfigurationException(
                    ConfigOptions.METRICS_REPORTER_OPENTELEMETRY_ENDPOINT.key() + " must be set.");
        }
        ConfigOptions.OpenTelemetryExporter exporterType =
                configuration.get(ConfigOptions.METRICS_REPORTER_OPENTELEMETRY_EXPORTER);
        if (exporterType == null) {
            throw new IllegalConfigurationException(
                    ConfigOptions.METRICS_REPORTER_OPENTELEMETRY_EXPORTER.key() + " must be set.");
        }
        Duration interval =
                configuration.get(ConfigOptions.METRICS_REPORTER_OPENTELEMETRY_EXPORT_INTERVAL);
        Duration timeout =
                configuration.get(ConfigOptions.METRICS_REPORTER_OPENTELEMETRY_EXPORT_TIMEOUT);
        String serviceName =
                configuration.get(ConfigOptions.METRICS_REPORTER_OPENTELEMETRY_SERVICE_NAME);
        String serviceVersion =
                configuration.get(ConfigOptions.METRICS_REPORTER_OPENTELEMETRY_SERVICE_VERSION);
        return new OpenTelemetryReporter(
                endpoint, exporterType, interval, timeout, serviceName, serviceVersion);
    }

    @Override
    public String identifier() {
        return PLUGIN_NAME;
    }
}

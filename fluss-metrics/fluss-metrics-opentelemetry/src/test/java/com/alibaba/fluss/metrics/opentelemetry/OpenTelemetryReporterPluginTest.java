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

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.IllegalConfigurationException;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link OpenTelemetryReporterPlugin}. */
public class OpenTelemetryReporterPluginTest {

    private final OpenTelemetryReporterPlugin openTelemetryReporterPlugin =
            new OpenTelemetryReporterPlugin();

    @ParameterizedTest
    @EnumSource(ConfigOptions.OpenTelemetryExporter.class)
    void testValidConfiguration(ConfigOptions.OpenTelemetryExporter exporterType) {
        // mandatory options
        Configuration configuration = new Configuration();
        configuration.setString(
                ConfigOptions.METRICS_REPORTER_OPENTELEMETRY_ENDPOINT,
                "http://opentelemetry-metric-collector:4317");
        configuration.set(ConfigOptions.METRICS_REPORTER_OPENTELEMETRY_EXPORTER, exporterType);
        assertThatCode(() -> openTelemetryReporterPlugin.createMetricReporter(configuration))
                .doesNotThrowAnyException();

        // optional options
        configuration.set(
                ConfigOptions.METRICS_REPORTER_OPENTELEMETRY_EXPORT_INTERVAL,
                Duration.ofSeconds(5));
        configuration.set(
                ConfigOptions.METRICS_REPORTER_OPENTELEMETRY_EXPORT_TIMEOUT, Duration.ofSeconds(5));
        assertThatCode(() -> openTelemetryReporterPlugin.createMetricReporter(configuration))
                .doesNotThrowAnyException();

        configuration.set(ConfigOptions.METRICS_REPORTER_OPENTELEMETRY_SERVICE_NAME, "fluss");
        configuration.set(ConfigOptions.METRICS_REPORTER_OPENTELEMETRY_SERVICE_NAME, "v42");
        assertThatCode(() -> openTelemetryReporterPlugin.createMetricReporter(configuration))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest
    @EnumSource(ConfigOptions.OpenTelemetryExporter.class)
    void testInvalidConfiguration(ConfigOptions.OpenTelemetryExporter exporterType) {
        Configuration configuration = new Configuration();
        // invalid endpoint and no exporter type
        assertThatThrownBy(() -> openTelemetryReporterPlugin.createMetricReporter(configuration))
                .isInstanceOf(IllegalConfigurationException.class);

        configuration.setString(ConfigOptions.METRICS_REPORTER_OPENTELEMETRY_ENDPOINT, "");
        assertThatThrownBy(() -> openTelemetryReporterPlugin.createMetricReporter(configuration))
                .isInstanceOf(IllegalConfigurationException.class);

        configuration.setString(ConfigOptions.METRICS_REPORTER_OPENTELEMETRY_ENDPOINT, "   ");
        assertThatThrownBy(() -> openTelemetryReporterPlugin.createMetricReporter(configuration))
                .isInstanceOf(IllegalConfigurationException.class);

        // endpoint is still invalid
        configuration.set(ConfigOptions.METRICS_REPORTER_OPENTELEMETRY_EXPORTER, exporterType);
        assertThatThrownBy(() -> openTelemetryReporterPlugin.createMetricReporter(configuration))
                .isInstanceOf(IllegalConfigurationException.class);
    }
}

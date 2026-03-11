/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.metrics.influxdb;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.util.MetricReporterTestUtils;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link InfluxdbReporterPlugin}. */
class InfluxdbReporterPluginTest {

    @Test
    void testCreateMetricReporterWithValidConfig() {
        Configuration config = new Configuration();
        config.setString(ConfigOptions.METRICS_REPORTER_INFLUXDB_HOST_URL, "http://localhost:8086");
        config.setString(ConfigOptions.METRICS_REPORTER_INFLUXDB_ORG, "test-org");
        config.setString(ConfigOptions.METRICS_REPORTER_INFLUXDB_BUCKET, "test-bucket");
        config.setString(ConfigOptions.METRICS_REPORTER_INFLUXDB_TOKEN, "test-token");
        config.set(ConfigOptions.METRICS_REPORTER_INFLUXDB_PUSH_INTERVAL, Duration.ofSeconds(15));

        InfluxdbReporterPlugin plugin = new InfluxdbReporterPlugin();
        InfluxdbReporter reporter = (InfluxdbReporter) plugin.createMetricReporter(config);

        try {
            assertThat(reporter).isNotNull();
            assertThat(reporter.scheduleInterval()).isEqualTo(Duration.ofSeconds(15));
        } finally {
            reporter.close();
        }
    }

    @Test
    void testMetricReporterSetupViaSPI() {
        MetricReporterTestUtils.testMetricReporterSetupViaSPI(InfluxdbReporterPlugin.class);
    }
}

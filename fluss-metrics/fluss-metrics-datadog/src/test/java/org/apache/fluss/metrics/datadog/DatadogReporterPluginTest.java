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

package org.apache.fluss.metrics.datadog;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.util.MetricReporterTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.time.Duration;

import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_DATADOG_API_KEY;
import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_DATADOG_MAX_METRICS_PER_REQUEST;
import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_DATADOG_PROXY_HOST;
import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_DATADOG_PROXY_PORT;
import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_DATADOG_PUSH_INTERVAL;
import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_DATADOG_SITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link DatadogReporterPlugin}. */
class DatadogReporterPluginTest {

    @Test
    void testCreateMetricReporterWithDefaults() {
        Configuration config = new Configuration();
        config.setString(METRICS_REPORTER_DATADOG_API_KEY.key(), "test-api-key");

        DatadogReporter reporter =
                (DatadogReporter) new DatadogReporterPlugin().createMetricReporter(config);

        assertThat(reporter.scheduleInterval()).isEqualTo(Duration.ofSeconds(10));
        assertThat(reporter.client().seriesUrl().toString())
                .isEqualTo("https://api.datadoghq.com/api/v2/series");
        assertThat(reporter.client().proxy()).isNull();
    }

    @Test
    void testCreateMetricReporterWithSiteIntervalAndProxy() {
        Configuration config = new Configuration();
        config.setString(METRICS_REPORTER_DATADOG_API_KEY.key(), "test-api-key");
        config.set(METRICS_REPORTER_DATADOG_SITE, "us5.datadoghq.com");
        config.set(METRICS_REPORTER_DATADOG_PUSH_INTERVAL, Duration.ofSeconds(30));
        config.set(METRICS_REPORTER_DATADOG_PROXY_HOST, "proxy.internal");
        config.set(METRICS_REPORTER_DATADOG_PROXY_PORT, 3128);

        DatadogReporter reporter =
                (DatadogReporter) new DatadogReporterPlugin().createMetricReporter(config);

        assertThat(reporter.scheduleInterval()).isEqualTo(Duration.ofSeconds(30));
        assertThat(reporter.client().seriesUrl().toString())
                .isEqualTo("https://api.us5.datadoghq.com/api/v2/series");
        assertThat(reporter.client().proxy())
                .isEqualTo(
                        new Proxy(
                                Proxy.Type.HTTP,
                                InetSocketAddress.createUnresolved("proxy.internal", 3128)));
    }

    @Test
    void testMissingApiKeyIsRejected() {
        assertThatThrownBy(
                        () -> new DatadogReporterPlugin().createMetricReporter(new Configuration()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(METRICS_REPORTER_DATADOG_API_KEY.key());
    }

    @ParameterizedTest
    @ValueSource(strings = {"https://datadoghq.com", "datadoghq.com/path", "evil.com?x=", ""})
    void testSiteThatIsNotAHostNameIsRejected(String site) {
        Configuration config = new Configuration();
        config.setString(METRICS_REPORTER_DATADOG_API_KEY.key(), "test-api-key");
        config.set(METRICS_REPORTER_DATADOG_SITE, site);

        assertThatThrownBy(() -> new DatadogReporterPlugin().createMetricReporter(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid Datadog site");
    }

    @Test
    void testNonPositiveMaxMetricsPerRequestIsRejected() {
        Configuration config = new Configuration();
        config.setString(METRICS_REPORTER_DATADOG_API_KEY.key(), "test-api-key");
        config.set(METRICS_REPORTER_DATADOG_MAX_METRICS_PER_REQUEST, 0);

        assertThatThrownBy(() -> new DatadogReporterPlugin().createMetricReporter(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(METRICS_REPORTER_DATADOG_MAX_METRICS_PER_REQUEST.key());
    }

    @Test
    void testMetricReporterSetupViaSPI() {
        MetricReporterTestUtils.testMetricReporterSetupViaSPI(DatadogReporterPlugin.class);
    }
}

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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.SimpleCounter;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.util.TestMetricGroup;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link InfluxdbReporter}. */
class InfluxdbReporterTest {

    private static final String METRIC_HOSTNAME = "localhost";

    private static final Map<String, String> variables;
    private static final MetricGroup metricGroup;

    static {
        variables = new HashMap<>();
        variables.put("<host>", METRIC_HOSTNAME);
        variables.put("table", "test_table");

        metricGroup =
                TestMetricGroup.newBuilder()
                        .setLogicalScopeFunction((characterFilter, character) -> "tabletServer")
                        .setVariables(variables)
                        .build();
    }

    private WireMockServer wireMockServer;
    private InfluxdbReporter reporter;

    @BeforeEach
    void setUp() {
        wireMockServer = new WireMockServer(wireMockConfig().dynamicPort());
        wireMockServer.start();
    }

    @AfterEach
    void tearDown() {
        if (reporter != null) {
            reporter.close();
        }
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    @Test
    void testMetricRegistration() {
        reporter = createReporter();

        String metricName = "TestCounter";
        Counter counter = new SimpleCounter();
        reporter.notifyOfAddedMetric(counter, metricName, metricGroup);

        String name = reporter.metricNames.get(counter);
        assertThat(name).isNotNull();
        assertThat(name).isEqualTo("fluss_tabletServer_" + metricName);

        List<Map.Entry<String, String>> tags = reporter.metricTags.get(counter);
        assertThat(tags).isNotNull();
        assertThat(tags)
                .anyMatch(e -> e.getKey().equals("_host_") && e.getValue().equals(METRIC_HOSTNAME));
        assertThat(tags)
                .anyMatch(e -> e.getKey().equals("table") && e.getValue().equals("test_table"));
    }

    @Test
    void testMetricReporting() {
        wireMockServer.stubFor(
                post(urlPathEqualTo("/api/v2/write")).willReturn(aResponse().withStatus(200)));

        reporter = createReporter();

        String metricName = "TestCounter";
        Counter counter = new SimpleCounter();
        reporter.notifyOfAddedMetric(counter, metricName, metricGroup);
        counter.inc(42);

        reporter.report();

        wireMockServer.verify(
                postRequestedFor(urlPathEqualTo("/api/v2/write"))
                        .withRequestBody(containing("fluss_tabletServer_" + metricName))
                        .withRequestBody(containing("count=42i")));
    }

    private InfluxdbReporter createReporter() {
        InfluxdbReporter reporter =
                new InfluxdbReporter(
                        wireMockServer.baseUrl(),
                        "test-org",
                        "test-bucket",
                        "test-token",
                        Duration.ofSeconds(10));
        reporter.open(new Configuration());
        return reporter;
    }
}

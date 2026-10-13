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

package org.apache.fluss.metrics.prometheus;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.Password;
import org.apache.fluss.metrics.groups.GenericMetricGroup;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.metrics.registry.MetricRegistryImpl;
import org.apache.fluss.metrics.reporter.MetricReporter;
import org.apache.fluss.metrics.reporter.ReporterAndSettings;
import org.apache.fluss.metrics.reporter.ReporterSetup;
import org.apache.fluss.metrics.util.TestHistogram;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PrometheusPushGatewayReporter}. */
class PrometheusPushGatewayReporterTest {

    private HttpServer server;
    private BlockingQueue<String> receivedAuthHeaders;
    private BlockingQueue<String> receivedBodies;

    @BeforeEach
    void startFakePushGateway() throws IOException {
        receivedAuthHeaders = new ArrayBlockingQueue<>(8);
        receivedBodies = new ArrayBlockingQueue<>(8);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/",
                (HttpExchange exchange) -> {
                    // capture (possibly null) Authorization header, using empty string as absent
                    String auth = exchange.getRequestHeaders().getFirst("Authorization");
                    receivedAuthHeaders.offer(auth == null ? "" : auth);
                    try (InputStream body = exchange.getRequestBody();
                            ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
                        byte[] buf = new byte[1024];
                        int length;
                        while ((length = body.read(buf)) != -1) {
                            bytes.write(buf, 0, length);
                        }
                        receivedBodies.offer(
                                new String(bytes.toByteArray(), StandardCharsets.UTF_8));
                    }

                    exchange.sendResponseHeaders(202, -1);
                    exchange.close();
                });
        server.start();
    }

    @AfterEach
    void stopFakePushGateway() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void reportSendsAuthorizationHeaderWhenBasicAuthConfigured() throws Exception {
        PrometheusPushGatewayReporter reporter =
                new PrometheusPushGatewayReporter(
                        pushGatewayUrl(),
                        "test-job",
                        Collections.emptyMap(),
                        false,
                        Duration.ofSeconds(10),
                        "myuser",
                        "mypassword");
        try {
            reporter.report();

            String header = receivedAuthHeaders.poll(5, TimeUnit.SECONDS);
            assertThat(header).isNotNull().startsWith("Basic ");

            String decoded =
                    new String(
                            Base64.getDecoder().decode(header.substring("Basic ".length())),
                            StandardCharsets.UTF_8);
            assertThat(decoded).isEqualTo("myuser:mypassword");
        } finally {
            reporter.close();
        }
    }

    @Test
    void reportSendsNoAuthorizationHeaderWhenBasicAuthNotConfigured() throws Exception {
        PrometheusPushGatewayReporter reporter =
                new PrometheusPushGatewayReporter(
                        pushGatewayUrl(),
                        "test-job",
                        Collections.emptyMap(),
                        false,
                        Duration.ofSeconds(10),
                        null,
                        null);
        try {
            reporter.report();

            String header = receivedAuthHeaders.poll(5, TimeUnit.SECONDS);
            assertThat(header).isNotNull().isEmpty();
        } finally {
            reporter.close();
        }
    }

    @Test
    void reportSendsNoAuthorizationHeaderWhenUsernameIsBlank() throws Exception {
        // password without username should NOT enable basic auth
        PrometheusPushGatewayReporter reporter =
                new PrometheusPushGatewayReporter(
                        pushGatewayUrl(),
                        "test-job",
                        Collections.emptyMap(),
                        false,
                        Duration.ofSeconds(10),
                        "",
                        "somePwd");
        try {
            reporter.report();

            String header = receivedAuthHeaders.poll(5, TimeUnit.SECONDS);
            assertThat(header).isNotNull().isEmpty();
        } finally {
            reporter.close();
        }
    }

    @Test
    void pluginCreatesReporterCarryingBasicAuth() throws Exception {
        Configuration config = new Configuration();
        config.setString(
                ConfigOptions.METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_HOST_URL,
                pushGatewayUrl().toString());
        config.setString(
                ConfigOptions.METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_JOB_NAME, "plugin-job");
        config.setString(
                ConfigOptions.METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_USERNAME, "plugUser");
        config.set(
                ConfigOptions.METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_PASSWORD,
                new Password("plugPwd"));
        config.setString(
                ConfigOptions.METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_GROUPING_KEY, "k1=v1");

        PrometheusPushGatewayReporterPlugin plugin = new PrometheusPushGatewayReporterPlugin();
        assertThat(plugin.identifier()).isEqualTo("prometheus-push");

        MetricReporter reporter = plugin.createMetricReporter(config);
        assertThat(reporter).isInstanceOf(PrometheusPushGatewayReporter.class);
        try {
            ((PrometheusPushGatewayReporter) reporter).report();

            String header = receivedAuthHeaders.poll(5, TimeUnit.SECONDS);
            assertThat(header).isNotNull().startsWith("Basic ");
            String decoded =
                    new String(
                            Base64.getDecoder().decode(header.substring("Basic ".length())),
                            StandardCharsets.UTF_8);
            assertThat(decoded).isEqualTo("plugUser:plugPwd");
        } finally {
            reporter.close();
        }
    }

    @Test
    void testFiltersReducePushedMetrics() throws Exception {
        Configuration config = new Configuration();
        config.set(ConfigOptions.METRICS_REPORTERS, Collections.singletonList("prometheus-push"));
        config.setString(
                ConfigOptions.METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_HOST_URL,
                pushGatewayUrl().toString());
        config.setString(
                ConfigOptions.METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_JOB_NAME, "filtered-job");
        config.setString(ConfigOptions.METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_GROUPING_KEY, "");
        config.set(ConfigOptions.METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_DELETE_ON_SHUTDOWN, false);
        config.set(
                ConfigOptions.METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_PUSH_INTERVAL,
                Duration.ofHours(1));
        config.setString(
                "metrics.reporter.prometheus-push.filter.includes",
                "tabletserver.*:bytesIn,latency");
        config.setString(
                "metrics.reporter.prometheus-push.filter.excludes",
                "*.bucket;*.bucket.*;*:*:histogram");

        List<ReporterAndSettings> reporters = ReporterSetup.fromConfiguration(config, null);
        assertThat(reporters).hasSize(1);
        PrometheusPushGatewayReporter reporter =
                (PrometheusPushGatewayReporter) reporters.get(0).getReporter();
        try (MetricRegistry registry = MetricRegistryImpl.fromReporters(reporters)) {
            GenericMetricGroup root = new GenericMetricGroup(registry, null, "tabletserver");
            MetricGroup table = root.addGroup("table", "orders");
            table.counter("bytesIn").inc(42);
            table.counter("bytesOut").inc(99);
            table.histogram("latency", new TestHistogram());
            MetricGroup bucket = table.addGroup("bucket", "0");
            bucket.counter("bytesIn").inc(21);
            bucket.addGroup("log").counter("bytesIn").inc(7);
            table.gauge(
                    "debug",
                    () -> {
                        throw new AssertionError("Filtered gauge must not be collected");
                    });

            reporter.report();

            assertThat(receivedBodies.poll(5, TimeUnit.SECONDS))
                    .contains("fluss_tabletserver_table_bytesIn", "table=\"orders\"", "42.0")
                    .doesNotContain("bytesOut", "bucket", "latency", "debug");

            root.close();
            reporter.report();

            assertThat(receivedBodies.poll(5, TimeUnit.SECONDS)).isNotNull().isEmpty();
        }
    }

    private URL pushGatewayUrl() throws IOException {
        return new URL("http://127.0.0.1:" + server.getAddress().getPort());
    }
}

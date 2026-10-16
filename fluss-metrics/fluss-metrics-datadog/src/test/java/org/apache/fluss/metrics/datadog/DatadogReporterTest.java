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

import org.apache.fluss.config.Password;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.SimpleCounter;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.util.TestHistogram;
import org.apache.fluss.metrics.util.TestMeter;
import org.apache.fluss.metrics.util.TestMetricGroup;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link DatadogReporter}. */
class DatadogReporterTest {

    private static final String API_KEY = "test-api-key";

    private static final MetricGroup TABLET_SERVER_GROUP;

    static {
        Map<String, String> variables = new HashMap<>();
        variables.put("host", "fluss-host-1");
        variables.put("table", "orders");
        variables.put("partition", "");
        TABLET_SERVER_GROUP =
                TestMetricGroup.newBuilder()
                        .setLogicalScopeFunction((filter, separator) -> "tabletserver")
                        .setVariables(variables)
                        .build();
    }

    private final ObjectMapper mapper = new ObjectMapper();
    private final List<Request> requests = Collections.synchronizedList(new ArrayList<>());
    private final ConcurrentLinkedQueue<Integer> statuses = new ConcurrentLinkedQueue<>();

    private HttpServer server;

    /** A request the stub Datadog API received. */
    private static final class Request {
        final String path;
        final String apiKey;
        final String contentEncoding;
        final JsonNode body;

        Request(String path, String apiKey, String contentEncoding, JsonNode body) {
            this.path = path;
            this.apiKey = apiKey;
            this.contentEncoding = contentEncoding;
            this.body = body;
        }

        JsonNode series() {
            return body.get("series");
        }
    }

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/",
                exchange -> {
                    byte[] body;
                    try (InputStream in = new GZIPInputStream(exchange.getRequestBody())) {
                        body = readAll(in);
                    }
                    requests.add(
                            new Request(
                                    exchange.getRequestURI().getPath(),
                                    exchange.getRequestHeaders().getFirst("DD-API-KEY"),
                                    exchange.getRequestHeaders().getFirst("Content-Encoding"),
                                    mapper.readTree(body)));
                    Integer status = statuses.poll();
                    exchange.sendResponseHeaders(status == null ? 202 : status, -1);
                    exchange.close();
                });
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void testMetricRegistration() throws Exception {
        DatadogReporter reporter = createReporter(2000, Collections.singletonList("env:test"));
        Counter counter = new SimpleCounter();

        reporter.notifyOfAddedMetric(counter, "bytesIn Rate", TABLET_SERVER_GROUP);

        DatadogReporter.MetricInfo info = reporter.metrics.get(counter);
        assertThat(info.name).isEqualTo("fluss.tabletserver.bytesIn_Rate");
        // Variables without a value, such as the partition of a non-partitioned table, are dropped.
        assertThat(info.tags)
                .containsExactlyInAnyOrder("env:test", "host:fluss-host-1", "table:orders");
        assertThat(info.host).isEqualTo("fluss-host-1");

        reporter.notifyOfRemovedMetric(counter, "bytesIn Rate", TABLET_SERVER_GROUP);
        assertThat(reporter.metrics).isEmpty();
    }

    @Test
    void testReportSendsEveryMetricType() throws Exception {
        DatadogReporter reporter = createReporter(2000, Collections.emptyList());
        Counter counter = new SimpleCounter();
        counter.inc(42);
        reporter.notifyOfAddedMetric(counter, "requests", TABLET_SERVER_GROUP);
        reporter.notifyOfAddedMetric((Gauge<Integer>) () -> 7, "queueSize", TABLET_SERVER_GROUP);
        reporter.notifyOfAddedMetric((Gauge<Boolean>) () -> true, "isLeader", TABLET_SERVER_GROUP);
        reporter.notifyOfAddedMetric((Gauge<String>) () -> "v1", "version", TABLET_SERVER_GROUP);
        reporter.notifyOfAddedMetric(new TestMeter(100, 5), "bytesInRate", TABLET_SERVER_GROUP);
        reporter.notifyOfAddedMetric(new TestHistogram(), "latency", TABLET_SERVER_GROUP);

        reporter.report();

        assertThat(requests).hasSize(1);
        Request request = requests.get(0);
        assertThat(request.path).isEqualTo("/api/v2/series");
        assertThat(request.apiKey).isEqualTo(API_KEY);
        assertThat(request.contentEncoding).isEqualTo("gzip");

        Map<String, JsonNode> series = byMetric(request.series());
        // String gauges cannot be stored by Datadog, so "version" is left out.
        assertThat(series)
                .containsOnlyKeys(
                        "fluss.tabletserver.requests",
                        "fluss.tabletserver.queueSize",
                        "fluss.tabletserver.isLeader",
                        "fluss.tabletserver.bytesInRate",
                        "fluss.tabletserver.latency.count",
                        "fluss.tabletserver.latency.avg",
                        "fluss.tabletserver.latency.stddev",
                        "fluss.tabletserver.latency.min",
                        "fluss.tabletserver.latency.max",
                        "fluss.tabletserver.latency.median",
                        "fluss.tabletserver.latency.75percentile",
                        "fluss.tabletserver.latency.95percentile",
                        "fluss.tabletserver.latency.98percentile",
                        "fluss.tabletserver.latency.99percentile",
                        "fluss.tabletserver.latency.999percentile");

        JsonNode requestsSeries = series.get("fluss.tabletserver.requests");
        assertThat(requestsSeries.get("type").asInt()).isEqualTo(1);
        assertThat(requestsSeries.get("interval").asLong()).isEqualTo(10);
        assertThat(value(requestsSeries)).isEqualTo(42);
        assertThat(requestsSeries.get("resources").get(0).get("name").asText())
                .isEqualTo("fluss-host-1");
        assertThat(requestsSeries.get("resources").get(0).get("type").asText()).isEqualTo("host");
        assertThat(tags(requestsSeries))
                .containsExactlyInAnyOrder("host:fluss-host-1", "table:orders");

        assertThat(series.get("fluss.tabletserver.queueSize").get("type").asInt()).isEqualTo(3);
        assertThat(series.get("fluss.tabletserver.queueSize").has("interval")).isFalse();
        assertThat(value(series.get("fluss.tabletserver.queueSize"))).isEqualTo(7);
        assertThat(value(series.get("fluss.tabletserver.isLeader"))).isEqualTo(1);
        assertThat(value(series.get("fluss.tabletserver.bytesInRate"))).isEqualTo(5);
        assertThat(value(series.get("fluss.tabletserver.latency.count"))).isEqualTo(1);
        assertThat(value(series.get("fluss.tabletserver.latency.avg"))).isEqualTo(4);
        assertThat(value(series.get("fluss.tabletserver.latency.median"))).isEqualTo(0.5);
        assertThat(value(series.get("fluss.tabletserver.latency.999percentile"))).isEqualTo(0.999);
    }

    @Test
    void testCounterReportsIncreaseSinceLastAcceptedReport() throws Exception {
        DatadogReporter reporter = createReporter(2000, Collections.emptyList());
        Counter counter = new SimpleCounter();
        reporter.notifyOfAddedMetric(counter, "requests", TABLET_SERVER_GROUP);

        counter.inc(5);
        reporter.report();
        counter.inc(3);
        reporter.report();
        // Datadog rejects the next report, so its increase must be sent again with the next one.
        statuses.add(500);
        counter.inc(2);
        reporter.report();
        counter.inc(1);
        reporter.report();

        assertThat(requests).hasSize(4);
        assertThat(value(requests.get(0).series().get(0))).isEqualTo(5);
        assertThat(value(requests.get(1).series().get(0))).isEqualTo(3);
        assertThat(value(requests.get(2).series().get(0))).isEqualTo(2);
        assertThat(value(requests.get(3).series().get(0))).isEqualTo(3);
    }

    @Test
    void testLargeReportsAreSplitIntoRequests() throws Exception {
        DatadogReporter reporter = createReporter(4, Collections.emptyList());
        reporter.notifyOfAddedMetric((Gauge<Integer>) () -> 1, "gauge", TABLET_SERVER_GROUP);
        // A histogram becomes 11 series, more than fit in one request.
        reporter.notifyOfAddedMetric(new TestHistogram(), "latency", TABLET_SERVER_GROUP);

        reporter.report();

        List<Integer> sizes = new ArrayList<>();
        for (Request request : requests) {
            sizes.add(request.series().size());
        }
        assertThat(sizes).containsExactly(4, 4, 4);
    }

    @Test
    void testFailedRequestDoesNotStopLaterChunks() throws Exception {
        DatadogReporter reporter = createReporter(1, Collections.emptyList());
        Counter first = new SimpleCounter();
        Counter second = new SimpleCounter();
        first.inc(1);
        second.inc(1);
        reporter.notifyOfAddedMetric(first, "first", TABLET_SERVER_GROUP);
        reporter.notifyOfAddedMetric(second, "second", TABLET_SERVER_GROUP);
        statuses.add(500);

        reporter.report();
        reporter.report();

        assertThat(requests).hasSize(4);
        List<String> resent = new ArrayList<>();
        for (Request request : requests.subList(2, 4)) {
            JsonNode series = request.series().get(0);
            if (value(series) == 1) {
                resent.add(series.get("metric").asText());
            }
        }
        // Only the counter whose request was rejected is sent again with its increase.
        assertThat(resent).hasSize(1);
        assertThat(requests.get(0).series().get(0).get("metric").asText()).isEqualTo(resent.get(0));
    }

    private DatadogReporter createReporter(int maxSeriesPerRequest, List<String> tags)
            throws Exception {
        URL url = new URL("http://127.0.0.1:" + server.getAddress().getPort() + "/api/v2/series");
        return new DatadogReporter(
                new DatadogHttpClient(url, new Password(API_KEY), null, 10),
                Duration.ofSeconds(10),
                maxSeriesPerRequest,
                tags);
    }

    private static Map<String, JsonNode> byMetric(JsonNode series) {
        Map<String, JsonNode> byMetric = new HashMap<>();
        for (JsonNode s : series) {
            byMetric.put(s.get("metric").asText(), s);
        }
        return byMetric;
    }

    private static double value(JsonNode series) {
        assertThat(series.get("points")).hasSize(1);
        return series.get("points").get(0).get("value").asDouble();
    }

    private static List<String> tags(JsonNode series) {
        List<String> tags = new ArrayList<>();
        for (JsonNode tag : series.get("tags")) {
            tags.add(tag.asText());
        }
        return tags;
    }

    private static byte[] readAll(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int read;
        while ((read = in.read(buffer)) != -1) {
            out.write(buffer, 0, read);
        }
        return out.toByteArray();
    }
}

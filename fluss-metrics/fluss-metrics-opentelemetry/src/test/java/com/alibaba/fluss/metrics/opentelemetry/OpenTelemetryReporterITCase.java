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
import com.alibaba.fluss.metrics.Gauge;
import com.alibaba.fluss.metrics.Histogram;
import com.alibaba.fluss.metrics.MeterView;
import com.alibaba.fluss.metrics.SimpleCounter;
import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.metrics.util.TestHistogram;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import com.alibaba.fluss.testutils.common.TestLoggerExtension;

import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Tests for {@link OpenTelemetryReporter}. */
@ExtendWith(TestLoggerExtension.class)
public class OpenTelemetryReporterITCase extends OpenTelemetryReporterITCaseBase {

    private static final String LOGICAL_SCOPE = "logical.scope";

    private MetricGroup group;
    private final Histogram histogram = new TestHistogram();

    @BeforeEach
    public void setUpEach() {
        group = TestUtils.createTestMetricGroup(LOGICAL_SCOPE, new HashMap<>());
    }

    @ParameterizedTest
    @EnumSource(ConfigOptions.OpenTelemetryExporter.class)
    public void testReport(ConfigOptions.OpenTelemetryExporter exporterType) throws Exception {
        OpenTelemetryReporter reporter = createReporter(exporterType);

        SimpleCounter counter = new SimpleCounter();
        reporter.notifyOfAddedMetric(counter, "foo.counter", group);

        Gauge<Double> gauge = () -> 123.456d;
        reporter.notifyOfAddedMetric(gauge, "foo.gauge", group);

        reporter.report();

        MeterView meter = new MeterView(counter);
        reporter.notifyOfAddedMetric(meter, "foo.meter", group);

        reporter.notifyOfAddedMetric(histogram, "foo.histogram", group);

        reporter.report();
        reporter.close();

        eventuallyConsumeJson(
                (json) -> {
                    JsonNode scopeMetrics =
                            json.findPath("resourceMetrics").findPath("scopeMetrics");
                    assertThat(scopeMetrics.findPath("scope").findPath("name").asText())
                            .isEqualTo("com.alibaba.fluss.metrics");
                    JsonNode metrics = scopeMetrics.findPath("metrics");

                    List<String> metricNames = extractMetricNames(json);
                    assertThat(metricNames)
                            .contains(
                                    "fluss.logical.scope.foo.counter",
                                    "fluss.logical.scope.foo.gauge",
                                    "fluss.logical.scope.foo.meter.count",
                                    "fluss.logical.scope.foo.meter.rate",
                                    "fluss.logical.scope.foo.histogram");

                    metrics.forEach(OpenTelemetryReporterITCase::assertMetrics);
                });
    }

    private static void assertMetrics(JsonNode metric) {
        String name = metric.findPath("name").asText();
        if (name.equals("fluss.logical.scope.foo.counter")) {
            assertThat(metric.at("/sum/dataPoints").findPath("asInt").asInt()).isEqualTo(0);
        } else if (name.equals("fluss.logical.scope.foo.gauge")) {
            assertThat(metric.at("/gauge/dataPoints").findPath("asDouble").asDouble())
                    .isCloseTo(123.456, Percentage.withPercentage(1));
        } else if (name.equals("fluss.logical.scope.foo.meter.count")) {
            assertThat(metric.at("/sum/dataPoints").findPath("asInt").asInt()).isEqualTo(0);
        } else if (name.equals("fluss.logical.scope.foo.meter.rate")) {
            assertThat(metric.at("/gauge/dataPoints").findPath("asDouble").asDouble())
                    .isEqualTo(0.0);
        } else if (name.equals("fluss.logical.scope.foo.histogram")) {
            assertThat(metric.at("/summary/dataPoints").findPath("sum").asInt()).isEqualTo(4);
        }
    }

    @ParameterizedTest
    @EnumSource(ConfigOptions.OpenTelemetryExporter.class)
    public void testReportAfterUnregister(ConfigOptions.OpenTelemetryExporter exporterType)
            throws Exception {
        OpenTelemetryReporter reporter = createReporter(exporterType);

        SimpleCounter counter1 = new SimpleCounter();
        SimpleCounter counter2 = new SimpleCounter();
        SimpleCounter counter3 = new SimpleCounter();
        reporter.notifyOfAddedMetric(counter1, "foo.counter1", group);
        reporter.notifyOfAddedMetric(counter2, "foo.counter2", group);
        reporter.notifyOfAddedMetric(counter3, "foo.counter3", group);

        reporter.notifyOfRemovedMetric(counter2, "foo.counter2", group);

        reporter.report();
        reporter.close();

        eventuallyConsumeJson(
                json -> {
                    List<String> metricNames = extractMetricNames(json);
                    assertThat(metricNames)
                            .contains(
                                    "fluss.logical.scope.foo.counter1",
                                    "fluss.logical.scope.foo.counter3");
                });
    }

    @ParameterizedTest
    @EnumSource(ConfigOptions.OpenTelemetryExporter.class)
    public void testCounterDelta(ConfigOptions.OpenTelemetryExporter exporterType)
            throws Exception {
        OpenTelemetryReporter reporter = createReporter(exporterType);

        SimpleCounter counter = new SimpleCounter();
        reporter.notifyOfAddedMetric(counter, "foo.counter", group);

        counter.inc(1234);
        assertThat(counter.getCount()).isEqualTo(1234L);
        reporter.report();

        eventuallyConsumeJson(
                json -> {
                    List<String> metricNames = extractMetricNames(json);
                    assertThat(metricNames).contains("fluss.logical.scope.foo.counter");

                    JsonNode metrics =
                            json.findPath("resourceMetrics")
                                    .findPath("scopeMetrics")
                                    .findPath("metrics");

                    metrics.forEach(
                            metric -> {
                                assertThat(metric.at("/sum/dataPoints").findPath("asInt").asInt())
                                        .isEqualTo(1234);
                            });
                });

        counter.inc(25);
        assertThat(counter.getCount()).isEqualTo(1259L);

        reporter.report();
        reporter.close();

        eventuallyConsumeJson(
                json -> {
                    List<String> metricNames = extractMetricNames(json);
                    assertThat(metricNames).contains("fluss.logical.scope.foo.counter");

                    JsonNode metrics =
                            json.findPath("resourceMetrics")
                                    .findPath("scopeMetrics")
                                    .findPath("metrics");

                    metrics.forEach(
                            metric -> {
                                assertThat(metric.at("/sum/dataPoints").findPath("asInt").asInt())
                                        .isEqualTo(1234);
                            });
                });
    }

    @ParameterizedTest
    @EnumSource(ConfigOptions.OpenTelemetryExporter.class)
    public void testOpenTelemetryAttributes(ConfigOptions.OpenTelemetryExporter exporterType)
            throws Exception {
        String serviceName = "flink-bar";
        String serviceVersion = "v42";
        OpenTelemetryReporter reporter = createReporter(exporterType, serviceName, serviceVersion);

        SimpleCounter counter = new SimpleCounter();
        reporter.notifyOfAddedMetric(counter, "foo.counter", group);

        reporter.report();
        reporter.close();

        eventuallyConsumeJson(
                json -> {
                    List<String> metricNames = extractMetricNames(json);
                    assertThat(metricNames).contains("fluss.logical.scope.foo.counter");

                    JsonNode attributes =
                            json.findPath("resourceMetrics")
                                    .findPath("resource")
                                    .findPath("attributes");

                    List<String> attributeKeys =
                            attributes.findValues("key").stream()
                                    .map(JsonNode::asText)
                                    .collect(Collectors.toList());

                    assertThat(attributeKeys).contains("service.name", "service.version");

                    attributes.forEach(
                            attribute -> {
                                if (attribute.get("key").asText().equals("service.name")) {
                                    assertThat(attribute.at("/value/stringValue").asText())
                                            .isEqualTo(serviceName);
                                } else if (attribute
                                        .get("key")
                                        .asText()
                                        .equals("service.version")) {
                                    assertThat(attribute.at("/value/stringValue").asText())
                                            .isEqualTo(serviceVersion);
                                }
                            });
                });
    }

    private static OpenTelemetryReporter createReporter(
            ConfigOptions.OpenTelemetryExporter exporterType,
            String serviceName,
            String serviceVersion) {
        String endpoint;
        switch (exporterType) {
            case GRPC:
                endpoint =
                        OpenTelemetryReporterITCaseBase.getOpenTelemetryContainer()
                                .getGrpcEndpoint();
                break;
            case HTTP:
                endpoint =
                        OpenTelemetryReporterITCaseBase.getOpenTelemetryContainer()
                                .getHttpEndpoint();
                break;
            default:
                throw new IllegalStateException("OpenTelemetry exporter type: " + exporterType);
        }

        OpenTelemetryReporter reporter =
                new OpenTelemetryReporter(
                        endpoint,
                        exporterType,
                        Duration.ofSeconds(10),
                        Duration.ofSeconds(10),
                        serviceName,
                        serviceVersion);
        return reporter;
    }

    private static OpenTelemetryReporter createReporter(
            ConfigOptions.OpenTelemetryExporter exporterType) {
        return createReporter(exporterType, null, null);
    }
}

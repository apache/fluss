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

package org.apache.fluss.metrics.opentelemetry;

import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.Meter;
import org.apache.fluss.metrics.SimpleCounter;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.util.TestHistogram;
import org.apache.fluss.metrics.util.TestMeter;

import io.opentelemetry.semconv.ServiceAttributes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link OpenTelemetryReporter}. */
public class OpenTelemetryReporterTest {

    private static final String LOGICAL_SCOPE = "logical.scope";
    private static final Map<String, String> labels =
            Collections.unmodifiableMap(
                    Stream.of(
                                    new AbstractMap.SimpleEntry<>("label1", "value1"),
                                    new AbstractMap.SimpleEntry<>("label2", "value2"))
                            .collect(
                                    Collectors.toMap(
                                            AbstractMap.SimpleEntry::getKey,
                                            AbstractMap.SimpleEntry::getValue)));

    private MetricGroup metricGroup;

    @BeforeEach
    void setupReporter() {
        metricGroup = TestUtils.createTestMetricGroup(LOGICAL_SCOPE, labels);
    }

    @Test
    void testInvalidEndpoint() {
        assertThatThrownBy(
                        () ->
                                new OpenTelemetryReporter(
                                        "endpoint-with-missing-protocol",
                                        Duration.ofSeconds(5),
                                        Duration.ofSeconds(5),
                                        null,
                                        null))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(
                        () ->
                                new OpenTelemetryReporter(
                                        "invalid://protocol",
                                        Duration.ofSeconds(5),
                                        Duration.ofSeconds(5),
                                        null,
                                        null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testOpenTelemetryResourceIsConstructedCorrectly() {
        OpenTelemetryReporter reporter =
                new OpenTelemetryReporter(
                        "http://opentelemetry-collector:4317",
                        Duration.ofSeconds(5),
                        Duration.ofSeconds(5),
                        "fluss",
                        "v42");
        assertThat(reporter.resource.getAttribute(ServiceAttributes.SERVICE_NAME))
                .isEqualTo("fluss");
        assertThat(reporter.resource.getAttribute(ServiceAttributes.SERVICE_VERSION))
                .isEqualTo("v42");
    }

    @Test
    void testAddAndRemoveCounter() {
        OpenTelemetryReporter reporter = createReporter();
        Counter counter = new SimpleCounter();
        reporter.notifyOfAddedMetric(counter, "counter", metricGroup);
        assertThat(reporter.counters.containsKey(counter)).isTrue();
        reporter.notifyOfRemovedMetric(counter, "counter", metricGroup);
        assertThat(reporter.counters.containsKey(counter)).isFalse();
    }

    @Test
    void testAddAndRemoveGauge() {
        OpenTelemetryReporter reporter = createReporter();
        Gauge<Integer> gauge = () -> 1;
        reporter.notifyOfAddedMetric(gauge, "gauge", metricGroup);
        assertThat(reporter.gauges.containsKey(gauge)).isTrue();
        reporter.notifyOfRemovedMetric(gauge, "meter", metricGroup);
        assertThat(reporter.gauges.containsKey(gauge)).isFalse();
    }

    @Test
    void testAddAndRemoveHistogram() {
        OpenTelemetryReporter reporter = createReporter();
        Meter meter = new TestMeter();
        reporter.notifyOfAddedMetric(meter, "meter", metricGroup);
        assertThat(reporter.meters.containsKey(meter)).isTrue();
        reporter.notifyOfRemovedMetric(meter, "meter", metricGroup);
        assertThat(reporter.meters.containsKey(meter)).isFalse();
    }

    @Test
    void testAddAndRemoveMeter() {
        OpenTelemetryReporter reporter = createReporter();
        Histogram histogram = new TestHistogram();
        reporter.notifyOfAddedMetric(histogram, "histogram", metricGroup);
        assertThat(reporter.histograms.containsKey(histogram)).isTrue();
        reporter.notifyOfRemovedMetric(histogram, "histogram", metricGroup);
        assertThat(reporter.histograms.containsKey(histogram)).isFalse();
    }

    @Test
    void testRemoveEnclosingAngleBrackets() {
        assertThat(OpenTelemetryReporter.removeEnclosingAngleBrackets("<t>")).isEqualTo("t");
        assertThat(OpenTelemetryReporter.removeEnclosingAngleBrackets("<t")).isEqualTo("<t");
        assertThat(OpenTelemetryReporter.removeEnclosingAngleBrackets("t>")).isEqualTo("t>");
        assertThat(OpenTelemetryReporter.removeEnclosingAngleBrackets("<")).isEqualTo("<");
        assertThat(OpenTelemetryReporter.removeEnclosingAngleBrackets(">")).isEqualTo(">");
        assertThat(OpenTelemetryReporter.removeEnclosingAngleBrackets("<>")).isEqualTo("");
        assertThat(OpenTelemetryReporter.removeEnclosingAngleBrackets("")).isEqualTo("");
    }

    private OpenTelemetryReporter createReporter() {
        return new OpenTelemetryReporter(
                "http://endpoint-must-not-be-called-in-unit-tests",
                Duration.ofSeconds(5),
                Duration.ofSeconds(5),
                null,
                null);
    }
}

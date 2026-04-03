/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.SimpleCounter;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.util.TestHistogram;
import org.apache.fluss.metrics.util.TestMeter;
import org.apache.fluss.metrics.util.TestMetricGroup;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.Point;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Tests for the {@link InfluxdbReporter}. */
class InfluxdbReporterTest {

    private InfluxdbReporter reporter;
    private MetricGroup metricGroup;
    private InfluxDBClient mockClient;

    @BeforeEach
    void setUp() throws Exception {
        mockClient = mock(InfluxDBClient.class);
        doNothing().when(mockClient).writePoints(any());

        reporter =
                new InfluxdbReporter(
                        "http://localhost:8086",
                        "test-org",
                        "test-bucket",
                        "test-token",
                        Duration.ofSeconds(10)) {
                    @Override
                    public void open(Configuration config) {
                        // Override to inject mock client
                        try {
                            Field clientField = InfluxdbReporter.class.getDeclaredField("client");
                            clientField.setAccessible(true);
                            clientField.set(this, mockClient);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
        reporter.open(new Configuration());

        Map<String, String> variables = new HashMap<>();
        variables.put("<host>", "localhost");
        variables.put("table", "test_table");

        metricGroup =
                TestMetricGroup.newBuilder()
                        .setLogicalScopeFunction((characterFilter, character) -> "tabletServer")
                        .setVariables(variables)
                        .build();
    }

    @AfterEach
    void tearDown() {
        if (reporter != null) {
            reporter.close();
        }
    }

    // -------------------- Tests for notifyOfAddedMetric --------------------

    @Test
    void testNotifyOfAddedMetric() throws Exception {
        Counter counter = new SimpleCounter();
        counter.inc(42);

        reporter.notifyOfAddedMetric(counter, "test_counter", metricGroup);

        Map<org.apache.fluss.metrics.Metric, String> metricNames = getMetricNames();
        assertThat(metricNames).containsKey(counter);
        assertThat(metricNames.get(counter)).isEqualTo("fluss_tabletServer_test_counter");

        Map<org.apache.fluss.metrics.Metric, List<Map.Entry<String, String>>> metricTags =
                getMetricTags();
        assertThat(metricTags).containsKey(counter);
        assertThat(metricTags.get(counter))
                .containsExactlyInAnyOrder(
                        Map.entry("_host_", "localhost"), Map.entry("table", "test_table"));
    }

    @Test
    void testNotifyOfAddedMetricWithDifferentTypes() throws Exception {
        Counter counter = new SimpleCounter();
        reporter.notifyOfAddedMetric(counter, "counter_type_test", metricGroup);

        Gauge<Integer> gauge = () -> 123;
        reporter.notifyOfAddedMetric(gauge, "gauge_type_test", metricGroup);

        TestMeter meter = new TestMeter(100, 5.0);
        reporter.notifyOfAddedMetric(meter, "meter_type_test", metricGroup);

        TestHistogram histogram = new TestHistogram();
        reporter.notifyOfAddedMetric(histogram, "histogram_type_test", metricGroup);

        Map<org.apache.fluss.metrics.Metric, String> metricNames = getMetricNames();
        assertThat(metricNames).containsKeys(counter, gauge, meter, histogram);
        assertThat(metricNames.get(counter)).isEqualTo("fluss_tabletServer_counter_type_test");
        assertThat(metricNames.get(gauge)).isEqualTo("fluss_tabletServer_gauge_type_test");
        assertThat(metricNames.get(meter)).isEqualTo("fluss_tabletServer_meter_type_test");
        assertThat(metricNames.get(histogram)).isEqualTo("fluss_tabletServer_histogram_type_test");
    }

    // -------------------- Tests for notifyOfRemovedMetric --------------------

    @Test
    void testNotifyOfRemovedMetric() throws Exception {
        Counter counter = new SimpleCounter();
        reporter.notifyOfAddedMetric(counter, "test_counter", metricGroup);

        Map<org.apache.fluss.metrics.Metric, String> metricNames = getMetricNames();
        assertThat(metricNames).containsKey(counter);

        reporter.notifyOfRemovedMetric(counter, "test_counter", metricGroup);

        assertThat(metricNames).doesNotContainKey(counter);

        Map<org.apache.fluss.metrics.Metric, List<Map.Entry<String, String>>> metricTags =
                getMetricTags();
        assertThat(metricTags).doesNotContainKey(counter);
    }

    // -------------------- Tests for report --------------------

    @Test
    void testReport() throws Exception {
        // Test report with no metrics
        reporter.report();
        verify(mockClient, times(1)).writePoints(any());

        // Test report with Counter
        Counter counter = new SimpleCounter();
        counter.inc(42);
        reporter.notifyOfAddedMetric(counter, "report_test_counter", metricGroup);
        reporter.report();
        ArgumentCaptor<List<Point>> captor = ArgumentCaptor.forClass(List.class);
        verify(mockClient, times(2)).writePoints(captor.capture());
        List<Point> points = captor.getValue();
        assertThat(points).hasSize(1);

        // Test report with Gauge
        Gauge<Integer> gauge = () -> 123;
        reporter.notifyOfAddedMetric(gauge, "report_test_gauge", metricGroup);
        reporter.report();
        verify(mockClient, times(3)).writePoints(any());

        // Test report with Meter
        TestMeter meter = new TestMeter(1000, 5.0);
        reporter.notifyOfAddedMetric(meter, "report_test_meter", metricGroup);
        reporter.report();
        verify(mockClient, times(4)).writePoints(any());

        // Test report with Histogram
        TestHistogram histogram = new TestHistogram();
        histogram.setCount(50);
        histogram.setMean(100.0);
        reporter.notifyOfAddedMetric(histogram, "report_test_histogram", metricGroup);
        reporter.report();
        verify(mockClient, times(5)).writePoints(any());

        // Test report with multiple metrics
        Counter counter2 = new SimpleCounter();
        counter2.inc(10);
        reporter.notifyOfAddedMetric(counter2, "report_test_counter2", metricGroup);

        Gauge<Double> gauge2 = () -> 3.14;
        reporter.notifyOfAddedMetric(gauge2, "report_test_gauge2", metricGroup);
        reporter.report();
        verify(mockClient, times(6)).writePoints(any());
    }

    // -------------------- Helper methods --------------------

    @SuppressWarnings("unchecked")
    private Map<org.apache.fluss.metrics.Metric, String> getMetricNames() throws Exception {
        Field field = InfluxdbReporter.class.getDeclaredField("metricNames");
        field.setAccessible(true);
        return (Map<org.apache.fluss.metrics.Metric, String>) field.get(reporter);
    }

    @SuppressWarnings("unchecked")
    private Map<org.apache.fluss.metrics.Metric, List<Map.Entry<String, String>>> getMetricTags()
            throws Exception {
        Field field = InfluxdbReporter.class.getDeclaredField("metricTags");
        field.setAccessible(true);
        return (Map<org.apache.fluss.metrics.Metric, List<Map.Entry<String, String>>>)
                field.get(reporter);
    }
}

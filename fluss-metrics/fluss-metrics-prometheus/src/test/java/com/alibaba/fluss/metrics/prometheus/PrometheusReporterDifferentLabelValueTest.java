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

package com.alibaba.fluss.metrics.prometheus;

import com.alibaba.fluss.metrics.Counter;
import com.alibaba.fluss.metrics.Gauge;
import com.alibaba.fluss.metrics.Meter;
import com.alibaba.fluss.metrics.SimpleCounter;
import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.metrics.util.TestHistogram;
import com.alibaba.fluss.metrics.util.TestMeter;
import com.alibaba.fluss.utils.NetUtils;

import com.mashape.unirest.http.exceptions.UnirestException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.stream.Stream;

import static com.alibaba.fluss.metrics.prometheus.PrometheusReporterTest.pollMetrics;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link PrometheusReporter} that registers several instances of the same metric with
 * different label values.
 */
class PrometheusReporterDifferentLabelValueTest {

    private static final String[] LABEL_NAMES = {"label1", "label2"};
    private static final String LOGICAL_SCOPE = "logical_scope";
    private static final String METRIC_NAME = "myMetric";

    private PrometheusReporter reporter;

    @BeforeEach
    void setupReporter() {
        reporter = new PrometheusReporter(NetUtils.getPortRangeFromString("9400-9500"));
    }

    @AfterEach
    void tearDown() {
        if (reporter != null) {
            reporter.close();
        }
    }

    private static Stream<Arguments> provideParameters() {
        final String[] labelValues1 = new String[] {"value1_1", "value1_2"};
        final String[] labelValues2 = new String[] {"value2_1", "value2_2"};
        final String[] labelValues3 = new String[] {"value3_1", ""};
        final String[] labelValues4 = new String[] {"values4_1", ""};

        final MetricGroup metricGroup1 =
                TestUtils.createTestMetricGroup(
                        LOGICAL_SCOPE, TestUtils.toMap(LABEL_NAMES, labelValues1));
        final MetricGroup metricGroup2 =
                TestUtils.createTestMetricGroup(
                        LOGICAL_SCOPE, TestUtils.toMap(LABEL_NAMES, labelValues2));
        final MetricGroup metricGroup3 =
                TestUtils.createTestMetricGroup(
                        LOGICAL_SCOPE, TestUtils.toMap(LABEL_NAMES, labelValues3));
        final MetricGroup metricGroup4 =
                TestUtils.createTestMetricGroup(
                        LOGICAL_SCOPE, TestUtils.toMap(LABEL_NAMES, labelValues4));

        return Stream.of(
                Arguments.of(metricGroup1, labelValues1, metricGroup2, labelValues2),
                Arguments.of(metricGroup1, labelValues1, metricGroup3, labelValues3),
                Arguments.of(metricGroup3, labelValues3, metricGroup2, labelValues2),
                Arguments.of(metricGroup3, labelValues3, metricGroup4, labelValues4));
    }

    @ParameterizedTest
    @MethodSource("provideParameters")
    void countersCanBeAddedSeveralTimesIfTheyDifferInLabelValues(
            MetricGroup metricGroup1,
            String[] expectedLabelValues1,
            MetricGroup metricGroup2,
            String[] expectedLabelValues2) {
        Counter counter1 = new SimpleCounter();
        counter1.inc(1);
        Counter counter2 = new SimpleCounter();
        counter2.inc(2);

        reporter.notifyOfAddedMetric(counter1, METRIC_NAME, metricGroup1);
        reporter.notifyOfAddedMetric(counter2, METRIC_NAME, metricGroup2);

        assertThat(
                        reporter.registry.getSampleValue(
                                getLogicalScope(METRIC_NAME), LABEL_NAMES, expectedLabelValues1))
                .isEqualTo(1.);
        assertThat(
                        reporter.registry.getSampleValue(
                                getLogicalScope(METRIC_NAME), LABEL_NAMES, expectedLabelValues2))
                .isEqualTo(2.);
    }

    @ParameterizedTest
    @MethodSource("provideParameters")
    void gaugesCanBeAddedSeveralTimesIfTheyDifferInLabelValues(
            MetricGroup metricGroup1,
            String[] expectedLabelValues1,
            MetricGroup metricGroup2,
            String[] expectedLabelValues2) {
        Gauge<Integer> gauge1 = () -> 3;
        Gauge<Integer> gauge2 = () -> 4;

        reporter.notifyOfAddedMetric(gauge1, METRIC_NAME, metricGroup1);
        reporter.notifyOfAddedMetric(gauge2, METRIC_NAME, metricGroup2);

        assertThat(
                        reporter.registry.getSampleValue(
                                getLogicalScope(METRIC_NAME), LABEL_NAMES, expectedLabelValues1))
                .isEqualTo(3.);
        assertThat(
                        reporter.registry.getSampleValue(
                                getLogicalScope(METRIC_NAME), LABEL_NAMES, expectedLabelValues2))
                .isEqualTo(4.);
    }

    @ParameterizedTest
    @MethodSource("provideParameters")
    void metersCanBeAddedSeveralTimesIfTheyDifferInLabelValues(
            MetricGroup metricGroup1,
            String[] expectedLabelValues1,
            MetricGroup metricGroup2,
            String[] expectedLabelValues2) {
        Meter meter1 = new TestMeter(1, 1.0);
        Meter meter2 = new TestMeter(2, 2.0);

        reporter.notifyOfAddedMetric(meter1, METRIC_NAME, metricGroup1);
        reporter.notifyOfAddedMetric(meter2, METRIC_NAME, metricGroup2);

        assertThat(
                        reporter.registry.getSampleValue(
                                getLogicalScope(METRIC_NAME), LABEL_NAMES, expectedLabelValues1))
                .isEqualTo(meter1.getRate());
        assertThat(
                        reporter.registry.getSampleValue(
                                getLogicalScope(METRIC_NAME), LABEL_NAMES, expectedLabelValues2))
                .isEqualTo(meter2.getRate());
    }

    @ParameterizedTest
    @MethodSource("provideParameters")
    void histogramsCanBeAddedSeveralTimesIfTheyDifferInLabelValues(
            MetricGroup metricGroup1,
            String[] expectedLabelValues1,
            MetricGroup metricGroup2,
            String[] expectedLabelValues2)
            throws UnirestException {
        TestHistogram histogram1 = new TestHistogram();
        histogram1.setCount(1);
        TestHistogram histogram2 = new TestHistogram();
        histogram2.setCount(2);

        reporter.notifyOfAddedMetric(histogram1, METRIC_NAME, metricGroup1);
        reporter.notifyOfAddedMetric(histogram2, METRIC_NAME, metricGroup2);

        final String exportedMetrics = pollMetrics(reporter.getPort()).getBody();
        assertThat(exportedMetrics)
                .contains(formatAsPrometheusLabels(LABEL_NAMES, expectedLabelValues1) + " 1.0");
        assertThat(exportedMetrics)
                .contains(formatAsPrometheusLabels(LABEL_NAMES, expectedLabelValues2) + " 2.0");

        final String[] labelNamesWithQuantile = addToArray(LABEL_NAMES, "quantile");
        for (Double quantile : PrometheusReporter.HistogramSummaryProxy.QUANTILES) {
            assertThat(
                            reporter.registry.getSampleValue(
                                    getLogicalScope(METRIC_NAME),
                                    labelNamesWithQuantile,
                                    addToArray(expectedLabelValues1, "" + quantile)))
                    .isEqualTo(quantile);
            assertThat(
                            reporter.registry.getSampleValue(
                                    getLogicalScope(METRIC_NAME),
                                    labelNamesWithQuantile,
                                    addToArray(expectedLabelValues2, "" + quantile)))
                    .isEqualTo(quantile);
        }
    }

    @ParameterizedTest
    @MethodSource("provideParameters")
    void removingSingleInstanceOfMetricDoesNotBreakOtherInstances(
            MetricGroup metricGroup1,
            String[] expectedLabelValues1,
            MetricGroup metricGroup2,
            String[] expectedLabelValues2) {
        Counter counter1 = new SimpleCounter();
        counter1.inc(1);
        Counter counter2 = new SimpleCounter();
        counter2.inc(2);

        reporter.notifyOfAddedMetric(counter1, METRIC_NAME, metricGroup1);
        reporter.notifyOfAddedMetric(counter2, METRIC_NAME, metricGroup2);

        assertThat(
                        reporter.registry.getSampleValue(
                                getLogicalScope(METRIC_NAME), LABEL_NAMES, expectedLabelValues1))
                .isEqualTo(1.);
        assertThat(
                        reporter.registry.getSampleValue(
                                getLogicalScope(METRIC_NAME), LABEL_NAMES, expectedLabelValues2))
                .isEqualTo(2.);

        reporter.notifyOfRemovedMetric(counter2, METRIC_NAME, metricGroup2);
        assertThat(
                        reporter.registry.getSampleValue(
                                getLogicalScope(METRIC_NAME), LABEL_NAMES, expectedLabelValues1))
                .isEqualTo(1.);

        reporter.notifyOfRemovedMetric(counter1, METRIC_NAME, metricGroup1);
        assertThat(
                        reporter.registry.getSampleValue(
                                getLogicalScope(METRIC_NAME), LABEL_NAMES, expectedLabelValues2))
                .isNull();
    }

    private static String getLogicalScope(String metricName) {
        return PrometheusReporter.SCOPE_PREFIX
                + LOGICAL_SCOPE
                + PrometheusReporter.SCOPE_SEPARATOR
                + metricName;
    }

    private String[] addToArray(String[] array, String element) {
        final String[] labelNames = Arrays.copyOf(array, LABEL_NAMES.length + 1);
        labelNames[LABEL_NAMES.length] = element;
        return labelNames;
    }

    private static String formatAsPrometheusLabels(String[] labelNames, String[] labelValues) {
        if (labelNames == null
                || labelValues == null
                || labelNames.length == 0
                || labelValues.length == 0
                || labelNames.length != labelValues.length) {
            throw new IllegalStateException("Erroneous test setup!");
        }

        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < labelNames.length; i++) {
            sb.append(labelNames[i]);
            sb.append("=\"");
            sb.append(labelValues[i]);
            sb.append("\",");
        }
        sb.append("}");

        return sb.toString();
    }
}

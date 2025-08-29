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

import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.Meter;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.ValueAtQuantile;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableDoublePointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableGaugeData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableLongPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSumData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSummaryData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSummaryPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableValueAtQuantile;
import io.opentelemetry.sdk.resources.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * An adapter class which translates from Fluss metrics to OpenTelmetry metrics which can exported
 * with the standard OpenTelemetry {@link io.opentelemetry.sdk.metrics.export.MetricExporter}s.
 */
class OpenTelemetryAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(OpenTelemetryAdapter.class);
    static final double[] HISTOGRAM_QUANTILES = {0.5, 0.75, 0.95, 0.99};

    static final InstrumentationScopeInfo INSTRUMENTATION_SCOPE_INFO =
            InstrumentationScopeInfo.create("org.apache.fluss.metrics");

    public static Optional<MetricData> convertCounter(
            CollectionMetadata collectionMetadata,
            Long count,
            Long previousCount,
            MetricMetadata metricMetadata) {
        long delta = count - previousCount;
        if (delta < 0) {
            LOG.warn(
                    "Non-monotonic counter {}: current count {} is less than previous count {}",
                    metricMetadata.getName(),
                    count,
                    previousCount);
            return Optional.empty();
        }

        boolean isMonotonic = true;
        return Optional.of(
                ImmutableMetricData.createLongSum(
                        collectionMetadata.getOpenTelemetryResource(),
                        INSTRUMENTATION_SCOPE_INFO,
                        metricMetadata.getName(),
                        "",
                        "",
                        ImmutableSumData.create(
                                isMonotonic,
                                AggregationTemporality.DELTA,
                                Collections.singleton(
                                        ImmutableLongPointData.create(
                                                collectionMetadata.getStartEpochNanos(),
                                                collectionMetadata.getEpochNanos(),
                                                convertVariables(metricMetadata.getVariables()),
                                                delta)))));
    }

    /**
     * Converts a Fluss Gauge to a {@link MetricData}.
     *
     * @param collectionMetadata The common collection metadata
     * @param gauge The Fluss Gauge to convert
     * @param metricMetadata The metric metadata
     * @return A {@link MetricData} if it's able to convert successfully
     */
    public static Optional<MetricData> convertGauge(
            CollectionMetadata collectionMetadata, Gauge<?> gauge, MetricMetadata metricMetadata) {
        if (!(gauge.getValue() instanceof Number)) {
            LOG.debug(
                    "Couldn't adapt gauge {} with value {} and type {}",
                    metricMetadata.getName(),
                    gauge.getValue(),
                    gauge.getValue().getClass().getName());
            return Optional.empty();
        }

        Number number = (Number) gauge.getValue();
        if (number instanceof Long || number instanceof Integer) {
            return Optional.of(
                    ImmutableMetricData.createLongGauge(
                            collectionMetadata.getOpenTelemetryResource(),
                            INSTRUMENTATION_SCOPE_INFO,
                            metricMetadata.getName(),
                            "",
                            "",
                            ImmutableGaugeData.create(
                                    Collections.singleton(
                                            ImmutableLongPointData.create(
                                                    collectionMetadata.getStartEpochNanos(),
                                                    collectionMetadata.getEpochNanos(),
                                                    convertVariables(metricMetadata.getVariables()),
                                                    number.longValue())))));
        } else {
            return Optional.of(
                    ImmutableMetricData.createDoubleGauge(
                            collectionMetadata.getOpenTelemetryResource(),
                            INSTRUMENTATION_SCOPE_INFO,
                            metricMetadata.getName(),
                            "",
                            "",
                            ImmutableGaugeData.create(
                                    Collections.singleton(
                                            ImmutableDoublePointData.create(
                                                    collectionMetadata.getStartEpochNanos(),
                                                    collectionMetadata.getEpochNanos(),
                                                    convertVariables(metricMetadata.getVariables()),
                                                    number.doubleValue())))));
        }
    }

    /**
     * Converts a Fluss Meter to a {@link MetricData}.
     *
     * @param collectionMetadata The common collection metadata
     * @param meter The Fluss Meter to convert
     * @param metricMetadata The metric metadata
     * @return A {@link MetricData} if it's able to convert successfully
     */
    public static List<MetricData> convertMeter(
            CollectionMetadata collectionMetadata,
            Meter meter,
            Long count,
            Long previousCount,
            MetricMetadata metricMetadata) {
        List<MetricData> metricData = new ArrayList<>();
        convertCounter(collectionMetadata, count, previousCount, metricMetadata.subMetric("count"))
                .ifPresent(metricData::add);
        convertGauge(collectionMetadata, meter::getRate, metricMetadata.subMetric("rate"))
                .ifPresent(metricData::add);
        return metricData;
    }

    /**
     * Converts a Fluss Histogram to a list of {@link MetricData}s.
     *
     * @param collectionMetadata The common collection metadata
     * @param histogram The Fluss Histogram to convert
     * @param metricMetadata The metric metadata
     * @return A list of {@link MetricData}s if it's able to convert successfully, or empty if not
     */
    public static Optional<MetricData> convertHistogram(
            CollectionMetadata collectionMetadata,
            Histogram histogram,
            MetricMetadata metricMetadata) {
        List<ValueAtQuantile> quantileList = new ArrayList<>();
        quantileList.add(ImmutableValueAtQuantile.create(0, histogram.getStatistics().getMin()));
        for (double histogramQuantile : HISTOGRAM_QUANTILES) {
            quantileList.add(
                    ImmutableValueAtQuantile.create(
                            histogramQuantile,
                            histogram.getStatistics().getQuantile(histogramQuantile)));
        }
        quantileList.add(ImmutableValueAtQuantile.create(1, histogram.getStatistics().getMax()));
        quantileList.add(ImmutableValueAtQuantile.create(1, histogram.getStatistics().getMax()));
        return Optional.of(
                ImmutableMetricData.createDoubleSummary(
                        collectionMetadata.getOpenTelemetryResource(),
                        INSTRUMENTATION_SCOPE_INFO,
                        metricMetadata.getName(),
                        "",
                        "",
                        ImmutableSummaryData.create(
                                Collections.singleton(
                                        ImmutableSummaryPointData.create(
                                                collectionMetadata.getStartEpochNanos(),
                                                collectionMetadata.getEpochNanos(),
                                                convertVariables(metricMetadata.getVariables()),
                                                histogram.getCount(),
                                                histogram.getStatistics().getMean()
                                                        * histogram.getCount(),
                                                quantileList)))));
    }

    private static Attributes convertVariables(Map<String, String> variables) {
        AttributesBuilder builder = Attributes.builder();
        variables.forEach(builder::put);
        return builder.build();
    }

    /** The common metadata associated with a collection of the metrics. */
    public static class CollectionMetadata {

        private final Resource openTelemetryResource;
        private final long startEpochNanos;
        private final long epochNanos;

        public CollectionMetadata(
                Resource openTelemetryResource, long startEpochNanos, long epochNanos) {
            this.openTelemetryResource = openTelemetryResource;
            this.startEpochNanos = startEpochNanos;
            this.epochNanos = epochNanos;
        }

        public Resource getOpenTelemetryResource() {
            return openTelemetryResource;
        }

        public long getStartEpochNanos() {
            return startEpochNanos;
        }

        public long getEpochNanos() {
            return epochNanos;
        }
    }
}

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

package org.apache.fluss.rpc.netty.server;

import org.apache.fluss.metrics.CharacterFilter;
import org.apache.fluss.metrics.Meter;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.groups.GenericMetricGroup;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.Errors;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.fluss.rpc.netty.server.RequestsMetrics.KAFKA_METADATA_REQUEST_NAME;
import static org.apache.fluss.rpc.netty.server.RequestsMetrics.KAFKA_PRODUCE_REQUEST_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RequestsMetrics}. */
class RequestsMetricsTest {

    @Test
    void testTabletServerRegistersNativeRequests() {
        RecordingMetricRegistry registry = new RecordingMetricRegistry();
        MetricGroup serverMetricGroup = new GenericMetricGroup(registry, null, "tabletserver");
        RequestsMetrics metrics =
                RequestsMetrics.createTabletServerRequestMetrics(serverMetricGroup);

        assertThat(metrics.getMetrics(ApiKeys.FETCH_LOG.id, false, false)).isPresent();
        assertThat(metrics.getMetrics(ApiKeys.FETCH_LOG.id, true, false)).isPresent();
        assertThat(metrics.getMetrics(ApiKeys.LOOKUP.id, false, false)).isPresent();
        assertThat(metrics.getMetrics(ApiKeys.LOOKUP.id, false, true)).isPresent();
        assertThat(metrics.getMetrics(ApiKeys.PUT_KV.id, false, false)).isPresent();
        assertThat(metrics.getMetrics(ApiKeys.PUT_KV.id, false, true)).isPresent();
        assertThat(registry.metrics(MetricNames.REQUESTS_RATE))
                .extracting(metric -> metric.group.getAllVariables().get("request"))
                .containsExactlyInAnyOrder(
                        "produceLog",
                        "putKv",
                        "historicalPutKv",
                        "lookup",
                        "historicalLookup",
                        "fetchLogClient",
                        "fetchLogFollower",
                        "prefixLookup",
                        "metadata");
    }

    @Test
    void testCoordinatorServerCreatesOnlyRequestGaugeGroup() {
        RecordingMetricRegistry registry = new RecordingMetricRegistry();
        AtomicInteger requestGroupLookups = new AtomicInteger();
        MetricGroup serverMetricGroup =
                new GenericMetricGroup(registry, null, "coordinator") {
                    @Override
                    public MetricGroup addGroup(String name) {
                        if ("request".equals(name)) {
                            requestGroupLookups.incrementAndGet();
                        }
                        return super.addGroup(name);
                    }
                };
        RequestsMetrics metrics =
                RequestsMetrics.createCoordinatorServerRequestMetrics(serverMetricGroup);

        metrics.gauge(MetricNames.REQUEST_QUEUE_SIZE, () -> 0);
        metrics.gauge("processor_index", "0", MetricNames.REQUEST_QUEUE_SIZE, () -> 0);
        assertThat(requestGroupLookups.get()).isOne();
        assertThat(registry.metrics(MetricNames.REQUESTS_RATE)).isEmpty();
        assertThat(registry.metrics(MetricNames.REQUEST_QUEUE_SIZE))
                .extracting(
                        metric -> metric.group.getLogicalScope(CharacterFilter.NO_OP_FILTER, '_'))
                .containsExactlyInAnyOrder(
                        "coordinator_request", "coordinator_request_processor_index");
    }

    @Test
    void testKafkaRegistersOnlyProduceAndMetadata() {
        RecordingMetricRegistry registry = new RecordingMetricRegistry();
        MetricGroup serverMetricGroup = new GenericMetricGroup(registry, null, "tabletserver");
        RequestsMetrics metrics =
                RequestsMetrics.createTabletServerRequestMetrics(serverMetricGroup);
        List<RegisteredMetric> nativeRates = registry.metrics(MetricNames.REQUESTS_RATE);
        metrics.registerKafkaRequestMetrics();

        assertThat(metrics.getMetrics(KAFKA_PRODUCE_REQUEST_NAME)).isPresent();
        assertThat(metrics.getMetrics(KAFKA_METADATA_REQUEST_NAME)).isPresent();
        assertThat(registry.metrics(MetricNames.REQUESTS_RATE))
                .filteredOn(metric -> !nativeRates.contains(metric))
                .extracting(metric -> String.join("_", metric.group.getScopeComponents()))
                .containsExactlyInAnyOrder(
                        "tabletserver_request_kafka_produce",
                        "tabletserver_request_kafka_metadata");
    }

    @Test
    void testRequestMetricsCalculations() {
        RequestsMetrics kafka = KafkaMetricsTestUtils.createRequestMetrics();
        RequestsMetrics.Metrics metrics = kafka.getMetrics(KAFKA_PRODUCE_REQUEST_NAME).get();
        metrics.recordRequest(42, 100, 105, 116, 123);

        assertThat(metrics.getRequestsCount().getCount()).isOne();
        assertThat(metrics.getErrorsCount().getCount()).isZero();
        assertThat(metrics.getRequestBytes().getStatistics().getValues()).containsExactly(42);
        assertThat(metrics.getRequestQueueTimeMs().getStatistics().getValues()).containsExactly(5);
        assertThat(metrics.getRequestProcessTimeMs().getStatistics().getValues())
                .containsExactly(11);
        assertThat(metrics.getResponseSendTimeMs().getStatistics().getValues()).containsExactly(7);
        assertThat(metrics.getTotalTimeMs().getStatistics().getValues()).containsExactly(23);
    }

    @Test
    void errorsKeepAggregateAndRegisterBreakdownByError() {
        RecordingMetricRegistry registry = new RecordingMetricRegistry();
        MetricGroup serverMetricGroup = new GenericMetricGroup(registry, null, "tabletserver");
        RequestsMetrics requestsMetrics =
                RequestsMetrics.createTabletServerRequestMetrics(serverMetricGroup);

        List<RegisteredMetric> initialErrorMeters =
                registry.metrics(MetricNames.ERRORS_RATE, "produceLog");
        assertThat(initialErrorMeters).hasSize(1);
        assertThat(initialErrorMeters.get(0).group.getAllVariables()).doesNotContainKey("error");
        assertThat(((Meter) initialErrorMeters.get(0).metric).getCount()).isZero();

        RequestsMetrics.Metrics metrics =
                requestsMetrics.getMetrics(ApiKeys.PRODUCE_LOG.id, false, false).get();
        metrics.markError(Errors.TABLE_NOT_EXIST);
        metrics.markError(Errors.TABLE_NOT_EXIST);
        metrics.markError(Errors.UNKNOWN_SERVER_ERROR);

        List<RegisteredMetric> errorMeters =
                registry.metrics(MetricNames.ERRORS_RATE, "produceLog");
        assertThat(errorMeters).hasSize(3);
        assertThat(errorMeters)
                .allSatisfy(
                        registered -> {
                            assertThat(registered.metricName).isEqualTo("errorsPerSecond");
                            assertThat(registered.group.getAllVariables())
                                    .containsEntry("request", "produceLog");
                            assertThat(registered.metric).isInstanceOf(Meter.class);
                        });
        assertThat(errorMeters)
                .anySatisfy(
                        registered -> {
                            assertThat(
                                            registered.group.getLogicalScope(
                                                    CharacterFilter.NO_OP_FILTER, '_'))
                                    .isEqualTo("tabletserver_request");
                            assertThat(registered.group.getAllVariables())
                                    .doesNotContainKey("error");
                            assertThat(((Meter) registered.metric).getCount()).isEqualTo(3);
                        });
        assertThat(errorMeters)
                .filteredOn(registered -> registered.group.getAllVariables().containsKey("error"))
                .allSatisfy(
                        registered -> {
                            assertThat(
                                            registered.group.getLogicalScope(
                                                    CharacterFilter.NO_OP_FILTER, '_'))
                                    .isEqualTo("tabletserver_request_error");
                        });
        assertThat(errorMeters)
                .anySatisfy(
                        registered -> {
                            assertThat(registered.group.getAllVariables())
                                    .containsEntry("error", Errors.TABLE_NOT_EXIST.name());
                            assertThat(((Meter) registered.metric).getCount()).isEqualTo(2);
                        });
        assertThat(errorMeters)
                .anySatisfy(
                        registered -> {
                            assertThat(registered.group.getAllVariables())
                                    .containsEntry("error", Errors.UNKNOWN_SERVER_ERROR.name());
                            assertThat(((Meter) registered.metric).getCount()).isEqualTo(1);
                        });
    }

    static class RecordingMetricRegistry implements MetricRegistry {

        private final List<RegisteredMetric> registeredMetrics = new ArrayList<>();

        @Override
        public int getNumberReporters() {
            return 0;
        }

        @Override
        public void register(Metric metric, String metricName, AbstractMetricGroup group) {
            registeredMetrics.add(new RegisteredMetric(metric, metricName, group));
        }

        @Override
        public void unregister(Metric metric, String metricName, AbstractMetricGroup group) {}

        @Override
        public CompletableFuture<Void> closeAsync() {
            return CompletableFuture.completedFuture(null);
        }

        List<RegisteredMetric> metrics(String metricName) {
            return registeredMetrics.stream()
                    .filter(metric -> metric.metricName.equals(metricName))
                    .collect(Collectors.toList());
        }

        List<RegisteredMetric> metrics(String metricName, String request) {
            return metrics(metricName).stream()
                    .filter(metric -> request.equals(metric.group.getAllVariables().get("request")))
                    .collect(Collectors.toList());
        }
    }

    static class RegisteredMetric {

        final Metric metric;
        final String metricName;
        final MetricGroup group;

        private RegisteredMetric(Metric metric, String metricName, MetricGroup group) {
            this.metric = metric;
            this.metricName = metricName;
            this.group = group;
        }
    }
}

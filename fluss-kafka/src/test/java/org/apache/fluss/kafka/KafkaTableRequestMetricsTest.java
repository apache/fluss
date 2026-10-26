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

package org.apache.fluss.kafka;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.kafka.mapping.KafkaTopicMapper;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.Meter;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.server.metrics.group.TableMetricGroup;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.fluss.utils.clock.ManualClock;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse;
import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponse;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests Produce request attribution to canonical tables and table metric lifecycles. */
class KafkaTableRequestMetricsTest {

    @Test
    void testMultipleTablesShareRequestDurationsButOnlyFailedTableCountsFailure() {
        try (Harness harness = new Harness()) {
            harness.addTable("orders", 1);
            harness.addTable("payments", 2);
            KafkaRequest request = harness.receive((short) 1, "orders", "payments");
            assertThat(harness.count(null, "requestsPerSecond")).isOne();
            assertThat(harness.count("orders", "requestsPerSecond")).isOne();
            assertThat(harness.count("payments", "requestsPerSecond")).isOne();
            assertThat(harness.inFlight("orders")).isOne();
            assertThat(harness.inFlight("payments")).isOne();
            harness.clock.advanceTime(Duration.ofMillis(5));
            request.processingStarted();
            harness.clock.advanceTime(Duration.ofMillis(9));
            request.complete(response("orders", "payments"));
            harness.clock.advanceTime(Duration.ofMillis(3));
            harness.channel.runPendingTasks();

            assertThat(harness.count(null, "failedRequestsPerSecond")).isOne();
            assertThat(harness.count("orders", "failedRequestsPerSecond")).isZero();
            assertThat(harness.count("payments", "failedRequestsPerSecond")).isOne();
            for (String table : Arrays.asList(null, "orders", "payments")) {
                assertThat(harness.inFlight(table)).isZero();
                harness.assertSamples(table, "requestQueueTimeMs", 5);
                harness.assertSamples(table, "requestProcessTimeMs", 9);
                harness.assertSamples(table, "responseQueueTimeMs", 3);
                harness.assertSamples(table, "totalTimeMs", 17);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    void testAliasesAndMultipleFailedPartitionsCountTableOnlyOnce(short acks) {
        try (Harness harness = new Harness()) {
            harness.addTable("orders", 1);
            KafkaRequest request = harness.receive(acks, "orders", "analytics.orders");
            request.processingStarted();
            request.complete(response("orders", "analytics.orders"));
            harness.channel.runPendingTasks();
            request.cancel();
            assertThat(harness.count("orders", "requestsPerSecond")).isOne();
            assertThat(harness.count("orders", "failedRequestsPerSecond")).isOne();
            assertThat(harness.inFlight("orders")).isZero();
            assertThat(harness.histogram("orders", "totalTimeMs").getCount()).isOne();
            assertThat(harness.histogram("orders", "responseQueueTimeMs").getCount())
                    .isEqualTo(acks == 0 ? 0 : 1);
        }
    }

    @Test
    void testDisconnectCountsEachAffectedTableOnceAndIgnoresLateCompletion() {
        try (Harness harness = new Harness()) {
            harness.addTable("orders", 1);
            harness.addTable("payments", 2);
            KafkaRequest request = harness.receive((short) 1, "orders", "payments");
            request.processingStarted();
            harness.clock.advanceTime(Duration.ofMillis(4));
            harness.channel.close();
            request.complete(response("orders", "payments"));
            request.cancel();
            harness.channel.runPendingTasks();
            for (String table : Arrays.asList(null, "orders", "payments")) {
                assertThat(harness.count(table, "failedRequestsPerSecond")).isOne();
                assertThat(harness.inFlight(table)).isZero();
                harness.assertSamples(table, "totalTimeMs", 4);
                assertThat(harness.histogram(table, "requestProcessTimeMs").getCount()).isZero();
            }
        }
    }

    @Test
    void testUnknownTableOnlyContributesToApiMetrics() {
        try (Harness harness = new Harness()) {
            KafkaRequest request = harness.receive((short) 1, "unknown");
            request.fail(new IllegalStateException("unknown table"));
            harness.channel.runPendingTasks();
            assertThat(harness.count(null, "requestsPerSecond")).isOne();
            assertThat(harness.count(null, "failedRequestsPerSecond")).isOne();
            assertThat(harness.registry.metrics.keySet())
                    .noneMatch(key -> key.startsWith("analytics."));
        }
    }

    @Test
    void testRemovingAndRecreatingTableUnregistersAndRecreatesMetrics() {
        try (Harness harness = new Harness()) {
            harness.addTable("orders", 1);
            KafkaRequest first = harness.receive((short) 0, "orders");
            first.processingStarted();
            first.complete(response("orders", null));
            harness.channel.runPendingTasks();
            assertThat(harness.count("orders", "requestsPerSecond")).isOne();
            harness.root.removeTableBucketMetricGroup(
                    TablePath.of("analytics", "orders"), new TableBucket(1, 0));
            assertThat(harness.registry.metrics.keySet())
                    .noneMatch(key -> key.startsWith("analytics.orders."));
            assertThat(harness.count(null, "requestsPerSecond")).isOne();

            harness.addTable("orders", 2);
            KafkaRequest second = harness.receive((short) 0, "orders");
            second.processingStarted();
            second.complete(response("orders", null));
            harness.channel.runPendingTasks();
            assertThat(harness.count("orders", "requestsPerSecond")).isOne();
            assertThat(harness.count(null, "requestsPerSecond")).isEqualTo(2);
        }
    }

    private static ProduceResponse response(String successfulTopic, String failedTopic) {
        ProduceResponseData data = new ProduceResponseData();
        data.responses()
                .add(
                        new TopicProduceResponse()
                                .setName(successfulTopic)
                                .setPartitionResponses(
                                        Arrays.asList(
                                                new PartitionProduceResponse()
                                                        .setIndex(0)
                                                        .setErrorCode(Errors.NONE.code()),
                                                new PartitionProduceResponse()
                                                        .setIndex(1)
                                                        .setErrorCode(Errors.NONE.code()))));
        if (failedTopic != null) {
            data.responses()
                    .add(
                            new TopicProduceResponse()
                                    .setName(failedTopic)
                                    .setPartitionResponses(
                                            Arrays.asList(
                                                    new PartitionProduceResponse()
                                                            .setIndex(0)
                                                            .setErrorCode(
                                                                    Errors.INVALID_RECORD.code()),
                                                    new PartitionProduceResponse()
                                                            .setIndex(1)
                                                            .setErrorCode(
                                                                    Errors.INVALID_RECORD
                                                                            .code()))));
        }
        return new ProduceResponse(data);
    }

    private static final class Harness implements AutoCloseable {
        private final RecordingRegistry registry = new RecordingRegistry();
        private final TabletServerMetricGroup root =
                new TabletServerMetricGroup(registry, "cluster", "rack", "host", 0);
        private final ManualClock clock = new ManualClock();
        private final RequestChannel requests = new RequestChannel(100);
        private final Map<TablePath, TableBucket> tables = new HashMap<>();
        private final EmbeddedChannel channel;

        private Harness() {
            Configuration conf = new Configuration();
            conf.set(ConfigOptions.KAFKA_DEFAULT_DATABASE, "analytics");
            KafkaTopicMapper mapper = new KafkaTopicMapper(conf);
            MetricGroup apiGroup = root.addGroup("kafka").addGroup("listener", "KAFKA");
            KafkaRequestMetrics metrics =
                    new KafkaRequestMetrics(
                            apiGroup,
                            topic -> {
                                TableMetricGroup table =
                                        root.getTableMetricGroup(mapper.toTablePath(topic));
                                return table == null
                                        ? null
                                        : table.addGroup("kafka")
                                                .addGroup("listener", "KAFKA")
                                                .addGroup("request", "PRODUCE");
                            },
                            clock);
            channel =
                    new EmbeddedChannel(new KafkaCommandDecoder(requests, "KAFKA", null, metrics));
        }

        private void addTable(String name, long id) {
            TablePath path = TablePath.of("analytics", name);
            TableBucket bucket = new TableBucket(id, 0);
            root.addTableBucketMetricGroup(PhysicalTablePath.of(path), bucket, false);
            tables.put(path, bucket);
        }

        private KafkaRequest receive(short acks, String... topics) {
            ProduceRequestData data = new ProduceRequestData().setAcks(acks).setTimeoutMs(1000);
            for (String topic : topics) {
                data.topicData()
                        .add(
                                new TopicProduceData()
                                        .setName(topic)
                                        .setPartitionData(
                                                Arrays.asList(
                                                        new PartitionProduceData()
                                                                .setIndex(0)
                                                                .setRecords(MemoryRecords.EMPTY),
                                                        new PartitionProduceData()
                                                                .setIndex(1)
                                                                .setRecords(MemoryRecords.EMPTY))));
            }
            ProduceRequest request = new ProduceRequest(data, (short) 3);
            RequestHeader header =
                    new RequestHeader(request.apiKey(), request.version(), "client", 1);
            channel.writeInbound(
                    Unpooled.wrappedBuffer(
                            RequestUtils.serialize(
                                    header.data(),
                                    header.headerVersion(),
                                    request.data(),
                                    request.version())));
            KafkaRequest result = (KafkaRequest) requests.pollRequest(0);
            assertThat(result).isNotNull();
            return result;
        }

        private Metric metric(String table, String name) {
            return registry.metrics.get(
                    (table == null ? "all" : "analytics." + table) + "." + name);
        }

        private long count(String table, String name) {
            return ((Meter) metric(table, name)).getCount();
        }

        private long inFlight(String table) {
            return ((Number) ((Gauge<?>) metric(table, "inFlightRequests")).getValue()).longValue();
        }

        private Histogram histogram(String table, String name) {
            return (Histogram) metric(table, name);
        }

        private void assertSamples(String table, String name, long... values) {
            assertThat(histogram(table, name).getStatistics().getValues())
                    .containsExactlyInAnyOrder(values);
        }

        @Override
        public void close() {
            channel.finishAndReleaseAll();
            tables.forEach(root::removeTableBucketMetricGroup);
            root.close();
        }
    }

    private static final class RecordingRegistry extends NOPMetricRegistry {
        private final Map<String, Metric> metrics = new HashMap<>();

        @Override
        public void register(Metric metric, String name, AbstractMetricGroup group) {
            if ("PRODUCE".equals(group.getAllVariables().get("request"))) {
                metrics.put(key(group, name), metric);
            }
        }

        @Override
        public void unregister(Metric metric, String name, AbstractMetricGroup group) {
            metrics.remove(key(group, name), metric);
        }

        private static String key(AbstractMetricGroup group, String name) {
            Map<String, String> variables = group.getAllVariables();
            String table = variables.get("table");
            return (table == null ? "all" : variables.get("database") + "." + table) + "." + name;
        }
    }
}

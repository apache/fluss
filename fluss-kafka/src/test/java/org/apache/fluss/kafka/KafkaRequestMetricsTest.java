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
import org.apache.fluss.metrics.registry.NOPMetricRegistry;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.server.metrics.group.TableMetricGroup;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.fluss.shaded.netty4.io.netty.util.ReferenceCountUtil;
import org.apache.fluss.utils.clock.ManualClock;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Core request timing, table attribution, and cleanup checks through the Kafka pipeline. */
class KafkaRequestMetricsTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTimingAndWriteCompletion(boolean writeSucceeds) {
        try (Harness harness = new Harness()) {
            KafkaRequest request =
                    harness.receive(new ApiVersionsRequest.Builder().build((short) 0));
            assertThat(harness.count("API_VERSIONS", "requestsPerSecond")).isOne();
            assertThat(harness.inFlight("API_VERSIONS")).isOne();
            harness.advance(5);
            request.processingStarted();
            harness.advance(11);
            request.complete(new ApiVersionsResponse(new ApiVersionsResponseData()));
            harness.advance(7);
            harness.channel.runPendingTasks();
            assertThat(harness.inFlight("API_VERSIONS")).isOne();
            harness.assertSamples("API_VERSIONS", "totalTimeMs");
            harness.advance(13);
            harness.write.complete(writeSucceeds);
            harness.channel.close();

            harness.assertSamples("API_VERSIONS", "requestQueueTimeMs", 5);
            harness.assertSamples("API_VERSIONS", "requestProcessTimeMs", 11);
            harness.assertSamples("API_VERSIONS", "responseQueueTimeMs", 7);
            harness.assertSamples("API_VERSIONS", "totalTimeMs", 36);
            assertThat(harness.count("API_VERSIONS", "failedRequestsPerSecond"))
                    .isEqualTo(writeSucceeds ? 0 : 1);
            assertThat(harness.inFlight("API_VERSIONS")).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    void testTableAliasesAndPartialFailure(short acks) {
        try (Harness harness = new Harness()) {
            harness.addTable("orders", 1);
            harness.addTable("payments", 2);
            KafkaRequest request =
                    harness.receive(produce(acks, "orders", "analytics.orders", "payments"));
            harness.advance(5);
            request.processingStarted();
            harness.advance(9);
            request.complete(response("payments", "orders", "analytics.orders", "payments"));
            assertThat(harness.inFlight("PRODUCE")).isEqualTo(acks == 0 ? 0 : 1);
            harness.advance(3);
            harness.channel.runPendingTasks();
            if (acks != 0) {
                harness.write.complete(true);
            }
            request.cancel();

            assertThat(harness.count("PRODUCE", "failedRequestsPerSecond")).isOne();
            assertThat(harness.count("analytics.orders", "failedRequestsPerSecond")).isZero();
            assertThat(harness.count("analytics.payments", "failedRequestsPerSecond")).isOne();
            for (String scope :
                    Arrays.asList("PRODUCE", "analytics.orders", "analytics.payments")) {
                assertThat(harness.metrics.get(scope)).hasSize(7);
                assertThat(harness.count(scope, "requestsPerSecond")).isOne();
                assertThat(harness.inFlight(scope)).isZero();
                harness.assertSamples(scope, "requestQueueTimeMs", 5);
                harness.assertSamples(scope, "requestProcessTimeMs", 9);
                harness.assertSamples(
                        scope, "responseQueueTimeMs", acks == 0 ? new long[0] : new long[] {3});
                harness.assertSamples(scope, "totalTimeMs", acks == 0 ? 14 : 17);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDisconnectIgnoresLateCompletion(boolean processing) {
        try (Harness harness = new Harness()) {
            harness.addTable("orders", 1);
            KafkaRequest request = harness.receive(produce((short) 1, "orders"));
            if (processing) {
                request.processingStarted();
            }
            harness.advance(4);
            harness.channel.close();
            request.processingStarted();
            request.complete(response(null, "orders"));
            request.cancel();
            harness.channel.runPendingTasks();

            for (String scope : Arrays.asList("PRODUCE", "analytics.orders")) {
                assertThat(harness.count(scope, "failedRequestsPerSecond")).isOne();
                assertThat(harness.inFlight(scope)).isZero();
                harness.assertSamples(scope, "totalTimeMs", 4);
                harness.assertSamples(scope, "requestProcessTimeMs");
                harness.assertSamples(scope, "responseQueueTimeMs");
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testProcessingAndSerializationFailures(boolean serializationFailure) {
        try (Harness harness = new Harness()) {
            KafkaRequest request =
                    harness.receive(new ApiVersionsRequest.Builder().build((short) 0));
            request.processingStarted();
            if (serializationFailure) {
                request.complete(
                        new ApiVersionsResponse(new ApiVersionsResponseData()) {
                            @Override
                            public ApiVersionsResponseData data() {
                                throw new IllegalStateException("test serialization failure");
                            }
                        });
            } else {
                request.fail(new IOException("test processing failure"));
            }
            harness.channel.runPendingTasks();
            harness.write.complete(true);
            request.cancel();
            assertThat(harness.count("API_VERSIONS", "failedRequestsPerSecond")).isOne();
            assertThat(harness.inFlight("API_VERSIONS")).isZero();
            harness.assertSamples("API_VERSIONS", "totalTimeMs", 0);
        }
    }

    @Test
    void testTableMetricsAreRemovedAndRecreated() {
        try (Harness harness = new Harness()) {
            harness.addTable("orders", 1);
            KafkaRequest first = harness.receive(produce((short) 0, "orders"));
            first.complete(response(null, "orders"));
            harness.root.removeTableBucketMetricGroup(
                    TablePath.of("analytics", "orders"), new TableBucket(1, 0));
            assertThat(harness.metrics.get("analytics.orders")).isEmpty();

            harness.addTable("orders", 2);
            KafkaRequest second = harness.receive(produce((short) 0, "orders", "unknown"));
            second.complete(response(null, "orders", "unknown"));
            assertThat(harness.count("analytics.orders", "requestsPerSecond")).isOne();
            assertThat(harness.count("PRODUCE", "requestsPerSecond")).isEqualTo(2);
            assertThat(harness.metrics).doesNotContainKey("analytics.unknown");
        }
    }

    private static ProduceRequest produce(short acks, String... topics) {
        ProduceRequestData data = new ProduceRequestData().setAcks(acks).setTimeoutMs(1000);
        for (String topic : topics) {
            data.topicData()
                    .add(
                            new ProduceRequestData.TopicProduceData()
                                    .setName(topic)
                                    .setPartitionData(
                                            Collections.singletonList(
                                                    new ProduceRequestData.PartitionProduceData()
                                                            .setIndex(0)
                                                            .setRecords(MemoryRecords.EMPTY))));
        }
        return new ProduceRequest(data, (short) 3);
    }

    private static ProduceResponse response(String failedTopic, String... topics) {
        ProduceResponseData data = new ProduceResponseData();
        for (String topic : topics) {
            data.responses()
                    .add(
                            new ProduceResponseData.TopicProduceResponse()
                                    .setName(topic)
                                    .setPartitionResponses(
                                            Collections.singletonList(
                                                    new ProduceResponseData
                                                                    .PartitionProduceResponse()
                                                            .setIndex(0)
                                                            .setErrorCode(
                                                                    topic.equals(failedTopic)
                                                                            ? Errors.INVALID_RECORD
                                                                                    .code()
                                                                            : Errors.NONE
                                                                                    .code()))));
        }
        return new ProduceResponse(data);
    }

    private static final class Harness implements AutoCloseable {
        private final Map<String, Map<String, Metric>> metrics = new HashMap<>();
        private final TabletServerMetricGroup root =
                new TabletServerMetricGroup(
                        new NOPMetricRegistry() {
                            @Override
                            public void register(
                                    Metric metric, String name, AbstractMetricGroup group) {
                                if (group.getAllVariables().containsKey("request")) {
                                    metrics.computeIfAbsent(
                                                    scope(group), ignored -> new HashMap<>())
                                            .put(name, metric);
                                }
                            }

                            @Override
                            public void unregister(
                                    Metric metric, String name, AbstractMetricGroup group) {
                                if (group.getAllVariables().containsKey("request")) {
                                    metrics.get(scope(group)).remove(name, metric);
                                }
                            }
                        },
                        "cluster",
                        "rack",
                        "host",
                        0);
        private final ManualClock clock = new ManualClock();
        private final RequestChannel requests = new RequestChannel(100);
        private final PendingWrite write = new PendingWrite();
        private final EmbeddedChannel channel;

        private Harness() {
            Configuration conf = new Configuration();
            conf.set(ConfigOptions.KAFKA_DEFAULT_DATABASE, "analytics");
            KafkaTopicMapper mapper = new KafkaTopicMapper(conf);
            KafkaRequestMetrics requestMetrics =
                    new KafkaRequestMetrics(
                            root.addGroup("kafka").addGroup("listener", "KAFKA"),
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
                    new EmbeddedChannel(
                            write,
                            new KafkaCommandDecoder(requests, "KAFKA", null, requestMetrics));
        }

        private static String scope(AbstractMetricGroup group) {
            Map<String, String> variables = group.getAllVariables();
            return variables.containsKey("table")
                    ? variables.get("database") + "." + variables.get("table")
                    : variables.get("request");
        }

        private void addTable(String name, long id) {
            root.addTableBucketMetricGroup(
                    PhysicalTablePath.of(TablePath.of("analytics", name)),
                    new TableBucket(id, 0),
                    false);
        }

        private KafkaRequest receive(AbstractRequest request) {
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

        private void advance(long millis) {
            clock.advanceTime(Duration.ofMillis(millis));
        }

        private long count(String scope, String name) {
            return ((Meter) metrics.get(scope).get(name)).getCount();
        }

        private long inFlight(String scope) {
            return ((Number) ((Gauge<?>) metrics.get(scope).get("inFlightRequests")).getValue())
                    .longValue();
        }

        private void assertSamples(String scope, String name, long... values) {
            assertThat(((Histogram) metrics.get(scope).get(name)).getStatistics().getValues())
                    .containsExactlyInAnyOrder(values);
        }

        @Override
        public void close() {
            if (write.promise != null && !write.promise.isDone()) {
                write.complete(false);
            }
            channel.finishAndReleaseAll();
            root.close();
        }
    }

    private static final class PendingWrite extends ChannelOutboundHandlerAdapter {
        private Object message;
        private ChannelPromise promise;

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
            this.message = msg;
            this.promise = promise;
        }

        private void complete(boolean success) {
            ReferenceCountUtil.release(message);
            message = null;
            if (success) {
                promise.setSuccess();
            } else {
                promise.setFailure(new IOException("test write failure"));
            }
        }
    }
}

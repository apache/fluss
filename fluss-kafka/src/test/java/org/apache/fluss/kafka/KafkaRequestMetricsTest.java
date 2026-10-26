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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.kafka.backend.metadata.KafkaClusterMetadata;
import org.apache.fluss.kafka.mapping.KafkaTopicMapper;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.Meter;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.groups.GenericMetricGroup;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;
import org.apache.fluss.metrics.util.TestMetricGroup;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.fluss.shaded.netty4.io.netty.util.ReferenceCountUtil;
import org.apache.fluss.utils.clock.ManualClock;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse;
import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests metric boundaries through the Kafka request and response pipeline. */
class KafkaRequestMetricsTest {

    @Test
    void testLatencyIncludesAsyncProcessingResponseQueueAndPendingWrite() {
        try (Harness harness = new Harness(true)) {
            KafkaRequest request = harness.receive(apiVersions());
            assertThat(harness.count("requestsPerSecond")).isOne();
            assertThat(harness.inFlight()).isOne();
            harness.advance(5);
            request.processingStarted();
            harness.advance(11);
            request.complete(success());
            assertThat(harness.histogram("requestProcessTimeMs").getCount()).isOne();
            harness.advance(7);
            harness.channel.runPendingTasks();
            assertThat(harness.inFlight()).isOne();
            assertThat(harness.histogram("totalTimeMs").getCount()).isZero();
            harness.advance(13);
            harness.write.complete(true);

            harness.assertSamples("requestQueueTimeMs", 5);
            harness.assertSamples("requestProcessTimeMs", 11);
            harness.assertSamples("responseQueueTimeMs", 7);
            harness.assertSamples("totalTimeMs", 36);
            assertThat(harness.count("failedRequestsPerSecond")).isZero();
            assertThat(harness.inFlight()).isZero();
        }
    }

    @Test
    void testOrderedResponsesIncludeHeadOfLineWaiting() {
        try (Harness harness = new Harness(false)) {
            KafkaRequest first = harness.receive(apiVersions());
            KafkaRequest second = harness.receive(apiVersions());
            first.processingStarted();
            second.processingStarted();
            harness.advance(2);
            second.complete(success());
            harness.channel.runPendingTasks();
            assertThat(harness.channel.outboundMessages()).isEmpty();
            harness.advance(8);
            first.complete(success());
            harness.channel.runPendingTasks();

            harness.assertSamples("responseQueueTimeMs", 0, 8);
            harness.assertSamples("requestProcessTimeMs", 2, 10);
            harness.assertSamples("totalTimeMs", 10, 10);
            assertThat(harness.channel.outboundMessages()).hasSize(2);
            assertThat(harness.inFlight()).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    void testPartialProduceFailureCountsOneFailedRequest(short acks) {
        try (Harness harness = new Harness(false)) {
            KafkaRequest request = harness.receive(produce(acks));
            request.processingStarted();
            request.complete(partialFailure());
            // A repeated completion or a subsequent disconnect must not count another failure.
            request.fail(new InvalidRequestException("late failure"));
            harness.channel.runPendingTasks();
            harness.channel.close();

            assertThat(harness.count("requestsPerSecond")).isOne();
            assertThat(harness.count("failedRequestsPerSecond")).isOne();
            assertThat(harness.inFlight()).isZero();
            assertThat(harness.histogram("totalTimeMs").getCount()).isOne();
            assertThat(harness.histogram("responseQueueTimeMs").getCount())
                    .isEqualTo(acks == 0 ? 0 : 1);
        }
    }

    @Test
    void testAcksZeroFinishesAtBusinessCompletionWhileEarlierResponseIsBlocked() {
        try (Harness harness = new Harness(false)) {
            KafkaRequest first = harness.receive(produce((short) 1));
            KafkaRequest second = harness.receive(produce((short) 0));
            first.processingStarted();
            second.processingStarted();
            harness.advance(3);
            second.complete(new ProduceResponse(new ProduceResponseData()));
            assertThat(harness.inFlight()).isOne();
            harness.assertSamples("totalTimeMs", 3);
            assertThat(harness.histogram("responseQueueTimeMs").getCount()).isZero();
            harness.advance(7);
            first.complete(new ProduceResponse(new ProduceResponseData()));
            harness.channel.runPendingTasks();

            harness.assertSamples("totalTimeMs", 3, 10);
            harness.assertSamples("responseQueueTimeMs", 0);
            assertThat(harness.channel.outboundMessages()).hasSize(1);
            assertThat(harness.inFlight()).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDisconnectEndsRequestOnlyOnce(boolean processing) {
        try (Harness harness = new Harness(false)) {
            KafkaRequest request = harness.receive(apiVersions());
            if (processing) {
                request.processingStarted();
            }
            harness.advance(4);
            harness.channel.close();
            harness.advance(5);
            request.processingStarted();
            request.complete(success());
            request.cancel();
            harness.channel.runPendingTasks();

            assertThat(harness.inFlight()).isZero();
            assertThat(harness.count("failedRequestsPerSecond")).isOne();
            harness.assertSamples("totalTimeMs", 4);
            assertThat(harness.histogram("requestProcessTimeMs").getCount()).isZero();
            assertThat(harness.histogram("responseQueueTimeMs").getCount()).isZero();
        }
    }

    @Test
    void testFailedWriteEndsRequestAndClosesConnection() {
        try (Harness harness = new Harness(true)) {
            KafkaRequest request = harness.receive(apiVersions());
            request.processingStarted();
            request.complete(success());
            harness.channel.runPendingTasks();
            harness.write.complete(false);
            harness.channel.runPendingTasks();

            assertThat(harness.channel.isActive()).isFalse();
            assertThat(harness.inFlight()).isZero();
            assertThat(harness.count("failedRequestsPerSecond")).isOne();
            assertThat(harness.histogram("totalTimeMs").getCount()).isOne();
        }
    }

    @Test
    void testSerializationFallbackCountsFailure() {
        try (Harness harness = new Harness(false)) {
            KafkaRequest request = harness.receive(apiVersions());
            request.processingStarted();
            request.complete(
                    new ApiVersionsResponse(new ApiVersionsResponseData()) {
                        @Override
                        public ApiVersionsResponseData data() {
                            throw new IllegalStateException("test serialization failure");
                        }

                        @Override
                        public Map<Errors, Integer> errorCounts() {
                            return Collections.singletonMap(Errors.NONE, 1);
                        }
                    });
            harness.channel.runPendingTasks();
            assertThat(harness.count("failedRequestsPerSecond")).isOne();
            assertThat(harness.inFlight()).isZero();
            assertThat(harness.channel.outboundMessages()).hasSize(1);
        }
    }

    @Test
    void testExceptionalCompletionIsCounted() {
        try (Harness harness = new Harness(false)) {
            KafkaRequest request = harness.receive(apiVersions());
            request.processingStarted();
            request.fail(new InvalidRequestException("invalid request"));
            harness.channel.runPendingTasks();
            assertThat(harness.count("failedRequestsPerSecond")).isOne();
            assertThat(harness.inFlight()).isZero();
            assertThat(harness.channel.outboundMessages()).hasSize(1);
        }
    }

    @Test
    void testHandlerTracksAsyncBackendFailureMappedToResponse() throws Exception {
        CompletableFuture<KafkaClusterMetadata> result = new CompletableFuture<>();
        try (Harness harness = new Harness(false);
                KafkaRequestHandler handler =
                        new KafkaRequestHandler(
                                new KafkaTopicMapper(new Configuration()),
                                query -> result,
                                command -> {
                                    throw new AssertionError("Unexpected produce");
                                })) {
            KafkaRequest request =
                    harness.receive(
                            new MetadataRequest.Builder(Collections.singletonList("topic"), false)
                                    .build((short) 4));
            harness.advance(5);
            handler.processRequest(request);
            assertThat(request.future()).isNotDone();
            assertThat(harness.histogram("requestProcessTimeMs").getCount()).isZero();
            harness.advance(9);
            result.completeExceptionally(new InvalidRequestException("backend failure"));
            harness.channel.runPendingTasks();

            harness.assertSamples("requestQueueTimeMs", 5);
            harness.assertSamples("requestProcessTimeMs", 9);
            assertThat(harness.count("failedRequestsPerSecond")).isOne();
            assertThat(harness.inFlight()).isZero();
        }
    }

    @Test
    void testRegistersOnlySelectedMetricsAndSeparatesListenersAndApis() {
        List<Map<String, String>> variables = new ArrayList<>();
        List<String> names = new ArrayList<>();
        NOPMetricRegistry registry =
                new NOPMetricRegistry() {
                    @Override
                    public void register(Metric metric, String name, AbstractMetricGroup group) {
                        variables.add(group.getAllVariables());
                        names.add(name);
                    }
                };
        MetricGroup root = new GenericMetricGroup(registry, null, "tabletserver");
        try {
            KafkaRequestMetrics internal =
                    new KafkaRequestMetrics(
                            root.addGroup("kafka").addGroup("listener", "INTERNAL"));
            KafkaRequestMetrics external =
                    new KafkaRequestMetrics(
                            root.addGroup("kafka").addGroup("listener", "EXTERNAL"));
            internal.startRequest(ApiKeys.PRODUCE, internal.nanoTime()).finish(false);
            internal.startRequest(ApiKeys.PRODUCE, internal.nanoTime()).finish(false);
            internal.startRequest(ApiKeys.METADATA, internal.nanoTime()).finish(false);
            external.startRequest(ApiKeys.PRODUCE, external.nanoTime()).finish(false);
            assertThat(names).hasSize(21);
            assertThat(names)
                    .containsOnly(
                            "requestsPerSecond",
                            "failedRequestsPerSecond",
                            "requestQueueTimeMs",
                            "requestProcessTimeMs",
                            "responseQueueTimeMs",
                            "totalTimeMs",
                            "inFlightRequests");
            assertThat(variables.subList(0, 7))
                    .allSatisfy(
                            values ->
                                    assertThat(values)
                                            .containsEntry("listener", "INTERNAL")
                                            .containsEntry("request", "PRODUCE"));
            assertThat(variables.subList(7, 14))
                    .allSatisfy(values -> assertThat(values).containsEntry("request", "METADATA"));
            assertThat(variables.subList(14, 21))
                    .allSatisfy(
                            values ->
                                    assertThat(values)
                                            .containsEntry("listener", "EXTERNAL")
                                            .containsEntry("request", "PRODUCE"));
        } finally {
            root.close();
        }
    }

    private static ApiVersionsRequest apiVersions() {
        return new ApiVersionsRequest.Builder().build((short) 0);
    }

    private static ApiVersionsResponse success() {
        return new ApiVersionsResponse(new ApiVersionsResponseData());
    }

    private static ProduceRequest produce(short acks) {
        return new ProduceRequest(
                new ProduceRequestData().setAcks(acks).setTimeoutMs(1000), (short) 3);
    }

    private static ProduceResponse partialFailure() {
        ProduceResponseData data = new ProduceResponseData();
        data.responses()
                .add(
                        new TopicProduceResponse()
                                .setName("topic")
                                .setPartitionResponses(
                                        Arrays.asList(
                                                new PartitionProduceResponse()
                                                        .setIndex(0)
                                                        .setErrorCode(Errors.NONE.code()),
                                                new PartitionProduceResponse()
                                                        .setIndex(1)
                                                        .setErrorCode(
                                                                Errors.NOT_LEADER_OR_FOLLOWER
                                                                        .code()),
                                                new PartitionProduceResponse()
                                                        .setIndex(2)
                                                        .setErrorCode(
                                                                Errors.UNKNOWN_TOPIC_OR_PARTITION
                                                                        .code()))));
        return new ProduceResponse(data);
    }

    private static final class Harness implements AutoCloseable {
        private final ManualClock clock = new ManualClock();
        private final TestMetricGroup group = TestMetricGroup.createTestMetricGroup();
        private final RequestChannel requests = new RequestChannel(100);
        private final PendingWrite write = new PendingWrite();
        private final EmbeddedChannel channel;
        private int correlationId;

        private Harness(boolean delayWrites) {
            KafkaCommandDecoder decoder =
                    new KafkaCommandDecoder(
                            requests, "KAFKA", null, new KafkaRequestMetrics(group, clock));
            channel =
                    delayWrites
                            ? new EmbeddedChannel(write, decoder)
                            : new EmbeddedChannel(decoder);
        }

        private KafkaRequest receive(AbstractRequest request) {
            RequestHeader header =
                    new RequestHeader(
                            request.apiKey(), request.version(), "client", correlationId++);
            ByteBuf bytes =
                    Unpooled.wrappedBuffer(
                            RequestUtils.serialize(
                                    header.data(),
                                    header.headerVersion(),
                                    request.data(),
                                    request.version()));
            channel.writeInbound(bytes);
            KafkaRequest received = (KafkaRequest) requests.pollRequest(0);
            assertThat(received).isNotNull();
            return received;
        }

        private void advance(long millis) {
            clock.advanceTime(Duration.ofMillis(millis));
        }

        private long count(String name) {
            return ((Meter) group.getMetric(name)).getCount();
        }

        private long inFlight() {
            return ((Number) ((Gauge<?>) group.getMetric("inFlightRequests")).getValue())
                    .longValue();
        }

        private Histogram histogram(String name) {
            return (Histogram) group.getMetric(name);
        }

        private void assertSamples(String name, long... values) {
            assertThat(histogram(name).getStatistics().getValues())
                    .containsExactlyInAnyOrder(values);
        }

        @Override
        public void close() {
            if (write.promise != null && !write.promise.isDone()) {
                write.complete(false);
            }
            channel.finishAndReleaseAll();
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

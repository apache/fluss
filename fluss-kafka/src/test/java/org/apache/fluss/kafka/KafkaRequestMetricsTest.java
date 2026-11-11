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

import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.rpc.netty.server.RequestsMetrics;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.fluss.shaded.netty4.io.netty.util.ReferenceCountUtil;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.fluss.rpc.netty.server.KafkaMetricsTestUtils.createRequestMetrics;
import static org.apache.fluss.rpc.netty.server.RequestsMetrics.KAFKA_METADATA_REQUEST_NAME;
import static org.apache.fluss.rpc.netty.server.RequestsMetrics.KAFKA_PRODUCE_REQUEST_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests request metric updates through the Kafka pipeline. */
class KafkaRequestMetricsTest {

    @Test
    void testApiVersionsDoesNotUpdateRequestMetrics() {
        try (Harness harness = new Harness()) {
            ApiVersionsRequest apiVersions = new ApiVersionsRequest.Builder().build((short) 0);
            harness.receive(apiVersions)
                    .complete(new ApiVersionsResponse(new ApiVersionsResponseData()));
            harness.receive(apiVersions).fail(new IOException("test processing failure"));
            harness.channel.runPendingTasks();

            assertThat(harness.channel.outboundMessages()).hasSize(2);
            for (String name :
                    Arrays.asList(KAFKA_PRODUCE_REQUEST_NAME, KAFKA_METADATA_REQUEST_NAME)) {
                RequestsMetrics.Metrics metrics = harness.metrics(name);
                assertThat(metrics.getRequestsCount().getCount()).isZero();
                assertThat(metrics.getErrorsCount().getCount()).isZero();
                assertThat(metrics.getTotalTimeMs().getCount()).isZero();
            }
        }
    }

    @Test
    void testRecordsAtResponseSubmission() {
        try (Harness harness = new Harness()) {
            PendingWrite write = new PendingWrite();
            harness.channel.pipeline().addFirst(write);
            KafkaRequest request = harness.receive(metadataRequest());
            RequestsMetrics.Metrics metrics = harness.metrics(KAFKA_METADATA_REQUEST_NAME);
            assertThat(metrics.getRequestsCount().getCount()).isZero();

            request.complete(
                    new MetadataResponse(new MetadataResponseData(), request.apiVersion()));
            harness.channel.runPendingTasks();

            assertThat(write.promise.isDone()).isFalse();
            assertThat(metrics.getRequestsCount().getCount()).isOne();
            assertThat(metrics.getRequestBytes().getStatistics().getValues())
                    .containsExactly(request.requestBytes());
            assertThat(metrics.getRequestQueueTimeMs().getCount()).isOne();
            assertThat(metrics.getRequestProcessTimeMs().getCount()).isOne();
            assertThat(metrics.getResponseSendTimeMs().getCount()).isOne();
            assertThat(metrics.getTotalTimeMs().getCount()).isOne();

            write.promise.setFailure(new IOException("test write failure"));
            assertThat(metrics.getRequestsCount().getCount()).isOne();
            assertThat(metrics.getErrorsCount().getCount()).isZero();
            assertThat(harness.metrics(KAFKA_PRODUCE_REQUEST_NAME).getRequestsCount().getCount())
                    .isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    void testProduceSuccessAndPartitionErrors(short acks) {
        try (Harness harness = new Harness()) {
            RequestsMetrics.Metrics metrics = harness.metrics(KAFKA_PRODUCE_REQUEST_NAME);
            KafkaRequest success = harness.receive(produce(acks));
            success.complete(response(Errors.NONE));
            harness.channel.runPendingTasks();
            assertThat(metrics.getRequestsCount().getCount()).isOne();
            assertThat(harness.channel.outboundMessages()).hasSize(acks == 0 ? 0 : 1);

            KafkaRequest failure = harness.receive(produce(acks));
            failure.complete(response(Errors.INVALID_RECORD));
            harness.channel.runPendingTasks();
            assertThat(metrics.getErrorsCount().getCount()).isOne();
            assertThat(metrics.getRequestsCount().getCount()).isOne();
            assertThat(metrics.getTotalTimeMs().getCount()).isOne();
        }
    }

    @Test
    void testFirstCompletionDeterminesMetricsForUnacknowledgedProduce() {
        try (Harness harness = new Harness()) {
            RequestsMetrics.Metrics metrics = harness.metrics(KAFKA_PRODUCE_REQUEST_NAME);
            KafkaRequest success = harness.receive(produce((short) 0));
            success.complete(response(Errors.NONE));
            success.fail(new IOException("channel inactive"));
            harness.channel.runPendingTasks();
            assertThat(metrics.getRequestsCount().getCount()).isOne();
            assertThat(metrics.getErrorsCount().getCount()).isZero();

            KafkaRequest failure = harness.receive(produce((short) 0));
            failure.fail(new IOException("channel inactive"));
            failure.complete(response(Errors.NONE));
            harness.channel.runPendingTasks();
            assertThat(metrics.getRequestsCount().getCount()).isOne();
            assertThat(metrics.getErrorsCount().getCount()).isOne();
            assertThat(metrics.getTotalTimeMs().getCount()).isOne();
        }
    }

    @Test
    void testProcessingFailureCountsAsRequestError() {
        try (Harness harness = new Harness()) {
            KafkaRequest request = harness.receive(metadataRequest());
            request.fail(new IOException("test processing failure"));
            harness.channel.runPendingTasks();
            RequestsMetrics.Metrics metrics = harness.metrics(KAFKA_METADATA_REQUEST_NAME);
            assertThat(metrics.getErrorsCount().getCount()).isOne();
            assertThat(metrics.getRequestsCount().getCount()).isZero();
            assertThat(metrics.getTotalTimeMs().getCount()).isZero();
        }
    }

    @Test
    void testSerializationFailureAfterSuccessfulProcessing() {
        try (Harness harness = new Harness()) {
            KafkaRequest request = harness.receive(metadataRequest());
            request.complete(
                    new MetadataResponse(new MetadataResponseData(), request.apiVersion()) {
                        @Override
                        public MetadataResponseData data() {
                            throw new IllegalStateException("test serialization failure");
                        }
                    });
            harness.channel.runPendingTasks();

            ByteBuf buffer = harness.channel.readOutbound();
            try {
                AbstractResponse response =
                        AbstractResponse.parseResponse(buffer.nioBuffer(), request.header());
                assertThat(response.errorCounts()).containsEntry(Errors.UNKNOWN_SERVER_ERROR, 1);
            } finally {
                buffer.release();
            }
            RequestsMetrics.Metrics metrics = harness.metrics(KAFKA_METADATA_REQUEST_NAME);
            assertThat(metrics.getErrorsCount().getCount()).isZero();
            assertThat(metrics.getRequestsCount().getCount()).isOne();
            assertThat(metrics.getTotalTimeMs().getCount()).isOne();
        }
    }

    @Test
    void testOrderedResponsesAndCancellation() {
        try (Harness harness = new Harness()) {
            KafkaRequest first = harness.receive(produce((short) 0));
            KafkaRequest second = harness.receive(metadataRequest());
            RequestsMetrics.Metrics metadataMetrics = harness.metrics(KAFKA_METADATA_REQUEST_NAME);
            RequestsMetrics.Metrics produceMetrics = harness.metrics(KAFKA_PRODUCE_REQUEST_NAME);
            second.complete(new MetadataResponse(new MetadataResponseData(), second.apiVersion()));
            harness.channel.runPendingTasks();
            assertThat(metadataMetrics.getRequestsCount().getCount()).isZero();

            first.complete(response(Errors.NONE));
            harness.channel.runPendingTasks();
            assertThat(metadataMetrics.getRequestsCount().getCount()).isOne();
            assertThat(produceMetrics.getRequestsCount().getCount()).isOne();

            KafkaRequest cancelled = harness.receive(produce((short) 1));
            harness.channel.close();
            cancelled.complete(response(Errors.NONE));
            harness.channel.runPendingTasks();
            assertThat(produceMetrics.getRequestsCount().getCount()).isOne();
            assertThat(produceMetrics.getErrorsCount().getCount()).isZero();
        }
    }

    private static MetadataRequest metadataRequest() {
        return new MetadataRequest.Builder(Collections.singletonList("orders"), false)
                .build((short) 9);
    }

    private static ProduceRequest produce(short acks) {
        ProduceRequestData data = new ProduceRequestData().setAcks(acks).setTimeoutMs(1000);
        data.topicData()
                .add(
                        new ProduceRequestData.TopicProduceData()
                                .setName("orders")
                                .setPartitionData(
                                        Collections.singletonList(
                                                new ProduceRequestData.PartitionProduceData()
                                                        .setIndex(0)
                                                        .setRecords(MemoryRecords.EMPTY))));
        return new ProduceRequest(data, (short) 3);
    }

    private static ProduceResponse response(Errors error) {
        ProduceResponseData data = new ProduceResponseData();
        data.responses()
                .add(
                        new ProduceResponseData.TopicProduceResponse()
                                .setName("orders")
                                .setPartitionResponses(
                                        Arrays.asList(
                                                new ProduceResponseData.PartitionProduceResponse()
                                                        .setIndex(0)
                                                        .setErrorCode(error.code()),
                                                new ProduceResponseData.PartitionProduceResponse()
                                                        .setIndex(1)
                                                        .setErrorCode(error.code()))));
        return new ProduceResponse(data);
    }

    private static final class Harness implements AutoCloseable {
        private final RequestsMetrics requestMetrics = createRequestMetrics();
        private final RequestChannel requests = new RequestChannel(100);
        private final EmbeddedChannel channel =
                new EmbeddedChannel(
                        new KafkaCommandDecoder(requests, "KAFKA", null, requestMetrics));

        private KafkaRequest receive(AbstractRequest request) {
            RequestHeader header =
                    new RequestHeader(request.apiKey(), request.version(), "client", 1);
            ByteBuffer serialized = request.serializeWithHeader(header);
            int requestSize = serialized.remaining();
            channel.writeInbound(Unpooled.wrappedBuffer(serialized));
            KafkaRequest result = (KafkaRequest) requests.pollRequest(0);
            assertThat(result).isNotNull();
            assertThat(result.requestBytes()).isEqualTo(requestSize);
            result.setRequestDequeTimeMs(System.currentTimeMillis());
            return result;
        }

        private RequestsMetrics.Metrics metrics(String requestName) {
            return requestMetrics.getMetrics(requestName).get();
        }

        @Override
        public void close() {
            channel.finishAndReleaseAll();
        }
    }

    private static final class PendingWrite extends ChannelOutboundHandlerAdapter {
        private ChannelPromise promise;

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
            ReferenceCountUtil.release(msg);
            this.promise = promise;
        }
    }
}

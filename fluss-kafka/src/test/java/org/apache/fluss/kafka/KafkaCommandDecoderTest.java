/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.LengthFieldPrepender;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** Tests response ordering and ownership in {@link KafkaCommandDecoder}. */
public class KafkaCommandDecoderTest {

    @Test
    public void testApiVersionsDuringAuthenticationIsFlushedBeforeChannelCloses() throws Exception {
        RequestChannel requestChannel = new RequestChannel(100);
        TestingTabletGatewayService service = new TestingTabletGatewayService();
        KafkaRequestHandler requestHandler = new KafkaRequestHandler(service, service, "kafka");
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new LengthFieldPrepender(4),
                        new KafkaCommandDecoder(
                                new RequestChannel[] {requestChannel},
                                "KAFKA",
                                () -> mock(ServerAuthenticator.class)));

        short handshakeVersion = 1;
        RequestHeader handshakeHeader =
                new RequestHeader(ApiKeys.SASL_HANDSHAKE, handshakeVersion, "client", 16);
        SaslHandshakeRequest handshakeRequest =
                new SaslHandshakeRequest(
                        new SaslHandshakeRequestData().setMechanism("PLAIN"), handshakeVersion);
        ByteBuf handshakeBuffer = serialize(handshakeHeader, handshakeRequest);

        short unsupportedVersion = (short) (ApiKeys.API_VERSIONS.latestVersion() + 1);
        RequestHeader apiVersionsHeader =
                new RequestHeader(ApiKeys.API_VERSIONS, unsupportedVersion, "client", 17);
        ByteBuf apiVersionsBuffer = serializeHeaderOnly(apiVersionsHeader);

        try {
            channel.writeInbound(handshakeBuffer);
            processNextRequest(requestChannel, requestHandler);
            channel.runPendingTasks();

            ByteBuf handshakeResponseLength = channel.readOutbound();
            ByteBuf handshakeResponseBuffer = channel.readOutbound();
            try {
                assertThat(handshakeResponseLength).isNotNull();
                assertThat(handshakeResponseBuffer).isNotNull();
                assertThat(handshakeResponseLength.readInt())
                        .isEqualTo(handshakeResponseBuffer.readableBytes());
                SaslHandshakeResponse handshakeResponse =
                        (SaslHandshakeResponse)
                                AbstractResponse.parseResponse(
                                        handshakeResponseBuffer.nioBuffer(), handshakeHeader);
                assertThat(handshakeResponse.errorCounts())
                        .containsExactlyEntriesOf(Collections.singletonMap(Errors.NONE, 1));
            } finally {
                if (handshakeResponseLength != null) {
                    handshakeResponseLength.release();
                }
                if (handshakeResponseBuffer != null) {
                    handshakeResponseBuffer.release();
                }
            }
            assertThat(channel.isActive()).isTrue();

            channel.writeInbound(apiVersionsBuffer);
            processNextRequest(requestChannel, requestHandler);
            channel.runPendingTasks();

            ByteBuf apiVersionsResponseLength = channel.readOutbound();
            ByteBuf apiVersionsResponseBuffer = channel.readOutbound();
            try {
                assertThat(apiVersionsResponseLength).isNotNull();
                assertThat(apiVersionsResponseBuffer).isNotNull();
                assertThat(apiVersionsResponseLength.readInt())
                        .isEqualTo(apiVersionsResponseBuffer.readableBytes());
                ByteBuffer responsePayload = apiVersionsResponseBuffer.nioBuffer();
                ResponseHeader responseHeader =
                        ResponseHeader.parse(
                                responsePayload,
                                apiVersionsHeader.toResponseHeader().headerVersion());
                ApiVersionsResponse apiVersionsResponse =
                        ApiVersionsResponse.parse(
                                responsePayload, ApiKeys.API_VERSIONS.oldestVersion());

                assertThat(responseHeader.correlationId()).isEqualTo(17);
                assertThat(apiVersionsResponse.errorCounts())
                        .containsExactlyEntriesOf(
                                Collections.singletonMap(Errors.ILLEGAL_SASL_STATE, 1));
                assertThat(apiVersionsResponse.data().apiKeys()).isEmpty();
            } finally {
                if (apiVersionsResponseLength != null) {
                    apiVersionsResponseLength.release();
                }
                if (apiVersionsResponseBuffer != null) {
                    apiVersionsResponseBuffer.release();
                }
            }
            assertThat(channel.isActive()).isFalse();
            Object additionalResponse = channel.readOutbound();
            assertThat(additionalResponse).isNull();
            assertThat(handshakeBuffer.refCnt()).isZero();
            assertThat(apiVersionsBuffer.refCnt()).isZero();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void testUnauthenticatedProduceIsRejectedBeforeBodyParsing() {
        RequestChannel requestChannel = new RequestChannel(100);
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new KafkaCommandDecoder(
                                new RequestChannel[] {requestChannel},
                                "KAFKA",
                                () -> {
                                    throw new AssertionError(
                                            "Authenticator must not be created for a Produce request.");
                                }));
        short produceVersion = ApiKeys.PRODUCE.latestVersion();
        RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, produceVersion, "client", 1);
        ByteBuf headerOnlyBuffer = serializeHeader(header);

        try {
            channel.writeInbound(headerOnlyBuffer);
            channel.runPendingTasks();

            assertThat(requestChannel.requestsCount()).isZero();
            assertThat(channel.isActive()).isFalse();
            assertThat(headerOnlyBuffer.refCnt()).isZero();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void testDisconnectDoesNotReleaseQueuedRequestBuffer() {
        RequestChannel requestChannel = new RequestChannel(100);
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new KafkaCommandDecoder(new RequestChannel[] {requestChannel}, "KAFKA"));
        short produceVersion = ApiKeys.PRODUCE.latestVersion();
        ProduceRequest produceRequest =
                new ProduceRequest(
                        new ProduceRequestData().setAcks((short) 1).setTimeoutMs(1000),
                        produceVersion);
        RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, produceVersion, "client", 1);
        ByteBuf requestBuffer = serialize(header, produceRequest);

        try {
            channel.writeInbound(requestBuffer);
            KafkaRequest queuedRequest = (KafkaRequest) requestChannel.pollRequest(1000);
            assertThat(queuedRequest).isNotNull();
            assertThat(requestBuffer.refCnt()).isEqualTo(2);

            channel.close();
            channel.runPendingTasks();

            // The response-queue reference is released on disconnect, while the independent
            // RequestProcessor ownership remains valid until the worker finishes the request.
            assertThat(requestBuffer.refCnt()).isOne();
            assertThat(queuedRequest.<ProduceRequest>request().acks()).isEqualTo((short) 1);
            queuedRequest.releaseBuffer();
            assertThat(requestBuffer.refCnt()).isZero();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void testAcksZeroSuppressesResponseAndUnblocksFollowingResponse() {
        RequestChannel requestChannel = new RequestChannel(100);
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new KafkaCommandDecoder(new RequestChannel[] {requestChannel}, "KAFKA"));
        short produceVersion = ApiKeys.PRODUCE.latestVersion();
        ProduceRequest produceRequest =
                new ProduceRequest(
                        new ProduceRequestData().setAcks((short) 0).setTimeoutMs(1000),
                        produceVersion);
        RequestHeader produceHeader =
                new RequestHeader(ApiKeys.PRODUCE, produceVersion, "client", 1);
        ByteBuf produceBuffer = serialize(produceHeader, produceRequest);

        short apiVersionsVersion = ApiKeys.API_VERSIONS.latestVersion();
        ApiVersionsRequest apiVersionsRequest =
                new ApiVersionsRequest.Builder(
                                new ApiVersionsRequestData(),
                                apiVersionsVersion,
                                apiVersionsVersion)
                        .build();
        RequestHeader apiVersionsHeader =
                new RequestHeader(ApiKeys.API_VERSIONS, apiVersionsVersion, "client", 2);
        ByteBuf apiVersionsBuffer = serialize(apiVersionsHeader, apiVersionsRequest);

        try {
            channel.writeInbound(produceBuffer);
            channel.writeInbound(apiVersionsBuffer);
            KafkaRequest first = (KafkaRequest) requestChannel.pollRequest(1000);
            KafkaRequest second = (KafkaRequest) requestChannel.pollRequest(1000);
            assertThat(first).isNotNull();
            assertThat(second).isNotNull();

            // Polling the requests above stands in for RequestProcessor. In production its finally
            // block releases the processor-owned reference after dispatching each request, while
            // the ordered-response queue keeps its independent reference until completion.
            first.releaseBuffer();
            second.releaseBuffer();

            second.complete(new ApiVersionsResponse(new ApiVersionsResponseData()));
            channel.runPendingTasks();
            Object blockedResponse = channel.readOutbound();
            assertThat(blockedResponse).isNull();

            first.complete(new ProduceResponse(new ProduceResponseData()));
            channel.runPendingTasks();

            ByteBuf response = channel.readOutbound();
            try {
                assertThat(response).isNotNull();
                ResponseHeader responseHeader =
                        ResponseHeader.parse(
                                response.nioBuffer(),
                                apiVersionsHeader.toResponseHeader().headerVersion());
                assertThat(responseHeader.correlationId()).isEqualTo(2);
                Object additionalResponse = channel.readOutbound();
                assertThat(additionalResponse).isNull();
            } finally {
                if (response != null) {
                    response.release();
                }
            }

            assertThat(produceBuffer.refCnt()).isZero();
            assertThat(apiVersionsBuffer.refCnt()).isZero();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static ByteBuf serialize(RequestHeader header, AbstractRequest request) {
        ByteBuffer serialized =
                RequestUtils.serialize(
                        header.data(), header.headerVersion(), request.data(), request.version());
        return Unpooled.wrappedBuffer(serialized);
    }

    private static ByteBuf serializeHeaderOnly(RequestHeader header) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        int headerSize = header.data().size(cache, header.headerVersion());
        ByteBuffer serialized = ByteBuffer.allocate(headerSize);
        header.data().write(new ByteBufferAccessor(serialized), cache, header.headerVersion());
        serialized.flip();
        return Unpooled.wrappedBuffer(serialized);
    }

    private static void processNextRequest(
            RequestChannel requestChannel, KafkaRequestHandler requestHandler) throws Exception {
        KafkaRequest request = (KafkaRequest) requestChannel.pollRequest(1000);
        assertThat(request).isNotNull();
        try {
            requestHandler.processRequest(request);
        } finally {
            request.releaseBuffer();
        }
    }

    private static ByteBuf serializeHeader(RequestHeader header) {
        ProduceRequest emptyProduceRequest =
                new ProduceRequest(
                        new ProduceRequestData().setAcks((short) 1).setTimeoutMs(1000),
                        header.apiVersion());
        ByteBuffer serialized =
                RequestUtils.serialize(
                        header.data(),
                        header.headerVersion(),
                        emptyProduceRequest.data(),
                        emptyProduceRequest.version());
        int headerSize = header.data().size(new ObjectSerializationCache(), header.headerVersion());
        serialized.limit(headerSize);
        return Unpooled.wrappedBuffer(serialized);
    }
}

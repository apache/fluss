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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.requests.ResponseHeader;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests response ordering and ownership in {@link KafkaCommandDecoder}. */
public class KafkaCommandDecoderTest {

    @Test
    public void testUnsupportedApiVersionsRequestReturnsVersionZeroErrorResponse()
            throws Exception {
        RequestChannel requestChannel = new RequestChannel(1);
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new KafkaCommandDecoder(
                                new RequestChannel[] {requestChannel}, "KAFKA_EXTERNAL"));
        short unsupportedVersion = (short) (ApiKeys.API_VERSIONS.latestVersion() + 1);
        RequestHeader header =
                new RequestHeader(ApiKeys.API_VERSIONS, unsupportedVersion, "client", 42);
        // The unsupported request body must not be decoded. A version-zero body is empty.
        ByteBuf requestBuffer =
                serialize(header, new ApiVersionsRequest.Builder().build((short) 0));
        KafkaRequest request = null;
        ByteBuf responseBuffer = null;
        try {
            channel.writeInbound(requestBuffer);
            request = (KafkaRequest) requestChannel.pollRequest(1000);
            assertThat(request).isNotNull();
            assertThat(request.apiVersion()).isEqualTo(unsupportedVersion);
            assertThat(request.listenerName()).isEqualTo("KAFKA_EXTERNAL");
            ApiVersionsRequest parsedRequest = request.request();
            assertThat(parsedRequest.hasUnsupportedRequestVersion()).isTrue();
            assertThat(parsedRequest.version()).isZero();

            TestingTabletGatewayService service = new TestingTabletGatewayService();
            try (KafkaRequestHandler handler =
                    new KafkaRequestHandler(new Configuration(), service)) {
                handler.processRequest(request);
            }
            channel.runPendingTasks();

            responseBuffer = channel.readOutbound();
            assertThat(responseBuffer).isNotNull();
            ByteBuffer responseBytes = responseBuffer.nioBuffer();
            assertThat(ResponseHeader.parse(responseBytes, (short) 0).correlationId())
                    .isEqualTo(42);
            ApiVersionsResponse response = ApiVersionsResponse.parse(responseBytes, (short) 0);
            assertThat(response.data().errorCode()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
            assertThat(responseBytes.hasRemaining()).isFalse();
            assertThat(requestBuffer.refCnt()).isZero();
            assertThat(channel.isActive()).isTrue();
        } finally {
            if (responseBuffer != null) {
                responseBuffer.release();
            }
            if (request != null) {
                request.releaseBuffer();
            }
            channel.finishAndReleaseAll();
            if (requestBuffer.refCnt() > 0) {
                requestBuffer.release();
            }
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
}

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

package com.alibaba.fluss.rpc.protocol;

import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.metrics.util.NOPMetricsGroup;
import com.alibaba.fluss.record.send.Send;
import com.alibaba.fluss.rpc.messages.ApiMessage;
import com.alibaba.fluss.rpc.messages.ApiVersionsRequest;
import com.alibaba.fluss.rpc.messages.ApiVersionsResponse;
import com.alibaba.fluss.rpc.messages.PbApiVersion;
import com.alibaba.fluss.rpc.netty.client.ClientHandlerCallback;
import com.alibaba.fluss.rpc.netty.client.NettyClientHandler;
import com.alibaba.fluss.rpc.netty.server.FlussRequest;
import com.alibaba.fluss.rpc.netty.server.NettyServerHandler;
import com.alibaba.fluss.rpc.netty.server.RequestChannel;
import com.alibaba.fluss.rpc.netty.server.RequestsMetrics;
import com.alibaba.fluss.security.auth.PlainTextAuthenticationPlugin;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.Channel;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelId;
import com.alibaba.fluss.shaded.netty4.io.netty.util.concurrent.EventExecutor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.Collections;

import static com.alibaba.fluss.testutils.ByteBufChannel.toByteBuf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link MessageCodec}. */
class MessageCodecTest {

    private NettyClientHandler clientHandler;
    private RequestChannel requestChannel;
    private NettyServerHandler serverHandler;
    private ResponseReceiver responseReceiver;
    private ChannelHandlerContext ctx;

    @BeforeEach
    void beforeEach() {
        this.responseReceiver = new ResponseReceiver();
        this.clientHandler = new NettyClientHandler(responseReceiver, false);
        this.requestChannel = new RequestChannel(100);
        MetricGroup metricGroup = NOPMetricsGroup.newInstance();
        this.serverHandler =
                new NettyServerHandler(
                        requestChannel,
                        new ApiManager(ServerType.TABLET_SERVER),
                        "FLUSS",
                        true,
                        RequestsMetrics.createCoordinatorServerRequestMetrics(metricGroup),
                        new PlainTextAuthenticationPlugin.PlainTextServerAuthenticator());
        this.ctx = mockChannelHandlerContext();
    }

    @Test
    void testEncodeRequest() throws Exception {
        ApiVersionsRequest request = new ApiVersionsRequest();
        request.setClientSoftwareName("test").setClientSoftwareVersion("1.0.0");
        ByteBuf byteBuf =
                MessageCodec.encodeRequest(
                        ByteBufAllocator.DEFAULT,
                        ApiKeys.API_VERSIONS.id,
                        ApiKeys.API_VERSIONS.highestSupportedVersion,
                        1001,
                        request);
        serverHandler.channelRead(ctx, byteBuf);

        FlussRequest rpcRequest = (FlussRequest) requestChannel.pollRequest(1000);
        assertThat(rpcRequest).isNotNull();
        assertThat(rpcRequest.getApiKey()).isEqualTo(ApiKeys.API_VERSIONS.id);
        assertThat(rpcRequest.getApiVersion())
                .isEqualTo(ApiKeys.API_VERSIONS.highestSupportedVersion);
        assertThat(rpcRequest.getRequestId()).isEqualTo(1001);
        assertThat(rpcRequest.getMessage().totalSize()).isEqualTo(request.totalSize());
        assertThat(rpcRequest.getMessage()).isInstanceOf(ApiVersionsRequest.class);

        ApiVersionsRequest actual = (ApiVersionsRequest) rpcRequest.getMessage();
        assertThat(actual.getClientSoftwareName()).isEqualTo("test");
        assertThat(actual.getClientSoftwareVersion()).isEqualTo("1.0.0");
        assertThat(byteBuf.readerIndex()).isEqualTo(byteBuf.writerIndex());
    }

    @Test
    void testEncodeSuccessResponse() throws Exception {
        ApiVersionsResponse response = new ApiVersionsResponse();
        PbApiVersion apiVersion = new PbApiVersion();
        apiVersion
                .setApiKey(ApiKeys.API_VERSIONS.id)
                .setMinVersion(ApiKeys.API_VERSIONS.lowestSupportedVersion)
                .setMaxVersion(ApiKeys.API_VERSIONS.highestSupportedVersion);
        response.addAllApiVersions(Collections.singletonList(apiVersion));
        Send send = MessageCodec.encodeSuccessResponse(ByteBufAllocator.DEFAULT, 1001, response);
        ByteBuf byteBuf = toByteBuf(send);
        clientHandler.channelRead(ctx, byteBuf);

        assertThat(responseReceiver.requestId).isEqualTo(1001);
        assertThat(responseReceiver.response).isNotNull();
        assertThat(responseReceiver.response).isInstanceOf(ApiVersionsResponse.class);
        ApiVersionsResponse actualResp = (ApiVersionsResponse) responseReceiver.response;
        assertThat(actualResp.getApiVersionsCount()).isEqualTo(1);
        PbApiVersion actualApiVersion = actualResp.getApiVersionAt(0);
        assertThat(actualApiVersion.getApiKey()).isEqualTo(apiVersion.getApiKey());
        assertThat(actualApiVersion.getMinVersion()).isEqualTo(apiVersion.getMinVersion());
        assertThat(actualApiVersion.getMaxVersion()).isEqualTo(apiVersion.getMaxVersion());
        assertThat(byteBuf.readerIndex()).isEqualTo(byteBuf.writerIndex());
    }

    @Test
    void testEncodeErrorResponse() throws Exception {
        ApiError error = new ApiError(Errors.NETWORK_EXCEPTION, "response error");
        ByteBuf byteBuf = MessageCodec.encodeErrorResponse(ByteBufAllocator.DEFAULT, 1001, error);
        clientHandler.channelRead(null, byteBuf);

        assertThat(responseReceiver.requestId).isEqualTo(1001);
        assertThat(responseReceiver.response).isNull();
        assertThat(responseReceiver.responseError)
                .isNotNull()
                .isInstanceOf(Errors.NETWORK_EXCEPTION.exception().getClass())
                .hasMessage("response error");
        assertThat(byteBuf.readerIndex()).isEqualTo(byteBuf.writerIndex());
    }

    @Test
    void testEncodingServerFailure() throws Exception {
        ApiError error = new ApiError(Errors.CORRUPT_MESSAGE, "server error");
        ByteBuf byteBuf = MessageCodec.encodeServerFailure(ByteBufAllocator.DEFAULT, error);
        clientHandler.channelRead(ctx, byteBuf);

        assertThat(responseReceiver.requestId).isEqualTo(-1);
        assertThat(responseReceiver.response).isNull();
        assertThat(responseReceiver.serverError)
                .isNotNull()
                .isInstanceOf(Errors.CORRUPT_MESSAGE.exception().getClass())
                .hasMessage("server error");
        assertThat(byteBuf.readerIndex()).isEqualTo(byteBuf.writerIndex());
    }

    // ------------------------------------------------------------------------------------

    private static class ResponseReceiver implements ClientHandlerCallback {

        int requestId = -1;
        ApiMessage response;
        Throwable responseError;
        Throwable serverError;

        @Override
        public ApiMethod getRequestApiMethod(int requestId) {
            return ApiManager.forApiKey(ApiKeys.API_VERSIONS.id);
        }

        @Override
        public void onRequestResult(int requestId, ApiMessage response) {
            this.requestId = requestId;
            this.response = response;
        }

        @Override
        public void onRequestFailure(int requestId, Throwable cause) {
            this.requestId = requestId;
            this.responseError = cause;
        }

        @Override
        public void onFailure(Throwable cause) {
            this.serverError = cause;
        }
    }

    private static ChannelHandlerContext mockChannelHandlerContext() {
        ChannelId channelId = mock(ChannelId.class);
        when(channelId.asShortText()).thenReturn("short_text");
        when(channelId.asLongText()).thenReturn("long_text");
        Channel channel = mock(Channel.class);
        when(channel.id()).thenReturn(channelId);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("localhost", 8080));
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(channel);
        EventExecutor eventExecutor = mock(EventExecutor.class);
        when(ctx.executor()).thenReturn(eventExecutor);
        return ctx;
    }
}

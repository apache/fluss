/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.rpc.netty.server;

import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.metrics.util.NOPMetricsGroup;
import com.alibaba.fluss.rpc.messages.ApiVersionsRequest;
import com.alibaba.fluss.rpc.messages.ApiVersionsResponse;
import com.alibaba.fluss.rpc.messages.PbApiVersion;
import com.alibaba.fluss.rpc.protocol.ApiKeys;
import com.alibaba.fluss.rpc.protocol.ApiManager;
import com.alibaba.fluss.rpc.protocol.MessageCodec;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.Channel;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelId;
import com.alibaba.fluss.shaded.netty4.io.netty.util.concurrent.DefaultEventExecutor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Test for {@link NettyServerHandler}. */
final class NettyServerHandlerTest {

    private NettyServerHandler serverHandler;
    private TestingRequestChannel requestChannel;
    private ChannelHandlerContext ctx;

    @BeforeEach
    void beforeEach() {
        this.requestChannel = new TestingRequestChannel(100);
        MetricGroup metricGroup = NOPMetricsGroup.newInstance();
        this.serverHandler =
                new NettyServerHandler(
                        requestChannel,
                        new ApiManager(ServerType.TABLET_SERVER),
                        "CLIENT",
                        RequestsMetrics.createCoordinatorServerRequestMetrics(metricGroup));
        this.ctx = mockChannelHandlerContext();
    }

    @Test
    void testResponseReturnInOrder() throws Exception {
        // first write 10 requests to serverHandler.
        for (int i = 0; i < 10; i++) {
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
        }

        Deque<FlussRequest> inflightResponses = serverHandler.inflightResponses();
        assertThat(requestChannel.requestsCount()).isEqualTo(10);
        assertThat(inflightResponses.size()).isEqualTo(10);

        // 1. try to response first request, it will return immediately.
        FlussRequest request1 = (FlussRequest) requestChannel.getRequest(0);
        request1.setRequestCompletedTimeMs(System.currentTimeMillis());
        request1.setRequestDequeTimeMs(System.currentTimeMillis());
        ApiVersionsResponse response1 = new ApiVersionsResponse();
        PbApiVersion apiVersion = new PbApiVersion();
        apiVersion
                .setApiKey(ApiKeys.API_VERSIONS.id)
                .setMinVersion(ApiKeys.API_VERSIONS.lowestSupportedVersion)
                .setMaxVersion(ApiKeys.API_VERSIONS.highestSupportedVersion);
        response1.addAllApiVersions(Collections.singletonList(apiVersion));
        request1.complete(response1);
        retry(Duration.ofSeconds(20), () -> assertThat(inflightResponses.size()).isEqualTo(9));

        // 2. try to response 6th, 7th, 8th requests, but it will not return immediately.
        Set<Integer> finishedRequests = new HashSet<>();
        for (int i = 5; i < 8; i++) {
            // always get index 5 as request will be removed from requestChannel after get.
            FlussRequest request = (FlussRequest) requestChannel.getRequest(5);
            request.setRequestCompletedTimeMs(System.currentTimeMillis());
            request.setRequestDequeTimeMs(System.currentTimeMillis());
            ApiVersionsResponse response = new ApiVersionsResponse();
            apiVersion = new PbApiVersion();
            apiVersion
                    .setApiKey(ApiKeys.API_VERSIONS.id)
                    .setMinVersion(ApiKeys.API_VERSIONS.lowestSupportedVersion)
                    .setMaxVersion(ApiKeys.API_VERSIONS.highestSupportedVersion);
            response.addAllApiVersions(Collections.singletonList(apiVersion));
            request.complete(response);
            assertThat(inflightResponses.size()).isEqualTo(9);

            finishedRequests.add(i);
            int currentIndex = 0;
            for (FlussRequest rpcRequest : inflightResponses) {
                if (finishedRequests.contains(currentIndex)) {
                    assertThat(rpcRequest.getResponseFuture().isDone()).isTrue();
                } else {
                    assertThat(rpcRequest.getResponseFuture().isDone()).isFalse();
                }
                currentIndex++;
            }
        }

        // 3. try to finish the requests 0 - 3.
        for (int i = 0; i < 4; i++) {
            FlussRequest request = (FlussRequest) requestChannel.getRequest(0);
            request.setRequestCompletedTimeMs(System.currentTimeMillis());
            request.setRequestDequeTimeMs(System.currentTimeMillis());
            ApiVersionsResponse response = new ApiVersionsResponse();
            apiVersion = new PbApiVersion();
            apiVersion
                    .setApiKey(ApiKeys.API_VERSIONS.id)
                    .setMinVersion(ApiKeys.API_VERSIONS.lowestSupportedVersion)
                    .setMaxVersion(ApiKeys.API_VERSIONS.highestSupportedVersion);
            response.addAllApiVersions(Collections.singletonList(apiVersion));
            request.complete(response);
            final int size = 8 - i;
            retry(
                    Duration.ofSeconds(20),
                    () -> assertThat(inflightResponses.size()).isEqualTo(size));
        }

        // 4. try to finish 5th request, 6th, 7th, 8th requests will also return as it has been done
        // before.
        FlussRequest request = (FlussRequest) requestChannel.getRequest(0);
        request.setRequestCompletedTimeMs(System.currentTimeMillis());
        request.setRequestDequeTimeMs(System.currentTimeMillis());
        ApiVersionsResponse response = new ApiVersionsResponse();
        apiVersion = new PbApiVersion();
        apiVersion
                .setApiKey(ApiKeys.API_VERSIONS.id)
                .setMinVersion(ApiKeys.API_VERSIONS.lowestSupportedVersion)
                .setMaxVersion(ApiKeys.API_VERSIONS.highestSupportedVersion);
        response.addAllApiVersions(Collections.singletonList(apiVersion));
        request.complete(response);
        retry(Duration.ofSeconds(20), () -> assertThat(inflightResponses.size()).isEqualTo(1));
    }

    private static ChannelHandlerContext mockChannelHandlerContext() {
        ChannelId channelId = mock(ChannelId.class);
        when(channelId.asShortText()).thenReturn("short_text");
        when(channelId.asLongText()).thenReturn("long_text");
        Channel channel = mock(Channel.class);
        when(channel.id()).thenReturn(channelId);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(channel);
        when(ctx.alloc()).thenReturn(ByteBufAllocator.DEFAULT);
        when(ctx.executor()).thenReturn(new DefaultEventExecutor());
        return ctx;
    }
}

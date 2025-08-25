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

package com.alibaba.fluss.rpc.netty.server;

import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.metrics.util.NOPMetricsGroup;
import com.alibaba.fluss.rpc.messages.ApiVersionsRequest;
import com.alibaba.fluss.rpc.messages.ApiVersionsResponse;
import com.alibaba.fluss.rpc.messages.LookupRequest;
import com.alibaba.fluss.rpc.messages.LookupResponse;
import com.alibaba.fluss.rpc.messages.PbApiVersion;
import com.alibaba.fluss.rpc.messages.PbLookupReqForBucket;
import com.alibaba.fluss.rpc.messages.PbLookupRespForBucket;
import com.alibaba.fluss.rpc.messages.PbValue;
import com.alibaba.fluss.rpc.protocol.ApiKeys;
import com.alibaba.fluss.rpc.protocol.ApiManager;
import com.alibaba.fluss.rpc.protocol.MessageCodec;
import com.alibaba.fluss.security.auth.PlainTextAuthenticationPlugin;

import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelId;
import org.apache.fluss.shaded.netty4.io.netty.util.concurrent.DefaultEventExecutor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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
    void beforeEach() throws Exception {
        this.requestChannel = new TestingRequestChannel(100);
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
        serverHandler.channelActive(ctx);
    }

    @Test
    @Disabled("TODO: add back in https://github.com/alibaba/fluss/issues/771")
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

        Deque<FlussRequest> inflightResponses =
                serverHandler.inflightResponses(ApiKeys.API_VERSIONS.id);
        assertThat(requestChannel.requestsCount()).isEqualTo(10);
        assertThat(inflightResponses.size()).isEqualTo(10);

        // 1. try to response first request, it will return immediately.
        FlussRequest request1 = (FlussRequest) requestChannel.getAndRemoveRequest(0);
        request1.setRequestCompletedTimeMs(System.currentTimeMillis());
        request1.setRequestDequeTimeMs(System.currentTimeMillis());
        request1.complete(makeApiVersionResponse());
        retry(Duration.ofSeconds(20), () -> assertThat(inflightResponses.size()).isEqualTo(9));

        // 2. try to response 6th, 7th, 8th requests, but it will not return immediately.
        Set<Integer> finishedRequests = new HashSet<>();
        for (int i = 5; i < 8; i++) {
            // always get index 5 as request will be removed from requestChannel after get.
            FlussRequest request = (FlussRequest) requestChannel.getAndRemoveRequest(5);
            request.setRequestCompletedTimeMs(System.currentTimeMillis());
            request.setRequestDequeTimeMs(System.currentTimeMillis());
            request.complete(makeApiVersionResponse());
            assertThat(inflightResponses.size()).isEqualTo(9);
            finishedRequests.add(i);
        }
        int currentIndex = 0;
        for (FlussRequest rpcRequest : inflightResponses) {
            if (finishedRequests.contains(currentIndex)) {
                assertThat(rpcRequest.getResponseFuture().isDone()).isTrue();
            } else {
                assertThat(rpcRequest.getResponseFuture().isDone()).isFalse();
            }
            currentIndex++;
        }

        // 3. try to finish the requests 0 - 3.
        for (int i = 0; i < 4; i++) {
            FlussRequest request = (FlussRequest) requestChannel.getAndRemoveRequest(0);
            request.setRequestCompletedTimeMs(System.currentTimeMillis());
            request.setRequestDequeTimeMs(System.currentTimeMillis());
            request.complete(makeApiVersionResponse());
            final int size = 8 - i;
            retry(
                    Duration.ofSeconds(20),
                    () -> assertThat(inflightResponses.size()).isEqualTo(size));
        }

        // 4. try to finish 5th request, 6th, 7th, 8th requests will also return as it has been done
        // before.
        FlussRequest request = (FlussRequest) requestChannel.getAndRemoveRequest(0);
        request.setRequestCompletedTimeMs(System.currentTimeMillis());
        request.setRequestDequeTimeMs(System.currentTimeMillis());
        request.complete(makeApiVersionResponse());
        retry(Duration.ofSeconds(20), () -> assertThat(inflightResponses.size()).isEqualTo(1));
    }

    @Test
    @Disabled("TODO: add back in https://github.com/alibaba/fluss/issues/771")
    void testDifferentResponseTypeReturnInSeparateOrder() throws Exception {
        // 1. first write 5 requests with api as ApiKeys.API_VERSIONS to serverHandler.
        for (int i = 0; i < 5; i++) {
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

        // 2. second write 5 with api as ApiKeys.LOOKUP to serverHandler.
        LookupRequest lookupRequest = new LookupRequest().setTableId(1);
        PbLookupReqForBucket pbLookupReqForBucket =
                new PbLookupReqForBucket().setPartitionId(1).setBucketId(1);
        pbLookupReqForBucket.addKey("key".getBytes());
        lookupRequest.addAllBucketsReqs(Collections.singleton(pbLookupReqForBucket));
        for (int i = 0; i < 5; i++) {
            ByteBuf byteBuf =
                    MessageCodec.encodeRequest(
                            ByteBufAllocator.DEFAULT,
                            ApiKeys.LOOKUP.id,
                            ApiKeys.LOOKUP.highestSupportedVersion,
                            1001,
                            lookupRequest);
            serverHandler.channelRead(ctx, byteBuf);
        }

        assertThat(requestChannel.requestsCount()).isEqualTo(10);
        Deque<FlussRequest> inflightApiVersionResponses =
                serverHandler.inflightResponses(ApiKeys.API_VERSIONS.id);
        assertThat(inflightApiVersionResponses.size()).isEqualTo(5);

        Deque<FlussRequest> inflightLookupResponses =
                serverHandler.inflightResponses(ApiKeys.LOOKUP.id);
        assertThat(inflightLookupResponses.size()).isEqualTo(5);

        // 3. try to finish one Lookup request, return immediately not to wait the previous five
        // ApiVersionsRequest response first.
        FlussRequest request = (FlussRequest) requestChannel.getAndRemoveRequest(5);
        request.setRequestCompletedTimeMs(System.currentTimeMillis());
        request.setRequestDequeTimeMs(System.currentTimeMillis());
        request.complete(makeLookupResponse());
        retry(
                Duration.ofSeconds(20),
                () -> assertThat(inflightLookupResponses.size()).isEqualTo(4));
        assertThat(inflightApiVersionResponses.size()).isEqualTo(5);
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

    private ApiVersionsResponse makeApiVersionResponse() {
        ApiVersionsResponse response = new ApiVersionsResponse();
        PbApiVersion apiVersion = new PbApiVersion();
        apiVersion
                .setApiKey(ApiKeys.API_VERSIONS.id)
                .setMinVersion(ApiKeys.API_VERSIONS.lowestSupportedVersion)
                .setMaxVersion(ApiKeys.API_VERSIONS.highestSupportedVersion);
        response.addAllApiVersions(Collections.singletonList(apiVersion));
        return response;
    }

    private LookupResponse makeLookupResponse() {
        LookupResponse response = new LookupResponse();
        PbLookupRespForBucket pbLookupRespForBucket =
                new PbLookupRespForBucket()
                        .setPartitionId(1)
                        .setBucketId(1)
                        .addAllValues(Collections.singleton(new PbValue()));
        response.addAllBucketsResps(Collections.singletonList(pbLookupRespForBucket));
        return response;
    }
}

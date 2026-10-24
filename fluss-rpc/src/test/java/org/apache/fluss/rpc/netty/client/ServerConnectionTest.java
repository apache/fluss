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

package org.apache.fluss.rpc.netty.client;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.DisconnectException;
import org.apache.fluss.exception.InvalidServerTypeException;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.MetricType;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;
import org.apache.fluss.metrics.util.NOPMetricsGroup;
import org.apache.fluss.rpc.TestingGatewayService;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.messages.AlterTableRequest;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.ApiVersionsRequest;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.messages.GetTableSchemaRequest;
import org.apache.fluss.rpc.messages.ListDatabasesRequest;
import org.apache.fluss.rpc.messages.LookupRequest;
import org.apache.fluss.rpc.messages.PbApiVersion;
import org.apache.fluss.rpc.messages.PbLookupReqForBucket;
import org.apache.fluss.rpc.messages.PbTablePath;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.rpc.netty.client.ServerConnection.ConnectionState;
import org.apache.fluss.rpc.netty.server.NettyServer;
import org.apache.fluss.rpc.netty.server.RequestsMetrics;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.security.auth.AuthenticationFactory;
import org.apache.fluss.security.auth.ClientAuthenticator;
import org.apache.fluss.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.fluss.shaded.netty4.io.netty.channel.EventLoop;
import org.apache.fluss.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.timeout.IdleStateEvent;
import org.apache.fluss.shaded.netty4.io.netty.util.ReferenceCountUtil;
import org.apache.fluss.shaded.netty4.io.netty.util.concurrent.ScheduledFuture;
import org.apache.fluss.utils.NetUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.metrics.MetricNames.CLIENT_BYTES_IN_RATE_AVG;
import static org.apache.fluss.metrics.MetricNames.CLIENT_BYTES_IN_RATE_TOTAL;
import static org.apache.fluss.metrics.MetricNames.CLIENT_BYTES_OUT_RATE_AVG;
import static org.apache.fluss.metrics.MetricNames.CLIENT_BYTES_OUT_RATE_TOTAL;
import static org.apache.fluss.metrics.MetricNames.CLIENT_REQUESTS_IN_FLIGHT_TOTAL;
import static org.apache.fluss.metrics.MetricNames.CLIENT_REQUESTS_RATE_AVG;
import static org.apache.fluss.metrics.MetricNames.CLIENT_REQUESTS_RATE_TOTAL;
import static org.apache.fluss.metrics.MetricNames.CLIENT_REQUEST_LATENCY_MS_AVG;
import static org.apache.fluss.metrics.MetricNames.CLIENT_REQUEST_LATENCY_MS_MAX;
import static org.apache.fluss.metrics.MetricNames.CLIENT_RESPONSES_RATE_AVG;
import static org.apache.fluss.metrics.MetricNames.CLIENT_RESPONSES_RATE_TOTAL;
import static org.apache.fluss.rpc.netty.NettyUtils.getClientSocketChannelClass;
import static org.apache.fluss.rpc.netty.NettyUtils.newEventLoopGroup;
import static org.apache.fluss.utils.NetUtils.getAvailablePort;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Test for {@link ServerConnection}. */
public class ServerConnectionTest {

    private EventLoopGroup eventLoopGroup;
    private Bootstrap bootstrap;
    private ClientAuthenticator clientAuthenticator;
    private Configuration conf;
    private NettyServer nettyServer;
    private ServerNode serverNode;
    private ServerNode serverNode2;
    private TestingGatewayService service;

    @BeforeEach
    void setUp() throws Exception {
        conf = new Configuration();
        buildNettyServer();

        eventLoopGroup = newEventLoopGroup(1, "fluss-netty-client-test");
        bootstrap =
                new Bootstrap()
                        .group(eventLoopGroup)
                        .channel(getClientSocketChannelClass(eventLoopGroup))
                        .handler(new ClientChannelInitializer(5000, false));
        clientAuthenticator =
                AuthenticationFactory.loadClientAuthenticatorSupplier(new Configuration()).get();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (nettyServer != null) {
            nettyServer.close();
        }

        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully();
        }
    }

    @Test
    void testDelayedConnectCancellationAndSchedulingRejection() throws Exception {
        EventLoopGroup group = mock(EventLoopGroup.class);
        EventLoop loop = mock(EventLoop.class);
        when(group.next()).thenReturn(loop);
        Bootstrap deferred = spy(new Bootstrap().group(group));
        AtomicReference<Runnable> timer = new AtomicReference<>();
        ScheduledFuture<?> scheduled = mock(ScheduledFuture.class);
        when(loop.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.NANOSECONDS)))
                .thenAnswer(
                        invocation -> {
                            timer.set(invocation.getArgument(0));
                            return scheduled;
                        });
        ServerConnection connection =
                new ServerConnection(
                        serverNode,
                        TestingClientMetricGroup.newInstance(),
                        clientAuthenticator,
                        (con, failure) -> {},
                        con -> {});
        CompletableFuture<ApiMessage> pending =
                connection.send(
                        ApiKeys.API_VERSIONS,
                        new ApiVersionsRequest()
                                .setClientSoftwareName("test")
                                .setClientSoftwareVersion("1.0"));
        CompletableFuture<ApiMessage> secondPending =
                connection.send(
                        ApiKeys.API_VERSIONS,
                        new ApiVersionsRequest()
                                .setClientSoftwareName("second")
                                .setClientSoftwareVersion("1.0"));
        connection.connect(deferred, 100);
        connection.connect(deferred, 100);
        verify(loop).schedule(any(Runnable.class), eq(100L), eq(TimeUnit.NANOSECONDS));
        assertThat(pending).isNotDone();
        connection.close();
        verify(scheduled).cancel(false);
        timer.get().run();
        verify(deferred, never()).connect(any(String.class), anyInt());
        assertThatThrownBy(() -> pending.get(10, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(ClosedChannelException.class);
        assertThatThrownBy(() -> secondPending.get(10, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(ClosedChannelException.class);
        assertThatThrownBy(
                        () ->
                                connection
                                        .send(ApiKeys.API_VERSIONS, new ApiVersionsRequest())
                                        .get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(NetworkException.class);

        when(loop.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.NANOSECONDS)))
                .thenThrow(new RejectedExecutionException("stopped"));
        AtomicReference<Boolean> rejectionFailure = new AtomicReference<>();
        ServerConnection rejected =
                new ServerConnection(
                        serverNode,
                        TestingClientMetricGroup.newInstance(),
                        AuthenticationFactory.loadClientAuthenticatorSupplier(conf).get(),
                        (con, failure) -> rejectionFailure.set(failure),
                        con -> {});
        CompletableFuture<ApiMessage> response =
                rejected.send(
                        ApiKeys.API_VERSIONS,
                        new ApiVersionsRequest()
                                .setClientSoftwareName("test")
                                .setClientSoftwareVersion("1.0"));
        CompletableFuture<ApiMessage> secondResponse =
                rejected.send(
                        ApiKeys.API_VERSIONS,
                        new ApiVersionsRequest()
                                .setClientSoftwareName("second")
                                .setClientSoftwareVersion("1.0"));
        rejected.connect(deferred, 100);
        assertThat(rejected.getConnectionState()).isEqualTo(ConnectionState.DISCONNECTED);
        assertThatThrownBy(() -> response.get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(NetworkException.class)
                .hasRootCauseInstanceOf(RejectedExecutionException.class);
        assertThatThrownBy(() -> secondResponse.get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(NetworkException.class)
                .hasRootCauseInstanceOf(RejectedExecutionException.class);
        assertThat(rejectionFailure.get()).isFalse();
        assertThatThrownBy(
                        () ->
                                rejected.send(ApiKeys.API_VERSIONS, new ApiVersionsRequest())
                                        .get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(NetworkException.class);
    }

    @Test
    void testScheduledConnectMakesOnlyOneDemandDrivenAttempt() throws Exception {
        EventLoopGroup group = mock(EventLoopGroup.class);
        EventLoop loop = mock(EventLoop.class);
        when(group.next()).thenReturn(loop);
        Bootstrap deferred = spy(new Bootstrap().group(group));
        AtomicReference<Runnable> timer = new AtomicReference<>();
        when(loop.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.NANOSECONDS)))
                .thenAnswer(
                        invocation -> {
                            timer.set(invocation.getArgument(0));
                            return mock(ScheduledFuture.class);
                        });
        EmbeddedChannel channel = new EmbeddedChannel();
        doReturn(channel.newFailedFuture(new ConnectException("refused")))
                .when(deferred)
                .connect(any(String.class), anyInt());
        AtomicInteger failures = new AtomicInteger();
        ServerConnection connection =
                new ServerConnection(
                        serverNode,
                        TestingClientMetricGroup.newInstance(),
                        clientAuthenticator,
                        (con, failure) -> {
                            if (failure) {
                                failures.incrementAndGet();
                            }
                        },
                        con -> {});
        try {
            CompletableFuture<ApiMessage> first =
                    connection.send(ApiKeys.API_VERSIONS, new ApiVersionsRequest());
            CompletableFuture<ApiMessage> second =
                    connection.send(ApiKeys.API_VERSIONS, new ApiVersionsRequest());
            connection.connect(deferred, 100);
            connection.connect(deferred, 100);
            verify(deferred, never()).connect(any(String.class), anyInt());
            timer.get().run();
            assertThatThrownBy(() -> first.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(NetworkException.class);
            assertThatThrownBy(() -> second.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(NetworkException.class);
            connection.connect(deferred, 0);
            timer.get().run();
            verify(deferred).connect(any(String.class), anyInt());
            verify(loop).schedule(any(Runnable.class), eq(100L), eq(TimeUnit.NANOSECONDS));
            assertThat(failures.get()).isEqualTo(1);
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testWriteFailurePublishesBeforeCompletingQueuedRequests(boolean wrapped) throws Exception {
        Throwable failure =
                wrapped ? new NetworkException("write failed") : new IOException("write failed");
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new ChannelOutboundHandlerAdapter() {
                            @Override
                            public void write(
                                    ChannelHandlerContext ctx,
                                    Object message,
                                    ChannelPromise promise) {
                                ReferenceCountUtil.release(message);
                                promise.setFailure(failure);
                            }
                        });
        Bootstrap connected = spy(new Bootstrap().group(eventLoopGroup));
        doReturn(channel.newSucceededFuture()).when(connected).connect(any(String.class), anyInt());
        AtomicReference<Boolean> published = new AtomicReference<>();
        ServerConnection connection =
                new ServerConnection(
                        serverNode,
                        TestingClientMetricGroup.newInstance(),
                        clientAuthenticator,
                        (con, transportFailure) -> published.set(transportFailure),
                        con -> {});
        try {
            CompletableFuture<ApiMessage> first =
                    connection.send(
                            ApiKeys.API_VERSIONS,
                            new ApiVersionsRequest()
                                    .setClientSoftwareName("first")
                                    .setClientSoftwareVersion("1.0"));
            CompletableFuture<ApiMessage> second =
                    connection.send(
                            ApiKeys.API_VERSIONS,
                            new ApiVersionsRequest()
                                    .setClientSoftwareName("second")
                                    .setClientSoftwareVersion("1.0"));
            CompletableFuture<Boolean> observed =
                    first.handle(
                            (response, error) ->
                                    error != null
                                            && Boolean.TRUE.equals(published.get())
                                            && connection.getConnectionState()
                                                    == ConnectionState.DISCONNECTED);
            connection.connect(connected, 0);
            assertThat(observed.get(10, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> second.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(NetworkException.class);
            assertThat(connection.numInflightRequests()).isZero();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void testIdleCloseIsLocalBeforeChannelInactive() {
        AtomicReference<Boolean> transportFailure = new AtomicReference<>();
        ServerConnection connection =
                new ServerConnection(
                        serverNode,
                        TestingClientMetricGroup.newInstance(),
                        clientAuthenticator,
                        (con, failure) -> transportFailure.set(failure),
                        con -> {});
        ClientHandlerCallback callback = connection.new ResponseCallback();
        EmbeddedChannel channel = new EmbeddedChannel(new NettyClientHandler(callback));
        try {
            channel.pipeline().fireUserEventTriggered(IdleStateEvent.ALL_IDLE_STATE_EVENT);
            assertThat(channel.isActive()).isFalse();
            assertThat(transportFailure.get()).isFalse();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void testConnectionClose() {
        ServerConnection connection =
                new ServerConnection(
                        bootstrap,
                        serverNode,
                        TestingClientMetricGroup.newInstance(),
                        clientAuthenticator,
                        (con, ignore) -> {});
        ConnectionState connectionState = connection.getConnectionState();
        assertThat(connectionState).isEqualTo(ConnectionState.CONNECTING);

        GetTableSchemaRequest request =
                new GetTableSchemaRequest()
                        .setTablePath(
                                new PbTablePath().setDatabaseName("test").setTableName("test"))
                        .setSchemaId(0);
        connection.send(ApiKeys.GET_TABLE_SCHEMA, request);

        CompletableFuture<Void> future = connection.close();
        connectionState = connection.getConnectionState();
        assertThat(connectionState).isEqualTo(ConnectionState.DISCONNECTED);
        assertThat(future.isDone()).isTrue();

        assertThatThrownBy(() -> connection.send(ApiKeys.GET_TABLE_SCHEMA, request).get())
                .rootCause()
                .isInstanceOf(DisconnectException.class)
                .hasMessageContaining("Cannot send request to server");
        future = connection.close();
        assertThat(future.isDone()).isTrue();
    }

    @Test
    void testConnectionMetrics() throws ExecutionException, InterruptedException {
        MockMetricRegistry metricRegistry = new MockMetricRegistry();
        ClientMetricGroup client = new ClientMetricGroup(metricRegistry, "client");
        ServerConnection connection =
                new ServerConnection(
                        bootstrap, serverNode, client, clientAuthenticator, (con, ignore) -> {});
        ServerConnection connection2 =
                new ServerConnection(
                        bootstrap, serverNode2, client, clientAuthenticator, (con, ignore) -> {});
        LookupRequest request = new LookupRequest().setTableId(1);
        PbLookupReqForBucket pbLookupReqForBucket = request.addBucketsReq();
        pbLookupReqForBucket.setBucketId(1);
        assertThat(metricRegistry.registeredMetrics).hasSize(11);

        connection.send(ApiKeys.LOOKUP, request).get();
        connection2.send(ApiKeys.LOOKUP, request).get();

        assertThat(metricRegistry.registeredMetrics).hasSize(11);
        assertThat(metricRegistry.registeredMetrics.keySet())
                .containsExactlyInAnyOrder(
                        CLIENT_REQUESTS_RATE_AVG,
                        CLIENT_REQUESTS_RATE_TOTAL,
                        CLIENT_RESPONSES_RATE_AVG,
                        CLIENT_RESPONSES_RATE_TOTAL,
                        CLIENT_BYTES_IN_RATE_AVG,
                        CLIENT_BYTES_IN_RATE_TOTAL,
                        CLIENT_BYTES_OUT_RATE_AVG,
                        CLIENT_BYTES_OUT_RATE_TOTAL,
                        CLIENT_REQUEST_LATENCY_MS_AVG,
                        CLIENT_REQUEST_LATENCY_MS_MAX,
                        CLIENT_REQUESTS_IN_FLIGHT_TOTAL);
        Metric metric = metricRegistry.registeredMetrics.get(CLIENT_REQUESTS_RATE_AVG);
        assertThat(metric.getMetricType()).isEqualTo(MetricType.GAUGE);
        assertThat(((Gauge<?>) metric).getValue()).isEqualTo(1.0);
        metric = metricRegistry.registeredMetrics.get(CLIENT_REQUESTS_RATE_TOTAL);
        assertThat(metric.getMetricType()).isEqualTo(MetricType.GAUGE);
        assertThat(((Gauge<?>) metric).getValue()).isEqualTo(2L);
        connection.close().get();
    }

    @Test
    void testEncodingErrorDoesNotInterruptPendingRequests() throws Exception {
        CountDownLatch connectionLatch = new CountDownLatch(1);
        Bootstrap delayedBootstrap =
                new Bootstrap() {
                    @Override
                    public ChannelFuture connect(String host, int port) {
                        return bootstrap
                                .connect(host, port)
                                .addListener(f -> connectionLatch.await(1, TimeUnit.MINUTES));
                    }
                };
        ServerConnection connection =
                new ServerConnection(
                        delayedBootstrap,
                        serverNode,
                        TestingClientMetricGroup.newInstance(),
                        clientAuthenticator,
                        (con, ignore) -> {});
        try {
            OutOfMemoryError error = new OutOfMemoryError("Direct buffer memory");
            ApiMessage request = mock(ApiMessage.class);
            when(request.totalSize()).thenReturn(0).thenThrow(error);
            when(request.writeTo(any(ByteBuf.class))).thenReturn(0);

            CompletableFuture<ApiMessage> failedFuture = connection.send(ApiKeys.LOOKUP, request);
            LookupRequest validRequest = new LookupRequest().setTableId(1);
            validRequest.addBucketsReq().setBucketId(1);
            CompletableFuture<ApiMessage> successfulFuture =
                    connection.send(ApiKeys.LOOKUP, validRequest);
            assertThat(failedFuture).isNotDone();
            assertThat(successfulFuture).isNotDone();

            connectionLatch.countDown();

            assertThatThrownBy(() -> failedFuture.get(20, TimeUnit.SECONDS)).hasCause(error);
            assertThat(successfulFuture.get(20, TimeUnit.SECONDS)).isNotNull();
            assertThat(connection.numInflightRequests()).isZero();
        } finally {
            connectionLatch.countDown();
            connection.close().get();
        }
    }

    @Test
    void testWrongServerType() {
        ServerNode wrongServerTypeNode =
                new ServerNode(
                        serverNode.id(),
                        serverNode.host(),
                        serverNode.port(),
                        ServerType.COORDINATOR);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Bootstrap mockBootstrap =
                new Bootstrap() {
                    @Override
                    public ChannelFuture connect(String host, int port) {
                        return bootstrap
                                .connect(host, port)
                                .addListener(f -> countDownLatch.await(1, TimeUnit.MINUTES));
                    }
                };
        ServerConnection connection =
                new ServerConnection(
                        mockBootstrap,
                        wrongServerTypeNode,
                        TestingClientMetricGroup.newInstance(),
                        clientAuthenticator,
                        (con, ignore) -> {});

        // Pending request will be rejected with InvalidServerTypeException which is
        // InvalidRequestException.
        CompletableFuture<ApiMessage> future =
                connection.send(ApiKeys.LIST_DATABASES, new ListDatabasesRequest());
        assertThat(future).isNotDone();
        countDownLatch.countDown();
        assertThatThrownBy(future::get)
                .rootCause()
                .isInstanceOf(InvalidServerTypeException.class)
                .hasMessageContaining("Server type mismatch");

        // Later request will be rejected with DisconnectException which is also
        // InvalidRequestException.
        assertThatThrownBy(
                        () ->
                                connection
                                        .send(ApiKeys.LIST_DATABASES, new ListDatabasesRequest())
                                        .get())
                .rootCause()
                .isInstanceOf(DisconnectException.class);
    }

    @Test
    void testRejectBucketCountChangeForOldServer() throws Exception {
        ServerConnection connection =
                new ServerConnection(
                        bootstrap,
                        serverNode,
                        TestingClientMetricGroup.newInstance(),
                        clientAuthenticator,
                        (con, ignore) -> {});
        try {
            connection.validateVersionCompatibility(
                    ApiKeys.ALTER_TABLE, (short) 0, new AlterTableRequest());

            AlterTableRequest bucketCountRequest = new AlterTableRequest();
            bucketCountRequest.setModifyBucketCount().setNewBucketCount(8);
            assertThatThrownBy(
                            () ->
                                    connection.validateVersionCompatibility(
                                            ApiKeys.ALTER_TABLE, (short) 0, bucketCountRequest))
                    .isInstanceOf(UnsupportedVersionException.class)
                    .hasMessageContaining("requires ALTER_TABLE version 1 or newer")
                    .hasMessageContaining("negotiated version 0");
        } finally {
            connection.close().get();
        }
    }

    @Test
    void testRejectHistoricalWritesForOldServer() throws Exception {
        nettyServer.close();
        buildNettyServer(new OldWriteGatewayService());

        ServerConnection connection =
                new ServerConnection(
                        bootstrap,
                        serverNode,
                        TestingClientMetricGroup.newInstance(),
                        clientAuthenticator,
                        (con, ignore) -> {});
        try {
            assertThat(connection.send(ApiKeys.PUT_KV, putKvRequest(null)).get())
                    .isInstanceOf(PutKvResponse.class);

            assertThatThrownBy(
                            () ->
                                    connection
                                            .send(ApiKeys.PUT_KV, putKvRequest("dt=20260823"))
                                            .get())
                    .rootCause()
                    .isInstanceOf(UnsupportedVersionException.class)
                    .hasMessageContaining("require PUT_KV version 3 or newer")
                    .hasMessageContaining("negotiated version 2");

            assertThat(connection.send(ApiKeys.PRODUCE_LOG, produceLogRequest(null)).get())
                    .isInstanceOf(ProduceLogResponse.class);

            assertThatThrownBy(
                            () ->
                                    connection
                                            .send(
                                                    ApiKeys.PRODUCE_LOG,
                                                    produceLogRequest("dt=20260823"))
                                            .get())
                    .rootCause()
                    .isInstanceOf(UnsupportedVersionException.class)
                    .hasMessageContaining("require PRODUCE_LOG version 1 or newer")
                    .hasMessageContaining("negotiated version 0");
        } finally {
            connection.close().get();
        }
    }

    private static PutKvRequest putKvRequest(String originalPartitionName) {
        PutKvRequest request = new PutKvRequest().setTableId(1L).setAcks(1).setTimeoutMs(10_000);
        request.addBucketsReq().setBucketId(0).setRecords(new byte[0]);
        if (originalPartitionName != null) {
            request.getBucketsReqAt(0).setOriginalPartitionName(originalPartitionName);
        }
        return request;
    }

    private static ProduceLogRequest produceLogRequest(String originalPartitionName) {
        ProduceLogRequest request =
                new ProduceLogRequest().setTableId(1L).setAcks(1).setTimeoutMs(10_000);
        request.addBucketsReq().setBucketId(0).setRecords(new byte[0]);
        if (originalPartitionName != null) {
            request.getBucketsReqAt(0).setOriginalPartitionName(originalPartitionName);
        }
        return request;
    }

    private void buildNettyServer() throws Exception {
        buildNettyServer(new TestingTabletGatewayService());
    }

    private void buildNettyServer(TestingGatewayService gatewayService) throws Exception {
        try (NetUtils.Port availablePort = getAvailablePort();
                NetUtils.Port availablePort2 = getAvailablePort()) {
            serverNode =
                    new ServerNode(
                            1, "localhost", availablePort.getPort(), ServerType.TABLET_SERVER);
            serverNode2 =
                    new ServerNode(
                            2, "localhost", availablePort2.getPort(), ServerType.TABLET_SERVER);
            service = gatewayService;
            MetricGroup metricGroup = NOPMetricsGroup.newInstance();
            nettyServer =
                    new NettyServer(
                            conf,
                            Arrays.asList(
                                    new Endpoint(serverNode.host(), serverNode.port(), "INTERNAL"),
                                    new Endpoint(serverNode2.host(), serverNode2.port(), "CLIENT")),
                            service,
                            metricGroup,
                            RequestsMetrics.createCoordinatorServerRequestMetrics(metricGroup));
            nettyServer.start();
        }
    }

    private static class OldWriteGatewayService extends TestingTabletGatewayService {
        @Override
        public CompletableFuture<ApiVersionsResponse> apiVersions(ApiVersionsRequest request) {
            return super.apiVersions(request)
                    .thenApply(
                            response -> {
                                for (PbApiVersion apiVersion : response.getApiVersionsList()) {
                                    if (apiVersion.getApiKey() == ApiKeys.PUT_KV.id) {
                                        apiVersion.setMaxVersion(2);
                                    } else if (apiVersion.getApiKey() == ApiKeys.PRODUCE_LOG.id) {
                                        apiVersion.setMaxVersion(0);
                                    }
                                }
                                return response;
                            });
        }

        @Override
        public CompletableFuture<PutKvResponse> putKv(PutKvRequest request) {
            return CompletableFuture.completedFuture(new PutKvResponse());
        }

        @Override
        public CompletableFuture<ProduceLogResponse> produceLog(ProduceLogRequest request) {
            return CompletableFuture.completedFuture(new ProduceLogResponse());
        }
    }

    private static class MockMetricRegistry extends NOPMetricRegistry {

        Map<String, Metric> registeredMetrics = new HashMap<>();

        @Override
        public void register(Metric metric, String metricName, AbstractMetricGroup group) {
            registeredMetrics.put(metricName, metric);
        }

        @Override
        public void unregister(Metric metric, String metricName, AbstractMetricGroup group) {
            registeredMetrics.remove(metricName, metric);
        }
    }
}

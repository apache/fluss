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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.DisconnectException;
import org.apache.fluss.exception.InvalidServerTypeException;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.exception.TimeoutException;
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
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.FetchLogResponse;
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
import org.apache.fluss.rpc.metrics.ConnectionMetrics;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.rpc.netty.client.ServerConnection.ConnectionState;
import org.apache.fluss.rpc.netty.server.NettyServer;
import org.apache.fluss.rpc.netty.server.RequestsMetrics;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.rpc.protocol.MessageCodec;
import org.apache.fluss.security.auth.AuthenticationFactory;
import org.apache.fluss.security.auth.ClientAuthenticator;
import org.apache.fluss.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.fluss.utils.NetUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
import static org.apache.fluss.testutils.ByteBufChannel.toByteBuf;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.apache.fluss.utils.NetUtils.getAvailablePort;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Test for {@link ServerConnection}. */
public class ServerConnectionTest {

    private static final long REQUEST_TIMEOUT_MS =
            ConfigOptions.CLIENT_REQUEST_TIMEOUT.defaultValue().toMillis();

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
    void testConnectionClose() {
        ServerConnection connection =
                new ServerConnection(
                        bootstrap,
                        serverNode,
                        TestingClientMetricGroup.newInstance(),
                        clientAuthenticator,
                        REQUEST_TIMEOUT_MS,
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
                        bootstrap,
                        serverNode,
                        client,
                        clientAuthenticator,
                        REQUEST_TIMEOUT_MS,
                        (con, ignore) -> {});
        ServerConnection connection2 =
                new ServerConnection(
                        bootstrap,
                        serverNode2,
                        client,
                        clientAuthenticator,
                        REQUEST_TIMEOUT_MS,
                        (con, ignore) -> {});
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
                        REQUEST_TIMEOUT_MS,
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
                        REQUEST_TIMEOUT_MS,
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
                        REQUEST_TIMEOUT_MS,
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
                        REQUEST_TIMEOUT_MS,
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

    @Test
    void testRequestTimeoutSurfacesRetriableFlussTimeoutException() throws Exception {
        nettyServer.close();
        buildNettyServer(new NeverRespondingGatewayService());
        ServerConnection connection =
                new ServerConnection(
                        bootstrap,
                        serverNode,
                        TestingClientMetricGroup.newInstance(),
                        clientAuthenticator,
                        1000L,
                        (con, ignore) -> {});
        try {
            CompletableFuture<ApiMessage> future =
                    connection.send(ApiKeys.PRODUCE_LOG, produceLogRequest(null));

            Throwable cause = awaitFailure(future);
            assertThat(cause)
                    .as("an expired RPC deadline must surface a retriable Fluss timeout")
                    .isInstanceOf(TimeoutException.class);
            assertThat(cause)
                    .as("an expired RPC deadline must stay retriable for the writers")
                    .isInstanceOf(RetriableException.class);
            assertThat(Errors.forException(cause)).isEqualTo(Errors.REQUEST_TIME_OUT);
            assertThat(connection.numInflightRequests()).isZero();
        } finally {
            connection.close().get();
        }
    }

    @Test
    void testInFlightMetricsUpdatedOnceWhenDeadlineWinsSuccessfulDelivery() throws Exception {
        assertInFlightMetricsUpdatedOnce(false);
    }

    @Test
    void testInFlightMetricsUpdatedOnceWhenDeadlineWinsFailedDelivery() throws Exception {
        assertInFlightMetricsUpdatedOnce(true);
    }

    /**
     * Completes the future during response removal to check that in-flight metrics are updated
     * once.
     */
    private void assertInFlightMetricsUpdatedOnce(boolean deliverFailure) throws Exception {
        nettyServer.close();
        buildNettyServer(new NeverRespondingGatewayService());
        MockMetricRegistry metricRegistry = new MockMetricRegistry();
        ClientMetricGroup client = new ClientMetricGroup(metricRegistry, "client");
        ServerConnection connection =
                new ServerConnection(
                        bootstrap,
                        serverNode,
                        client,
                        clientAuthenticator,
                        REQUEST_TIMEOUT_MS,
                        (con, ignore) -> {});
        RacyInflightRequests inflightRequests = new RacyInflightRequests();
        swapInflightRequests(connection, inflightRequests);
        try {
            connection.send(ApiKeys.PRODUCE_LOG, produceLogRequest(null));
            assertThat(inflightRequests.awaitRegistration(30, TimeUnit.SECONDS))
                    .as("the produce request must be registered as inflight")
                    .isTrue();
            // Registration precedes accounting; drain the sending event loop before simulating
            // the response.
            awaitClientHandlerIdle(connection);
            int requestId = inflightRequests.registeredRequestId();
            inflightRequests.completeOnRemove(requestId);

            ClientHandlerCallback callback = responseCallback(connection);
            if (deliverFailure) {
                callback.onRequestFailure(
                        requestId, new NetworkException("simulated delivery failure"));
            } else {
                callback.onRequestResult(requestId, new ProduceLogResponse());
            }

            assertThat(metricValue(metricRegistry, CLIENT_REQUESTS_IN_FLIGHT_TOTAL))
                    .as("the in-flight metric must be decremented exactly once")
                    .isZero();
            assertThat(metricValue(metricRegistry, CLIENT_RESPONSES_RATE_TOTAL))
                    .as("the response must be counted exactly once")
                    .isOne();
            assertThat(connection.numInflightRequests()).isZero();
        } finally {
            closeQuietly(connection);
        }
    }

    @Test
    void testDeadlineCompletionIsNotBlockedByCallbackOnAnotherEventLoop() throws Exception {
        nettyServer.close();
        buildNettyServer(new NeverRespondingGatewayService());
        EventLoopGroup blockedEventLoopGroup =
                newEventLoopGroup(1, "fluss-netty-client-test-blocked");
        Bootstrap blockedBootstrap =
                new Bootstrap()
                        .group(blockedEventLoopGroup)
                        .channel(getClientSocketChannelClass(blockedEventLoopGroup))
                        .handler(new ClientChannelInitializer(5000, false));
        ServerConnection blockedConnection =
                new ServerConnection(
                        blockedBootstrap,
                        serverNode,
                        TestingClientMetricGroup.newInstance(),
                        clientAuthenticator,
                        1000L,
                        (con, ignore) -> {});
        ServerConnection freeConnection =
                new ServerConnection(
                        bootstrap,
                        serverNode,
                        TestingClientMetricGroup.newInstance(),
                        clientAuthenticator,
                        1000L,
                        (con, ignore) -> {});
        CountDownLatch blockedCallbackEntered = new CountDownLatch(1);
        CountDownLatch releaseBlockedCallback = new CountDownLatch(1);
        AtomicReference<Thread> blockedCallbackThread = new AtomicReference<>();
        AtomicBoolean blockedCallbackExited = new AtomicBoolean();
        try {
            waitUntil(blockedConnection::isReady, Duration.ofSeconds(30), "Handshake timed out.");
            CompletableFuture<ApiMessage> blockedFuture =
                    blockedConnection.send(ApiKeys.PRODUCE_LOG, produceLogRequest(null));
            assertThat(blockedFuture).isNotDone();
            // Simulate a consumer whose completion callback stalls: it must not delay deadlines
            // on another event loop. Connections sharing its event loop may still be delayed.
            blockedFuture.whenComplete(
                    (response, throwable) -> {
                        blockedCallbackThread.set(Thread.currentThread());
                        blockedCallbackEntered.countDown();
                        try {
                            releaseBlockedCallback.await(30, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            blockedCallbackExited.set(true);
                        }
                    });
            assertThat(blockedCallbackEntered.await(30, TimeUnit.SECONDS))
                    .as("the stalled callback must be running before the other request is sent")
                    .isTrue();
            assertThat(blockedCallbackThread.get()).isNotSameAs(Thread.currentThread());

            CompletableFuture<ApiMessage> freeFuture =
                    freeConnection.send(ApiKeys.PRODUCE_LOG, produceLogRequest(null));

            // A timeout of get() must fail the test; only a Fluss timeout proves the deadline
            // fired.
            assertThatThrownBy(() -> freeFuture.get(5, TimeUnit.SECONDS))
                    .as(
                            "a blocked deadline callback must not delay a deadline on another event loop")
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TimeoutException.class);
            assertThat(blockedCallbackExited.get()).isFalse();
            assertThat(releaseBlockedCallback.getCount())
                    .as(
                            "the free connection's deadline must complete while the callback is"
                                    + " stalled")
                    .isEqualTo(1L);
        } finally {
            releaseBlockedCallback.countDown();
            closeQuietly(blockedConnection);
            closeQuietly(freeConnection);
            blockedEventLoopGroup.shutdownGracefully();
        }
    }

    private static Throwable awaitFailure(CompletableFuture<ApiMessage> future) throws Exception {
        try {
            future.get(30, TimeUnit.SECONDS);
            throw new AssertionError("Expected the request to fail because of the RPC deadline.");
        } catch (ExecutionException e) {
            return e.getCause();
        }
    }

    @Test
    void testLazyResponseBufferReleasedWhenResponseDroppedForCompletedRequest() throws Exception {
        assertLazyResponseBufferReleasedWhenDeliveryDropped(LazyDropCause.FUTURE_ALREADY_DONE);
    }

    @Test
    void testLazyResponseBufferReleasedWhenDeadlineWinsDuringDelivery() throws Exception {
        assertLazyResponseBufferReleasedWhenDeliveryDropped(
                LazyDropCause.COMPLETED_WHILE_DELIVERING);
    }

    @Test
    void testLazyResponseBufferReleasedWhenRequestRemovedAfterResponseLookup() throws Exception {
        assertLazyResponseBufferReleasedWhenDeliveryDropped(
                LazyDropCause.REMOVED_AFTER_RESPONSE_LOOKUP);
    }

    /** Checks lazy-response buffer cleanup for failed-delivery races. */
    private void assertLazyResponseBufferReleasedWhenDeliveryDropped(LazyDropCause dropCause)
            throws Exception {
        nettyServer.close();
        buildNettyServer(new NeverRespondingGatewayService());
        MockMetricRegistry metricRegistry = new MockMetricRegistry();
        ServerConnection connection =
                new ServerConnection(
                        bootstrap,
                        serverNode,
                        new ClientMetricGroup(metricRegistry, "client"),
                        clientAuthenticator,
                        REQUEST_TIMEOUT_MS,
                        (con, ignore) -> {});
        RacyInflightRequests inflightRequests = new RacyInflightRequests();
        swapInflightRequests(connection, inflightRequests);
        ByteBuf frame = null;
        try {
            connection.send(ApiKeys.PRODUCE_LOG, produceLogRequest(null));
            assertThat(inflightRequests.awaitRegistration(30, TimeUnit.SECONDS))
                    .as("the connection must be ready to send requests")
                    .isTrue();
            awaitClientHandlerIdle(connection);

            // Keep the synthetic response separate from the connection's live request.
            int requestId = 4242;
            CompletableFuture<ApiMessage> responseFuture = new CompletableFuture<>();
            if (dropCause == LazyDropCause.FUTURE_ALREADY_DONE) {
                responseFuture.completeExceptionally(new TimeoutException("simulated deadline"));
            }
            inflightRequests.put(
                    requestId, newInflightRequest(ApiKeys.FETCH_LOG.id, requestId, responseFuture));
            Field metricsField = ServerConnection.class.getDeclaredField("connectionMetrics");
            metricsField.setAccessible(true);
            ((ConnectionMetrics) metricsField.get(connection))
                    .updateMetricsBeforeSendRequest(ApiKeys.FETCH_LOG, 0);
            ClientHandlerCallback callback = responseCallback(connection);
            switch (dropCause) {
                case COMPLETED_WHILE_DELIVERING:
                    inflightRequests.completeOnRemove(requestId);
                    break;
                case REMOVED_AFTER_RESPONSE_LOOKUP:
                    inflightRequests.runOnGet(
                            requestId,
                            () ->
                                    callback.onRequestFailure(
                                            requestId, new TimeoutException("simulated deadline")));
                    break;
                default:
                    break;
            }

            frame =
                    toByteBuf(
                            MessageCodec.encodeSuccessResponse(
                                    ByteBufAllocator.DEFAULT, requestId, new FetchLogResponse()));
            ChannelHandlerContext context = clientHandlerContext(connection);
            ((NettyClientHandler) context.handler()).channelRead(context, frame);

            assertThat(responseFuture)
                    .as("the late response must not be delivered to the completed request")
                    .isCompletedExceptionally();
            assertThat(frame.refCnt())
                    .as(
                            "a lazily parsed response dropped for an undeliverable request "
                                    + "must release its buffer")
                    .isZero();
            assertThat(metricValue(metricRegistry, CLIENT_REQUESTS_IN_FLIGHT_TOTAL)).isOne();
            assertThat(connection.numInflightRequests()).isOne();
        } finally {
            if (frame != null && frame.refCnt() > 0) {
                frame.release(frame.refCnt());
            }
            closeQuietly(connection);
        }
    }

    /** The interleaving that makes the connection drop an already parsed response. */
    private enum LazyDropCause {
        /** The response future is already completed when the response arrives. */
        FUTURE_ALREADY_DONE,
        /** The deadline completes the response future while the entry is being removed. */
        COMPLETED_WHILE_DELIVERING,
        /** The deadline removes the entry right after the response type has been resolved. */
        REMOVED_AFTER_RESPONSE_LOOKUP
    }

    /** Waits for the event loop to account for and write the registered request. */
    private static void awaitClientHandlerIdle(ServerConnection connection) throws Exception {
        CountDownLatch idle = new CountDownLatch(1);
        clientHandlerContext(connection).executor().execute(idle::countDown);
        assertThat(idle.await(30, TimeUnit.SECONDS))
                .as("the connection event loop must drain the request registration")
                .isTrue();
    }

    /** Closes within a bounded wait without hiding the test's assertion failure. */
    private static void closeQuietly(ServerConnection connection) {
        try {
            connection.close().get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | java.util.concurrent.TimeoutException e) {
            // Preserve assertion failures if cleanup fails.
        }
    }

    private static ChannelHandlerContext clientHandlerContext(ServerConnection connection)
            throws Exception {
        Field channelField = ServerConnection.class.getDeclaredField("channel");
        channelField.setAccessible(true);
        Channel channel = (Channel) channelField.get(connection);
        assertThat(channel).as("the client channel must be established").isNotNull();
        ChannelHandlerContext context = channel.pipeline().context(NettyClientHandler.class);
        assertThat(context).as("the client handler must be attached").isNotNull();
        return context;
    }

    private static Object newInflightRequest(
            int apiKey, int requestId, CompletableFuture<ApiMessage> responseFuture)
            throws Exception {
        Class<?> inflightRequestClass = null;
        for (Class<?> declaredClass : ServerConnection.class.getDeclaredClasses()) {
            if ("InflightRequest".equals(declaredClass.getSimpleName())) {
                inflightRequestClass = declaredClass;
                break;
            }
        }
        assertThat(inflightRequestClass).as("InflightRequest must exist").isNotNull();
        Constructor<?> constructor =
                inflightRequestClass.getDeclaredConstructor(
                        short.class,
                        short.class,
                        int.class,
                        ApiMessage.class,
                        CompletableFuture.class);
        constructor.setAccessible(true);
        return constructor.newInstance(
                (short) apiKey,
                ApiKeys.FETCH_LOG.highestSupportedVersion,
                requestId,
                new FetchLogRequest(),
                responseFuture);
    }

    private static long metricValue(MockMetricRegistry metricRegistry, String metricName) {
        Metric metric = metricRegistry.registeredMetrics.get(metricName);
        assertThat(metric).as("metric '%s' must be registered", metricName).isNotNull();
        return ((Number) ((Gauge<?>) metric).getValue()).longValue();
    }

    private static void swapInflightRequests(
            ServerConnection connection, Map<Integer, Object> inflightRequests) throws Exception {
        waitUntil(connection::isReady, Duration.ofSeconds(30), "Handshake timed out.");
        Field field = ServerConnection.class.getDeclaredField("inflightRequests");
        field.setAccessible(true);
        field.set(connection, inflightRequests);
    }

    private static ClientHandlerCallback responseCallback(ServerConnection connection)
            throws Exception {
        NettyClientHandler handler =
                (NettyClientHandler) clientHandlerContext(connection).handler();
        for (Field field : NettyClientHandler.class.getDeclaredFields()) {
            if (ClientHandlerCallback.class.isAssignableFrom(field.getType())) {
                field.setAccessible(true);
                return (ClientHandlerCallback) field.get(handler);
            }
        }
        throw new IllegalStateException("No client handler callback found on NettyClientHandler.");
    }

    private static CompletableFuture<ApiMessage> responseFutureOf(Object inflightRequest) {
        try {
            Field field = inflightRequest.getClass().getDeclaredField("responseFuture");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            CompletableFuture<ApiMessage> future =
                    (CompletableFuture<ApiMessage>) field.get(inflightRequest);
            return future;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot read the response future of the request.", e);
        }
    }

    private static int apiKeyOf(Object inflightRequest) {
        try {
            Field field = inflightRequest.getClass().getDeclaredField("apiKey");
            field.setAccessible(true);
            return ((Number) field.get(inflightRequest)).intValue();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot read the api key of the request.", e);
        }
    }

    /** A gateway that accepts write requests but never completes them. */
    private static class NeverRespondingGatewayService extends TestingTabletGatewayService {
        @Override
        public CompletableFuture<ProduceLogResponse> produceLog(ProduceLogRequest request) {
            return new CompletableFuture<>();
        }
    }

    /** Injects deadline races during request lookup or removal. */
    private static class RacyInflightRequests extends ConcurrentHashMap<Integer, Object> {

        private final CompletableFuture<Void> registration = new CompletableFuture<>();
        private volatile int registeredRequestId = -1;
        private volatile int completeOnRemoveRequestId = -1;
        private volatile int removeOnGetRequestId = -1;
        private Runnable onGet;

        @Override
        public Object put(Integer key, Object inflightRequest) {
            Object previous = super.put(key, inflightRequest);
            if (apiKeyOf(inflightRequest) == ApiKeys.PRODUCE_LOG.id) {
                registeredRequestId = key;
                registration.complete(null);
            }
            return previous;
        }

        @Override
        public Object get(Object key) {
            Object registered = super.get(key);
            if (registered != null && isRaceTarget(key, removeOnGetRequestId)) {
                // fire once, and only for the targeted request
                removeOnGetRequestId = -1;
                // Expire the request between lookup and delivery.
                onGet.run();
            }
            return registered;
        }

        @Override
        public Object remove(Object key) {
            Object removed = super.remove(key);
            if (removed != null && isRaceTarget(key, completeOnRemoveRequestId)) {
                // fire once, and only for the targeted request
                completeOnRemoveRequestId = -1;
                // Complete the future before response delivery checks it.
                completeWithDeadline(removed);
            }
            return removed;
        }

        void completeOnRemove(int requestId) {
            this.completeOnRemoveRequestId = requestId;
        }

        void runOnGet(int requestId, Runnable onGet) {
            this.onGet = onGet;
            this.removeOnGetRequestId = requestId;
        }

        int registeredRequestId() {
            return registeredRequestId;
        }

        boolean awaitRegistration(long timeout, TimeUnit unit) throws Exception {
            try {
                registration.get(timeout, unit);
                return true;
            } catch (java.util.concurrent.TimeoutException e) {
                return false;
            }
        }

        private static boolean isRaceTarget(Object key, int requestId) {
            return requestId >= 0 && key instanceof Integer && ((Integer) key) == requestId;
        }

        private static void completeWithDeadline(Object inflightRequest) {
            responseFutureOf(inflightRequest)
                    .completeExceptionally(new TimeoutException("simulated RPC deadline expired"));
        }
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

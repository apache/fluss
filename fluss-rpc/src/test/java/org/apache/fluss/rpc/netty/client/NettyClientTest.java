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
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.util.NOPMetricsGroup;
import org.apache.fluss.rpc.TestingGatewayService;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.ApiVersionsRequest;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.messages.LookupRequest;
import org.apache.fluss.rpc.messages.PbLookupReqForBucket;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.rpc.netty.NettyUtils;
import org.apache.fluss.rpc.netty.server.NettyServer;
import org.apache.fluss.rpc.netty.server.RequestsMetrics;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.security.auth.AuthenticationFactory;
import org.apache.fluss.utils.ExponentialBackoff;
import org.apache.fluss.utils.NetUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.BindException;
import java.net.ConnectException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.utils.NetUtils.getAvailablePort;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link NettyClient}. */
final class NettyClientTest {

    private Configuration conf;
    private NettyClient nettyClient;
    private ServerNode serverNode;
    private NettyServer nettyServer;
    private TestingGatewayService service;

    @BeforeEach
    public void setup() throws Exception {
        conf = new Configuration();
        // 3 worker threads is enough for this test
        conf.setInt(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS, 3);
        nettyClient = new NettyClient(conf, TestingClientMetricGroup.newInstance());
        buildNettyServer(1);
    }

    @AfterEach
    public void cleanup() throws Exception {
        if (nettyServer != null) {
            nettyServer.close();
        }
        if (nettyClient != null) {
            nettyClient.close();
        }
    }

    @Test
    void testSendIncompleteRequest() {
        GetTableInfoRequest request = new GetTableInfoRequest();

        // get table request without table path.
        assertThatThrownBy(
                        () ->
                                nettyClient
                                        .sendRequest(serverNode, ApiKeys.GET_TABLE_INFO, request)
                                        .get())
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("Failed to encode request for 'GET_TABLE_INFO(1007)'")
                .hasRootCauseMessage("Some required fields are missing");
    }

    @Test
    void testSendRequestToWrongServerType() {
        LookupRequest lookupRequest = new LookupRequest().setTableId(1);
        PbLookupReqForBucket pbLookupReqForBucket =
                new PbLookupReqForBucket().setPartitionId(1).setBucketId(1);
        pbLookupReqForBucket.addKey("key".getBytes());
        lookupRequest.addAllBucketsReqs(Collections.singleton(pbLookupReqForBucket));

        // LookupRequest isn't support by Coordinator server, See ApiManager for details. In this
        // case, we will get an exception.
        assertThatThrownBy(
                        () ->
                                nettyClient
                                        .sendRequest(serverNode, ApiKeys.LOOKUP, lookupRequest)
                                        .get())
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("The server does not support LOOKUP(1017)")
                .hasRootCauseMessage("The server does not support LOOKUP(1017)");
    }

    @Test
    void testRequestsProcessedInOrder() throws Exception {
        int numRequests = 100;
        List<CompletableFuture<ApiMessage>> futures = new ArrayList<>();
        for (int i = 0; i < numRequests; i++) {
            ApiVersionsRequest request =
                    new ApiVersionsRequest()
                            .setClientSoftwareName("testing_client" + i)
                            .setClientSoftwareVersion("1.0");
            futures.add(nettyClient.sendRequest(serverNode, ApiKeys.API_VERSIONS, request));
        }
        FutureUtils.waitForAll(futures).get();
        // we have one more api version request for rpc handshake.
        assertThat(service.getProcessorThreadNames()).hasSize(numRequests + 1);
        Set<String> deduplicatedThreadNames = new HashSet<>(service.getProcessorThreadNames());
        // there should only one thread to process the requests
        // since all requests are from the same client.
        assertThat(deduplicatedThreadNames).hasSize(1);
    }

    @Test
    void testServerDisconnection() throws Exception {
        ApiVersionsRequest request =
                new ApiVersionsRequest()
                        .setClientSoftwareName("testing_client_100")
                        .setClientSoftwareVersion("1.0");
        nettyClient.sendRequest(serverNode, ApiKeys.API_VERSIONS, request).get();
        assertThat(nettyClient.connections().size()).isEqualTo(1);
        assertThat(nettyClient.connections().get(serverNode.uid()).getServerNode())
                .isEqualTo(serverNode);

        // close the netty server.
        nettyServer.close();
        // Wait for the old connection to be removed before attempting a new connection.
        retry(Duration.ofSeconds(10), () -> assertThat(nettyClient.connections()).isEmpty());

        assertThatThrownBy(
                        () ->
                                nettyClient
                                        .sendRequest(serverNode, ApiKeys.API_VERSIONS, request)
                                        .get())
                .rootCause()
                .isInstanceOfAny(ConnectException.class, DisconnectException.class);
        retry(Duration.ofSeconds(10), () -> assertThat(nettyClient.connections()).isEmpty());

        // restart the netty server.
        buildNettyServer(1);
        nettyClient.sendRequest(serverNode, ApiKeys.API_VERSIONS, request).get();
        assertThat(nettyClient.connections().size()).isEqualTo(1);
        assertThat(nettyClient.connections().get(serverNode.uid()).getServerNode())
                .isEqualTo(serverNode);
    }

    @Test
    void testFailedConnectionRemovedBeforeRequestCompletion() throws Exception {
        nettyServer.close();
        ServerConnection failed = nettyClient.getOrCreateConnection(serverNode);
        CompletableFuture<Boolean> removed =
                failed.send(
                                ApiKeys.API_VERSIONS,
                                new ApiVersionsRequest()
                                        .setClientSoftwareName("test")
                                        .setClientSoftwareVersion("1.0"))
                        .handle(
                                (response, failure) ->
                                        !nettyClient.connections().containsValue(failed));
        assertThat(removed.get(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void testReconnectBackoffGrowthCapAndReadyReset() throws Exception {
        AtomicLong clock = new AtomicLong();
        nettyClient.close();
        nettyClient =
                new NettyClient(
                        conf,
                        TestingClientMetricGroup.newInstance(),
                        clock::get,
                        new ExponentialBackoff(100, 2, 5000, 0));
        nettyServer.close();
        for (long delay : new long[] {100, 200, 400, 800, 1600, 3200, 5000, 5000}) {
            assertThatThrownBy(() -> sendApiVersions().get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(ConnectException.class);
            assertThat(nettyClient.connections()).isEmpty();
            assertThat(nettyClient.isBackingOff(serverNode)).isTrue();
            clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(delay - 2));
            assertThat(nettyClient.isBackingOff(serverNode)).isTrue();
            clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(2));
            assertThat(nettyClient.isBackingOff(serverNode)).isFalse();
        }

        startNettyServer();
        sendApiVersions().get(10, TimeUnit.SECONDS);
        assertThat(nettyClient.isBackingOff(serverNode)).isFalse();
        nettyServer.close();
        retry(Duration.ofSeconds(10), () -> assertThat(nettyClient.connections()).isEmpty());
        assertThat(nettyClient.isBackingOff(serverNode)).isTrue();
        clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(100));
        assertThat(nettyClient.isBackingOff(serverNode)).isFalse();
    }

    @Test
    void testReentrantRetryWaitsForBackoffAndSurvivesRestart() throws Exception {
        AtomicLong clock = new AtomicLong();
        nettyClient.close();
        nettyClient =
                new NettyClient(
                        conf,
                        TestingClientMetricGroup.newInstance(),
                        clock::get,
                        new ExponentialBackoff(300, 2, 5000, 0));
        nettyServer.close();
        AtomicReference<ServerConnection> retryConnection = new AtomicReference<>();
        long firstAttemptStartedAt = System.nanoTime();
        CompletableFuture<ApiMessage> retryResponse =
                sendApiVersions()
                        .handle(
                                (response, failure) -> {
                                    assertThat(failure).isNotNull();
                                    assertThat(nettyClient.isBackingOff(serverNode)).isTrue();
                                    assertThat(nettyClient.connections()).isEmpty();
                                    try {
                                        startNettyServer();
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                    CompletableFuture<ApiMessage> next = sendApiVersions();
                                    retryConnection.set(
                                            nettyClient.connections().get(serverNode.uid()));
                                    return next;
                                })
                        .thenCompose(response -> response);
        retry(Duration.ofSeconds(10), () -> assertThat(retryConnection.get()).isNotNull());

        List<CompletableFuture<ApiMessage>> requests = new ArrayList<>();
        requests.add(retryResponse);
        for (int i = 0; i < 20; i++) {
            requests.add(
                    CompletableFuture.supplyAsync(() -> sendApiVersions()).thenCompose(f -> f));
        }
        FutureUtils.waitForAll(requests).get(10, TimeUnit.SECONDS);
        assertThat(System.nanoTime() - firstAttemptStartedAt)
                .isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(250));
        assertThat(nettyClient.connections().get(serverNode.uid())).isSameAs(retryConnection.get());
        // One handshake and all new requests, with no replay of the failed request.
        assertThat(service.getProcessorThreadNames()).hasSize(22);
        assertThat(nettyClient.isBackingOff(serverNode)).isFalse();
    }

    @Test
    void testEndpointReplacementIgnoresBackoffAndOldCallbacks() throws Exception {
        AtomicLong clock = new AtomicLong();
        nettyClient.close();
        nettyClient =
                new NettyClient(
                        conf,
                        TestingClientMetricGroup.newInstance(),
                        clock::get,
                        new ExponentialBackoff(60000, 2, 60000, 0));
        ServerNode first = serverNode;
        sendApiVersions().get(10, TimeUnit.SECONDS);
        ServerConnection old = nettyClient.connections().get(first.uid());
        nettyServer.close();
        retry(Duration.ofSeconds(10), () -> assertThat(nettyClient.connections()).isEmpty());
        assertThat(nettyClient.isBackingOff(first)).isTrue();
        buildNettyServer(first.id());
        assertThat(nettyClient.isBackingOff(serverNode)).isFalse();
        sendApiVersions().get(10, TimeUnit.SECONDS);
        ServerConnection replacement = nettyClient.connections().get(serverNode.uid());
        old.new ResponseCallback().onFailure(new IOException("obsolete channel"));
        assertThat(nettyClient.connections().get(serverNode.uid())).isSameAs(replacement);
        assertThat(nettyClient.isBackingOff(serverNode)).isFalse();

        // Bootstrap endpoints share ts--1 but must not share a connection or backoff.
        ServerNode bootstrap = new ServerNode(-1, first.host(), first.port(), ServerType.UNKNOWN);
        assertThatThrownBy(
                        () ->
                                nettyClient
                                        .sendRequest(
                                                bootstrap,
                                                ApiKeys.API_VERSIONS,
                                                apiVersionsRequest())
                                        .get(10, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(ConnectException.class);
        ServerNode liveBootstrap =
                new ServerNode(-1, serverNode.host(), serverNode.port(), ServerType.UNKNOWN);
        assertThat(nettyClient.isBackingOff(bootstrap)).isTrue();
        assertThat(nettyClient.isBackingOff(liveBootstrap)).isFalse();
        nettyClient
                .sendRequest(liveBootstrap, ApiKeys.API_VERSIONS, apiVersionsRequest())
                .get(10, TimeUnit.SECONDS);
    }

    @Test
    void testDisconnectAndClientCloseCancelDelayedConnect() throws Exception {
        nettyClient.close();
        AtomicLong clock = new AtomicLong();
        nettyClient =
                new NettyClient(
                        conf,
                        TestingClientMetricGroup.newInstance(),
                        clock::get,
                        new ExponentialBackoff(60000, 2, 60000, 0));
        nettyServer.close();
        assertThatThrownBy(() -> sendApiVersions().get(10, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(ConnectException.class);
        CompletableFuture<ApiMessage> pending = sendApiVersions();
        assertThat(pending).isNotDone();
        nettyClient.disconnect(serverNode.uid()).get(10, TimeUnit.SECONDS);
        assertThatThrownBy(() -> pending.get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(NetworkException.class);
        assertThat(nettyClient.connections()).isEmpty();
        // Local close does not extend the existing failure's backoff.
        clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(60000));
        assertThat(nettyClient.isBackingOff(serverNode)).isFalse();

        assertThatThrownBy(() -> sendApiVersions().get(10, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(ConnectException.class);
        CompletableFuture<ApiMessage> closing = sendApiVersions();
        assertThat(closing).isNotDone();
        nettyClient.close();
        assertThatThrownBy(() -> closing.get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(NetworkException.class);
        assertThat(nettyClient.connections()).isEmpty();
    }

    private ApiVersionsRequest apiVersionsRequest() {
        return new ApiVersionsRequest()
                .setClientSoftwareName("test")
                .setClientSoftwareVersion("1.0");
    }

    private CompletableFuture<ApiMessage> sendApiVersions() {
        return nettyClient.sendRequest(serverNode, ApiKeys.API_VERSIONS, apiVersionsRequest());
    }

    @Test
    void testEndpointReplacementConstructionFailureKeepsOldConnectionTracked() throws Exception {
        nettyClient.close();
        AtomicBoolean reject = new AtomicBoolean();
        nettyClient =
                new NettyClient(
                        conf,
                        TestingClientMetricGroup.newInstance(),
                        System::nanoTime,
                        new ExponentialBackoff(100, 2, 5000, 0),
                        () -> {
                            if (reject.get()) {
                                throw new IllegalStateException("authenticator unavailable");
                            }
                            return AuthenticationFactory.loadClientAuthenticatorSupplier(conf)
                                    .get();
                        });
        sendApiVersions().get(10, TimeUnit.SECONDS);
        ServerConnection original = nettyClient.connections().get(serverNode.uid());
        ServerNode changed =
                new ServerNode(
                        serverNode.id(),
                        serverNode.host(),
                        serverNode.port() + 1,
                        serverNode.serverType());
        reject.set(true);
        assertThatThrownBy(() -> nettyClient.connect(changed))
                .hasMessage("authenticator unavailable");
        assertThat(nettyClient.connections().get(serverNode.uid())).isSameAs(original);
        assertThat(original.isReady()).isTrue();
        nettyClient.close();
        assertThat(original.getConnectionState())
                .isEqualTo(ServerConnection.ConnectionState.DISCONNECTED);
    }

    @Test
    void testProtocolAndBusinessErrorsDoNotBackOff() throws Exception {
        ServerNode wrongType =
                new ServerNode(
                        serverNode.id(),
                        serverNode.host(),
                        serverNode.port(),
                        ServerType.TABLET_SERVER);
        assertThatThrownBy(
                        () ->
                                nettyClient
                                        .sendRequest(
                                                wrongType,
                                                ApiKeys.API_VERSIONS,
                                                apiVersionsRequest())
                                        .get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(InvalidServerTypeException.class);
        assertThat(nettyClient.isBackingOff(wrongType)).isFalse();
        assertThatThrownBy(
                        () ->
                                nettyClient
                                        .sendRequest(
                                                serverNode,
                                                ApiKeys.GET_TABLE_INFO,
                                                new GetTableInfoRequest())
                                        .get(10, TimeUnit.SECONDS))
                .hasMessageContaining("Failed to encode request");
        assertThat(nettyClient.isBackingOff(serverNode)).isFalse();
        nettyClient.disconnect(serverNode.uid()).get(10, TimeUnit.SECONDS);
        assertThat(nettyClient.isBackingOff(serverNode)).isFalse();
    }

    @Test
    void testBindFailureDetection() {
        Throwable ex = new BindException();
        assertThat(NettyUtils.isBindFailure(ex)).isTrue();

        ex = new Exception(new BindException());
        assertThat(NettyUtils.isBindFailure(ex)).isTrue();

        ex = new Exception();
        assertThat(NettyUtils.isBindFailure(ex)).isFalse();

        ex = new RuntimeException();
        assertThat(NettyUtils.isBindFailure(ex)).isFalse();
    }

    @Test
    void testMultipleEndpoint() throws Exception {
        MetricGroup metricGroup = NOPMetricsGroup.newInstance();
        try (NetUtils.Port availablePort1 = getAvailablePort();
                NetUtils.Port availablePort2 = getAvailablePort();
                NettyServer multipleEndpointsServer =
                        new NettyServer(
                                conf,
                                Arrays.asList(
                                        new Endpoint(
                                                "localhost", availablePort1.getPort(), "INTERNAL"),
                                        new Endpoint(
                                                "localhost", availablePort2.getPort(), "CLIENT")),
                                service,
                                metricGroup,
                                RequestsMetrics.createCoordinatorServerRequestMetrics(
                                        metricGroup))) {
            multipleEndpointsServer.start();
            ApiVersionsRequest request =
                    new ApiVersionsRequest()
                            .setClientSoftwareName("testing_client_100")
                            .setClientSoftwareVersion("1.0");
            nettyClient
                    .sendRequest(
                            new ServerNode(
                                    2,
                                    "localhost",
                                    availablePort1.getPort(),
                                    ServerType.COORDINATOR),
                            ApiKeys.API_VERSIONS,
                            request)
                    .get();
            assertThat(nettyClient.connections().size()).isEqualTo(1);
            ServerConnection firstEndpoint = nettyClient.connections().get("cs-2");
            ServerNode secondEndpoint =
                    new ServerNode(
                            2, "localhost", availablePort2.getPort(), ServerType.COORDINATOR);
            nettyClient
                    .sendRequest(secondEndpoint, ApiKeys.API_VERSIONS, request)
                    .get(10, TimeUnit.SECONDS);
            ServerConnection replacement = nettyClient.connections().get(secondEndpoint.uid());
            assertThat(firstEndpoint.getConnectionState())
                    .isEqualTo(ServerConnection.ConnectionState.DISCONNECTED);
            firstEndpoint.new ResponseCallback().onFailure(new IOException("obsolete endpoint"));
            assertThat(nettyClient.connections().get(secondEndpoint.uid())).isSameAs(replacement);
            assertThat(nettyClient.isBackingOff(secondEndpoint)).isFalse();
            try (NettyClient client =
                    new NettyClient(conf, TestingClientMetricGroup.newInstance())) {
                client.sendRequest(
                                new ServerNode(
                                        2,
                                        "localhost",
                                        availablePort2.getPort(),
                                        ServerType.COORDINATOR),
                                ApiKeys.API_VERSIONS,
                                request)
                        .get();
                assertThat(client.connections().size()).isEqualTo(1);
            }
        }
    }

    @Test
    void testExceptionWhenInitializeServerConnection() throws Exception {
        ApiVersionsRequest request =
                new ApiVersionsRequest()
                        .setClientSoftwareName("testing_client_100")
                        .setClientSoftwareVersion("1.0");
        // close the netty server.
        nettyServer.close();

        // send request and create server connection.
        assertThatThrownBy(
                        () ->
                                nettyClient
                                        .sendRequest(serverNode, ApiKeys.API_VERSIONS, request)
                                        .get())
                .hasMessageContaining("Disconnected from node");
        assertThat(nettyClient.connections()).isEmpty();
    }

    /**
     * A request whose closed check passed right before the client closed must fail instead of
     * hanging: close() may have finished before the request created its connection, so the
     * connection is created on an already terminated event loop.
     */
    @Test
    void testConnectionCreatedAfterCloseFailsRequests() throws Exception {
        nettyClient.close();

        // simulates a sendRequest that passed the isClosed check right before close() started
        ServerConnection connection = nettyClient.getOrCreateConnection(serverNode);
        CompletableFuture<ApiMessage> response =
                connection.send(
                        ApiKeys.API_VERSIONS,
                        new ApiVersionsRequest()
                                .setClientSoftwareName("test")
                                .setClientSoftwareVersion("1.0"));

        assertThatThrownBy(() -> response.get(1, TimeUnit.MINUTES))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(NetworkException.class);
        assertThat(nettyClient.connections()).isEmpty();
    }

    private void buildNettyServer(int serverId) throws Exception {
        try (NetUtils.Port availablePort = getAvailablePort()) {
            serverNode =
                    new ServerNode(
                            serverId, "localhost", availablePort.getPort(), ServerType.COORDINATOR);
            startNettyServer();
        }
    }

    private void startNettyServer() throws Exception {
        service = new TestingGatewayService();
        MetricGroup metricGroup = NOPMetricsGroup.newInstance();
        nettyServer =
                new NettyServer(
                        conf,
                        Collections.singleton(
                                new Endpoint(serverNode.host(), serverNode.port(), "INTERNAL")),
                        service,
                        metricGroup,
                        RequestsMetrics.createCoordinatorServerRequestMetrics(metricGroup));
        nettyServer.start();
    }
}

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

package org.apache.fluss.rpc.netty.server;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.CharacterFilter;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.Meter;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.groups.GenericMetricGroup;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.util.NOPMetricsGroup;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.ApiVersionsRequest;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.FetchLogResponse;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.rpc.netty.client.NettyClient;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.utils.NetUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.utils.NetUtils.getAvailablePort;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies listener isolation through real RPC connections. */
class NettyServerListenerIsolationTest {

    @Test
    void testReplicationCompletesUnderExternalBackpressure() throws Exception {
        try (ListenerFixture fixture = new ListenerFixture(1, "INTERNAL", "FLUSS", "INTERNAL")) {
            fixture.fetch("INTERNAL", 1).get(10, TimeUnit.SECONDS);
            fixture.send("FLUSS", "block-external");
            fixture.awaitBlocked();
            fixture.send("FLUSS", "queued-external");
            retry(Duration.ofSeconds(10), () -> assertThat(fixture.queuedRequests()).isEqualTo(1));

            assertThat(fixture.fetch("INTERNAL", 1).get(10, TimeUnit.SECONDS))
                    .isInstanceOf(FetchLogResponse.class);
            assertThat(fixture.queuedRequests()).isEqualTo(1);
        }
    }

    @Test
    void testInternalNetworkRemainsAvailableWhenExternalNetworkIsBlocked() throws Exception {
        try (ListenerFixture fixture = new ListenerFixture(0, "INTERNAL", "FLUSS", "INTERNAL")) {
            fixture.fetch("INTERNAL", 1).get(10, TimeUnit.SECONDS);
            CountDownLatch networkBlocked = new CountDownLatch(1);
            fixture.server
                    .getScheduledExecutor()
                    .execute(
                            () -> {
                                networkBlocked.countDown();
                                fixture.release.join();
                            });
            assertThat(networkBlocked.await(10, TimeUnit.SECONDS)).isTrue();

            assertThat(fixture.fetch("INTERNAL", 1).get(10, TimeUnit.SECONDS))
                    .isInstanceOf(FetchLogResponse.class);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"FLUSS", "INTERNAL"})
    void testSingleListenerKeepsSharedCapacity(String listener) throws Exception {
        try (ListenerFixture fixture = new ListenerFixture(1, listener, listener)) {
            CompletableFuture<ApiMessage> blocked = fixture.send(listener, "block-shared");
            fixture.awaitBlocked();
            CompletableFuture<ApiMessage> queued = fixture.send(listener, "queued-shared");
            retry(Duration.ofSeconds(10), () -> assertThat(fixture.queuedRequests()).isEqualTo(1));
            assertThat(queued).isNotDone();
            assertThat(fixture.registry.metrics(MetricNames.REQUEST_QUEUE_SIZE)).hasSize(2);

            fixture.release.complete(null);
            assertThat(blocked.get(10, TimeUnit.SECONDS)).isInstanceOf(ApiVersionsResponse.class);
            assertThat(queued.get(10, TimeUnit.SECONDS)).isInstanceOf(ApiVersionsResponse.class);
        }
    }

    @Test
    void testExternalListenersShareCapacity() throws Exception {
        try (ListenerFixture fixture =
                new ListenerFixture(1, "INTERNAL", "FLUSS", "EXTERNAL", "INTERNAL")) {
            fixture.send("FLUSS", "block-external");
            fixture.awaitBlocked();
            CompletableFuture<ApiMessage> queued = fixture.send("EXTERNAL", "queued-external");
            retry(Duration.ofSeconds(10), () -> assertThat(fixture.queuedRequests()).isEqualTo(1));
            assertThat(queued).isNotDone();
            assertThat(fixture.registry.metrics(MetricNames.REQUEST_QUEUE_SIZE)).hasSize(3);
            assertThat(fixture.fetch("INTERNAL", 1).get(10, TimeUnit.SECONDS))
                    .isInstanceOf(FetchLogResponse.class);

            fixture.release.complete(null);
            assertThat(queued.get(10, TimeUnit.SECONDS)).isInstanceOf(ApiVersionsResponse.class);
        }
    }

    @Test
    void testQueueMetricsKeepProcessorAndTotalSemantics() throws Exception {
        try (ListenerFixture fixture = new ListenerFixture(2, "INTERNAL", "FLUSS", "INTERNAL")) {
            fixture.send("FLUSS", "block-external");
            fixture.send("INTERNAL", "block-internal");
            fixture.awaitBlocked();
            fixture.send("FLUSS", "queued-external");
            fixture.send("INTERNAL", "queued-internal");
            retry(Duration.ofSeconds(10), () -> assertThat(fixture.queuedRequests()).isEqualTo(2));

            List<RequestsMetricsTest.RegisteredMetric> gauges =
                    fixture.registry.metrics(MetricNames.REQUEST_QUEUE_SIZE);
            assertThat(gauges).hasSize(3);
            assertThat(gauges)
                    .filteredOn(metric -> scope(metric).equals("tabletserver_request"))
                    .singleElement()
                    .satisfies(
                            metric -> {
                                assertThat(
                                                metric.group.getLogicalScope(
                                                        CharacterFilter.NO_OP_FILTER, '_'))
                                        .isEqualTo("tabletserver_request");
                                assertThat(((Gauge<?>) metric.metric).getValue()).isEqualTo(2);
                            });
            assertThat(gauges)
                    .filteredOn(metric -> !scope(metric).equals("tabletserver_request"))
                    .extracting(NettyServerListenerIsolationTest::scope)
                    .containsExactlyInAnyOrder(
                            "tabletserver_request_processor_index_0",
                            "tabletserver_request_processor_index_1");
            assertThat(gauges)
                    .filteredOn(metric -> !scope(metric).equals("tabletserver_request"))
                    .allSatisfy(
                            metric -> {
                                assertThat(metric.group.getAllVariables()).isEmpty();
                                assertThat(((Gauge<?>) metric.metric).getValue()).isEqualTo(1);
                            });
        }
    }

    @Test
    void testRequestMetricsRemainSharedAndKeepFetchClassification() throws Exception {
        try (ListenerFixture fixture = new ListenerFixture(0, "INTERNAL", "FLUSS", "INTERNAL")) {
            fixture.fetch("FLUSS", -1).get(10, TimeUnit.SECONDS);
            fixture.fetch("INTERNAL", 1).get(10, TimeUnit.SECONDS);
            fixture.fetch("INTERNAL", -1).get(10, TimeUnit.SECONDS);

            retry(
                    Duration.ofSeconds(10),
                    () -> {
                        assertThat(
                                        fixture.registry.metrics(
                                                MetricNames.REQUESTS_RATE, "fetchLogClient"))
                                .singleElement()
                                .satisfies(
                                        metric ->
                                                assertThat(((Meter) metric.metric).getCount())
                                                        .isEqualTo(2));
                        assertThat(
                                        fixture.registry.metrics(
                                                MetricNames.REQUESTS_RATE, "fetchLogFollower"))
                                .singleElement()
                                .satisfies(
                                        metric ->
                                                assertThat(((Meter) metric.metric).getCount())
                                                        .isEqualTo(1));
                    });
            assertThat(
                            fixture.registry.metrics(
                                    MetricNames.REQUEST_QUEUE_TIME_MS, "fetchLogClient"))
                    .singleElement()
                    .satisfies(
                            metric -> {
                                assertThat(metric.group.getAllVariables())
                                        .containsOnlyKeys("request")
                                        .containsEntry("request", "fetchLogClient");
                                assertThat(
                                                metric.group.getLogicalScope(
                                                        CharacterFilter.NO_OP_FILTER, '_'))
                                        .isEqualTo("tabletserver_request");
                            });
        }
    }

    @Test
    void testClosingServerStopsBothListeners() throws Exception {
        try (ListenerFixture fixture = new ListenerFixture(0, "INTERNAL", "FLUSS", "INTERNAL")) {
            List<Endpoint> endpoints = fixture.server.getBindEndpoints();
            fixture.fetch("FLUSS", -1).get(10, TimeUnit.SECONDS);
            fixture.fetch("INTERNAL", 1).get(10, TimeUnit.SECONDS);
            fixture.server.closeAsync().get(10, TimeUnit.SECONDS);
            fixture.server.closeAsync().get(10, TimeUnit.SECONDS);

            for (Endpoint endpoint : endpoints) {
                try (Socket socket = new Socket()) {
                    assertThatThrownBy(
                                    () ->
                                            socket.connect(
                                                    new InetSocketAddress(
                                                            "localhost", endpoint.getPort()),
                                                    1000))
                            .isInstanceOf(ConnectException.class);
                }
            }
        }
    }

    @Test
    void testFailedSecondBindReleasesFirstListener() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.INTERNAL_LISTENER_NAME, "INTERNAL");
        conf.setInt(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS, 1);
        conf.setInt(ConfigOptions.NETTY_SERVER_NUM_NETWORK_THREADS, 1);
        MetricGroup metrics = NOPMetricsGroup.newInstance();
        try (NetUtils.Port available = getAvailablePort();
                ServerSocket occupied =
                        new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
                NettyServer server =
                        new NettyServer(
                                conf,
                                Arrays.asList(
                                        new Endpoint("127.0.0.1", available.getPort(), "FLUSS"),
                                        new Endpoint(
                                                "localhost", occupied.getLocalPort(), "INTERNAL")),
                                new TestingTabletGatewayService(),
                                metrics,
                                RequestsMetrics.createTabletServerRequestMetrics(metrics))) {
            assertThatThrownBy(server::start).isInstanceOf(IOException.class);
            server.closeAsync().get(10, TimeUnit.SECONDS);
            try (ServerSocket rebound =
                    new ServerSocket(available.getPort(), 50, InetAddress.getByName("127.0.0.1"))) {
                assertThat(rebound.isBound()).isTrue();
            }
        }
    }

    private static String scope(RequestsMetricsTest.RegisteredMetric metric) {
        return metric.group.getLogicalScope(CharacterFilter.NO_OP_FILTER, '_');
    }

    private static final class ListenerFixture implements AutoCloseable {
        private final RequestsMetricsTest.RecordingMetricRegistry registry =
                new RequestsMetricsTest.RecordingMetricRegistry();
        private final CompletableFuture<Void> release = new CompletableFuture<>();
        private final CountDownLatch blocked;
        private final NettyServer server;
        private final Map<String, NettyClient> clients = new HashMap<>();
        private final Map<String, ServerNode> nodes = new HashMap<>();

        private ListenerFixture(int blockedRequests, String internalListener, String... listeners)
                throws Exception {
            blocked = new CountDownLatch(blockedRequests);
            Configuration conf = new Configuration();
            conf.set(ConfigOptions.INTERNAL_LISTENER_NAME, internalListener);
            conf.setInt(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS, 1);
            conf.setInt(ConfigOptions.NETTY_SERVER_NUM_NETWORK_THREADS, 1);
            conf.setInt(ConfigOptions.NETTY_SERVER_MAX_QUEUED_REQUESTS, 1);
            TestingTabletGatewayService service =
                    new TestingTabletGatewayService() {
                        @Override
                        public CompletableFuture<ApiVersionsResponse> apiVersions(
                                ApiVersionsRequest request) {
                            if (request.getClientSoftwareName().startsWith("block-")) {
                                blocked.countDown();
                                release.join();
                            }
                            return super.apiVersions(request);
                        }

                        @Override
                        public CompletableFuture<FetchLogResponse> fetchLog(
                                FetchLogRequest request) {
                            return CompletableFuture.completedFuture(new FetchLogResponse());
                        }
                    };
            MetricGroup metrics = new GenericMetricGroup(registry, null, "tabletserver");
            server =
                    new NettyServer(
                            conf,
                            Arrays.stream(listeners)
                                    .map(listener -> new Endpoint("localhost", 0, listener))
                                    .collect(Collectors.toList()),
                            service,
                            metrics,
                            RequestsMetrics.createTabletServerRequestMetrics(metrics));
            try {
                server.start();
                for (Endpoint endpoint : server.getBindEndpoints()) {
                    clients.put(
                            endpoint.getListenerName(),
                            new NettyClient(conf, TestingClientMetricGroup.newInstance()));
                    nodes.put(
                            endpoint.getListenerName(),
                            new ServerNode(
                                    1, "localhost", endpoint.getPort(), ServerType.TABLET_SERVER));
                }
            } catch (Exception e) {
                close();
                throw e;
            }
        }

        private CompletableFuture<ApiMessage> send(String listener, String name) {
            return clients.get(listener)
                    .sendRequest(
                            nodes.get(listener),
                            ApiKeys.API_VERSIONS,
                            new ApiVersionsRequest()
                                    .setClientSoftwareName(name)
                                    .setClientSoftwareVersion("1"));
        }

        private CompletableFuture<ApiMessage> fetch(String listener, int followerId) {
            return clients.get(listener)
                    .sendRequest(
                            nodes.get(listener),
                            ApiKeys.FETCH_LOG,
                            new FetchLogRequest()
                                    .setFollowerServerId(followerId)
                                    .setMaxBytes(1024));
        }

        private void awaitBlocked() throws InterruptedException {
            assertThat(blocked.await(10, TimeUnit.SECONDS)).isTrue();
        }

        private int queuedRequests() {
            return registry.metrics(MetricNames.REQUEST_QUEUE_SIZE).stream()
                    .filter(metric -> scope(metric).equals("tabletserver_request"))
                    .mapToInt(metric -> ((Number) ((Gauge<?>) metric.metric).getValue()).intValue())
                    .sum();
        }

        @Override
        public void close() throws Exception {
            release.complete(null);
            for (NettyClient client : clients.values()) {
                client.close();
            }
            server.close();
        }
    }
}

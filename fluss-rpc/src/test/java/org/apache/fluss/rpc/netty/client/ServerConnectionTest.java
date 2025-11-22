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
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.MetricType;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;
import org.apache.fluss.metrics.util.NOPMetricsGroup;
import org.apache.fluss.rpc.TestingGatewayService;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.messages.GetTableSchemaRequest;
import org.apache.fluss.rpc.messages.LookupRequest;
import org.apache.fluss.rpc.messages.PbLookupReqForBucket;
import org.apache.fluss.rpc.messages.PbTablePath;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.rpc.netty.client.ServerConnection.ConnectionState;
import org.apache.fluss.rpc.netty.server.NettyServer;
import org.apache.fluss.rpc.netty.server.RequestsMetrics;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.security.auth.AuthenticationFactory;
import org.apache.fluss.security.auth.ClientAuthenticator;
import org.apache.fluss.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.fluss.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.fluss.utils.NetUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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
                        .handler(new ClientChannelInitializer(5000));
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
                        (con, ignore) -> {},
                        false);
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
                        (con, ignore) -> {},
                        false);
        ServerConnection connection2 =
                new ServerConnection(
                        bootstrap,
                        serverNode2,
                        client,
                        clientAuthenticator,
                        (con, ignore) -> {},
                        false);
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

    private void buildNettyServer() throws Exception {
        try (NetUtils.Port availablePort = getAvailablePort();
                NetUtils.Port availablePort2 = getAvailablePort()) {
            serverNode =
                    new ServerNode(
                            1, "localhost", availablePort.getPort(), ServerType.TABLET_SERVER);
            serverNode2 =
                    new ServerNode(
                            2, "localhost", availablePort2.getPort(), ServerType.TABLET_SERVER);
            service = new TestingTabletGatewayService();
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

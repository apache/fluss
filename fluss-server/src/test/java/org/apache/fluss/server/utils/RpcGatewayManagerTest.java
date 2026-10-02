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

package org.apache.fluss.server.utils;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.util.NOPMetricsGroup;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.LookupRequest;
import org.apache.fluss.rpc.messages.LookupResponse;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.rpc.netty.client.NettyClient;
import org.apache.fluss.rpc.netty.server.NettyServer;
import org.apache.fluss.rpc.netty.server.RequestsMetrics;
import org.apache.fluss.utils.NetUtils;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.utils.NetUtils.getAvailablePort;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link org.apache.fluss.server.utils.RpcGatewayManager}. */
class RpcGatewayManagerTest {

    @Test
    void testRpcGatewayManage() throws Exception {
        RpcGatewayManager<TabletServerGateway> gatewayRpcGatewayManager =
                new RpcGatewayManager<>(
                        new NettyClient(
                                new Configuration(), TestingClientMetricGroup.newInstance()),
                        TabletServerGateway.class);

        ServerNode serverNode1 =
                new ServerNode(1, "localhost", 1234, ServerType.TABLET_SERVER, "rack1");
        // should be empty at the beginning
        assertThat(gatewayRpcGatewayManager.getRpcGateway(serverNode1.id())).isEmpty();
        gatewayRpcGatewayManager.addServer(serverNode1);
        // shouldn't be empty then
        assertThat(gatewayRpcGatewayManager.getRpcGateway(serverNode1.id())).isPresent();

        // disconnect should retain the gateway so the next request can reconnect
        gatewayRpcGatewayManager.disconnectServer(serverNode1.id()).get();
        assertThat(gatewayRpcGatewayManager.getRpcGateway(serverNode1.id())).isPresent();

        // add the server again
        gatewayRpcGatewayManager.addServer(serverNode1);
        assertThat(gatewayRpcGatewayManager.getRpcGateway(serverNode1.id())).isPresent();

        // add another server2
        ServerNode serverNode2 =
                new ServerNode(2, "localhost", 1234, ServerType.TABLET_SERVER, "rack2");
        gatewayRpcGatewayManager.addServer(serverNode2);
        assertThat(gatewayRpcGatewayManager.getRpcGateway(serverNode2.id())).isPresent();

        // test remove
        gatewayRpcGatewayManager.removeServer(serverNode1.id());
        assertThat(gatewayRpcGatewayManager.getRpcGateway(serverNode1.id())).isEmpty();
        assertThat(gatewayRpcGatewayManager.getRpcGateway(serverNode2.id())).isPresent();
        // remove server2
        gatewayRpcGatewayManager.removeServer(serverNode2.id());
        assertThat(gatewayRpcGatewayManager.getRpcGateway(serverNode2.id())).isEmpty();

        gatewayRpcGatewayManager.close();
    }

    @Test
    void testDisconnectFailsInflightRequestAndAllowsReconnect() throws Exception {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS, 3);
        DelayedLookupGatewayService service = new DelayedLookupGatewayService();
        MetricGroup metricGroup = NOPMetricsGroup.newInstance();

        try (NetUtils.Port availablePort = getAvailablePort()) {
            ServerNode serverNode =
                    new ServerNode(
                            1, "localhost", availablePort.getPort(), ServerType.TABLET_SERVER);
            try (NettyServer nettyServer =
                            new NettyServer(
                                    conf,
                                    Collections.singleton(
                                            new Endpoint(
                                                    serverNode.host(),
                                                    serverNode.port(),
                                                    "INTERNAL")),
                                    service,
                                    metricGroup,
                                    RequestsMetrics.createTabletServerRequestMetrics(metricGroup));
                    NettyClient nettyClient =
                            new NettyClient(conf, TestingClientMetricGroup.newInstance())) {
                try {
                    nettyServer.start();

                    RpcGatewayManager<TabletServerGateway> gatewayManager =
                            new RpcGatewayManager<>(nettyClient, TabletServerGateway.class);
                    gatewayManager.addServer(serverNode);
                    TabletServerGateway gateway =
                            gatewayManager.getRpcGateway(serverNode.id()).get();
                    LookupRequest request = new LookupRequest().setTableId(1);
                    request.addBucketsReq().setBucketId(1);

                    CompletableFuture<LookupResponse> firstRequest = gateway.lookup(request);
                    assertThat(service.awaitFirstLookup()).isTrue();
                    assertThat(firstRequest).isNotDone();

                    gatewayManager.disconnectServer(serverNode.id()).get(5, TimeUnit.SECONDS);
                    assertThat(firstRequest).isCompletedExceptionally();
                    assertThat(gatewayManager.getRpcGateway(serverNode.id())).containsSame(gateway);

                    assertThat(gateway.lookup(request).get(5, TimeUnit.SECONDS)).isNotNull();
                    assertThat(service.getInvocationCount()).isEqualTo(2);
                } finally {
                    service.completeFirstLookup();
                }
            }
        }
    }

    private static class DelayedLookupGatewayService extends TestingTabletGatewayService {
        private final AtomicInteger invocationCount = new AtomicInteger();
        private final CountDownLatch firstLookup = new CountDownLatch(1);
        private final CompletableFuture<LookupResponse> firstLookupResponse =
                new CompletableFuture<>();

        @Override
        public CompletableFuture<LookupResponse> lookup(LookupRequest request) {
            if (invocationCount.incrementAndGet() == 1) {
                firstLookup.countDown();
                return firstLookupResponse;
            }
            return CompletableFuture.completedFuture(new LookupResponse());
        }

        private boolean awaitFirstLookup() throws InterruptedException {
            return firstLookup.await(5, TimeUnit.SECONDS);
        }

        private int getInvocationCount() {
            return invocationCount.get();
        }

        private void completeFirstLookup() {
            firstLookupResponse.complete(new LookupResponse());
        }
    }
}

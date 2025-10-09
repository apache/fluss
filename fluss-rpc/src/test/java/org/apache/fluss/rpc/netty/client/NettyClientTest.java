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
import org.apache.fluss.utils.NetUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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
        nettyClient = new NettyClient(conf, TestingClientMetricGroup.newInstance(), false);
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
        assertThatThrownBy(
                        () ->
                                nettyClient
                                        .sendRequest(serverNode, ApiKeys.API_VERSIONS, request)
                                        .get())
                .rootCause()
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("Connection refused");
        assertThat(nettyClient.connections().size()).isEqualTo(0);

        // restart the netty server.
        buildNettyServer(1);
        nettyClient.sendRequest(serverNode, ApiKeys.API_VERSIONS, request).get();
        assertThat(nettyClient.connections().size()).isEqualTo(1);
        assertThat(nettyClient.connections().get(serverNode.uid()).getServerNode())
                .isEqualTo(serverNode);
    }

    @Test
    void testBindFailureDetection() {
        Throwable ex = new java.net.BindException();
        assertThat(NettyUtils.isBindFailure(ex)).isTrue();

        ex = new Exception(new java.net.BindException());
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
            try (NettyClient client =
                    new NettyClient(conf, TestingClientMetricGroup.newInstance(), false); ) {
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

    private void buildNettyServer(int serverId) throws Exception {
        try (NetUtils.Port availablePort = getAvailablePort()) {
            serverNode =
                    new ServerNode(
                            serverId, "localhost", availablePort.getPort(), ServerType.COORDINATOR);
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
}

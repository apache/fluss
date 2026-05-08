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
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.util.NOPMetricsGroup;
import org.apache.fluss.rpc.RpcServer;
import org.apache.fluss.rpc.TestingGatewayService;
import org.apache.fluss.rpc.messages.ApiVersionsRequest;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.rpc.netty.client.NettyClient;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.utils.NetUtils;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.apache.fluss.utils.NetUtils.getAvailablePort;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link NettyServer}. */
public class NettyServerTest {

    @Test
    void testPortAsZero() throws Exception {
        List<Endpoint> endpoints = Endpoint.fromListenersString("FLUSS://localhost:0");
        MetricGroup metricGroup = NOPMetricsGroup.newInstance();
        try (RpcServer server =
                RpcServer.create(
                        new Configuration(),
                        endpoints,
                        new TestingGatewayService(),
                        metricGroup,
                        RequestsMetrics.createCoordinatorServerRequestMetrics(metricGroup))) {
            server.start();
            List<Endpoint> bindEndpoints = server.getBindEndpoints();
            assertThat(bindEndpoints).hasSize(1);
            assertThat(bindEndpoints.get(0).getPort()).isGreaterThan(0);
        }
    }

    @Test
    void testOversizedRequestRejected() throws Exception {
        // Configure an extremely small max request size (16 bytes) so that even the
        // initial API_VERSIONS handshake request exceeds the limit and is rejected
        // by the server's LengthFieldBasedFrameDecoder.
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE, MemorySize.parse("16b"));
        conf.setInt(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS, 3);

        MetricGroup metricGroup = NOPMetricsGroup.newInstance();
        try (NetUtils.Port availablePort = getAvailablePort()) {
            ServerNode serverNode =
                    new ServerNode(1, "localhost", availablePort.getPort(), ServerType.COORDINATOR);
            try (NettyServer nettyServer =
                            new NettyServer(
                                    conf,
                                    Collections.singleton(
                                            new Endpoint(
                                                    serverNode.host(),
                                                    serverNode.port(),
                                                    "INTERNAL")),
                                    new TestingGatewayService(),
                                    metricGroup,
                                    RequestsMetrics.createCoordinatorServerRequestMetrics(
                                            metricGroup));
                    NettyClient nettyClient =
                            new NettyClient(
                                    new Configuration(), TestingClientMetricGroup.newInstance())) {
                nettyServer.start();

                // The NettyClient will try to send an API_VERSIONS handshake request.
                // Since the request size exceeds the server's 16-byte limit, the server's
                // frame decoder will reject the frame and close the connection, causing
                // the client request to fail.
                ApiVersionsRequest request =
                        new ApiVersionsRequest()
                                .setClientSoftwareName("test")
                                .setClientSoftwareVersion("1.0");
                assertThatThrownBy(
                                () ->
                                        nettyClient
                                                .sendRequest(
                                                        serverNode, ApiKeys.API_VERSIONS, request)
                                                .get())
                        .hasMessageContaining("Adjusted frame length exceeds 16");
            }
        }
    }
}

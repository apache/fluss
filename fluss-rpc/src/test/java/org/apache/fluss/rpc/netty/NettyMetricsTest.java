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

package org.apache.fluss.rpc.netty;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.groups.GenericMetricGroup;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.util.NOPMetricsGroup;
import org.apache.fluss.rpc.TestingGatewayService;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.rpc.netty.client.NettyClient;
import org.apache.fluss.rpc.netty.server.NettyServer;
import org.apache.fluss.rpc.netty.server.RequestsMetrics;
import org.apache.fluss.utils.NetUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.apache.fluss.utils.NetUtils.getAvailablePort;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link NettyMetrics}. */
public class NettyMetricsTest {

    private NettyServer nettyServer;
    private NettyClient nettyClient;
    private MetricGroup serverGroup;
    private MetricGroup clientGroup;

    @BeforeEach
    public void setup() throws Exception {
        Configuration conf = new Configuration();
        try (NetUtils.Port availablePort = getAvailablePort()) {
            ServerNode serverNode =
                    new ServerNode(1, "localhost", availablePort.getPort(), ServerType.COORDINATOR);
            MetricGroup serverMetricGroup = NOPMetricsGroup.newInstance();
            serverGroup = serverMetricGroup.addGroup(NettyMetrics.NETTY_METRIC_GROUP);
            nettyServer =
                    new NettyServer(
                            conf,
                            Collections.singleton(
                                    new Endpoint(serverNode.host(), serverNode.port(), "INTERNAL")),
                            new TestingGatewayService(),
                            serverMetricGroup,
                            RequestsMetrics.createCoordinatorServerRequestMetrics(
                                    serverMetricGroup));
            nettyServer.start();
        }
        ClientMetricGroup clientMetricGroup = TestingClientMetricGroup.newInstance();
        clientGroup = clientMetricGroup.addGroup(NettyMetrics.NETTY_METRIC_GROUP);
        nettyClient = new NettyClient(conf, clientMetricGroup, false);
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
    void testNettyMetricsCompleteness() {
        assertNettyMetrics(serverGroup);
        assertNettyMetrics(clientGroup);
    }

    private void assertNettyMetrics(MetricGroup metricGroup) {
        Map<String, Metric> metrics = ((GenericMetricGroup) metricGroup).getMetrics();
        assertThat(metrics.get(MetricNames.NETTY_USED_DIRECT_MEMORY)).isNotNull();
        assertThat(metrics.get(MetricNames.NETTY_NUM_DIRECT_ARENAS)).isNotNull();
        assertThat(metrics.get(MetricNames.NETTY_NUM_ALLOCATIONS_PER_SECONDS)).isNotNull();
        assertThat(metrics.get(MetricNames.NETTY_NUM_HUGE_ALLOCATIONS_PER_SECONDS)).isNotNull();
    }
}

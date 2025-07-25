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

package com.alibaba.fluss.rpc.netty;

import com.alibaba.fluss.cluster.Endpoint;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metrics.Metric;
import com.alibaba.fluss.metrics.MetricNames;
import com.alibaba.fluss.metrics.groups.GenericMetricGroup;
import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.metrics.util.NOPMetricsGroup;
import com.alibaba.fluss.rpc.TestingGatewayService;
import com.alibaba.fluss.rpc.metrics.ClientMetricGroup;
import com.alibaba.fluss.rpc.metrics.TestingClientMetricGroup;
import com.alibaba.fluss.rpc.netty.client.NettyClient;
import com.alibaba.fluss.rpc.netty.server.NettyServer;
import com.alibaba.fluss.rpc.netty.server.RequestsMetrics;
import com.alibaba.fluss.utils.NetUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static com.alibaba.fluss.utils.NetUtils.getAvailablePort;
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

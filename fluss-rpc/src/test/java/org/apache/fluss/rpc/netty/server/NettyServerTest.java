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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.util.NOPMetricsGroup;
import org.apache.fluss.rpc.RpcServer;
import org.apache.fluss.rpc.TestingGatewayService;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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
}

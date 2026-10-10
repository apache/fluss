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

package org.apache.fluss.kafka;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.util.NOPMetricsGroup;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.netty.server.NettyServer;
import org.apache.fluss.rpc.netty.server.RequestsMetrics;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests idle connection closure through the Kafka listener's actual Netty pipeline. */
class KafkaProtocolPluginITCase {

    @Test
    void testKafkaConnectionClosesAfterConfiguredNettyIdleTimeout() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.set(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS, 1);
        conf.set(ConfigOptions.NETTY_SERVER_NUM_NETWORK_THREADS, 1);
        conf.set(ConfigOptions.NETTY_CONNECTION_MAX_IDLE_TIME, Duration.ofSeconds(1));
        conf.setString("kafka.connection.max-idle-time", "1 min");
        MetricGroup metricGroup = NOPMetricsGroup.newInstance();
        try (NettyServer server =
                        new NettyServer(
                                conf,
                                Collections.singletonList(new Endpoint("localhost", 0, "KAFKA")),
                                new TestingTabletGatewayService(),
                                metricGroup,
                                RequestsMetrics.createCoordinatorServerRequestMetrics(
                                        metricGroup));
                Socket socket = new Socket()) {
            server.start();
            Endpoint endpoint = server.getBindEndpoints().get(0);
            socket.connect(new InetSocketAddress(endpoint.getHost(), endpoint.getPort()), 10_000);
            socket.setSoTimeout(10_000);

            assertThat(socket.getInputStream().read()).isEqualTo(-1);
        }
    }
}

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
import org.apache.fluss.rpc.TestingGatewayService;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.netty.server.NettyServer;
import org.apache.fluss.rpc.netty.server.RequestsMetrics;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests that Kafka protocol endpoints are isolated to TabletServer. */
class KafkaProtocolServerRoleTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRejectsKafkaEndpointOnCoordinatorServer(boolean kafkaEnabled) {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, kafkaEnabled);
        MetricGroup metricGroup = NOPMetricsGroup.newInstance();

        assertThatThrownBy(
                        () ->
                                new NettyServer(
                                        conf,
                                        Collections.singletonList(
                                                new Endpoint("localhost", 0, "KAFKA")),
                                        new TestingGatewayService(),
                                        metricGroup,
                                        RequestsMetrics.createCoordinatorServerRequestMetrics(
                                                metricGroup)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("only supported on TabletServer")
                .hasMessageContaining("COORDINATOR");
    }

    @Test
    void testRejectsKafkaEndpointWhenKafkaServiceIsDisabled() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, false);
        MetricGroup metricGroup = NOPMetricsGroup.newInstance();

        assertThatThrownBy(
                        () ->
                                new NettyServer(
                                        conf,
                                        Collections.singletonList(
                                                new Endpoint("localhost", 0, "KAFKA")),
                                        new TestingTabletGatewayService(),
                                        metricGroup,
                                        RequestsMetrics.createTabletServerRequestMetrics(
                                                metricGroup)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Kafka service is disabled");
    }

    @Test
    void testCoordinatorServerStartsNativeEndpointWithKafkaEnabled() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.set(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS, 1);
        conf.set(ConfigOptions.NETTY_SERVER_NUM_NETWORK_THREADS, 1);
        MetricGroup metricGroup = NOPMetricsGroup.newInstance();
        try (NettyServer server =
                new NettyServer(
                        conf,
                        Collections.singletonList(new Endpoint("localhost", 0, "FLUSS")),
                        new TestingGatewayService(),
                        metricGroup,
                        RequestsMetrics.createCoordinatorServerRequestMetrics(metricGroup))) {
            server.start();

            assertThat(server.getBindEndpoints())
                    .singleElement()
                    .extracting(Endpoint::getListenerName)
                    .isEqualTo("FLUSS");
        }
    }
}

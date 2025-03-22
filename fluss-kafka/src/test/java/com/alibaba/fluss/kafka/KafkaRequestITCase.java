/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.kafka;

import com.alibaba.fluss.cluster.Endpoint;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.metrics.util.NOPMetricsGroup;
import com.alibaba.fluss.rpc.netty.server.NettyServer;
import com.alibaba.fluss.rpc.netty.server.RequestsMetrics;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.internals.ProducerMetrics;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for Kafka request handling. */
public class KafkaRequestITCase {
    private Configuration conf;
    private NettyServer nettyServer;
    private NetworkClient client;
    private Node node;

    @BeforeEach
    public void setup() throws Exception {
        conf = new Configuration();
        // 3 worker threads is enough for this test
        conf.set(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS, 3);
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        nettyServer = startNettyServer();
        client = createNetworkClient();
        Endpoint endpoint =
                nettyServer.getBindEndpoints().stream()
                        .filter(e -> e.getListenerName().equals("KAFKA"))
                        .findFirst()
                        .get();
        node = new Node(0, endpoint.getHost(), endpoint.getPort());
    }

    @AfterEach
    public void cleanup() throws Exception {
        if (nettyServer != null) {
            nettyServer.close();
        }
    }

    @Test
    public void testApiVersionsRequest() {
        // initiate the connection
        client.ready(node, 100);

        // handle the connection, send the ApiVersionsRequest
        client.poll(0, 1);

        // check that the ApiVersionsRequest has been initiated
        assertThat(client.hasInFlightRequests(node.idString())).isTrue();

        retry(
                Duration.ofMinutes(1),
                () -> {
                    // handle completed receives
                    client.poll(0, 100);

                    // the ApiVersionsRequest is gone
                    assertThat(client.hasInFlightRequests(node.idString())).isFalse();

                    // various assertions
                    assertThat(client.isReady(node, 100)).isTrue();
                });
    }

    private NettyServer startNettyServer() throws Exception {
        MetricGroup metricGroup = NOPMetricsGroup.newInstance();
        NettyServer server =
                new NettyServer(
                        conf,
                        Arrays.asList(
                                new Endpoint("localhost", 0, "INTERNAL"),
                                new Endpoint("localhost", 0, "KAFKA")),
                        new TestingTabletGatewayService(),
                        metricGroup,
                        RequestsMetrics.createCoordinatorServerRequestMetrics(metricGroup));
        server.start();
        return server;
    }

    private NetworkClient createNetworkClient() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("key.serializer", StringSerializer.class.getName());
        config.put("value.serializer", StringSerializer.class.getName());
        Metrics metrics =
                new Metrics(
                        new MetricConfig(),
                        Collections.emptyList(),
                        Time.SYSTEM,
                        new KafkaMetricsContext("test"));
        long refreshBackoffMs = 100;
        long refreshBackoffMaxMs = 1000;
        long metadataExpireMs = 1000;
        Metadata metadata =
                new Metadata(
                        refreshBackoffMs,
                        refreshBackoffMaxMs,
                        metadataExpireMs,
                        new LogContext(),
                        new ClusterResourceListeners());
        ProducerMetrics metricsRegistry = new ProducerMetrics(metrics);
        Sensor sensor = Sender.throttleTimeSensor(metricsRegistry.senderMetrics);
        return ClientUtils.createNetworkClient(
                new ProducerConfig(config),
                metrics,
                "test-client",
                new LogContext(),
                new ApiVersions(),
                Time.SYSTEM,
                5,
                metadata,
                sensor,
                null);
    }
}

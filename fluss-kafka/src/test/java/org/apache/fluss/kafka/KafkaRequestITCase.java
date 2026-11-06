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
import org.apache.fluss.rpc.netty.server.NettyServer;
import org.apache.fluss.rpc.netty.server.RequestsMetrics;

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

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.utils.IOUtils.closeAll;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for Kafka request handling. */
public class KafkaRequestITCase extends KafkaServerTestBase {
    private Configuration conf;
    private NettyServer nettyServer;
    private NetworkClient client;
    private ApiVersions apiVersions;
    private Metrics metrics;
    private Node node;

    @BeforeEach
    public void setup() throws Exception {
        conf = new Configuration();
        // 3 worker threads is enough for this test
        conf.set(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS, 3);
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        nettyServer = startNettyServer();
        Endpoint endpoint =
                nettyServer.getBindEndpoints().stream()
                        .filter(e -> e.getListenerName().equals("KAFKA"))
                        .findFirst()
                        .get();
        client = createNetworkClient(endpoint);
        node = new Node(0, endpoint.getHost(), endpoint.getPort());
    }

    @AfterEach
    public void cleanup() throws Exception {
        closeAll(client, metrics, nettyServer);
    }

    @Test
    public void testApiVersionsRequest() {
        client.ready(node, Time.SYSTEM.milliseconds());

        retry(
                Duration.ofMinutes(1),
                () -> {
                    client.poll(10, Time.SYSTEM.milliseconds());
                    assertThat(apiVersions.get(node.idString())).isNotNull();
                    assertThat(client.isReady(node, Time.SYSTEM.milliseconds())).isTrue();
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
                        createTabletService(),
                        metricGroup,
                        RequestsMetrics.createCoordinatorServerRequestMetrics(metricGroup));
        server.start();
        return server;
    }

    private NetworkClient createNetworkClient(Endpoint endpoint) throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("key.serializer", StringSerializer.class.getName());
        config.put("value.serializer", StringSerializer.class.getName());
        metrics =
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
        // the bootstrap is required to initialize the metadata with the broker address, otherwise
        // the client will not be able to send requests
        metadata.bootstrap(
                Collections.singletonList(
                        new InetSocketAddress(endpoint.getHost(), endpoint.getPort())));
        ProducerMetrics metricsRegistry = new ProducerMetrics(metrics);
        Sensor sensor = Sender.throttleTimeSensor(metricsRegistry.senderMetrics);
        apiVersions = new ApiVersions();
        return ClientUtils.createNetworkClient(
                new ProducerConfig(config),
                metrics,
                "test-client",
                new LogContext(),
                apiVersions,
                Time.SYSTEM,
                5,
                metadata,
                sensor,
                null);
    }
}

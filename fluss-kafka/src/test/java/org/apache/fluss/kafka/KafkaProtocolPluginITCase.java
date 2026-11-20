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
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.CharacterFilter;
import org.apache.fluss.metrics.Meter;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.groups.GenericMetricGroup;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;
import org.apache.fluss.metrics.util.NOPMetricsGroup;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.rpc.netty.client.NettyClient;
import org.apache.fluss.rpc.netty.server.NettyServer;
import org.apache.fluss.rpc.netty.server.RequestsMetrics;
import org.apache.fluss.rpc.protocol.ApiKeys;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.kafka.KafkaSocketTestUtils.sendRequest;
import static org.apache.fluss.rpc.netty.server.RequestsMetrics.KAFKA_METADATA_REQUEST_NAME;
import static org.apache.fluss.rpc.netty.server.RequestsMetrics.KAFKA_PRODUCE_REQUEST_NAME;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests metrics and idle connection closure through the Kafka listener's Netty pipeline. */
class KafkaProtocolPluginITCase extends KafkaServerTestBase {

    @Test
    void testFlussProxyGrantsWithKafkaEnabled() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.setString(ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(), "FLUSS:SASL,KAFKA:SASL");
        conf.set(
                ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG,
                Collections.singletonList("PLAIN"));
        conf.setString(ConfigOptions.SERVER_SASL_CREDENTIALS.key(), "gateway:gateway-secret");
        conf.setString("security.sasl.listener.name.fluss.plain.proxy-users", "gateway:alice");
        conf.set(ConfigOptions.CLIENT_SECURITY_PROTOCOL, "SASL");
        conf.set(ConfigOptions.CLIENT_SASL_JAAS_USERNAME, "gateway");
        conf.set(ConfigOptions.CLIENT_SASL_JAAS_PASSWORD, "gateway-secret");
        conf.set(ConfigOptions.CLIENT_SASL_AUTHORIZATION_ID, "alice");
        MetricGroup metricGroup = NOPMetricsGroup.newInstance();
        try (NettyServer server =
                        new NettyServer(
                                conf,
                                Arrays.asList(
                                        new Endpoint("localhost", 0, "FLUSS"),
                                        new Endpoint("localhost", 0, "KAFKA")),
                                createTabletService(),
                                metricGroup,
                                RequestsMetrics.createTabletServerRequestMetrics(metricGroup));
                NettyClient client =
                        new NettyClient(conf, TestingClientMetricGroup.newInstance())) {
            server.start();
            assertThat(server.getBindEndpoints())
                    .extracting(Endpoint::getListenerName)
                    .containsExactlyInAnyOrder("FLUSS", "KAFKA");
            Endpoint fluss =
                    server.getBindEndpoints().stream()
                            .filter(endpoint -> "FLUSS".equals(endpoint.getListenerName()))
                            .findFirst()
                            .get();
            client.sendRequest(
                            new ServerNode(
                                    1, fluss.getHost(), fluss.getPort(), ServerType.TABLET_SERVER),
                            ApiKeys.FETCH_LOG,
                            new FetchLogRequest().setFollowerServerId(-1).setMaxBytes(1024))
                    .get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void testFlussAndKafkaListenersShareRequestMetrics() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.set(ConfigOptions.KAFKA_LISTENER_NAMES, Arrays.asList("KAFKA", "KAFKA_SECOND"));
        conf.set(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS, 1);
        conf.set(ConfigOptions.NETTY_SERVER_NUM_NETWORK_THREADS, 1);
        Map<String, Metric> produceMetrics = new ConcurrentHashMap<>();
        Map<String, Metric> metadataMetrics = new ConcurrentHashMap<>();
        NOPMetricRegistry registry =
                new NOPMetricRegistry() {
                    @Override
                    public void register(Metric metric, String name, AbstractMetricGroup group) {
                        Map<String, String> variables = group.getAllVariables();
                        String requestName = variables.get("request");
                        if (KAFKA_PRODUCE_REQUEST_NAME.equals(requestName)
                                || KAFKA_METADATA_REQUEST_NAME.equals(requestName)) {
                            assertThat(group.getLogicalScope(CharacterFilter.NO_OP_FILTER, '_'))
                                    .isEqualTo("tabletserver_request");
                            assertThat(variables)
                                    .doesNotContainKeys("listener", "database", "table");
                            if (KAFKA_PRODUCE_REQUEST_NAME.equals(requestName)) {
                                produceMetrics.put(name, metric);
                            } else {
                                metadataMetrics.put(name, metric);
                            }
                        }
                    }
                };
        MetricGroup metricGroup = new GenericMetricGroup(registry, null, "tabletserver");
        RequestsMetrics sharedMetrics =
                RequestsMetrics.createTabletServerRequestMetrics(metricGroup);
        try (NettyServer server =
                        new NettyServer(
                                conf,
                                Arrays.asList(
                                        new Endpoint("localhost", 0, "FLUSS"),
                                        new Endpoint("localhost", 0, "KAFKA"),
                                        new Endpoint("localhost", 0, "KAFKA_SECOND")),
                                createTabletService(),
                                metricGroup,
                                sharedMetrics);
                NettyClient client =
                        new NettyClient(conf, TestingClientMetricGroup.newInstance())) {
            server.start();
            RequestsMetrics.Metrics fetch =
                    sharedMetrics.getMetrics(ApiKeys.FETCH_LOG.id, false, false).get();
            RequestsMetrics.Metrics produce =
                    sharedMetrics.getMetrics(KAFKA_PRODUCE_REQUEST_NAME).get();
            RequestsMetrics.Metrics metadata =
                    sharedMetrics.getMetrics(KAFKA_METADATA_REQUEST_NAME).get();
            // Invalid acks reject the whole Produce request.
            ProduceRequestData data =
                    new ProduceRequestData().setAcks((short) 2).setTimeoutMs(1000);
            data.topicData()
                    .add(
                            new ProduceRequestData.TopicProduceData()
                                    .setName("orders")
                                    .setPartitionData(
                                            Collections.singletonList(
                                                    new ProduceRequestData.PartitionProduceData()
                                                            .setIndex(0)
                                                            .setRecords(MemoryRecords.EMPTY))));
            for (Endpoint endpoint : server.getBindEndpoints()) {
                if ("FLUSS".equals(endpoint.getListenerName())) {
                    client.sendRequest(
                                    new ServerNode(
                                            1,
                                            endpoint.getHost(),
                                            endpoint.getPort(),
                                            ServerType.TABLET_SERVER),
                                    ApiKeys.FETCH_LOG,
                                    new FetchLogRequest().setFollowerServerId(-1).setMaxBytes(1024))
                            .get(10, TimeUnit.SECONDS);
                    continue;
                }
                try (Socket socket = new Socket()) {
                    socket.connect(
                            new InetSocketAddress(endpoint.getHost(), endpoint.getPort()), 10_000);
                    socket.setSoTimeout(10_000);
                    sendRequest(socket, new ApiVersionsRequest.Builder().build((short) 0));
                    sendRequest(
                            socket,
                            new MetadataRequest.Builder(Collections.emptyList(), false)
                                    .build((short) 9));
                    sendRequest(socket, new ProduceRequest(data, (short) 3));
                }
            }
            waitUntil(
                    () ->
                            fetch.getRequestsCount().getCount() == 1
                                    && ((Meter) produceMetrics.get("errorsPerSecond")).getCount()
                                            == 2
                                    && ((Meter) metadataMetrics.get("requestsPerSecond")).getCount()
                                            == 2,
                    Duration.ofSeconds(10),
                    "Fluss and Kafka request metrics were not updated");
            assertThat(((Meter) metadataMetrics.get("errorsPerSecond")).getCount()).isZero();
            assertThat(((Meter) produceMetrics.get("requestsPerSecond")).getCount()).isZero();
            assertThat(metadata.getRequestsCount().getCount()).isEqualTo(2);
            assertThat(produce.getErrorsCount().getCount()).isEqualTo(2);
        } finally {
            metricGroup.close();
        }
    }

    @Test
    void testFlussOnlyEndpointDoesNotRegisterKafkaMetrics() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.set(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS, 1);
        conf.set(ConfigOptions.NETTY_SERVER_NUM_NETWORK_THREADS, 1);
        MetricGroup group = NOPMetricsGroup.newInstance();
        RequestsMetrics metrics = RequestsMetrics.createTabletServerRequestMetrics(group);
        try (NettyServer server =
                new NettyServer(
                        conf,
                        Collections.singletonList(new Endpoint("localhost", 0, "FLUSS")),
                        createTabletService(),
                        group,
                        metrics)) {
            server.start();
            assertThat(metrics.getMetrics(KAFKA_PRODUCE_REQUEST_NAME)).isEmpty();
            assertThat(metrics.getMetrics(KAFKA_METADATA_REQUEST_NAME)).isEmpty();
        }
    }

    @Test
    void testKafkaConnectionClosesAfterConfiguredNettyIdleTimeout() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.set(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS, 1);
        conf.set(ConfigOptions.NETTY_SERVER_NUM_NETWORK_THREADS, 1);
        conf.set(ConfigOptions.NETTY_CONNECTION_MAX_IDLE_TIME, Duration.ofSeconds(1));
        MetricGroup metricGroup = NOPMetricsGroup.newInstance();
        try (NettyServer server =
                        new NettyServer(
                                conf,
                                Collections.singletonList(new Endpoint("localhost", 0, "KAFKA")),
                                createTabletService(),
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

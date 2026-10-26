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
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.Meter;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.groups.GenericMetricGroup;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;
import org.apache.fluss.metrics.util.NOPMetricsGroup;
import org.apache.fluss.rpc.netty.server.NettyServer;
import org.apache.fluss.rpc.netty.server.RequestsMetrics;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests idle connection closure through the Kafka listener's actual Netty pipeline. */
class KafkaProtocolPluginITCase extends KafkaServerTestBase {

    @Test
    void testKafkaRequestMetricsAreRegisteredThroughNettyServer() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.set(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS, 1);
        conf.set(ConfigOptions.NETTY_SERVER_NUM_NETWORK_THREADS, 1);
        Map<String, Metric> metrics = new ConcurrentHashMap<>();
        NOPMetricRegistry registry =
                new NOPMetricRegistry() {
                    @Override
                    public void register(Metric metric, String name, AbstractMetricGroup group) {
                        Map<String, String> variables = group.getAllVariables();
                        if ("KAFKA".equals(variables.get("listener"))
                                && "API_VERSIONS".equals(variables.get("request"))) {
                            metrics.put(name, metric);
                        }
                    }
                };
        MetricGroup metricGroup = new GenericMetricGroup(registry, null, "tabletserver");
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
            ApiVersionsRequest request = new ApiVersionsRequest.Builder().build((short) 0);
            exchangeRequest(socket, request);

            waitUntil(
                    () ->
                            metrics.get("totalTimeMs") != null
                                    && ((Histogram) metrics.get("totalTimeMs")).getCount() == 1,
                    Duration.ofSeconds(10),
                    "Kafka response metrics were not updated");
            assertThat(((Meter) metrics.get("requestsPerSecond")).getCount()).isOne();
            assertThat(((Meter) metrics.get("failedRequestsPerSecond")).getCount()).isZero();
            assertThat(((Gauge<?>) metrics.get("inFlightRequests")).getValue()).isEqualTo(0L);
            assertThat(((Histogram) metrics.get("requestQueueTimeMs")).getCount()).isOne();
            assertThat(((Histogram) metrics.get("requestProcessTimeMs")).getCount()).isOne();
            assertThat(((Histogram) metrics.get("responseQueueTimeMs")).getCount()).isOne();
        } finally {
            metricGroup.close();
        }
    }

    @Test
    void testProduceMetricsIncludeCanonicalTableThroughNettyServer() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.set(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS, 1);
        conf.set(ConfigOptions.NETTY_SERVER_NUM_NETWORK_THREADS, 1);
        Map<String, Metric> tableMetrics = new ConcurrentHashMap<>();
        NOPMetricRegistry registry =
                new NOPMetricRegistry() {
                    @Override
                    public void register(Metric metric, String name, AbstractMetricGroup group) {
                        Map<String, String> variables = group.getAllVariables();
                        if ("fluss".equals(variables.get("database"))
                                && "orders".equals(variables.get("table"))
                                && "KAFKA".equals(variables.get("listener"))
                                && "PRODUCE".equals(variables.get("request"))) {
                            tableMetrics.put(name, metric);
                        }
                    }
                };
        TabletServerMetricGroup metricGroup =
                new TabletServerMetricGroup(registry, "cluster", "rack", "host", 0);
        TablePath table = TablePath.of("fluss", "orders");
        TableBucket bucket = new TableBucket(999L, 0);
        metricGroup.addTableBucketMetricGroup(PhysicalTablePath.of(table), bucket, false);
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
            ProduceRequestData data =
                    new ProduceRequestData().setAcks((short) 1).setTimeoutMs(1000);
            data.topicData()
                    .add(
                            new ProduceRequestData.TopicProduceData()
                                    .setName("orders")
                                    .setPartitionData(
                                            Collections.singletonList(
                                                    new ProduceRequestData.PartitionProduceData()
                                                            .setIndex(0)
                                                            .setRecords(MemoryRecords.EMPTY))));
            // An empty batch is rejected before append; the failure must still be attributed to
            // orders.
            exchangeRequest(socket, new ProduceRequest(data, (short) 3));
            waitUntil(
                    () ->
                            tableMetrics.get("totalTimeMs") != null
                                    && ((Histogram) tableMetrics.get("totalTimeMs")).getCount()
                                            == 1,
                    Duration.ofSeconds(10),
                    "Table request metrics were not updated");
            assertThat(tableMetrics).hasSize(7);
            assertThat(((Meter) tableMetrics.get("requestsPerSecond")).getCount()).isOne();
            assertThat(((Meter) tableMetrics.get("failedRequestsPerSecond")).getCount()).isOne();
            assertThat(((Gauge<?>) tableMetrics.get("inFlightRequests")).getValue()).isEqualTo(0L);
        } finally {
            metricGroup.removeTableBucketMetricGroup(table, bucket);
            metricGroup.close();
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

    private static void exchangeRequest(Socket socket, AbstractRequest request) throws Exception {
        RequestHeader header =
                new RequestHeader(request.apiKey(), request.version(), "metrics-test", 42);
        ByteBuffer bytes =
                RequestUtils.serialize(
                        header.data(), header.headerVersion(), request.data(), request.version());
        byte[] payload = new byte[bytes.remaining()];
        bytes.get(payload);
        DataOutputStream output = new DataOutputStream(socket.getOutputStream());
        output.writeInt(payload.length);
        output.write(payload);
        output.flush();
        DataInputStream input = new DataInputStream(socket.getInputStream());
        int responseSize = input.readInt();
        assertThat(responseSize).isGreaterThan(4);
        assertThat(input.readInt()).isEqualTo(42);
        input.readFully(new byte[responseSize - 4]);
    }
}

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
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.util.NOPMetricsGroup;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.gateway.AdminGateway;
import org.apache.fluss.rpc.gateway.AdminGatewayProvider;
import org.apache.fluss.rpc.messages.CreateTableRequest;
import org.apache.fluss.rpc.messages.CreateTableResponse;
import org.apache.fluss.rpc.messages.DropTableRequest;
import org.apache.fluss.rpc.messages.DropTableResponse;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.ListTablesRequest;
import org.apache.fluss.rpc.messages.ListTablesResponse;
import org.apache.fluss.rpc.messages.PbBucketMetadata;
import org.apache.fluss.rpc.messages.PbProduceLogRespForBucket;
import org.apache.fluss.rpc.messages.PbServerNode;
import org.apache.fluss.rpc.messages.PbTableMetadata;
import org.apache.fluss.rpc.messages.PbTablePath;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.rpc.netty.server.NettyServer;
import org.apache.fluss.rpc.netty.server.RequestsMetrics;
import org.apache.fluss.types.DataTypes;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.internals.ProducerMetrics;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Integration test for Kafka request handling. */
public class KafkaRequestITCase {
    private Configuration conf;
    private NettyServer nettyServer;
    private NetworkClient client;
    private Node node;
    private TestingKafkaGatewayService gatewayService;

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
        node = new Node(0, endpoint.getHost(), endpoint.getPort());
        client = createNetworkClient(node);
        gatewayService.kafkaPort = endpoint.getPort();
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
        client.ready(node, Time.SYSTEM.milliseconds());

        // handle the connection, send the ApiVersionsRequest
        client.poll(0, Time.SYSTEM.milliseconds());

        retry(
                Duration.ofMinutes(1),
                () -> {
                    // handle completed receives
                    client.poll(0, Time.SYSTEM.milliseconds());

                    // the ApiVersionsRequest is gone
                    assertThat(client.hasInFlightRequests(node.idString())).isFalse();

                    // various assertions
                    assertThat(client.isReady(node, Time.SYSTEM.milliseconds())).isTrue();
                });
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1, 2, 3, 4})
    public void testApiVersionsWireVersions(short version) throws Exception {
        ApiVersionsRequestData requestData = new ApiVersionsRequestData();
        if (version >= 3) {
            requestData
                    .setClientSoftwareName("fluss-compatibility-test")
                    .setClientSoftwareVersion("1.0");
            requestData.unknownTaggedFields().add(new RawTaggedField(100, new byte[] {1, 2, 3}));
        }
        RequestHeader header =
                new RequestHeader(ApiKeys.API_VERSIONS, version, "test-client", 40 + version);
        ApiVersionsRequest request =
                new ApiVersionsRequest.Builder(requestData, version, version).build(version);

        ByteBuffer responseBuffer = sendRequest(header, request);
        ApiVersionsResponse response =
                (ApiVersionsResponse) AbstractResponse.parseResponse(responseBuffer, header);

        assertThat(response.errorCounts())
                .containsExactlyEntriesOf(Collections.singletonMap(Errors.NONE, 1));
        assertThat(response.data().throttleTimeMs()).isZero();
        assertThat(response.data().supportedFeatures()).isEmpty();
        assertThat(response.data().finalizedFeaturesEpoch()).isEqualTo(-1L);
        assertThat(response.data().finalizedFeatures()).isEmpty();
        assertThat(response.data().zkMigrationReady()).isFalse();
        assertThat(response.data().apiKeys())
                .extracting(ApiVersion::apiKey, ApiVersion::minVersion, ApiVersion::maxVersion)
                .containsExactly(
                        tuple(ApiKeys.PRODUCE.id, (short) 3, ApiKeys.PRODUCE.latestVersion()),
                        tuple(ApiKeys.METADATA.id, ApiKeys.METADATA.oldestVersion(), (short) 11),
                        tuple(
                                ApiKeys.API_VERSIONS.id,
                                ApiKeys.API_VERSIONS.oldestVersion(),
                                ApiKeys.API_VERSIONS.latestVersion()),
                        tuple(
                                ApiKeys.CREATE_TOPICS.id,
                                ApiKeys.CREATE_TOPICS.oldestVersion(),
                                ApiKeys.CREATE_TOPICS.latestVersion()),
                        tuple(
                                ApiKeys.DELETE_TOPICS.id,
                                ApiKeys.DELETE_TOPICS.oldestVersion(),
                                ApiKeys.DELETE_TOPICS.latestVersion()));
    }

    @Test
    public void testFutureApiVersionsWireVersion() throws Exception {
        short futureVersion = (short) (ApiKeys.API_VERSIONS.latestVersion() + 1);
        RequestHeader header =
                new RequestHeader(ApiKeys.API_VERSIONS, futureVersion, "test-client", 49);
        ByteBuffer serialized =
                RequestUtils.serialize(
                        header.data(),
                        header.headerVersion(),
                        new ApiVersionsRequestData(),
                        ApiKeys.API_VERSIONS.oldestVersion());

        ByteBuffer responseBuffer = sendSerializedRequest(serialized);
        ResponseHeader responseHeader =
                ResponseHeader.parse(responseBuffer, header.toResponseHeader().headerVersion());
        ApiVersionsResponse response =
                ApiVersionsResponse.parse(responseBuffer, ApiKeys.API_VERSIONS.oldestVersion());

        assertThat(responseHeader.correlationId()).isEqualTo(header.correlationId());
        assertThat(response.errorCounts())
                .containsExactlyEntriesOf(Collections.singletonMap(Errors.UNSUPPORTED_VERSION, 1));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
    public void testMetadataWireVersions(short version) throws Exception {
        RequestHeader header =
                new RequestHeader(ApiKeys.METADATA, version, "test-client", 42 + version);
        MetadataRequestData metadataRequestData = new MetadataRequestData();
        metadataRequestData.setTopics(version == 0 ? Collections.emptyList() : null);
        MetadataRequest request = new MetadataRequest(metadataRequestData, version);
        if (version >= 9) {
            request.data().unknownTaggedFields().add(new RawTaggedField(100, new byte[] {1, 2, 3}));
        }

        MetadataResponse response =
                (MetadataResponse)
                        AbstractResponse.parseResponse(sendRequest(header, request), header);

        assertThat(response.brokers()).hasSize(1);
        Node responseBroker = response.brokers().iterator().next();
        assertThat(responseBroker.host()).isEqualTo("localhost");
        assertThat(responseBroker.port()).isEqualTo(node.port());
        assertThat(response.errors()).isEmpty();
        assertThat(response.throttleTimeMs()).isZero();
    }

    private ByteBuffer sendRequest(RequestHeader header, AbstractRequest request) throws Exception {
        ByteBuffer serialized =
                RequestUtils.serialize(
                        header.data(), header.headerVersion(), request.data(), request.version());
        return sendSerializedRequest(serialized);
    }

    private ByteBuffer sendSerializedRequest(ByteBuffer serialized) throws Exception {
        try (Socket socket = new Socket(node.host(), node.port());
                DataOutputStream output = new DataOutputStream(socket.getOutputStream());
                DataInputStream input = new DataInputStream(socket.getInputStream())) {
            byte[] requestBytes = new byte[serialized.remaining()];
            serialized.get(requestBytes);
            output.writeInt(requestBytes.length);
            output.write(requestBytes);
            output.flush();

            int responseSize = input.readInt();
            byte[] responseBytes = new byte[responseSize];
            input.readFully(responseBytes);
            return ByteBuffer.wrap(responseBytes);
        }
    }

    @Test
    public void testProduceRequest() throws Exception {
        short version = ApiKeys.PRODUCE.latestVersion();
        RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, version, "test-client", 43);
        MemoryRecords records =
                MemoryRecords.withRecords(
                        org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2,
                        123L,
                        Compression.NONE,
                        new SimpleRecord(123L, new byte[] {1}, new byte[] {2}));
        TopicProduceData topic =
                new TopicProduceData()
                        .setName("topic")
                        .setPartitionData(
                                Collections.singletonList(
                                        new PartitionProduceData()
                                                .setIndex(0)
                                                .setRecords(records)));
        ProduceRequest request =
                new ProduceRequest(
                        new ProduceRequestData()
                                .setAcks((short) 1)
                                .setTimeoutMs(1000)
                                .setTopicData(
                                        new ProduceRequestData.TopicProduceDataCollection(
                                                Collections.singletonList(topic).iterator())),
                        version);
        ByteBuffer serialized =
                RequestUtils.serialize(
                        header.data(), header.headerVersion(), request.data(), request.version());

        try (Socket socket = new Socket(node.host(), node.port());
                DataOutputStream output = new DataOutputStream(socket.getOutputStream());
                DataInputStream input = new DataInputStream(socket.getInputStream())) {
            byte[] requestBytes = new byte[serialized.remaining()];
            serialized.get(requestBytes);
            output.writeInt(requestBytes.length);
            output.write(requestBytes);
            output.flush();

            int responseSize = input.readInt();
            byte[] responseBytes = new byte[responseSize];
            input.readFully(responseBytes);
            ProduceResponse response =
                    (ProduceResponse)
                            AbstractResponse.parseResponse(ByteBuffer.wrap(responseBytes), header);
            assertThat(response.errorCounts()).containsOnlyKeys(Errors.NONE);
            assertThat(
                            response.data()
                                    .responses()
                                    .find("topic")
                                    .partitionResponses()
                                    .get(0)
                                    .baseOffset())
                    .isEqualTo(42L);
        }
    }

    @Test
    public void testStandardClientCreateProduceDeleteLifecycle() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, node.host() + ":" + node.port());
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60000);
        try (Admin admin = Admin.create(config)) {
            admin.createTopics(Collections.singleton(new NewTopic("topic", 1, (short) 1)))
                    .all()
                    .get();

            Map<String, Object> producerConfig = new HashMap<>();
            producerConfig.put(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, node.host() + ":" + node.port());
            producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            producerConfig.put(
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
            producerConfig.put(ProducerConfig.ACKS_CONFIG, "1");
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig)) {
                assertThat(producer.send(new ProducerRecord<>("topic", "key", "value")).get())
                        .isNotNull();
            }

            admin.deleteTopics(Collections.singleton("topic")).all().get();
        }
        verify(gatewayService.adminGateway).createTable(any(CreateTableRequest.class));
        assertThat(gatewayService.produced).isTrue();
        verify(gatewayService.adminGateway).dropTable(any(DropTableRequest.class));
    }

    private NettyServer startNettyServer() throws Exception {
        MetricGroup metricGroup = NOPMetricsGroup.newInstance();
        gatewayService = new TestingKafkaGatewayService();
        NettyServer server =
                new NettyServer(
                        conf,
                        Arrays.asList(
                                new Endpoint("localhost", 0, "INTERNAL"),
                                new Endpoint("localhost", 0, "KAFKA")),
                        gatewayService,
                        metricGroup,
                        RequestsMetrics.createCoordinatorServerRequestMetrics(metricGroup));
        server.start();
        return server;
    }

    private NetworkClient createNetworkClient(Node bootstrapNode) throws Exception {
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
        metadata.bootstrap(
                Collections.singletonList(
                        new InetSocketAddress(bootstrapNode.host(), bootstrapNode.port())));
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

    private static final class TestingKafkaGatewayService extends TestingTabletGatewayService
            implements AdminGatewayProvider {

        private final AdminGateway adminGateway = mock(AdminGateway.class);
        private volatile int kafkaPort;
        private volatile boolean produced;

        private final TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("record_key", DataTypes.BYTES())
                                        .column("payload", DataTypes.BYTES())
                                        .column(
                                                "event_time",
                                                DataTypes.TIMESTAMP_LTZ(3).copy(false))
                                        .column(
                                                "headers",
                                                DataTypes.ARRAY(
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "name",
                                                                        DataTypes.STRING()
                                                                                .copy(false)),
                                                                DataTypes.FIELD(
                                                                        "value",
                                                                        DataTypes.BYTES()))))
                                        .build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_LOG_FORMAT, LogFormat.ARROW)
                        .customProperty(KafkaDataFormat.KEY_FORMAT_CONFIG, "raw")
                        .customProperty(KafkaDataFormat.KEY_FIELDS_CONFIG, "record_key")
                        .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "raw")
                        .customProperty(KafkaDataFormat.VALUE_FIELDS_INCLUDE_CONFIG, "EXCEPT_KEY")
                        .customProperty(KafkaDataFormat.TIMESTAMP_COLUMN_CONFIG, "event_time")
                        .customProperty(KafkaDataFormat.HEADERS_COLUMN_CONFIG, "headers")
                        .build();

        private TestingKafkaGatewayService() {
            when(adminGateway.createTable(any(CreateTableRequest.class)))
                    .thenReturn(CompletableFuture.completedFuture(new CreateTableResponse()));
            when(adminGateway.dropTable(any(DropTableRequest.class)))
                    .thenReturn(CompletableFuture.completedFuture(new DropTableResponse()));
            when(adminGateway.getTableInfo(any(GetTableInfoRequest.class)))
                    .thenReturn(
                            CompletableFuture.completedFuture(
                                    new GetTableInfoResponse()
                                            .setTableId(123L)
                                            .setSchemaId(1)
                                            .setTableJson(tableDescriptor.toJsonBytes())
                                            .setCreatedTime(1L)
                                            .setModifiedTime(1L)));
        }

        @Override
        public AdminGateway getAdminGateway() {
            return adminGateway;
        }

        @Override
        public CompletableFuture<ListTablesResponse> listTables(ListTablesRequest request) {
            return CompletableFuture.completedFuture(new ListTablesResponse());
        }

        @Override
        public CompletableFuture<org.apache.fluss.rpc.messages.MetadataResponse> metadata(
                org.apache.fluss.rpc.messages.MetadataRequest request) {
            org.apache.fluss.rpc.messages.MetadataResponse response =
                    new org.apache.fluss.rpc.messages.MetadataResponse()
                            .addAllTabletServers(
                                    Collections.singletonList(
                                            new PbServerNode()
                                                    .setNodeId(0)
                                                    .setHost("localhost")
                                                    .setPort(kafkaPort)));
            for (PbTablePath tablePath : request.getTablePathsList()) {
                if ("topic".equals(tablePath.getTableName())) {
                    response.addAllTableMetadatas(
                            Collections.singletonList(
                                    new PbTableMetadata()
                                            .setTablePath(tablePath)
                                            .setTableId(123L)
                                            .addAllBucketMetadatas(
                                                    Collections.singletonList(
                                                            new PbBucketMetadata()
                                                                    .setBucketId(0)
                                                                    .setLeaderId(0)
                                                                    .setLeaderEpoch(1)
                                                                    .setReplicaIds(
                                                                            new int[] {0})))));
                }
            }
            return CompletableFuture.completedFuture(response);
        }

        @Override
        public CompletableFuture<GetTableInfoResponse> getTableInfo(GetTableInfoRequest request) {
            return CompletableFuture.completedFuture(
                    new GetTableInfoResponse()
                            .setTableId(123L)
                            .setSchemaId(1)
                            .setTableJson(tableDescriptor.toJsonBytes())
                            .setCreatedTime(1L)
                            .setModifiedTime(1L));
        }

        @Override
        public CompletableFuture<ProduceLogResponse> produceLog(ProduceLogRequest request) {
            produced = true;
            return CompletableFuture.completedFuture(
                    new ProduceLogResponse()
                            .addAllBucketsResps(
                                    Collections.singletonList(
                                            new PbProduceLogRespForBucket()
                                                    .setBucketId(0)
                                                    .setBaseOffset(42L))));
        }
    }
}

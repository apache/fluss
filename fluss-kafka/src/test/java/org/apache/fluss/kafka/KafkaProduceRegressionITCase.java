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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceProgress;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.AlterConfig;
import org.apache.fluss.config.cluster.AlterConfigOpType;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.event.RecoverRebalanceEvent;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.RebalanceTask;
import org.apache.fluss.types.DataTypes;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

/** Regression coverage for native DDL lifecycle, metadata refresh, and Produce minimum ISR. */
class KafkaProduceRegressionITCase {
    private static final String DATABASE = "kafka";
    private static final short METADATA_VERSION = 11;

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(clusterConfig())
                    .setTabletServerListeners("FLUSS://localhost:0,KAFKA://localhost:0")
                    .build();

    @Test
    void testAdvertisedListenersAndTopicIdentityLifecycle() throws Exception {
        TablePath path = TablePath.of(DATABASE, "metadata_lifecycle");
        String topic = path.toString();
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            try {
                createTable(admin, path, 3);
                MetadataResponse initial = waitForTopic(topic);
                Uuid initialId = initial.data().topics().find(topic).topicId();
                assertThat(initialId).isNotEqualTo(Uuid.ZERO_UUID);
                assertKafkaListenerEndpoints(initial);
                assertThat(fetchMetadata(topic).data().topics().find(topic).topicId())
                        .isEqualTo(initialId);

                admin.dropTable(path, false).get();
                retry(
                        Duration.ofMinutes(1),
                        () ->
                                assertThat(fetchMetadata(topic).errors())
                                        .containsEntry(topic, Errors.UNKNOWN_TOPIC_OR_PARTITION));

                createTable(admin, path, 3);
                retry(
                        Duration.ofMinutes(1),
                        () -> {
                            MetadataResponseTopic recreated =
                                    fetchMetadata(topic).data().topics().find(topic);
                            assertThat(recreated.errorCode()).isEqualTo(Errors.NONE.code());
                            assertThat(recreated.topicId())
                                    .isNotEqualTo(Uuid.ZERO_UUID)
                                    .isNotEqualTo(initialId);
                        });
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    @Test
    void testProducerRefreshesMetadataAfterLeaderFailover() throws Exception {
        TablePath path = TablePath.of(DATABASE, "metadata_failover");
        String topic = path.toString();
        int stoppedLeader = -1;
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            try {
                createTable(admin, path, 3);
                MetadataResponsePartition initial =
                        waitForTopic(topic).data().topics().find(topic).partitions().get(0);
                stoppedLeader = initial.leaderId();
                try (KafkaProducer<byte[], byte[]> producer = producer("1", true)) {
                    assertThat(send(producer, topic, "before_failover").offset()).isNotNegative();
                    CLUSTER.stopTabletServer(stoppedLeader);
                    retry(
                            Duration.ofMinutes(1),
                            () -> {
                                MetadataResponseTopic metadata =
                                        fetchMetadata(topic).data().topics().find(topic);
                                assertThat(metadata.errorCode()).isEqualTo(Errors.NONE.code());
                                MetadataResponsePartition partition = metadata.partitions().get(0);
                                assertThat(partition.errorCode()).isEqualTo(Errors.NONE.code());
                                assertThat(partition.leaderId())
                                        .isNotEqualTo(-1)
                                        .isNotEqualTo(initial.leaderId());
                                assertThat(partition.leaderEpoch())
                                        .isGreaterThan(initial.leaderEpoch());
                            });
                    // Reuse the same producer to verify routing recovers after leader disconnect.
                    // acks=1 permits data loss and offset reuse after failover; this verifies
                    // continued routing, not durability of the first acknowledged record.
                    assertThat(send(producer, topic, "after_failover").offset()).isNotNegative();
                }
            } finally {
                try {
                    if (stoppedLeader >= 0 && CLUSTER.getTabletServerById(stoppedLeader) == null) {
                        CLUSTER.startTabletServer(stoppedLeader);
                        CLUSTER.assertHasTabletServerNumber(3);
                        CLUSTER.waitUntilAllGatewayHasSameMetadata();
                    }
                } finally {
                    admin.dropTable(path, true).get();
                }
            }
        }
    }

    @Test
    void testProducerRefreshesMetadataAfterOnlineLeaderChange() throws Exception {
        TablePath path = TablePath.of(DATABASE, "metadata_online_leader_change");
        String topic = path.toString();
        String rebalanceId = "kafka-online-leader-change";
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            try {
                createTable(admin, path, 3);
                TableBucket bucket =
                        new TableBucket(admin.getTableInfo(path).get().getTableId(), 0);
                MetadataResponsePartition initial =
                        waitForTopic(topic).data().topics().find(topic).partitions().get(0);
                int oldLeader = initial.leaderId();
                int newLeader =
                        initial.replicaNodes().stream()
                                .filter(id -> id != oldLeader)
                                .findFirst()
                                .get();
                List<ServerNode> nodes = CLUSTER.getTabletServerNodes("KAFKA");
                ServerNode oldNode =
                        nodes.stream().filter(node -> node.id() == oldLeader).findFirst().get();

                // Keep this socket open across the handoff to verify the old leader stays
                // reachable.
                try (Socket oldLeaderSocket = connect(oldNode);
                        KafkaProducer<byte[], byte[]> producer = producer("all", true)) {
                    MetadataResponse before =
                            (MetadataResponse) sendRequest(oldLeaderSocket, metadataRequest(topic));
                    assertThat(before.data().topics().find(topic).partitions().get(0).leaderId())
                            .isEqualTo(oldLeader);
                    assertThat(send(producer, topic, "before_handoff").offset()).isZero();
                    CLUSTER.waitUntilReplicaExpandToIsr(bucket, newLeader);
                    double retriesBefore = producerMetric(producer, "record-retry-total");
                    double disconnectsBefore = producerMetric(producer, "connection-close-total");

                    rebalanceLeader(bucket, initial, newLeader, rebalanceId);
                    retry(
                            Duration.ofMinutes(1),
                            () -> {
                                assertThat(admin.listRebalanceProgress(rebalanceId).get())
                                        .isPresent()
                                        .get()
                                        .extracting(RebalanceProgress::status)
                                        .isEqualTo(RebalanceStatus.COMPLETED);
                                MetadataResponsePartition updated =
                                        fetchMetadata(topic)
                                                .data()
                                                .topics()
                                                .find(topic)
                                                .partitions()
                                                .get(0);
                                assertThat(updated.errorCode()).isEqualTo(Errors.NONE.code());
                                assertThat(updated.leaderId()).isEqualTo(newLeader);
                                assertThat(updated.leaderEpoch())
                                        .isGreaterThan(initial.leaderEpoch());
                            });
                    CLUSTER.waitAndGetFollowerReplica(bucket, oldLeader);
                    assertThat(CLUSTER.getTabletServerNodes("KAFKA"))
                            .containsExactlyInAnyOrderElementsOf(nodes);

                    ProduceResponse rejected =
                            (ProduceResponse)
                                    sendRequest(oldLeaderSocket, produceRequest(topic, "rejected"));
                    assertThat(rejected.data().responses().find(topic).partitionResponses())
                            .singleElement()
                            .satisfies(
                                    partition -> {
                                        assertThat(partition.index()).isZero();
                                        assertThat(partition.errorCode())
                                                .isEqualTo(Errors.NOT_LEADER_OR_FOLLOWER.code());
                                        assertThat(partition.baseOffset()).isEqualTo(-1L);
                                    });

                    // partitionsFor uses the already populated producer cache. Separate raw
                    // Metadata requests above must not refresh this route before the next send.
                    assertThat(producer.partitionsFor(topic).get(0).leader().id())
                            .isEqualTo(oldLeader);
                    assertThat(send(producer, topic, "after_handoff").offset()).isEqualTo(1L);
                    assertThat(producerMetric(producer, "record-retry-total"))
                            .isGreaterThan(retriesBefore);
                    assertThat(producerMetric(producer, "connection-close-total"))
                            .isEqualTo(disconnectsBefore);
                    assertThat(producer.partitionsFor(topic).get(0).leader().id())
                            .isEqualTo(newLeader);
                    assertValues(connection, path, "before_handoff", "after_handoff");
                }
            } finally {
                try {
                    admin.cancelRebalance(rebalanceId).get();
                } finally {
                    admin.dropTable(path, true).get();
                }
            }
        }
    }

    @Test
    void testClusterMinIsrPolicyAndDynamicReload() throws Exception {
        TablePath replicated = TablePath.of(DATABASE, "acks_all_replicated");
        TablePath underReplicated = TablePath.of(DATABASE, "acks_all_under_replicated");
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            try {
                createTable(admin, replicated, 3);
                createTable(admin, underReplicated, 1);
                try (KafkaProducer<byte[], byte[]> producer = producer("all", false)) {
                    assertThat(send(producer, replicated.toString(), "replicated").offset())
                            .isZero();
                    assertThatThrownBy(() -> send(producer, underReplicated.toString(), "rejected"))
                            .isInstanceOf(ExecutionException.class)
                            .hasCauseInstanceOf(NotEnoughReplicasException.class);

                    setMinIsr(admin, 1);
                    assertThat(send(producer, underReplicated.toString(), "accepted").offset())
                            .isZero();
                    // Offset zero and native readback also prove the rejected write was not stored.
                    assertValues(connection, underReplicated, "accepted");
                }
            } finally {
                try {
                    setMinIsr(admin, 2);
                } finally {
                    admin.dropTable(replicated, true).get();
                    admin.dropTable(underReplicated, true).get();
                }
            }
        }
    }

    private static void createTable(Admin admin, TablePath path, int replicas) throws Exception {
        admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
        admin.createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("value", DataTypes.BYTES())
                                                .build())
                                .distributedBy(1)
                                .logFormat(LogFormat.ARROW)
                                .property(ConfigOptions.TABLE_REPLICATION_FACTOR, replicas)
                                .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "raw")
                                .build(),
                        false)
                .get();
        CLUSTER.waitUntilTableReady(admin.getTableInfo(path).get().getTableId());
    }

    private static void setMinIsr(Admin admin, int minIsr) throws Exception {
        admin.alterClusterConfigs(
                        Collections.singletonList(
                                new AlterConfig(
                                        ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER.key(),
                                        Integer.toString(minIsr),
                                        AlterConfigOpType.SET)))
                .get(30, TimeUnit.SECONDS);
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(CLUSTER.getTabletServers())
                                .allSatisfy(
                                        server ->
                                                assertThat(
                                                                server.getReplicaManager()
                                                                        .getMinInSyncReplicas())
                                                        .isEqualTo(minIsr)));
    }

    private static void assertValues(Connection connection, TablePath path, String... expected)
            throws Exception {
        try (Table table = connection.getTable(path);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(0);
            List<String> values = new ArrayList<>();
            retry(
                    Duration.ofSeconds(30),
                    () -> {
                        ScanRecords records = scanner.poll(Duration.ofSeconds(1));
                        for (ScanRecord record : records) {
                            values.add(
                                    new String(
                                            record.getRow().getBytes(0), StandardCharsets.UTF_8));
                        }
                        assertThat(values).containsExactly(expected);
                    });
        }
    }

    private static MetadataResponse waitForTopic(String topic) {
        AtomicReference<MetadataResponse> result = new AtomicReference<>();
        retry(
                Duration.ofMinutes(1),
                () -> {
                    MetadataResponse response = fetchMetadata(topic);
                    MetadataResponseTopic metadata = response.data().topics().find(topic);
                    assertThat(metadata.errorCode()).isEqualTo(Errors.NONE.code());
                    assertThat(metadata.partitions()).hasSize(1);
                    assertThat(metadata.partitions().get(0).leaderId()).isNotEqualTo(-1);
                    result.set(response);
                });
        return result.get();
    }

    private static void assertKafkaListenerEndpoints(MetadataResponse response) {
        assertThat(response.brokers())
                .extracting(
                        org.apache.kafka.common.Node::id,
                        org.apache.kafka.common.Node::host,
                        org.apache.kafka.common.Node::port,
                        org.apache.kafka.common.Node::rack)
                .containsExactlyInAnyOrderElementsOf(
                        CLUSTER.getTabletServerNodes("KAFKA").stream()
                                .map(
                                        node ->
                                                tuple(
                                                        node.id(),
                                                        node.host(),
                                                        node.port(),
                                                        node.rack()))
                                .collect(Collectors.toList()));
    }

    private static MetadataResponse fetchMetadata(String topic) throws Exception {
        try (Socket socket = connect(CLUSTER.getTabletServerNodes("KAFKA").get(0))) {
            return (MetadataResponse) sendRequest(socket, metadataRequest(topic));
        }
    }

    private static MetadataRequest metadataRequest(String topic) {
        return new MetadataRequest.Builder(Collections.singletonList(topic), false)
                .build(METADATA_VERSION);
    }

    private static ProduceRequest produceRequest(String topic, String value) {
        MemoryRecords records =
                MemoryRecords.withRecords(
                        Compression.NONE, new SimpleRecord(value.getBytes(StandardCharsets.UTF_8)));
        ProduceRequestData.TopicProduceData topicData =
                new ProduceRequestData.TopicProduceData()
                        .setName(topic)
                        .setPartitionData(
                                Collections.singletonList(
                                        new ProduceRequestData.PartitionProduceData()
                                                .setIndex(0)
                                                .setRecords(records)));
        return new ProduceRequest(
                new ProduceRequestData()
                        .setAcks((short) 1)
                        .setTimeoutMs(5000)
                        .setTopicData(
                                new ProduceRequestData.TopicProduceDataCollection(
                                        Collections.singletonList(topicData).iterator())),
                (short) 11);
    }

    private static Socket connect(ServerNode node) throws Exception {
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(node.host(), node.port()), 5000);
            socket.setSoTimeout(5000);
            return socket;
        } catch (Exception failure) {
            socket.close();
            throw failure;
        }
    }

    private static AbstractResponse sendRequest(Socket socket, AbstractRequest request)
            throws Exception {
        RequestHeader header =
                new RequestHeader(request.apiKey(), request.version(), "regression-test", 1);
        ByteBuffer serialized =
                RequestUtils.serialize(
                        header.data(), header.headerVersion(), request.data(), request.version());
        byte[] requestBytes = new byte[serialized.remaining()];
        serialized.get(requestBytes);
        // The caller owns the socket and may send another request on the same connection.
        DataOutputStream output = new DataOutputStream(socket.getOutputStream());
        DataInputStream input = new DataInputStream(socket.getInputStream());
        output.writeInt(requestBytes.length);
        output.write(requestBytes);
        output.flush();
        int size = input.readInt();
        assertThat(size).isBetween(1, 1024 * 1024);
        byte[] response = new byte[size];
        input.readFully(response);
        return AbstractResponse.parseResponse(ByteBuffer.wrap(response), header);
    }

    private static void rebalanceLeader(
            TableBucket bucket,
            MetadataResponsePartition initial,
            int newLeader,
            String rebalanceId)
            throws Exception {
        List<Integer> replicas = new ArrayList<>(initial.replicaNodes());
        Collections.swap(replicas, 0, replicas.indexOf(newLeader));
        RebalancePlanForBucket plan =
                new RebalancePlanForBucket(
                        bucket, initial.leaderId(), newLeader, initial.replicaNodes(), replicas);
        RebalanceTask task =
                new RebalanceTask(
                        rebalanceId,
                        RebalanceStatus.NOT_STARTED,
                        Collections.singletonMap(bucket, plan));
        // Submit a deterministic leader-only plan through the coordinator event thread.
        // All replicas and their client connections remain online during the real handoff.
        CLUSTER.getZooKeeperClient().registerRebalanceTask(task);
        CLUSTER.getCoordinatorServer()
                .getCoordinatorEventProcessor()
                .getCoordinatorEventManager()
                .put(new RecoverRebalanceEvent(task));
    }

    private static double producerMetric(KafkaProducer<byte[], byte[]> producer, String name) {
        return producer.metrics().entrySet().stream()
                .filter(
                        entry ->
                                entry.getKey().group().equals("producer-metrics")
                                        && entry.getKey().name().equals(name))
                .mapToDouble(entry -> ((Number) entry.getValue().metricValue()).doubleValue())
                .findFirst()
                .getAsDouble();
    }

    private static RecordMetadata send(
            KafkaProducer<byte[], byte[]> producer, String topic, String value) throws Exception {
        return producer.send(
                        new ProducerRecord<>(
                                topic, 0, null, value.getBytes(StandardCharsets.UTF_8)))
                .get(60, TimeUnit.SECONDS);
    }

    private static KafkaProducer<byte[], byte[]> producer(String acks, boolean retryOnFailure) {
        Map<String, Object> config = new HashMap<>();
        config.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                CLUSTER.getTabletServerNodes("KAFKA").stream()
                        .map(node -> node.host() + ":" + node.port())
                        .collect(Collectors.joining(",")));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        config.put(ProducerConfig.ACKS_CONFIG, acks);
        config.put(ProducerConfig.RETRIES_CONFIG, retryOnFailure ? 20 : 0);
        config.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 200);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000);
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30000);
        config.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, (int) Duration.ofHours(1).toMillis());
        return new KafkaProducer<>(config);
    }

    private static Configuration clusterConfig() {
        Configuration config = new Configuration();
        config.set(ConfigOptions.KAFKA_ENABLED, true);
        config.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        config.set(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER, 2);
        return config;
    }
}

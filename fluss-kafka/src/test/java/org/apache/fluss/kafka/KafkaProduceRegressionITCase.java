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
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.AlterConfig;
import org.apache.fluss.config.cluster.AlterConfigOpType;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
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
import java.util.Collections;
import java.util.HashMap;
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
        String topic = "metadata_lifecycle";
        TablePath path = TablePath.of(DATABASE, topic);
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            try {
                createTable(admin, topic, 3);
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

                createTable(admin, topic, 3);
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
        String topic = "metadata_failover";
        int stoppedLeader = -1;
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            try {
                createTable(admin, topic, 3);
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
                    // Reuse the producer's cached route so the failed request triggers a refresh.
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
                    admin.dropTable(TablePath.of(DATABASE, topic), true).get();
                }
            }
        }
    }

    @Test
    void testClusterMinIsrPolicyAndDynamicReload() throws Exception {
        String replicated = "acks_all_replicated";
        String underReplicated = "acks_all_under_replicated";
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            try {
                createTable(admin, replicated, 3);
                createTable(admin, underReplicated, 1);
                try (KafkaProducer<byte[], byte[]> producer = producer("all", false)) {
                    assertThat(send(producer, replicated, "replicated").offset()).isZero();
                    assertThatThrownBy(() -> send(producer, underReplicated, "rejected"))
                            .isInstanceOf(ExecutionException.class)
                            .hasCauseInstanceOf(NotEnoughReplicasException.class);

                    setMinIsr(admin, 1);
                    assertThat(send(producer, underReplicated, "accepted").offset()).isZero();
                    // Offset zero and native readback also prove the rejected write was not stored.
                    assertSingleValue(connection, underReplicated, "accepted");
                }
            } finally {
                try {
                    setMinIsr(admin, 2);
                } finally {
                    admin.dropTable(TablePath.of(DATABASE, replicated), true).get();
                    admin.dropTable(TablePath.of(DATABASE, underReplicated), true).get();
                }
            }
        }
    }

    private static void createTable(Admin admin, String topic, int replicas) throws Exception {
        admin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();
        TablePath path = TablePath.of(DATABASE, topic);
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

    private static void assertSingleValue(Connection connection, String topic, String expected)
            throws Exception {
        try (Table table = connection.getTable(TablePath.of(DATABASE, topic));
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(0);
            retry(
                    Duration.ofSeconds(30),
                    () -> {
                        ScanRecords records = scanner.poll(Duration.ofSeconds(1));
                        assertThat(records).hasSize(1);
                        assertThat(records.iterator().next().getRow().getBytes(0))
                                .isEqualTo(expected.getBytes(StandardCharsets.UTF_8));
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
        MetadataRequest request =
                new MetadataRequest.Builder(Collections.singletonList(topic), false)
                        .build(METADATA_VERSION);
        RequestHeader header =
                new RequestHeader(ApiKeys.METADATA, METADATA_VERSION, "regression-test", 1);
        ByteBuffer serialized =
                RequestUtils.serialize(
                        header.data(), header.headerVersion(), request.data(), request.version());
        byte[] requestBytes = new byte[serialized.remaining()];
        serialized.get(requestBytes);
        ServerNode node = CLUSTER.getTabletServerNodes("KAFKA").get(0);
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(node.host(), node.port()), 5000);
            socket.setSoTimeout(5000);
            try (DataOutputStream output = new DataOutputStream(socket.getOutputStream());
                    DataInputStream input = new DataInputStream(socket.getInputStream())) {
                output.writeInt(requestBytes.length);
                output.write(requestBytes);
                output.flush();
                int size = input.readInt();
                assertThat(size).isBetween(1, 1024 * 1024);
                byte[] response = new byte[size];
                input.readFully(response);
                return (MetadataResponse)
                        AbstractResponse.parseResponse(ByteBuffer.wrap(response), header);
            }
        }
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
        return new KafkaProducer<>(config);
    }

    private static Configuration clusterConfig() {
        Configuration config = new Configuration();
        config.set(ConfigOptions.KAFKA_ENABLED, true);
        config.set(ConfigOptions.KAFKA_DATABASE, DATABASE);
        config.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        config.set(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER, 2);
        return config;
    }
}

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
import org.apache.fluss.config.KafkaValueConversionType;
import org.apache.fluss.config.cluster.AlterConfig;
import org.apache.fluss.config.cluster.AlterConfigOpType;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.coordinator.event.RecoverRebalanceEvent;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.RebalanceTask;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.Socket;
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

import static org.apache.fluss.kafka.KafkaSocketTestUtils.connect;
import static org.apache.fluss.kafka.KafkaSocketTestUtils.sendRequest;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

/** Regression coverage for native DDL lifecycle, metadata refresh, and Produce minimum ISR. */
class KafkaProduceRegressionITCase {
    private static final String DATABASE = "fluss";
    private static final short METADATA_VERSION = 11;

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(clusterConfig())
                    .setTabletServerListeners("FLUSS://localhost:0,KAFKA://localhost:0")
                    .build();

    @Test
    void testProducerRefreshesMetadataAfterOnlineLeaderChange() throws Exception {
        TablePath path = TablePath.of(DATABASE, "metadata_online_leader_change");
        String topic = path.toString();
        String rebalanceId = "kafka-online-leader-change";
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            try {
                createTable(admin, path, 3);
                TableInfo tableInfo = admin.getTableInfo(path).get();
                TableBucket bucket = new TableBucket(tableInfo.getTableId(), 0);
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
                try (Socket oldLeaderSocket = connect(oldNode, 5000);
                        KafkaProducer<byte[], byte[]> producer = producer("all", true)) {
                    assertThat(send(producer, topic, "before_handoff").offset()).isZero();
                    CLUSTER.waitUntilReplicaExpandToIsr(bucket, newLeader);

                    rebalanceLeader(bucket, initial, newLeader, rebalanceId);
                    waitForLeaderChange(
                            admin, topic, 0, newLeader, initial.leaderEpoch(), rebalanceId);
                    CLUSTER.waitAndGetFollowerReplica(bucket, oldLeader);

                    ProduceResponse rejected =
                            (ProduceResponse)
                                    sendRequest(oldLeaderSocket, produceRequest(topic, "rejected"));
                    assertThat(rejected.errorCounts())
                            .containsEntry(Errors.NOT_LEADER_OR_FOLLOWER, 1);

                    assertThat(send(producer, topic, "after_handoff").offset()).isEqualTo(1L);
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

    @Test
    void testSameProducerAliasesFollowTableLifecycle() throws Exception {
        TablePath path = TablePath.of(DATABASE, "cached_lifecycle");
        String full = path.toString();
        String alias = path.getTableName();
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            try {
                admin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();
                admin.createTable(
                                path,
                                TableDescriptor.builder()
                                        .schema(
                                                Schema.newBuilder()
                                                        .column("_kafka_value", DataTypes.BYTES())
                                                        .build())
                                        .distributedBy(1)
                                        .property(
                                                ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE,
                                                KafkaValueConversionType.BINARY)
                                        .build(),
                                false)
                        .get();
                CLUSTER.waitUntilTableReady(admin.getTableInfo(path).get().getTableId());
                assertRejectedNames(
                        path, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_OR_PARTITION);
                setTableEnabled(admin, path, true);
                try (KafkaProducer<byte[], byte[]> producer = producer("all", true)) {
                    assertThat(send(producer, full, "full").offset()).isZero();
                    assertThat(send(producer, alias, "alias").offset()).isEqualTo(1L);
                    setTableEnabled(admin, path, false);
                    assertRejectedNames(
                            path,
                            Errors.UNKNOWN_TOPIC_OR_PARTITION,
                            Errors.UNKNOWN_TOPIC_OR_PARTITION);
                    setTableEnabled(admin, path, true);
                    assertThat(send(producer, alias, "enabled").offset()).isEqualTo(2L);
                    assertValues(connection, path, "full", "alias", "enabled");
                    admin.dropTable(path, false).get();
                    // Metadata propagates the native lookup failure without rediscovery.
                    assertRejectedNames(
                            path, Errors.UNKNOWN_SERVER_ERROR, Errors.UNKNOWN_TOPIC_OR_PARTITION);
                    createTable(admin, path, 3);
                    assertThat(send(producer, full, "recreated_full").offset()).isZero();
                    assertThat(send(producer, alias, "recreated_alias").offset()).isEqualTo(1L);
                    assertValues(connection, path, "recreated_full", "recreated_alias");
                }
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    @Test
    void testSameProducerAliasesUseAddedMetadataColumn() throws Exception {
        TablePath path = TablePath.of(DATABASE, "cached_schema_alter");
        String full = path.toString();
        String alias = path.getTableName();
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            try {
                createTable(admin, path, 3);
                try (KafkaProducer<byte[], byte[]> producer = producer("all", true)) {
                    // Populate both name mappings and the write plan before changing the schema.
                    assertThat(send(producer, full, "old_full").offset()).isZero();
                    assertThat(send(producer, alias, "old_alias").offset()).isEqualTo(1L);
                    assertValues(connection, path, "old_full", "old_alias");

                    admin.alterTable(
                                    path,
                                    Collections.singletonList(
                                            TableChange.addColumn(
                                                    "_kafka_metadata",
                                                    metadataType(),
                                                    null,
                                                    TableChange.ColumnPosition.last())),
                                    false)
                            .get();
                    TableInfo after = admin.getTableInfo(path).get();
                    waitForReplicaTableInfo(after);

                    // Neither producer recreation nor a Kafka Metadata refresh should be needed.
                    assertThat(send(producer, full, 0, 789L, "new_full").offset()).isEqualTo(2L);
                    assertThat(send(producer, alias, 0, 987L, "new_alias").offset()).isEqualTo(3L);
                    // Read the new batches using the new native schema, independently of the
                    // native client's earlier cached table handle.
                    try (Connection reader =
                            ConnectionFactory.createConnection(CLUSTER.getClientConfig())) {
                        assertRecordsWithMetadata(
                                reader,
                                path,
                                0,
                                2L,
                                tuple(2L, "new_full", 789L, 0, 0),
                                tuple(3L, "new_alias", 987L, 0, 0));
                    }
                }
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    @Test
    void testReplicatedRecordsSurviveLeaderLossAndRestart() throws Exception {
        TablePath path = TablePath.of(DATABASE, "durable_restart");
        String topic = path.toString();
        String rebalanceId = "kafka-recovered-leader";
        int partition = 1;
        int stopped = -1;
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            try {
                createTable(admin, path, 3, 2);
                TableInfo tableInfo = admin.getTableInfo(path).get();
                TableBucket bucket = new TableBucket(tableInfo.getTableId(), partition);
                MetadataResponsePartition initial =
                        waitForTopic(topic, 2)
                                .data()
                                .topics()
                                .find(topic)
                                .partitions()
                                .get(partition);
                for (int replica : initial.replicaNodes()) {
                    CLUSTER.waitUntilReplicaExpandToIsr(bucket, replica);
                }
                try (KafkaProducer<byte[], byte[]> producer = producer("all", true)) {
                    assertThat(send(producer, topic, partition, 123L, "before").offset()).isZero();
                    assertValues(connection, path, partition, "before");
                    stopped = initial.leaderId();
                    CLUSTER.stopTabletServer(stopped);
                    retry(
                            Duration.ofMinutes(1),
                            () -> {
                                MetadataResponsePartition elected =
                                        waitForTopic(topic, 2)
                                                .data()
                                                .topics()
                                                .find(topic)
                                                .partitions()
                                                .get(partition);
                                assertThat(elected.errorCode()).isEqualTo(Errors.NONE.code());
                                assertThat(elected.leaderId())
                                        .isNotEqualTo(-1)
                                        .isNotEqualTo(initial.leaderId());
                                assertThat(elected.leaderEpoch())
                                        .isGreaterThan(initial.leaderEpoch());
                            });
                    assertThat(send(producer, topic, partition, 456L, "after_loss").offset())
                            .isEqualTo(1L);
                    assertValues(connection, path, partition, "before", "after_loss");
                    CLUSTER.startTabletServer(stopped);
                    CLUSTER.assertHasTabletServerNumber(3);
                    CLUSTER.waitUntilReplicaExpandToIsr(bucket, stopped);
                    // Make the recovered replica serve reads and writes, including the record
                    // produced while it was offline. ISR membership alone does not prove readback.
                    MetadataResponsePartition current =
                            waitForTopic(topic, 2)
                                    .data()
                                    .topics()
                                    .find(topic)
                                    .partitions()
                                    .get(partition);
                    rebalanceLeader(bucket, current, stopped, rebalanceId);
                    waitForLeaderChange(
                            admin, topic, partition, stopped, current.leaderEpoch(), rebalanceId);
                    assertThat(
                                    send(
                                                    producer,
                                                    path.getTableName(),
                                                    partition,
                                                    789L,
                                                    "after_restart")
                                            .offset())
                            .isEqualTo(2L);
                    try (Connection reader =
                            ConnectionFactory.createConnection(CLUSTER.getClientConfig())) {
                        assertValues(
                                reader, path, partition, "before", "after_loss", "after_restart");
                    }
                }
            } finally {
                if (stopped >= 0 && CLUSTER.getTabletServerById(stopped) == null) {
                    CLUSTER.startTabletServer(stopped);
                    CLUSTER.assertHasTabletServerNumber(3);
                }
                try {
                    admin.cancelRebalance(rebalanceId).get();
                } finally {
                    admin.dropTable(path, true).get();
                }
            }
        }
    }

    private static void setTableEnabled(Admin admin, TablePath path, boolean enabled)
            throws Exception {
        admin.alterTable(
                        path,
                        Collections.singletonList(
                                TableChange.set(
                                        ConfigOptions.TABLE_KAFKA_ENABLED.key(),
                                        Boolean.toString(enabled))),
                        false)
                .get();
        waitForReplicaTableInfo(admin.getTableInfo(path).get());
    }

    private static void waitForReplicaTableInfo(TableInfo expected) {
        // ALTER acknowledgement precedes propagation to the replicas used by Produce.
        TableBucket bucket = new TableBucket(expected.getTableId(), 0);
        retry(
                Duration.ofSeconds(30),
                () -> {
                    for (TabletServer server : CLUSTER.getTabletServers()) {
                        TableInfo actual =
                                server.getReplicaManager()
                                        .getReplicaOrException(bucket)
                                        .getTableInfo();
                        assertThat(actual.getSchemaId()).isEqualTo(expected.getSchemaId());
                        assertThat(actual.getProperties().toMap())
                                .isEqualTo(expected.getProperties().toMap());
                    }
                });
    }

    private static void assertRejectedNames(
            TablePath path, Errors metadataError, Errors produceError) throws Exception {
        retry(
                Duration.ofMinutes(1),
                () -> {
                    for (String topic : new String[] {path.toString(), path.getTableName()}) {
                        for (ServerNode node : CLUSTER.getTabletServerNodes("KAFKA")) {
                            try (Socket socket = connect(node, 5000)) {
                                MetadataResponse response =
                                        (MetadataResponse)
                                                sendRequest(socket, metadataRequest(topic));
                                assertThat(response.errors()).containsEntry(topic, metadataError);
                            }
                        }
                    }
                });
        for (String topic : new String[] {path.toString(), path.getTableName()}) {
            for (ServerNode node : CLUSTER.getTabletServerNodes("KAFKA")) {
                try (Socket socket = connect(node, 5000)) {
                    ProduceResponse response =
                            (ProduceResponse)
                                    sendRequest(socket, produceRequest(topic, "rejected"));
                    assertThat(response.errorCounts()).containsEntry(produceError, 1);
                }
            }
        }
    }

    private static void createTable(Admin admin, TablePath path, int replicas) throws Exception {
        createTable(admin, path, replicas, 1);
    }

    private static void createTable(Admin admin, TablePath path, int replicas, int buckets)
            throws Exception {
        admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
        admin.createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("_kafka_value", DataTypes.BYTES())
                                                .build())
                                .distributedBy(buckets)
                                .logFormat(LogFormat.ARROW)
                                .property(ConfigOptions.TABLE_REPLICATION_FACTOR, replicas)
                                .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                                .property(
                                        ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE,
                                        KafkaValueConversionType.BINARY)
                                .build(),
                        false)
                .get();
        CLUSTER.waitUntilTableReady(admin.getTableInfo(path).get().getTableId());
    }

    private static RowType metadataType() {
        return DataTypes.ROW(
                DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(3)),
                DataTypes.FIELD("timestamp_type", DataTypes.INT()),
                DataTypes.FIELD("partition", DataTypes.INT()));
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
        assertValues(connection, path, 0, expected);
    }

    private static void assertValues(
            Connection connection, TablePath path, int partition, String... expected)
            throws Exception {
        try (Table table = connection.getTable(path);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(partition);
            List<String> values = new ArrayList<>();
            retry(
                    Duration.ofSeconds(30),
                    () -> {
                        ScanRecords records = scanner.poll(Duration.ofSeconds(1));
                        for (ScanRecord record : records) {
                            assertThat(record.logOffset()).isEqualTo(values.size());
                            values.add(
                                    new String(
                                            record.getRow().getBytes(0), StandardCharsets.UTF_8));
                        }
                        assertThat(values).containsExactly(expected);
                    });
        }
    }

    private static void assertRecordsWithMetadata(
            Connection connection,
            TablePath path,
            int partition,
            long startOffset,
            Tuple... expected)
            throws Exception {
        try (Table table = connection.getTable(path);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribe(partition, startOffset);
            List<Tuple> actual = new ArrayList<>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (actual.size() < expected.length && System.nanoTime() < deadline) {
                for (ScanRecord record : scanner.poll(Duration.ofSeconds(1))) {
                    InternalRow row = record.getRow();
                    assertThat(row.isNullAt(1)).isFalse();
                    InternalRow metadata = row.getRow(1, 3);
                    assertThat(metadata.isNullAt(0)).isFalse();
                    assertThat(metadata.isNullAt(1)).isFalse();
                    assertThat(metadata.isNullAt(2)).isFalse();
                    actual.add(
                            tuple(
                                    record.logOffset(),
                                    new String(row.getBytes(0), StandardCharsets.UTF_8),
                                    metadata.getTimestampLtz(0, 3).getEpochMillisecond(),
                                    metadata.getInt(1),
                                    metadata.getInt(2)));
                }
            }
            assertThat(actual)
                    .as("Native metadata readback for %s bucket %s", path, partition)
                    .containsExactly(expected);
        }
    }

    private static MetadataResponse waitForTopic(String topic) {
        return waitForTopic(topic, 1);
    }

    private static MetadataResponse waitForTopic(String topic, int partitions) {
        AtomicReference<MetadataResponse> result = new AtomicReference<>();
        retry(
                Duration.ofMinutes(1),
                () -> {
                    MetadataResponse response = fetchMetadata(topic);
                    MetadataResponseTopic metadata = response.data().topics().find(topic);
                    assertThat(metadata.errorCode()).isEqualTo(Errors.NONE.code());
                    assertThat(metadata.partitions())
                            .hasSize(partitions)
                            .allSatisfy(
                                    partition -> assertThat(partition.leaderId()).isNotEqualTo(-1));
                    result.set(response);
                });
        return result.get();
    }

    private static MetadataResponse fetchMetadata(String topic) throws Exception {
        try (Socket socket = connect(CLUSTER.getTabletServerNodes("KAFKA").get(0), 5000)) {
            return (MetadataResponse) sendRequest(socket, metadataRequest(topic));
        }
    }

    private static MetadataRequest metadataRequest(String topic) {
        return new MetadataRequest.Builder(Collections.singletonList(topic), false)
                .build(METADATA_VERSION);
    }

    private static ProduceRequest produceRequest(String topic, String value) {
        return produceRequest(topic, new SimpleRecord(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static ProduceRequest produceRequest(String topic, SimpleRecord... values) {
        MemoryRecords records = MemoryRecords.withRecords(Compression.NONE, values);
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

    private static void waitForLeaderChange(
            Admin admin,
            String topic,
            int partition,
            int newLeader,
            int oldEpoch,
            String rebalanceId) {
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
                                    .get(partition);
                    assertThat(updated.errorCode()).isEqualTo(Errors.NONE.code());
                    assertThat(updated.leaderId()).isEqualTo(newLeader);
                    assertThat(updated.leaderEpoch()).isGreaterThan(oldEpoch);
                });
    }

    private static RecordMetadata send(
            KafkaProducer<byte[], byte[]> producer, String topic, String value) throws Exception {
        return producer.send(
                        new ProducerRecord<>(
                                topic, 0, null, value.getBytes(StandardCharsets.UTF_8)))
                .get(60, TimeUnit.SECONDS);
    }

    private static RecordMetadata send(
            KafkaProducer<byte[], byte[]> producer,
            String topic,
            int partition,
            long timestamp,
            String value)
            throws Exception {
        return producer.send(
                        new ProducerRecord<>(
                                topic,
                                partition,
                                timestamp,
                                null,
                                value.getBytes(StandardCharsets.UTF_8)))
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

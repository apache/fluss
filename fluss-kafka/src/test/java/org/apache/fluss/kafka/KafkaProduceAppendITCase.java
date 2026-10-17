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
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newAlterTableRequest;
import static org.assertj.core.api.Assertions.assertThat;

/** Exercises native DDL, Kafka discovery and Produce, replication, and native Fluss readback. */
class KafkaProduceAppendITCase {
    private static final String DATABASE = "produce_default";
    private static final byte[] KEY = "key".getBytes(StandardCharsets.UTF_8);
    private static final byte[] VALUE = "value".getBytes(StandardCharsets.UTF_8);

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(clusterConfig())
                    .setTabletServerListeners("FLUSS://localhost:0,KAFKA://localhost:0")
                    .build();

    @ParameterizedTest
    @ValueSource(strings = {"binary", "string"})
    void testPrecreatedTableRoundTripForAllAckModes(String format) throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                org.apache.fluss.client.admin.Admin admin = connection.getAdmin()) {
            admin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();
            for (String acks : Arrays.asList("0", "1", "all")) {
                TablePath path = TablePath.of(DATABASE, "produce_" + format + "_" + acks);
                String topic = path.toString();
                admin.createTable(path, descriptor(format), false).get();
                CLUSTER.waitUntilAllGatewayHasSameMetadata();
                // Node discovery does not wait for table replicas. With acks=0 the producer
                // cannot observe or retry a write rejected before the leader is ready.
                CLUSTER.waitUntilAllReplicaReady(
                        new TableBucket(admin.getTableInfo(path).get().getTableId(), 0));
                try (KafkaProducer<byte[], byte[]> producer = producer(acks)) {
                    assertThat(producer.partitionsFor(topic)).hasSize(1);
                    ProducerRecord<byte[], byte[]> record =
                            new ProducerRecord<>(
                                    topic,
                                    0,
                                    123L,
                                    KEY,
                                    VALUE,
                                    Arrays.asList(
                                            new RecordHeader("source", VALUE),
                                            new RecordHeader("source", null)));
                    RecordMetadata first = producer.send(record).get(30, TimeUnit.SECONDS);
                    RecordMetadata second =
                            producer.send(
                                            new ProducerRecord<>(
                                                    path.getTableName(),
                                                    0,
                                                    123L,
                                                    KEY,
                                                    VALUE,
                                                    record.headers()))
                                    .get(30, TimeUnit.SECONDS);
                    assertThat(first.topic()).isEqualTo(topic);
                    assertThat(second.topic()).isEqualTo(path.getTableName());
                    if (!acks.equals("0")) {
                        assertThat(first.offset()).isZero();
                        assertThat(second.offset()).isEqualTo(1L);
                    }
                    producer.flush();
                    assertReadback(connection, path, format, VALUE);
                } finally {
                    admin.dropTable(path, true).get();
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"binary", "string"})
    void testNullValuesArePersistedForAllAckModes(String format) throws Exception {
        List<byte[]> values = Arrays.asList(null, new byte[0], VALUE, null);
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                org.apache.fluss.client.admin.Admin admin = connection.getAdmin()) {
            admin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();
            for (String acks : Arrays.asList("0", "1", "all")) {
                TablePath path = TablePath.of(DATABASE, "produce_null_" + format + "_" + acks);
                admin.createTable(path, descriptor(format), false).get();
                CLUSTER.waitUntilAllGatewayHasSameMetadata();
                CLUSTER.waitUntilAllReplicaReady(
                        new TableBucket(admin.getTableInfo(path).get().getTableId(), 0));
                try (KafkaProducer<byte[], byte[]> producer = producer(acks, 0)) {
                    for (int i = 0; i < values.size(); i++) {
                        ProducerRecord<byte[], byte[]> record =
                                new ProducerRecord<>(
                                        i % 2 == 0 ? path.toString() : path.getTableName(),
                                        0,
                                        123L,
                                        KEY,
                                        values.get(i),
                                        Arrays.asList(
                                                new RecordHeader("source", VALUE),
                                                new RecordHeader("source", null)));
                        RecordMetadata metadata = producer.send(record).get(30, TimeUnit.SECONDS);
                        if (!acks.equals("0")) {
                            assertThat(metadata.offset()).isEqualTo(i);
                        }
                    }
                    producer.flush();
                    assertReadback(connection, path, format, values);
                } finally {
                    admin.dropTable(path, true).get();
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"binary", "string"})
    void testAllNullAndMixedBatchesPreserveOffsets(String format) throws Exception {
        TablePath path = TablePath.of(DATABASE, "produce_null_batches_" + format);
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                org.apache.fluss.client.admin.Admin admin = connection.getAdmin()) {
            admin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(path, descriptor(format), false).get();
            CLUSTER.waitUntilAllGatewayHasSameMetadata();
            try (KafkaProducer<byte[], byte[]> producer = producer("all", 0)) {
                Node leader = producer.partitionsFor(path.toString()).get(0).leader();
                try (Socket socket = new Socket(leader.host(), leader.port())) {
                    String[] names = {path.toString()};
                    assertProduceOffsets(query(socket, recordBatch(null, null), names), names, 0L);
                    assertProduceOffsets(
                            query(socket, recordBatch(VALUE, null, new byte[0], null), names),
                            names,
                            2L);
                    assertProduceOffsets(query(socket, names), names, 6L);
                    assertReadback(
                            connection,
                            path,
                            format,
                            Arrays.asList(null, null, VALUE, null, new byte[0], null, VALUE));
                }
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"binary", "string"})
    void testNotNullValueRejectsWholeBatchWithoutAdvancingOffsets(String format) throws Exception {
        TablePath path = TablePath.of(DATABASE, "produce_not_null_" + format);
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                org.apache.fluss.client.admin.Admin admin = connection.getAdmin()) {
            admin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(path, descriptor(format, false), false).get();
            CLUSTER.waitUntilAllGatewayHasSameMetadata();
            try (KafkaProducer<byte[], byte[]> producer = producer("all", 0)) {
                Node leader = producer.partitionsFor(path.toString()).get(0).leader();
                try (Socket socket = new Socket(leader.host(), leader.port())) {
                    String[] names = {path.toString()};
                    assertProduceErrors(
                            query(socket, recordBatch(null, null), names),
                            names,
                            Errors.INVALID_RECORD);
                    assertProduceErrors(
                            query(socket, recordBatch(VALUE, null, VALUE), names),
                            names,
                            Errors.INVALID_RECORD);
                    assertProduceOffsets(query(socket, names), names, 0L);
                    assertReadback(connection, path, format, VALUE, 1);
                }
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"binary", "string"})
    void testNullAndEmptyValuesPreserveKeysAndOffsets(String format) throws Exception {
        TablePath path = TablePath.of(DATABASE, "null_values_" + format);
        List<byte[]> values = Arrays.asList(null, new byte[0], VALUE);
        List<byte[]> keys = Arrays.asList(new byte[] {(byte) 0xff}, null, null);
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                org.apache.fluss.client.admin.Admin admin = connection.getAdmin()) {
            admin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(path, descriptor(format), false).get();
            CLUSTER.waitUntilAllGatewayHasSameMetadata();
            try (KafkaProducer<byte[], byte[]> producer = producer("all", 0)) {
                for (int i = 0; i < values.size(); i++) {
                    RecordMetadata metadata =
                            producer.send(
                                            new ProducerRecord<>(
                                                    path.toString(),
                                                    0,
                                                    123L,
                                                    keys.get(i),
                                                    values.get(i)))
                                    .get(30, TimeUnit.SECONDS);
                    assertThat(metadata.offset()).isEqualTo(i);
                }
                try (Table table = connection.getTable(path);
                        LogScanner scanner = table.newScan().createLogScanner()) {
                    scanner.subscribeFromBeginning(0);
                    int count = 0;
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                    while (count < values.size() && System.nanoTime() < deadline) {
                        for (ScanRecord record : scanner.poll(Duration.ofSeconds(1))) {
                            InternalRow row = record.getRow();
                            byte[] expected = values.get(count);
                            if (expected == null) {
                                assertThat(row.isNullAt(0)).isTrue();
                            } else if (format.equals("binary")) {
                                assertThat(row.getBytes(0)).isEqualTo(expected);
                            } else {
                                assertThat(row.getString(0).toString())
                                        .isEqualTo(new String(expected, StandardCharsets.UTF_8));
                            }
                            if (keys.get(count) == null) {
                                assertThat(row.isNullAt(3)).isTrue();
                            } else {
                                assertThat(row.getBytes(3)).isEqualTo(keys.get(count));
                            }
                            assertThat(row.getMap(1).size()).isZero();
                            assertThat(row.isNullAt(2)).isTrue();
                            assertThat(record.logOffset()).isEqualTo(count);
                            count++;
                        }
                    }
                    assertThat(count).isEqualTo(values.size());
                }
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    @Test
    void testSameNamedTablesAcrossDatabases() throws Exception {
        Map<TablePath, byte[]> tables = new LinkedHashMap<>();
        tables.put(TablePath.of("produce_first", "events"), VALUE);
        tables.put(
                TablePath.of("produce_second", "events"),
                "second database".getBytes(StandardCharsets.UTF_8));
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                org.apache.fluss.client.admin.Admin admin = connection.getAdmin()) {
            try {
                for (TablePath path : tables.keySet()) {
                    admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, true)
                            .get();
                    admin.createTable(path, descriptor("binary"), false).get();
                }
                CLUSTER.waitUntilAllGatewayHasSameMetadata();
                try (KafkaProducer<byte[], byte[]> producer = producer("all")) {
                    for (Map.Entry<TablePath, byte[]> entry : tables.entrySet()) {
                        String topic = entry.getKey().toString();
                        assertThat(producer.partitionsFor(topic)).hasSize(1);
                        ProducerRecord<byte[], byte[]> record =
                                new ProducerRecord<>(
                                        topic,
                                        0,
                                        123L,
                                        KEY,
                                        entry.getValue(),
                                        Arrays.asList(
                                                new RecordHeader("source", entry.getValue()),
                                                new RecordHeader("source", null)));
                        assertThat(producer.send(record).get(30, TimeUnit.SECONDS).offset())
                                .isZero();
                        assertThat(producer.send(record).get(30, TimeUnit.SECONDS).offset())
                                .isEqualTo(1L);
                    }
                    for (Map.Entry<TablePath, byte[]> entry : tables.entrySet()) {
                        assertReadback(connection, entry.getKey(), "binary", entry.getValue());
                    }
                }
            } finally {
                for (TablePath path : tables.keySet()) {
                    admin.dropTable(path, true).get();
                }
            }
        }
    }

    @Test
    void testPipelinedProducePreservesAppendOrder() throws Exception {
        TablePath path = TablePath.of(DATABASE, "produce_pipelined");
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                org.apache.fluss.client.admin.Admin admin = connection.getAdmin()) {
            admin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(path, descriptor("binary"), false).get();
            CLUSTER.waitUntilAllGatewayHasSameMetadata();
            try (KafkaProducer<byte[], byte[]> producer = producer("all", 0)) {
                assertThat(producer.partitionsFor(path.toString())).hasSize(1);
                List<byte[]> values = new ArrayList<>();
                List<Future<RecordMetadata>> writes = new ArrayList<>();
                for (int i = 0; i < 30; i++) {
                    byte[] value = new byte[i % 2 == 0 ? 16384 : 1];
                    Arrays.fill(value, (byte) i);
                    values.add(value);
                    writes.add(
                            producer.send(
                                    new ProducerRecord<>(
                                            path.toString(), 0, (long) i, KEY, value)));
                }
                for (int i = 0; i < writes.size(); i++) {
                    assertThat(writes.get(i).get(30, TimeUnit.SECONDS).offset()).isEqualTo(i);
                }
                try (Table table = connection.getTable(path);
                        LogScanner scanner = table.newScan().createLogScanner()) {
                    scanner.subscribeFromBeginning(0);
                    int count = 0;
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                    while (count < values.size() && System.nanoTime() < deadline) {
                        for (ScanRecord record : scanner.poll(Duration.ofSeconds(1))) {
                            assertThat(record.getRow().getBytes(0)).isEqualTo(values.get(count));
                            count++;
                        }
                    }
                    assertThat(count).isEqualTo(values.size());
                }
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    @Test
    void testAliasesShareOffsetsAndObservePersistedEnablementOnOneConnection() throws Exception {
        TablePath path = TablePath.of(DATABASE, "produce_enablement");
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                org.apache.fluss.client.admin.Admin admin = connection.getAdmin()) {
            admin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(path, descriptor("binary"), false).get();
            CLUSTER.waitUntilAllGatewayHasSameMetadata();
            try (KafkaProducer<byte[], byte[]> producer = producer("all", 0)) {
                Node leader = producer.partitionsFor(path.toString()).get(0).leader();
                try (Socket socket = new Socket(leader.host(), leader.port())) {
                    String[] names = {path.getTableName(), path.toString()};
                    assertProduceOffsets(query(socket, names), names, 0L, 1L);
                    setEnabled(path, false);
                    assertProduceErrors(
                            query(socket, names), names, Errors.UNKNOWN_TOPIC_OR_PARTITION);
                    setEnabled(path, true);
                    assertProduceOffsets(query(socket, names), names, 2L, 3L);
                    CLUSTER.newCoordinatorClient()
                            .alterTable(
                                    newAlterTableRequest(
                                            path,
                                            Collections.emptyMap(),
                                            Collections.singletonList(
                                                    ConfigOptions.TABLE_KAFKA_ENABLED.key()),
                                            Collections.emptyList(),
                                            false))
                            .get();
                    assertProduceErrors(
                            query(socket, names), names, Errors.UNKNOWN_TOPIC_OR_PARTITION);
                    assertReadback(connection, path, "binary", VALUE, 4);
                    admin.dropTable(path, true).get();
                    assertProduceErrors(
                            query(socket, names), names, Errors.UNKNOWN_TOPIC_OR_PARTITION);
                }
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    @Test
    void testDeletedDefaultDatabaseDoesNotFallbackOrRecreateDatabase() throws Exception {
        Configuration config = clusterConfig();
        config.set(ConfigOptions.KAFKA_DEFAULT_DATABASE, "produce_missing_db");
        config.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 1);
        config.set(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER, 1);
        FlussClusterExtension cluster =
                FlussClusterExtension.builder()
                        .setNumOfTabletServers(1)
                        .setClusterConf(config)
                        .setTabletServerListeners("FLUSS://localhost:0,KAFKA://localhost:0")
                        .build();
        cluster.start();
        try (Connection connection = ConnectionFactory.createConnection(cluster.getClientConfig());
                org.apache.fluss.client.admin.Admin admin = connection.getAdmin()) {
            // Startup creates the configured default; Produce must not recreate a deleted one.
            assertThat(admin.databaseExists("produce_missing_db").get()).isTrue();
            admin.dropDatabase("produce_missing_db", false, false).get();
            assertThat(admin.databaseExists("produce_missing_db").get()).isFalse();
            TablePath existing = TablePath.of("fluss", "events");
            long tableId = createTable(cluster, existing, descriptor("binary"));
            cluster.waitUntilTableReady(tableId);
            ServerNode node = cluster.getTabletServerNodes("KAFKA").get(0);
            try (Socket socket = new Socket(node.host(), node.port())) {
                String[] missing = {
                    "events",
                    "produce_missing_db.events",
                    "another_missing_db.events",
                    "fluss.missing"
                };
                assertProduceErrors(
                        query(socket, missing), missing, Errors.UNKNOWN_TOPIC_OR_PARTITION);
                assertProduceOffsets(
                        query(socket, existing.toString()), new String[] {existing.toString()}, 0L);
                assertProduceOffsets(
                        query(socket, existing.toString()), new String[] {existing.toString()}, 1L);
                try (KafkaProducer<byte[], byte[]> producer = producer(node, "all", 0)) {
                    assertThat(
                                    producer.send(
                                                    new ProducerRecord<>(
                                                            existing.toString(),
                                                            0,
                                                            123L,
                                                            KEY,
                                                            VALUE,
                                                            Arrays.asList(
                                                                    new RecordHeader(
                                                                            "source", VALUE),
                                                                    new RecordHeader(
                                                                            "source", null))))
                                            .get(30, TimeUnit.SECONDS)
                                            .offset())
                            .isEqualTo(2L);
                }
                assertReadback(connection, existing, "binary", VALUE, 3);
                assertThat(admin.databaseExists("produce_missing_db").get()).isFalse();
                assertThat(admin.databaseExists("another_missing_db").get()).isFalse();
            }
        } finally {
            cluster.close();
        }
    }

    private static void setEnabled(TablePath path, boolean enabled) throws Exception {
        CLUSTER.newCoordinatorClient()
                .alterTable(
                        newAlterTableRequest(
                                path,
                                Collections.singletonMap(
                                        ConfigOptions.TABLE_KAFKA_ENABLED.key(),
                                        Boolean.toString(enabled)),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                false))
                .get();
    }

    private static MemoryRecords recordBatch(byte[]... values) {
        SimpleRecord[] records = new SimpleRecord[values.length];
        for (int i = 0; i < values.length; i++) {
            records[i] =
                    new SimpleRecord(
                            123L,
                            KEY,
                            values[i],
                            new Header[] {
                                new RecordHeader("source", VALUE), new RecordHeader("source", null)
                            });
        }
        return MemoryRecords.withRecords(Compression.NONE, records);
    }

    private static ProduceResponse query(Socket socket, String... names) throws Exception {
        return query(socket, recordBatch(VALUE), names);
    }

    private static ProduceResponse query(Socket socket, MemoryRecords records, String... names)
            throws Exception {
        List<TopicProduceData> topics = new ArrayList<>();
        for (String name : names) {
            topics.add(
                    new TopicProduceData()
                            .setName(name)
                            .setPartitionData(
                                    Collections.singletonList(
                                            new PartitionProduceData()
                                                    .setIndex(0)
                                                    .setRecords(records))));
        }
        ProduceRequest request =
                new ProduceRequest(
                        new ProduceRequestData()
                                .setAcks((short) -1)
                                .setTimeoutMs(10000)
                                .setTopicData(
                                        new ProduceRequestData.TopicProduceDataCollection(
                                                topics.iterator())),
                        (short) 9);
        RequestHeader header =
                new RequestHeader(ApiKeys.PRODUCE, request.version(), "produce-test", 1);
        ByteBuffer encoded = request.serializeWithHeader(header);
        byte[] bytes = new byte[encoded.remaining()];
        encoded.get(bytes);
        socket.setSoTimeout(15000);
        DataOutputStream output = new DataOutputStream(socket.getOutputStream());
        output.writeInt(bytes.length);
        output.write(bytes);
        output.flush();
        DataInputStream input = new DataInputStream(socket.getInputStream());
        byte[] response = new byte[input.readInt()];
        input.readFully(response);
        return (ProduceResponse) AbstractResponse.parseResponse(ByteBuffer.wrap(response), header);
    }

    private static void assertProduceOffsets(
            ProduceResponse response, String[] names, long... offsets) {
        assertProduceErrors(response, names, Errors.NONE);
        for (int i = 0; i < names.length; i++) {
            assertThat(
                            response.data()
                                    .responses()
                                    .find(names[i])
                                    .partitionResponses()
                                    .get(0)
                                    .baseOffset())
                    .isEqualTo(offsets[i]);
        }
    }

    private static void assertProduceErrors(
            ProduceResponse response, String[] names, Errors error) {
        assertThat(response.data().responses())
                .extracting(topic -> topic.name())
                .containsExactly(names);
        for (String name : names) {
            List<PartitionProduceResponse> partitions =
                    response.data().responses().find(name).partitionResponses();
            assertThat(partitions).hasSize(1);
            assertThat(partitions.get(0).index()).isZero();
            assertThat(partitions.get(0).errorCode()).isEqualTo(error.code());
            if (error != Errors.NONE) {
                assertThat(partitions.get(0).baseOffset()).isEqualTo(-1L);
            }
        }
    }

    private static void assertReadback(
            Connection connection, TablePath path, String format, byte[] expectedValue)
            throws Exception {
        assertReadback(connection, path, format, expectedValue, 2);
    }

    private static void assertReadback(
            Connection connection,
            TablePath path,
            String format,
            byte[] expectedValue,
            int expectedCount)
            throws Exception {
        assertReadback(connection, path, format, Collections.nCopies(expectedCount, expectedValue));
    }

    private static void assertReadback(
            Connection connection, TablePath path, String format, List<byte[]> expectedValues)
            throws Exception {
        try (Table table = connection.getTable(path);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(0);
            int count = 0;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (count < expectedValues.size() && System.nanoTime() < deadline) {
                ScanRecords records = scanner.poll(Duration.ofSeconds(1));
                for (ScanRecord record : records) {
                    assertThat(record.logOffset()).isEqualTo(count);
                    byte[] expectedValue = expectedValues.get(count);
                    InternalRow row = record.getRow();
                    if (expectedValue == null) {
                        assertThat(row.isNullAt(0)).isTrue();
                    } else if (format.equals("binary")) {
                        assertThat(row.getBytes(0)).isEqualTo(expectedValue);
                    } else {
                        assertThat(row.getString(0).toString())
                                .isEqualTo(new String(expectedValue, StandardCharsets.UTF_8));
                    }
                    assertThat(row.getBytes(3)).isEqualTo(KEY);
                    assertThat(row.getMap(1).size()).isEqualTo(1);
                    assertThat(row.getMap(1).keyArray().getString(0).toString())
                            .isEqualTo("source");
                    assertThat(row.getMap(1).valueArray().isNullAt(0)).isTrue();
                    assertThat(row.isNullAt(2)).isTrue();
                    InternalRow metadata = row.getRow(4, 3);
                    assertThat(metadata.getTimestampLtz(0, 3).getEpochMillisecond())
                            .isEqualTo(123L);
                    assertThat(metadata.getInt(1)).isZero();
                    assertThat(metadata.getInt(2)).isZero();
                    count++;
                }
            }
            assertThat(count)
                    .as("Native readback for %s (%s)", path, format)
                    .isEqualTo(expectedValues.size());
        }
    }

    private static TableDescriptor descriptor(String format) {
        return descriptor(format, true);
    }

    private static TableDescriptor descriptor(String format, boolean nullableValue) {
        boolean binary = format.equals("binary");
        return TableDescriptor.builder()
                .schema(
                        Schema.newBuilder()
                                .column(
                                        "_kafka_value",
                                        (binary ? DataTypes.BYTES() : DataTypes.STRING())
                                                .copy(nullableValue))
                                .column(
                                        "_kafka_headers",
                                        DataTypes.MAP(DataTypes.STRING(), DataTypes.BYTES()))
                                .column("_kafka_rescue", DataTypes.STRING())
                                .column("_kafka_key", DataTypes.BYTES())
                                .column(
                                        "_kafka_metadata",
                                        DataTypes.ROW(
                                                DataTypes.FIELD(
                                                        "timestamp", DataTypes.TIMESTAMP_LTZ(3)),
                                                DataTypes.FIELD("timestamp_type", DataTypes.INT()),
                                                DataTypes.FIELD("partition", DataTypes.INT())))
                                .build())
                .distributedBy(1)
                .logFormat(LogFormat.ARROW)
                .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE, format)
                .build();
    }

    private static KafkaProducer<byte[], byte[]> producer(String acks) {
        return producer(acks, 16384);
    }

    private static KafkaProducer<byte[], byte[]> producer(String acks, int batchSize) {
        return producer(CLUSTER.getTabletServerNodes("KAFKA").get(0), acks, batchSize);
    }

    private static KafkaProducer<byte[], byte[]> producer(
            ServerNode node, String acks, int batchSize) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, node.host() + ":" + node.port());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        config.put(ProducerConfig.ACKS_CONFIG, acks);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30000);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000);
        return new KafkaProducer<>(config);
    }

    private static Configuration clusterConfig() {
        Configuration config = new Configuration();
        config.set(ConfigOptions.KAFKA_ENABLED, true);
        config.set(ConfigOptions.KAFKA_DEFAULT_DATABASE, DATABASE);
        config.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        config.set(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER, 2);
        return config;
    }
}

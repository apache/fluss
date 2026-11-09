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
import org.apache.fluss.config.KafkaValueConversionType;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.tablet.TabletServer;
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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.kafka.KafkaSocketTestUtils.sendRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newAlterTableRequest;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
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
    @CsvSource({"binary,0", "binary,1", "binary,all", "string,all"})
    void testValuesAndTombstonesRoundTrip(String format, String acks) throws Exception {
        List<byte[]> values = Arrays.asList(null, new byte[0], VALUE, null);
        List<byte[]> keys = Arrays.asList(new byte[] {(byte) 0xff}, null, null, KEY);
        TablePath path = TablePath.of(DATABASE, "produce_null_" + format + "_" + acks);
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                org.apache.fluss.client.admin.Admin admin = connection.getAdmin()) {
            admin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(path, descriptor(format), false).get();
            CLUSTER.waitUntilAllGatewayHasSameMetadata();
            // acks=0 has no broker response, so wait before submitting an unretried write.
            CLUSTER.waitUntilAllReplicaReady(
                    new TableBucket(admin.getTableInfo(path).get().getTableId(), 0));
            try (KafkaProducer<byte[], byte[]> producer = producer(acks, 0)) {
                assertThat(producer.partitionsFor(path.toString())).hasSize(1);
                for (int i = 0; i < values.size(); i++) {
                    ProducerRecord<byte[], byte[]> record =
                            new ProducerRecord<>(
                                    i % 2 == 0 ? path.toString() : path.getTableName(),
                                    0,
                                    123L,
                                    keys.get(i),
                                    values.get(i),
                                    Arrays.asList(
                                            new RecordHeader("source", VALUE),
                                            new RecordHeader("source", null)));
                    RecordMetadata metadata = producer.send(record).get(30, TimeUnit.SECONDS);
                    assertThat(metadata.topic()).isEqualTo(record.topic());
                    if (!acks.equals("0")) {
                        assertThat(metadata.offset()).isEqualTo(i);
                    }
                }
                producer.flush();
                assertReadback(connection, path, format, values, keys);
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({
        "gzip,binary",
        "snappy,binary",
        "lz4,binary",
        "zstd,binary",
        "lz4,string",
        "zstd,string"
    })
    void testCompressedBatchPreservesRecordsInNativeStorage(String codec, String format)
            throws Exception {
        TablePath path = TablePath.of(DATABASE, "compressed_" + codec + "_" + format);
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                org.apache.fluss.client.admin.Admin admin = connection.getAdmin()) {
            admin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(path, descriptor(format), false).get();
            CLUSTER.waitUntilAllGatewayHasSameMetadata();
            CLUSTER.waitAndGetLeaderReplica(
                    new TableBucket(admin.getTableInfo(path).get().getTableId(), 0));
            try (KafkaProducer<byte[], byte[]> producer = compressedProducer(codec)) {
                producer.partitionsFor(path.toString());
                List<Future<RecordMetadata>> writes = new ArrayList<>();
                for (int i = 0; i < 50; i++) {
                    writes.add(
                            producer.send(
                                    new ProducerRecord<>(
                                            path.toString(),
                                            0,
                                            1000L + i,
                                            KEY,
                                            ("消息-" + i).getBytes(StandardCharsets.UTF_8),
                                            Arrays.asList(
                                                    new RecordHeader("source", null),
                                                    new RecordHeader("source", VALUE),
                                                    new RecordHeader("nullable", VALUE),
                                                    new RecordHeader("nullable", null)))));
                }
                producer.flush();
                for (int i = 0; i < writes.size(); i++) {
                    assertThat(writes.get(i).get(30, TimeUnit.SECONDS).offset()).isEqualTo(i);
                }
                try (Table table = connection.getTable(path);
                        LogScanner scanner = table.newScan().createLogScanner()) {
                    scanner.subscribeFromBeginning(0);
                    int count = 0;
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                    while (count < writes.size() && System.nanoTime() < deadline) {
                        for (ScanRecord record : scanner.poll(Duration.ofSeconds(1))) {
                            InternalRow row = record.getRow();
                            assertThat(record.logOffset()).isEqualTo(count);
                            String expectedValue = "消息-" + count;
                            if (format.equals("binary")) {
                                assertThat(row.getBytes(0))
                                        .isEqualTo(expectedValue.getBytes(StandardCharsets.UTF_8));
                            } else {
                                assertThat(row.getString(0).toString()).isEqualTo(expectedValue);
                            }
                            assertThat(row.getBytes(3)).isEqualTo(KEY);
                            assertThat(row.isNullAt(2)).isTrue();
                            InternalMap headers = row.getMap(1);
                            Map<String, byte[]> values = new HashMap<>();
                            for (int index = 0; index < headers.size(); index++) {
                                values.put(
                                        headers.keyArray().getString(index).toString(),
                                        headers.valueArray().isNullAt(index)
                                                ? null
                                                : headers.valueArray().getBytes(index));
                            }
                            assertThat(headers.size()).isEqualTo(2);
                            assertThat(values).containsOnlyKeys("source", "nullable");
                            assertThat(values.get("source")).isEqualTo(VALUE);
                            assertThat(values.get("nullable")).isNull();
                            count++;
                        }
                    }
                    assertThat(count).isEqualTo(50);
                    assertThat(scanner.poll(Duration.ofMillis(100))).isEmpty();
                }
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    private static KafkaProducer<byte[], byte[]> compressedProducer(String codec) {
        ServerNode node = CLUSTER.getTabletServerNodes("KAFKA").get(0);
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, node.host() + ":" + node.port());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, codec);
        // Explicit flush sends a single accumulated batch instead of one request per send.
        config.put(ProducerConfig.LINGER_MS_CONFIG, 60000L);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 90000);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30000);
        return new KafkaProducer<>(config);
    }

    @Test
    void testAllNullAndMixedBatchesPreserveOffsets() throws Exception {
        TablePath path = TablePath.of(DATABASE, "produce_null_batches");
        String topic = path.toString();
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                org.apache.fluss.client.admin.Admin admin = connection.getAdmin()) {
            admin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(path, descriptor("binary"), false).get();
            CLUSTER.waitUntilAllGatewayHasSameMetadata();
            CLUSTER.waitUntilAllReplicaReady(
                    new TableBucket(admin.getTableInfo(path).get().getTableId(), 0));
            try (KafkaProducer<byte[], byte[]> producer = producer("all", 0)) {
                Node leader = producer.partitionsFor(topic).get(0).leader();
                try (Socket socket = new Socket(leader.host(), leader.port())) {
                    assertProduceOffset(query(socket, topic, null, null), topic, 0L);
                    assertProduceOffset(
                            query(socket, topic, VALUE, null, new byte[0], null), topic, 2L);
                    assertProduceOffset(query(socket, topic, VALUE), topic, 6L);
                    assertReadback(
                            connection,
                            path,
                            "binary",
                            Arrays.asList(null, null, VALUE, null, new byte[0], null, VALUE));
                }
            } finally {
                admin.dropTable(path, true).get();
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
            long tableId = admin.getTableInfo(path).get().getTableId();
            CLUSTER.waitUntilAllReplicaReady(new TableBucket(tableId, 0));
            try (KafkaProducer<byte[], byte[]> producer = producer("all", 0)) {
                assertThat(producer.partitionsFor(path.toString())).hasSize(1);
                List<byte[]> values = new ArrayList<>();
                List<Future<RecordMetadata>> writes = new ArrayList<>();
                for (int i = 0; i < 8; i++) {
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
            long tableId = admin.getTableInfo(path).get().getTableId();
            CLUSTER.waitUntilAllReplicaReady(new TableBucket(tableId, 0));
            try (KafkaProducer<byte[], byte[]> producer = producer("all", 0)) {
                Node leader = producer.partitionsFor(path.toString()).get(0).leader();
                try (Socket socket = new Socket(leader.host(), leader.port())) {
                    String unqualifiedName = path.getTableName();
                    String qualifiedName = path.toString();
                    assertProduceOffset(query(socket, unqualifiedName, VALUE), unqualifiedName, 0L);
                    setEnabled(path, tableId, false);
                    assertProduceError(
                            query(socket, qualifiedName, VALUE),
                            qualifiedName,
                            Errors.UNKNOWN_TOPIC_OR_PARTITION);
                    setEnabled(path, tableId, true);
                    assertProduceOffset(query(socket, qualifiedName, VALUE), qualifiedName, 1L);
                    assertReadback(connection, path, "binary", Collections.nCopies(2, VALUE));
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
                for (String name : missing) {
                    assertProduceError(
                            query(socket, name, VALUE), name, Errors.UNKNOWN_TOPIC_OR_PARTITION);
                }
                String topic = existing.toString();
                assertProduceOffset(query(socket, topic, VALUE), topic, 0L);
                assertProduceOffset(query(socket, topic, VALUE), topic, 1L);
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
                assertReadback(connection, existing, "binary", Collections.nCopies(3, VALUE));
                assertThat(admin.databaseExists("produce_missing_db").get()).isFalse();
                assertThat(admin.databaseExists("another_missing_db").get()).isFalse();
            }
        } finally {
            cluster.close();
        }
    }

    private static void setEnabled(TablePath path, long tableId, boolean enabled) throws Exception {
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
        waitUntilEnabled(tableId, enabled);
    }

    private static void waitUntilEnabled(long tableId, boolean enabled) {
        // ALTER acknowledgement precedes asynchronous propagation to the local replicas.
        TableBucket bucket = new TableBucket(tableId, 0);
        retry(
                Duration.ofSeconds(30),
                () -> {
                    for (TabletServer server : CLUSTER.getTabletServers()) {
                        TableInfo tableInfo =
                                server.getReplicaManager()
                                        .getReplicaOrException(bucket)
                                        .getTableInfo();
                        assertThat(KafkaTableSchema.isEnabled(tableInfo.toTableDescriptor()))
                                .isEqualTo(enabled);
                    }
                });
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

    private static ProduceResponse query(Socket socket, String topicName, byte[]... values)
            throws Exception {
        TopicProduceData topic =
                new TopicProduceData()
                        .setName(topicName)
                        .setPartitionData(
                                Collections.singletonList(
                                        new PartitionProduceData()
                                                .setIndex(0)
                                                .setRecords(recordBatch(values))));
        ProduceRequest request =
                new ProduceRequest(
                        new ProduceRequestData()
                                .setAcks((short) -1)
                                .setTimeoutMs(10000)
                                .setTopicData(
                                        new ProduceRequestData.TopicProduceDataCollection(
                                                Collections.singletonList(topic).iterator())),
                        (short) 9);
        socket.setSoTimeout(15000);
        return (ProduceResponse) sendRequest(socket, request);
    }

    private static void assertProduceOffset(ProduceResponse response, String topic, long offset) {
        assertProduceError(response, topic, Errors.NONE);
        assertThat(response.data().responses().find(topic).partitionResponses().get(0).baseOffset())
                .isEqualTo(offset);
    }

    private static void assertProduceError(ProduceResponse response, String topic, Errors error) {
        assertThat(response.data().responses()).extracting(t -> t.name()).containsExactly(topic);
        List<PartitionProduceResponse> partitions =
                response.data().responses().find(topic).partitionResponses();
        assertThat(partitions).hasSize(1);
        PartitionProduceResponse partition = partitions.get(0);
        assertThat(partition.index()).isZero();
        assertThat(partition.errorCode())
                .as("Produce error for %s: %s", topic, partition.errorMessage())
                .isEqualTo(error.code());
        if (error != Errors.NONE) {
            assertThat(partition.baseOffset()).isEqualTo(-1L);
        }
    }

    private static void assertReadback(
            Connection connection, TablePath path, String format, List<byte[]> expectedValues)
            throws Exception {
        assertReadback(
                connection,
                path,
                format,
                expectedValues,
                Collections.nCopies(expectedValues.size(), KEY));
    }

    private static void assertReadback(
            Connection connection,
            TablePath path,
            String format,
            List<byte[]> expectedValues,
            List<byte[]> expectedKeys)
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
                    if (expectedKeys.get(count) == null) {
                        assertThat(row.isNullAt(3)).isTrue();
                    } else {
                        assertThat(row.getBytes(3)).isEqualTo(expectedKeys.get(count));
                    }
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
        boolean binary = format.equals("binary");
        return TableDescriptor.builder()
                .schema(
                        Schema.newBuilder()
                                .column(
                                        "_kafka_value",
                                        binary ? DataTypes.BYTES() : DataTypes.STRING())
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
                .property(
                        ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE,
                        format.equals("binary")
                                ? KafkaValueConversionType.BINARY
                                : KafkaValueConversionType.STRING)
                .build();
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

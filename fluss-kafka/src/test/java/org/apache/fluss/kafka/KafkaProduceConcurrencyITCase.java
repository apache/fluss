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
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.KafkaValueConversionType;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies pipelined Produce ordering and failure isolation against native persisted records. */
class KafkaProduceConcurrencyITCase {
    private static final String DATABASE = "concurrent_produce";

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(clusterConfig())
                    .setTabletServerListeners("FLUSS://localhost:0,KAFKA://localhost:0")
                    .build();

    @ParameterizedTest
    @ValueSource(shorts = {1, -1})
    void testPipelinedAliasesAndPartitionFailuresPreserveNativeOrder(short acks) throws Exception {
        List<TablePath> paths =
                Arrays.asList(
                        TablePath.of(DATABASE, "ordered_first"),
                        TablePath.of(DATABASE, "ordered_second"));
        Map<TopicPartition, List<String>> expected = new HashMap<>();
        List<Map<TopicPartition, Long>> expectedOffsets = new ArrayList<>();
        List<RequestHeader> headers = new ArrayList<>();
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            try {
                for (TablePath path : paths) {
                    createTable(admin, path, 2);
                    for (int partition = 0; partition < 2; partition++) {
                        expected.put(
                                new TopicPartition(path.toString(), partition), new ArrayList<>());
                    }
                }
                try (Socket socket = connect()) {
                    // Send every request before reading responses. The first conversion is
                    // much larger, and later requests mix aliases, nulls and a corrupt batch.
                    for (int round = 0; round < 8; round++) {
                        List<TopicProduceData> topics = new ArrayList<>();
                        Map<TopicPartition, Long> offsets = new HashMap<>();
                        for (int table = 0; table < paths.size(); table++) {
                            TablePath path = paths.get(table);
                            String topic = round % 2 == 0 ? path.toString() : path.getTableName();
                            List<PartitionProduceData> partitions = new ArrayList<>();
                            for (int partition = 0; partition < 2; partition++) {
                                List<String> values =
                                        expected.get(
                                                new TopicPartition(path.toString(), partition));
                                boolean invalid = round == 1 && table == 0 && partition == 0;
                                offsets.put(
                                        new TopicPartition(topic, partition),
                                        invalid ? -1L : (long) values.size());
                                SimpleRecord[] records = new SimpleRecord[round == 0 ? 512 : 1];
                                for (int record = 0; record < records.length; record++) {
                                    String value =
                                            round == 2
                                                    ? null
                                                    : table + ":" + partition + ":" + round + ":"
                                                            + record;
                                    byte[] bytes =
                                            value == null
                                                    ? null
                                                    : value.getBytes(StandardCharsets.UTF_8);
                                    records[record] = new SimpleRecord(123L, null, bytes);
                                    if (!invalid) {
                                        values.add(value);
                                    }
                                }
                                MemoryRecords batch =
                                        MemoryRecords.withRecords(Compression.NONE, records);
                                if (invalid) {
                                    // STRING accepts arbitrary bytes; corrupt the checksum instead.
                                    ByteBuffer bytes = batch.buffer();
                                    int last = batch.sizeInBytes() - 1;
                                    bytes.put(last, (byte) (bytes.get(last) ^ 1));
                                }
                                partitions.add(
                                        new PartitionProduceData()
                                                .setIndex(partition)
                                                .setRecords(batch));
                            }
                            topics.add(
                                    new TopicProduceData()
                                            .setName(topic)
                                            .setPartitionData(partitions));
                        }
                        headers.add(send(socket, request(acks, topics), round));
                        expectedOffsets.add(offsets);
                    }
                    for (int round = 0; round < headers.size(); round++) {
                        ProduceResponse response = receive(socket, headers.get(round));
                        Map<TopicPartition, Long> offsets = expectedOffsets.get(round);
                        assertThat(response.data().responses()).hasSize(paths.size());
                        response.data()
                                .responses()
                                .forEach(
                                        topic -> {
                                            assertThat(topic.partitionResponses()).hasSize(2);
                                            for (PartitionProduceResponse partition :
                                                    topic.partitionResponses()) {
                                                Long offset =
                                                        offsets.remove(
                                                                new TopicPartition(
                                                                        topic.name(),
                                                                        partition.index()));
                                                assertThat(offset).isNotNull();
                                                assertThat(partition.errorCode())
                                                        .isEqualTo(
                                                                offset < 0
                                                                        ? Errors.CORRUPT_MESSAGE
                                                                                .code()
                                                                        : Errors.NONE.code());
                                                assertThat(partition.baseOffset())
                                                        .isEqualTo(offset);
                                            }
                                        });
                        assertThat(offsets).isEmpty();
                    }
                }
                for (TablePath path : paths) {
                    for (int partition = 0; partition < 2; partition++) {
                        List<String> values =
                                expected.get(new TopicPartition(path.toString(), partition));
                        assertReadback(connection, path, partition, 0L, values);
                        assertThat(
                                        admin.listOffsets(
                                                        path,
                                                        Collections.singleton(partition),
                                                        new OffsetSpec.LatestSpec())
                                                .bucketResult(partition)
                                                .get(30, TimeUnit.SECONDS))
                                .isEqualTo((long) values.size());
                    }
                }
            } finally {
                for (TablePath path : paths) {
                    admin.dropTable(path, true).get();
                }
            }
        }
    }

    @Test
    void testDisconnectWithPipelinedRequestsDoesNotBlockNewConnection() throws Exception {
        TablePath path = TablePath.of(DATABASE, "disconnected_producer");
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            try {
                createTable(admin, path, 1);
                SimpleRecord[] records = new SimpleRecord[512];
                Arrays.fill(
                        records,
                        new SimpleRecord("before_disconnect".getBytes(StandardCharsets.UTF_8)));
                try (Socket disconnected = connect()) {
                    for (int i = 0; i < 8; i++) {
                        send(
                                disconnected,
                                request(
                                        (short) -1,
                                        Collections.singletonList(
                                                new TopicProduceData()
                                                        .setName(path.toString())
                                                        .setPartitionData(
                                                                Collections.singletonList(
                                                                        partition(0, records))))),
                                i);
                    }
                    // Disconnect without consuming ACKs while the shared executor may still
                    // contain this connection's work. Later producers must remain usable.
                }
                try (Socket socket = connect()) {
                    RequestHeader header =
                            send(
                                    socket,
                                    request(
                                            (short) -1,
                                            Collections.singletonList(
                                                    new TopicProduceData()
                                                            .setName(path.getTableName())
                                                            .setPartitionData(
                                                                    Collections.singletonList(
                                                                            partition(
                                                                                    0,
                                                                                    new SimpleRecord(
                                                                                            "after_disconnect"
                                                                                                    .getBytes(
                                                                                                            StandardCharsets
                                                                                                                    .UTF_8))))))),
                                    20);
                    PartitionProduceResponse response =
                            receive(socket, header)
                                    .data()
                                    .responses()
                                    .find(path.getTableName())
                                    .partitionResponses()
                                    .get(0);
                    assertThat(response.errorCode()).isEqualTo(Errors.NONE.code());
                    assertThat(response.baseOffset()).isNotNegative();
                    // The disconnected client's writes have no delivery guarantee. Check the
                    // new producer's acknowledged record at its authoritative native offset.
                    assertReadback(
                            connection,
                            path,
                            0,
                            response.baseOffset(),
                            Collections.singletonList("after_disconnect"));
                }
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    private static void createTable(Admin admin, TablePath path, int buckets) throws Exception {
        admin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();
        admin.createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("_kafka_value", DataTypes.STRING())
                                                .build())
                                .distributedBy(buckets)
                                .logFormat(LogFormat.ARROW)
                                .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                                .property(
                                        ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE,
                                        KafkaValueConversionType.STRING)
                                .build(),
                        false)
                .get();
        CLUSTER.waitUntilTableReady(admin.getTableInfo(path).get().getTableId());
    }

    private static PartitionProduceData partition(int id, SimpleRecord... records) {
        return new PartitionProduceData()
                .setIndex(id)
                .setRecords(MemoryRecords.withRecords(Compression.NONE, records));
    }

    private static ProduceRequest request(short acks, List<TopicProduceData> topics) {
        return new ProduceRequest(
                new ProduceRequestData()
                        .setAcks(acks)
                        .setTimeoutMs(10000)
                        .setTopicData(
                                new ProduceRequestData.TopicProduceDataCollection(
                                        topics.iterator())),
                (short) 9);
    }

    private static Socket connect() throws Exception {
        ServerNode node = CLUSTER.getTabletServerNodes("KAFKA").get(0);
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(node.host(), node.port()), 5000);
            socket.setSoTimeout(30000);
            return socket;
        } catch (Exception failure) {
            socket.close();
            throw failure;
        }
    }

    private static RequestHeader send(Socket socket, ProduceRequest request, int correlationId)
            throws Exception {
        RequestHeader header =
                new RequestHeader(
                        ApiKeys.PRODUCE, request.version(), "concurrent-test", correlationId);
        ByteBuffer encoded = request.serializeWithHeader(header);
        byte[] bytes = new byte[encoded.remaining()];
        encoded.get(bytes);
        DataOutputStream output = new DataOutputStream(socket.getOutputStream());
        output.writeInt(bytes.length);
        output.write(bytes);
        output.flush();
        return header;
    }

    private static ProduceResponse receive(Socket socket, RequestHeader header) throws Exception {
        DataInputStream input = new DataInputStream(socket.getInputStream());
        int size = input.readInt();
        assertThat(size).isBetween(1, 1024 * 1024);
        byte[] bytes = new byte[size];
        input.readFully(bytes);
        assertThat(ByteBuffer.wrap(bytes).getInt()).isEqualTo(header.correlationId());
        return (ProduceResponse) AbstractResponse.parseResponse(ByteBuffer.wrap(bytes), header);
    }

    private static void assertReadback(
            Connection connection,
            TablePath path,
            int partition,
            long startOffset,
            List<String> expected)
            throws Exception {
        try (Table table = connection.getTable(path);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribe(partition, startOffset);
            List<String> actual = new ArrayList<>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (actual.size() < expected.size() && System.nanoTime() < deadline) {
                for (ScanRecord record : scanner.poll(Duration.ofSeconds(1))) {
                    assertThat(record.logOffset()).isEqualTo(startOffset + actual.size());
                    actual.add(
                            record.getRow().isNullAt(0)
                                    ? null
                                    : record.getRow().getString(0).toString());
                    if (actual.size() == expected.size()) {
                        break;
                    }
                }
            }
            assertThat(actual).containsExactlyElementsOf(expected);
        }
    }

    private static Configuration clusterConfig() {
        Configuration config = new Configuration();
        config.set(ConfigOptions.KAFKA_ENABLED, true);
        config.set(ConfigOptions.KAFKA_DEFAULT_DATABASE, DATABASE);
        config.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 1);
        config.set(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER, 1);
        config.set(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS, 4);
        return config;
    }
}

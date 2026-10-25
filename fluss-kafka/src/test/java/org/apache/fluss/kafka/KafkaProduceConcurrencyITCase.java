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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

import static org.apache.fluss.kafka.KafkaSocketTestUtils.receive;
import static org.apache.fluss.kafka.KafkaSocketTestUtils.send;
import static org.apache.fluss.kafka.KafkaSocketTestUtils.sendRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

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
    void testPipelinedAliasesPreserveNativeOrder(short acks) throws Exception {
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
                    // much larger, and later requests alternate full names and aliases.
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
                                offsets.put(
                                        new TopicPartition(topic, partition), (long) values.size());
                                SimpleRecord[] records = new SimpleRecord[round == 0 ? 512 : 1];
                                for (int record = 0; record < records.length; record++) {
                                    String value =
                                            table + ":" + partition + ":" + round + ":" + record;
                                    records[record] =
                                            new SimpleRecord(
                                                    value.getBytes(StandardCharsets.UTF_8));
                                    values.add(value);
                                }
                                partitions.add(partition(partition, records));
                            }
                            topics.add(
                                    new TopicProduceData()
                                            .setName(topic)
                                            .setPartitionData(partitions));
                        }
                        headers.add(
                                send(
                                        socket,
                                        request(acks, topics.toArray(new TopicProduceData[0])),
                                        round));
                        expectedOffsets.add(offsets);
                    }
                    for (int round = 0; round < headers.size(); round++) {
                        ProduceResponse response =
                                (ProduceResponse) receive(socket, headers.get(round));
                        Map<TopicPartition, Long> offsets = expectedOffsets.get(round);
                        response.data()
                                .responses()
                                .forEach(
                                        topic -> {
                                            for (PartitionProduceResponse partition :
                                                    topic.partitionResponses()) {
                                                Long offset =
                                                        offsets.remove(
                                                                new TopicPartition(
                                                                        topic.name(),
                                                                        partition.index()));
                                                assertThat(offset).isNotNull();
                                                assertThat(partition.errorCode())
                                                        .isEqualTo(Errors.NONE.code());
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
    void testCorruptPartitionDoesNotPreventValidPartitionAppend() throws Exception {
        TablePath path = TablePath.of(DATABASE, "isolated_failure");
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            try {
                createTable(admin, path, 2);
                MemoryRecords corrupt =
                        MemoryRecords.withRecords(
                                Compression.NONE,
                                new SimpleRecord("rejected".getBytes(StandardCharsets.UTF_8)));
                ByteBuffer bytes = corrupt.buffer();
                int last = corrupt.sizeInBytes() - 1;
                bytes.put(last, (byte) (bytes.get(last) ^ 1));
                ProduceRequest request =
                        request(
                                (short) -1,
                                topic(
                                        path.toString(),
                                        new PartitionProduceData().setIndex(0).setRecords(corrupt),
                                        partition(
                                                1,
                                                new SimpleRecord(
                                                        "accepted"
                                                                .getBytes(
                                                                        StandardCharsets.UTF_8)))));
                try (Socket socket = connect()) {
                    ProduceResponse response = (ProduceResponse) sendRequest(socket, request);
                    assertThat(
                                    response.data()
                                            .responses()
                                            .find(path.toString())
                                            .partitionResponses())
                            .extracting(
                                    PartitionProduceResponse::index,
                                    PartitionProduceResponse::errorCode)
                            .containsExactlyInAnyOrder(
                                    tuple(0, Errors.CORRUPT_MESSAGE.code()),
                                    tuple(1, Errors.NONE.code()));
                }
                assertReadback(connection, path, 1, 0L, Collections.singletonList("accepted"));
                assertThat(
                                admin.listOffsets(
                                                path,
                                                Collections.singleton(0),
                                                new OffsetSpec.LatestSpec())
                                        .bucketResult(0)
                                        .get(30, TimeUnit.SECONDS))
                        .isZero();
            } finally {
                admin.dropTable(path, true).get();
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
                ProduceRequest pending =
                        request((short) -1, topic(path.toString(), partition(0, records)));
                try (Socket disconnected = connect()) {
                    for (int i = 0; i < 8; i++) {
                        send(disconnected, pending, i);
                    }
                    // Close without consuming ACKs; queued work must not block a new connection.
                }
                try (Socket socket = connect()) {
                    ProduceRequest next =
                            request(
                                    (short) -1,
                                    topic(
                                            path.getTableName(),
                                            partition(
                                                    0,
                                                    new SimpleRecord(
                                                            "after_disconnect"
                                                                    .getBytes(
                                                                            StandardCharsets
                                                                                    .UTF_8)))));
                    RequestHeader header = send(socket, next, 20);
                    PartitionProduceResponse response =
                            ((ProduceResponse) receive(socket, header))
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

    private static TopicProduceData topic(String name, PartitionProduceData... partitions) {
        return new TopicProduceData().setName(name).setPartitionData(Arrays.asList(partitions));
    }

    private static ProduceRequest request(short acks, TopicProduceData... topics) {
        return new ProduceRequest(
                new ProduceRequestData()
                        .setAcks(acks)
                        .setTimeoutMs(10000)
                        .setTopicData(
                                new ProduceRequestData.TopicProduceDataCollection(
                                        Arrays.asList(topics).iterator())),
                (short) 9);
    }

    private static Socket connect() throws Exception {
        return KafkaSocketTestUtils.connect(CLUSTER.getTabletServerNodes("KAFKA").get(0), 30000);
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
                    actual.add(record.getRow().getString(0).toString());
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

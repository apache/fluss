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
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises native DDL, Kafka discovery and Produce, replication, and native Fluss readback. */
class KafkaProduceAppendITCase {
    private static final String DATABASE = "kafka";
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
    @ValueSource(strings = {"raw", "string"})
    void testPrecreatedTableRoundTripForAllAckModes(String format) throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                org.apache.fluss.client.admin.Admin admin = connection.getAdmin()) {
            admin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();
            for (String acks : Arrays.asList("0", "1", "all")) {
                String topic = "produce_" + format + "_" + acks;
                TablePath path = TablePath.of(DATABASE, topic);
                admin.createTable(path, descriptor(format), false).get();
                CLUSTER.waitUntilAllGatewayHasSameMetadata();
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
                    RecordMetadata second = producer.send(record).get(30, TimeUnit.SECONDS);
                    if (!acks.equals("0")) {
                        assertThat(first.offset()).isZero();
                        assertThat(second.offset()).isEqualTo(1L);
                    }
                    producer.flush();
                    assertReadback(connection, path, format);
                } finally {
                    admin.dropTable(path, true).get();
                }
            }
        }
    }

    private static void assertReadback(Connection connection, TablePath path, String format)
            throws Exception {
        try (Table table = connection.getTable(path);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(0);
            int count = 0;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (count < 2 && System.nanoTime() < deadline) {
                ScanRecords records = scanner.poll(Duration.ofSeconds(1));
                for (ScanRecord record : records) {
                    InternalRow row = record.getRow();
                    if (format.equals("raw")) {
                        assertThat(row.getBytes(0)).isEqualTo(VALUE);
                        assertThat(row.getBytes(3)).isEqualTo(KEY);
                    } else {
                        assertThat(row.getString(0).toString()).isEqualTo("value");
                        assertThat(row.getString(3).toString()).isEqualTo("key");
                    }
                    assertThat(row.getTimestampLtz(1, 3).getEpochMillisecond()).isEqualTo(123L);
                    assertThat(row.getArray(2).size()).isEqualTo(2);
                    assertThat(row.getArray(2).getRow(0, 2).getString(0).toString())
                            .isEqualTo("source");
                    assertThat(row.getArray(2).getRow(1, 2).isNullAt(1)).isTrue();
                    count++;
                }
            }
            assertThat(count).isEqualTo(2);
        }
    }

    private static TableDescriptor descriptor(String format) {
        boolean raw = format.equals("raw");
        return TableDescriptor.builder()
                .schema(
                        Schema.newBuilder()
                                .column("body", raw ? DataTypes.BYTES() : DataTypes.STRING())
                                .column("received_at", DataTypes.TIMESTAMP_LTZ(3).copy(false))
                                .column(
                                        "attributes",
                                        DataTypes.ARRAY(
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                                        DataTypes.FIELD(
                                                                "value", DataTypes.BYTES()))))
                                .column("message_key", raw ? DataTypes.BYTES() : DataTypes.STRING())
                                .build())
                .distributedBy(1)
                .logFormat(LogFormat.ARROW)
                .customProperty(KafkaDataFormat.KEY_FORMAT_CONFIG, format)
                .customProperty(KafkaDataFormat.KEY_FIELDS_CONFIG, "message_key")
                .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, format)
                .customProperty(KafkaDataFormat.VALUE_FIELDS_INCLUDE_CONFIG, "EXCEPT_KEY")
                .customProperty(KafkaDataFormat.TIMESTAMP_COLUMN_CONFIG, "received_at")
                .customProperty(KafkaDataFormat.HEADERS_COLUMN_CONFIG, "attributes")
                .build();
    }

    private static KafkaProducer<byte[], byte[]> producer(String acks) {
        ServerNode node = CLUSTER.getTabletServerNodes("KAFKA").get(0);
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, node.host() + ":" + node.port());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        config.put(ProducerConfig.ACKS_CONFIG, acks);
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30000);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000);
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

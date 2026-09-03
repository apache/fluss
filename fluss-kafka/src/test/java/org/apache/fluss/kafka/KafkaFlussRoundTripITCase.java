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
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies that records written through Kafka can be consumed through the native Fluss client. */
public class KafkaFlussRoundTripITCase {

    private static final String DATABASE = "kafka";
    private static final String TOPIC = "round-trip-topic";
    private static final String STRING_TOPIC = "round-trip-string-topic";
    private static final long TIMESTAMP = 123456789L;
    private static final byte[] KEY = "kafka-key".getBytes(StandardCharsets.UTF_8);
    private static final byte[] VALUE = "kafka-value".getBytes(StandardCharsets.UTF_8);
    private static final String HEADER_KEY = "source";
    private static final byte[] HEADER_VALUE = "kafka".getBytes(StandardCharsets.UTF_8);

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(clusterConfig())
                    .setTabletServerListeners("FLUSS://localhost:0,KAFKA://localhost:0")
                    .build();

    private Connection connection;
    private org.apache.fluss.client.admin.Admin flussAdmin;
    private String kafkaBootstrapServer;

    @BeforeEach
    public void setup() throws Exception {
        connection = ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig());
        flussAdmin = connection.getAdmin();
        flussAdmin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();
        ServerNode kafkaNode = FLUSS_CLUSTER_EXTENSION.getTabletServerNodes("KAFKA").get(0);
        kafkaBootstrapServer = kafkaNode.host() + ":" + kafkaNode.port();
    }

    @AfterEach
    public void teardown() throws Exception {
        if (flussAdmin != null) {
            flussAdmin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void testKafkaWriteCanBeConsumedByFluss() throws Exception {
        testRoundTrip(TOPIC, Collections.emptyMap(), false);
    }

    @Test
    public void testKafkaStringFormatsCanBeConsumedByFluss() throws Exception {
        Map<String, String> configs = new HashMap<>();
        configs.put(KafkaDataFormat.KEY_FORMAT_CONFIG, "string");
        configs.put(KafkaDataFormat.VALUE_FORMAT_CONFIG, "string");
        testRoundTrip(STRING_TOPIC, configs, true);
    }

    private void testRoundTrip(String topic, Map<String, String> topicConfigs, boolean stringFormat)
            throws Exception {
        Map<String, Object> adminConfig = new HashMap<>();
        adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer);
        adminConfig.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60000);
        try (Admin kafkaAdmin = Admin.create(adminConfig)) {
            NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
            newTopic.configs(topicConfigs);
            kafkaAdmin.createTopics(Collections.singleton(newTopic)).all().get();

            writeKafkaRecord(topic);
            assertFlussRecord(topic, stringFormat);
            assertProjectedFlussRecord(topic, stringFormat);

            kafkaAdmin.deleteTopics(Collections.singleton(topic)).all().get();
        }
    }

    private void writeKafkaRecord(String topic) throws Exception {
        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "1");
        RecordHeaders headers =
                new RecordHeaders(
                        Collections.singletonList(new RecordHeader(HEADER_KEY, HEADER_VALUE)));
        ProducerRecord<byte[], byte[]> record =
                new ProducerRecord<>(topic, 0, TIMESTAMP, KEY, VALUE, headers);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerConfig)) {
            producer.send(record).get();
        }
    }

    private void assertFlussRecord(String topic, boolean stringFormat) throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, topic);
        try (Table table = connection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner()) {
            assertThat(table.getTableInfo().getTableConfig().getLogFormat())
                    .isEqualTo(LogFormat.ARROW);
            scanner.subscribeFromBeginning(0);
            for (int attempt = 0; attempt < 30; attempt++) {
                ScanRecords records = scanner.poll(Duration.ofSeconds(1));
                for (ScanRecord record : records) {
                    assertRow(record.getRow(), stringFormat);
                    return;
                }
            }
        }
        throw new AssertionError("Kafka record was not visible through the Fluss LogScanner.");
    }

    private void assertProjectedFlussRecord(String topic, boolean stringFormat) throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, topic);
        try (Table table = connection.getTable(tablePath);
                LogScanner scanner =
                        table.newScan().project(new int[] {0, 1, 2}).createLogScanner()) {
            scanner.subscribeFromBeginning(0);
            for (int attempt = 0; attempt < 30; attempt++) {
                ScanRecords records = scanner.poll(Duration.ofSeconds(1));
                for (ScanRecord record : records) {
                    assertProjectedRow(record.getRow(), stringFormat);
                    return;
                }
            }
        }
        throw new AssertionError(
                "Kafka record was not visible through the projected Fluss LogScanner.");
    }

    private static void assertRow(InternalRow row, boolean stringFormat) {
        if (stringFormat) {
            assertThat(row.getString(0).toString())
                    .isEqualTo(new String(KEY, StandardCharsets.UTF_8));
            assertThat(row.getString(1).toString())
                    .isEqualTo(new String(VALUE, StandardCharsets.UTF_8));
        } else {
            assertThat(row.getBytes(0)).containsExactly(KEY);
            assertThat(row.getBytes(1)).containsExactly(VALUE);
        }
        assertThat(row.getTimestampLtz(2, 3).getEpochMillisecond()).isEqualTo(TIMESTAMP);

        InternalArray headers = row.getArray(3);
        assertThat(headers.size()).isEqualTo(1);
        InternalRow header = headers.getRow(0, 2);
        assertThat(header.getString(0).toString()).isEqualTo(HEADER_KEY);
        assertThat(header.getBytes(1)).containsExactly(HEADER_VALUE);
    }

    private static void assertProjectedRow(InternalRow row, boolean stringFormat) {
        if (stringFormat) {
            assertThat(row.getString(0).toString())
                    .isEqualTo(new String(KEY, StandardCharsets.UTF_8));
            assertThat(row.getString(1).toString())
                    .isEqualTo(new String(VALUE, StandardCharsets.UTF_8));
        } else {
            assertThat(row.getBytes(0)).containsExactly(KEY);
            assertThat(row.getBytes(1)).containsExactly(VALUE);
        }
        assertThat(row.getTimestampLtz(2, 3).getEpochMillisecond()).isEqualTo(TIMESTAMP);
    }

    private static Configuration clusterConfig() {
        Configuration config = new Configuration();
        config.set(ConfigOptions.KAFKA_ENABLED, true);
        config.set(ConfigOptions.KAFKA_DATABASE, DATABASE);
        config.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 1);
        return config;
    }
}

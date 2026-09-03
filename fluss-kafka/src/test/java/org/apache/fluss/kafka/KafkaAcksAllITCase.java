/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import org.apache.fluss.config.cluster.AlterConfig;
import org.apache.fluss.config.cluster.AlterConfigOpType;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Certifies cluster-level minimum ISR semantics through the Kafka Produce path. */
public class KafkaAcksAllITCase {

    private static final String DATABASE = "kafka";
    private static final String REPLICATED_TOPIC = "acks-all-replicated";
    private static final String UNDER_REPLICATED_TOPIC = "acks-all-under-replicated";
    private static final byte[] KEY = "key".getBytes(StandardCharsets.UTF_8);
    private static final AtomicInteger CORRELATION_ID = new AtomicInteger();

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(clusterConfig())
                    .setTabletServerListeners("FLUSS://localhost:0,KAFKA://localhost:0")
                    .build();

    private Connection connection;
    private org.apache.fluss.client.admin.Admin flussAdmin;
    private String bootstrapServers;

    @BeforeEach
    public void setup() throws Exception {
        connection = ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig());
        flussAdmin = connection.getAdmin();
        flussAdmin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();
        bootstrapServers =
                FLUSS_CLUSTER_EXTENSION.getTabletServerNodes("KAFKA").stream()
                        .map(node -> node.host() + ":" + node.port())
                        .collect(Collectors.joining(","));
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
    public void testClusterMinIsrPolicyAndDynamicReload() throws Exception {
        try {
            CreateTopicsResponse createResponse = createTopics();
            assertThat(createResponse.errorCounts()).containsOnlyKeys(Errors.NONE);
            assertThat(createResponse.data().topics().find(REPLICATED_TOPIC).replicationFactor())
                    .isEqualTo((short) 3);
            assertThat(
                            createResponse
                                    .data()
                                    .topics()
                                    .find(UNDER_REPLICATED_TOPIC)
                                    .replicationFactor())
                    .isEqualTo((short) 1);

            RecordMetadata replicated = send(REPLICATED_TOPIC, "replicated-with-min-isr-two");
            assertThat(replicated.offset()).isZero();

            assertThatThrownBy(
                            () -> send(UNDER_REPLICATED_TOPIC, "must-not-append-with-min-isr-two"))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(NotEnoughReplicasException.class)
                    .hasMessageContaining("minimum ISR 2");

            flussAdmin
                    .alterClusterConfigs(
                            Collections.singletonList(
                                    new AlterConfig(
                                            ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER
                                                    .key(),
                                            "1",
                                            AlterConfigOpType.SET)))
                    .get(1, TimeUnit.MINUTES);

            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(FLUSS_CLUSTER_EXTENSION.getTabletServers())
                                    .allSatisfy(
                                            tabletServer ->
                                                    assertThat(
                                                                    tabletServer
                                                                            .getReplicaManager()
                                                                            .getMinInSyncReplicas())
                                                            .isEqualTo(1)));

            assertThat(send(UNDER_REPLICATED_TOPIC, "accepted-after-reconfigure").offset())
                    .isZero();

            assertSingleFlussValue(
                    UNDER_REPLICATED_TOPIC,
                    "accepted-after-reconfigure".getBytes(StandardCharsets.UTF_8));
        } finally {
            dropTablesIgnoringErrors();
            flussAdmin
                    .alterClusterConfigs(
                            Collections.singletonList(
                                    new AlterConfig(
                                            ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER
                                                    .key(),
                                            "2",
                                            AlterConfigOpType.SET)))
                    .get(1, TimeUnit.MINUTES);
        }
    }

    private CreateTopicsResponse createTopics() throws Exception {
        return sendCreateTopicsRequest();
    }

    private CreateTopicsResponse sendCreateTopicsRequest() throws Exception {
        short version = ApiKeys.CREATE_TOPICS.latestVersion();
        List<CreateTopicsRequestData.CreatableTopic> topics =
                Arrays.asList(
                        new CreateTopicsRequestData.CreatableTopic()
                                .setName(REPLICATED_TOPIC)
                                .setNumPartitions(1)
                                .setReplicationFactor((short) 3),
                        new CreateTopicsRequestData.CreatableTopic()
                                .setName(UNDER_REPLICATED_TOPIC)
                                .setNumPartitions(1)
                                .setReplicationFactor((short) 1));
        CreateTopicsRequest request =
                new CreateTopicsRequest.Builder(
                                new CreateTopicsRequestData()
                                        .setTimeoutMs(30000)
                                        .setTopics(
                                                new CreateTopicsRequestData
                                                        .CreatableTopicCollection(
                                                        topics.iterator())))
                        .build(version);
        ServerNode node = FLUSS_CLUSTER_EXTENSION.getTabletServerNodes("KAFKA").get(0);
        RequestHeader header =
                new RequestHeader(
                        ApiKeys.CREATE_TOPICS,
                        version,
                        "acks-all-test",
                        CORRELATION_ID.incrementAndGet());
        return sendRequest(node, header, request);
    }

    private static CreateTopicsResponse sendRequest(
            ServerNode node, RequestHeader header, CreateTopicsRequest request) throws Exception {
        ByteBuffer serialized =
                RequestUtils.serialize(
                        header.data(), header.headerVersion(), request.data(), request.version());
        try (Socket socket = new Socket(node.host(), node.port());
                DataOutputStream output = new DataOutputStream(socket.getOutputStream());
                DataInputStream input = new DataInputStream(socket.getInputStream())) {
            socket.setSoTimeout(10000);
            byte[] requestBytes = new byte[serialized.remaining()];
            serialized.get(requestBytes);
            output.writeInt(requestBytes.length);
            output.write(requestBytes);
            output.flush();

            int responseSize = input.readInt();
            byte[] responseBytes = new byte[responseSize];
            input.readFully(responseBytes);
            return (CreateTopicsResponse)
                    AbstractResponse.parseResponse(ByteBuffer.wrap(responseBytes), header);
        }
    }

    private void dropTablesIgnoringErrors() {
        try {
            flussAdmin.dropTable(TablePath.of(DATABASE, REPLICATED_TOPIC), true).get();
            flussAdmin.dropTable(TablePath.of(DATABASE, UNDER_REPLICATED_TOPIC), true).get();
        } catch (Exception ignored) {
            // Preserve the primary test failure when cleanup cannot complete.
        }
    }

    private RecordMetadata send(String topic, String value) throws Exception {
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerConfig())) {
            return producer.send(
                            new ProducerRecord<>(
                                    topic, KEY, value.getBytes(StandardCharsets.UTF_8)))
                    .get(30, TimeUnit.SECONDS);
        }
    }

    private void assertSingleFlussValue(String topic, byte[] expectedValue) throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, topic);
        try (Table table = connection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(0);
            ScanRecords records = scanner.poll(Duration.ofSeconds(10));
            assertThat(records).hasSize(1);
            ScanRecord record = records.iterator().next();
            assertThat(record.getRow().getBytes(0)).containsExactly(KEY);
            assertThat(record.getRow().getBytes(1)).containsExactly(expectedValue);
        }
    }

    private Map<String, Object> producerConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.RETRIES_CONFIG, 0);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000);
        return config;
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

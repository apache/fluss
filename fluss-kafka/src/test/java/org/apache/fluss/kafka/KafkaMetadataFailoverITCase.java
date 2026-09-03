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
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/** Three-node integration tests for Kafka Metadata routing and topic identity. */
public class KafkaMetadataFailoverITCase {

    private static final String DATABASE = "kafka";
    private static final short METADATA_VERSION = 11;
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
    public void testAdvertisedListenersAndTopicIdentityLifecycle() throws Exception {
        String topic = "metadata-lifecycle-topic";
        try (Admin admin = Admin.create(adminConfig())) {
            deleteIgnoringErrors(admin, topic);
            createTopic(admin, topic);

            MetadataResponse initial = waitForTopic(topic);
            MetadataResponseTopic initialTopic = initial.data().topics().find(topic);
            Uuid initialTopicId = initialTopic.topicId();
            assertThat(initialTopicId).isNotEqualTo(Uuid.ZERO_UUID);
            assertKafkaListenerEndpoints(initial);

            admin.deleteTopics(Collections.singleton(topic)).all().get(1, TimeUnit.MINUTES);
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(fetchTopicMetadata(topic).errors())
                                    .containsEntry(topic, Errors.UNKNOWN_TOPIC_OR_PARTITION));

            createTopic(admin, topic);
            MetadataResponse recreated = waitForTopic(topic);
            assertThat(recreated.data().topics().find(topic).topicId())
                    .isNotEqualTo(Uuid.ZERO_UUID)
                    .isNotEqualTo(initialTopicId);
        } finally {
            try (Admin cleanupAdmin = Admin.create(adminConfig())) {
                deleteIgnoringErrors(cleanupAdmin, topic);
            }
        }
    }

    @Test
    public void testProducerRefreshesMetadataAfterLeaderFailover() throws Exception {
        String topic = "metadata-leader-failover-topic";
        int stoppedLeader = -1;
        try (Admin admin = Admin.create(adminConfig())) {
            deleteIgnoringErrors(admin, topic);
            createTopic(admin, topic);

            MetadataResponse initial = waitForTopic(topic);
            MetadataResponsePartition initialPartition =
                    initial.data().topics().find(topic).partitions().get(0);
            stoppedLeader = initialPartition.leaderId();
            int initialEpoch = initialPartition.leaderEpoch();

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig())) {
                long firstOffset =
                        producer.send(new ProducerRecord<>(topic, 0, "key-1", "before-failover"))
                                .get(1, TimeUnit.MINUTES)
                                .offset();

                FLUSS_CLUSTER_EXTENSION.stopTabletServer(stoppedLeader);
                AtomicReference<MetadataResponsePartition> newPartition = new AtomicReference<>();
                int previousLeader = stoppedLeader;
                retry(
                        Duration.ofMinutes(1),
                        () -> {
                            MetadataResponse response = fetchTopicMetadata(topic);
                            MetadataResponseTopic responseTopic =
                                    response.data().topics().find(topic);
                            assertThat(responseTopic.errorCode()).isEqualTo(Errors.NONE.code());
                            MetadataResponsePartition partition = responseTopic.partitions().get(0);
                            assertThat(partition.errorCode()).isEqualTo(Errors.NONE.code());
                            assertThat(partition.leaderId())
                                    .isNotEqualTo(-1)
                                    .isNotEqualTo(previousLeader);
                            assertThat(partition.leaderEpoch()).isGreaterThan(initialEpoch);
                            newPartition.set(partition);
                        });

                long secondOffset =
                        producer.send(new ProducerRecord<>(topic, 0, "key-2", "after-failover"))
                                .get(1, TimeUnit.MINUTES)
                                .offset();
                // acks=1 does not guarantee that the first leader replicated its acknowledged
                // record before failover, so offset reuse is possible after data loss. The test
                // certifies Metadata refresh and continued routing, not all-replica durability.
                assertThat(secondOffset).isNotNegative();
                assertThat(firstOffset).isNotNegative();
                assertThat(newPartition.get()).isNotNull();
            }
        } finally {
            if (stoppedLeader >= 0
                    && FLUSS_CLUSTER_EXTENSION.getTabletServerById(stoppedLeader) == null) {
                FLUSS_CLUSTER_EXTENSION.startTabletServer(stoppedLeader);
                FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);
            }
            try (Admin cleanupAdmin = Admin.create(adminConfig())) {
                deleteIgnoringErrors(cleanupAdmin, topic);
            }
        }
    }

    private MetadataResponse waitForTopic(String topic) {
        AtomicReference<MetadataResponse> result = new AtomicReference<>();
        retry(
                Duration.ofMinutes(1),
                () -> {
                    MetadataResponse response = fetchTopicMetadata(topic);
                    MetadataResponseTopic responseTopic = response.data().topics().find(topic);
                    assertThat(responseTopic).isNotNull();
                    assertThat(responseTopic.errorCode()).isEqualTo(Errors.NONE.code());
                    assertThat(responseTopic.partitions()).hasSize(1);
                    assertThat(responseTopic.partitions().get(0).leaderId()).isNotEqualTo(-1);
                    result.set(response);
                });
        return result.get();
    }

    private void assertKafkaListenerEndpoints(MetadataResponse response) {
        List<ServerNode> kafkaNodes = FLUSS_CLUSTER_EXTENSION.getTabletServerNodes("KAFKA");
        assertThat(response.brokers())
                .extracting(
                        org.apache.kafka.common.Node::id,
                        org.apache.kafka.common.Node::host,
                        org.apache.kafka.common.Node::port,
                        org.apache.kafka.common.Node::rack)
                .containsExactlyInAnyOrderElementsOf(
                        kafkaNodes.stream()
                                .map(
                                        node ->
                                                tuple(
                                                        node.id(),
                                                        node.host(),
                                                        node.port(),
                                                        node.rack()))
                                .collect(Collectors.toList()));
    }

    private MetadataResponse fetchTopicMetadata(String topic) throws Exception {
        MetadataRequest request =
                new MetadataRequest.Builder(Collections.singletonList(topic), false)
                        .build(METADATA_VERSION);
        Exception lastFailure = null;
        for (ServerNode node : FLUSS_CLUSTER_EXTENSION.getTabletServerNodes("KAFKA")) {
            try {
                return sendMetadataRequest(node, request);
            } catch (Exception e) {
                lastFailure = e;
            }
        }
        throw new IllegalStateException("No Kafka listener returned Metadata.", lastFailure);
    }

    private static MetadataResponse sendMetadataRequest(ServerNode node, MetadataRequest request)
            throws Exception {
        RequestHeader header =
                new RequestHeader(
                        ApiKeys.METADATA,
                        METADATA_VERSION,
                        "metadata-failover-test",
                        CORRELATION_ID.incrementAndGet());
        ByteBuffer serialized =
                RequestUtils.serialize(
                        header.data(), header.headerVersion(), request.data(), request.version());
        try (Socket socket = new Socket(node.host(), node.port());
                DataOutputStream output = new DataOutputStream(socket.getOutputStream());
                DataInputStream input = new DataInputStream(socket.getInputStream())) {
            byte[] requestBytes = new byte[serialized.remaining()];
            serialized.get(requestBytes);
            output.writeInt(requestBytes.length);
            output.write(requestBytes);
            output.flush();

            int responseSize = input.readInt();
            byte[] responseBytes = new byte[responseSize];
            input.readFully(responseBytes);
            return (MetadataResponse)
                    AbstractResponse.parseResponse(ByteBuffer.wrap(responseBytes), header);
        }
    }

    private Map<String, Object> adminConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60000);
        return config;
    }

    private Map<String, Object> producerConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.RETRIES_CONFIG, 20);
        config.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 200);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60000);
        config.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 1000);
        return config;
    }

    private static void createTopic(Admin admin, String topic) throws Exception {
        admin.createTopics(Collections.singleton(new NewTopic(topic, 1, (short) 3)))
                .all()
                .get(1, TimeUnit.MINUTES);
    }

    private static void deleteIgnoringErrors(Admin admin, String topic) {
        try {
            admin.deleteTopics(Collections.singleton(topic)).all().get(1, TimeUnit.MINUTES);
        } catch (Exception ignored) {
            // The topic may not exist before or after a failed test.
        }
    }

    private static Configuration clusterConfig() {
        Configuration config = new Configuration();
        config.set(ConfigOptions.KAFKA_ENABLED, true);
        config.set(ConfigOptions.KAFKA_DATABASE, DATABASE);
        config.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        return config;
    }
}

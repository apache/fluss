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

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.KafkaValueConversionType;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.dropDatabase;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newAlterTableRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newDatabaseExistsRequest;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Verifies qualified Metadata discovery over a real Kafka listener and TabletServer. */
public class KafkaMetadataITCase {

    @RegisterExtension
    public static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(2)
                    .setClusterConf(clusterConfiguration())
                    .setTabletServerListeners("FLUSS://localhost:0,KAFKA://localhost:0")
                    .build();

    @Test
    public void testDeletedDatabaseDoesNotHideBrokersOrOtherTopics() throws Exception {
        TableDescriptor descriptor = topicDescriptor();
        long existing = createTable(CLUSTER, TablePath.of("existing_db", "events"), descriptor);
        long deleted = createTable(CLUSTER, TablePath.of("deleted_db", "events"), descriptor);
        CLUSTER.waitUntilTableReady(existing);
        CLUSTER.waitUntilTableReady(deleted);
        dropDatabase(CLUSTER, "deleted_db");
        ServerNode node = CLUSTER.getTabletServerInfos().get(0).node("KAFKA");
        awaitTopicError(node, "deleted_db.events", Errors.UNKNOWN_TOPIC_OR_PARTITION);

        for (short version = 0; version <= 11; version++) {
            MetadataResponse missing = query(node, named(version, "deleted_db.events"));
            assertThat(missing.brokers()).hasSize(2);
            assertThat(missing.data().topics().find("deleted_db.events").errorCode())
                    .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());

            MetadataResponse mixed =
                    query(node, named(version, "deleted_db.events", "existing_db.events"));
            assertThat(mixed.brokers()).hasSize(2);
            assertThat(mixed.brokers().iterator().next().port()).isEqualTo(node.port());
            assertThat(mixed.data().topics().find("deleted_db.events").errorCode())
                    .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
            assertThat(mixed.data().topics().find("existing_db.events").errorCode())
                    .isEqualTo(Errors.NONE.code());
            assertThat(mixed.data().topics().find("existing_db.events").partitions()).hasSize(2);

            MetadataResponse all = query(node, allTopics(version));
            assertThat(all.brokers()).hasSize(2);
            assertThat(all.data().topics())
                    .extracting(MetadataResponseTopic::name)
                    .containsExactly("existing_db.events");
            assertThat(all.errorCounts()).containsOnlyKeys(Errors.NONE);
        }
    }

    @Test
    public void testAliasesAndOptionalMetadataAcrossDatabases() throws Exception {
        long other = createTable(CLUSTER, TablePath.of("sales", "events"), topicDescriptor());
        CLUSTER.waitUntilTableReady(other);
        ServerNode node = CLUSTER.getTabletServerInfos().get(0).node("KAFKA");
        for (short version = 0; version <= 11; version++) {
            MetadataResponse before = query(node, named(version, "events", "sales.events"));
            assertThat(before.data().topics().find("events").errorCode())
                    .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
            assertThat(before.data().topics().find("sales.events").errorCode())
                    .isEqualTo(Errors.NONE.code());
        }
        // Exercise PR03's optional metadata column through the same alias and discovery paths.
        TableDescriptor withMetadata =
                TableDescriptor.builder(topicDescriptor())
                        .schema(
                                Schema.newBuilder()
                                        .column("_kafka_value", DataTypes.BYTES())
                                        .column(
                                                "_kafka_metadata",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "timestamp",
                                                                DataTypes.TIMESTAMP_LTZ(3)),
                                                        DataTypes.FIELD(
                                                                "timestamp_type", DataTypes.INT()),
                                                        DataTypes.FIELD(
                                                                "partition", DataTypes.INT())))
                                        .build())
                        .build();
        long tableId = createTable(CLUSTER, TablePath.of("fluss", "events"), withMetadata);
        CLUSTER.waitUntilTableReady(tableId);
        for (short version = 0; version <= 11; version++) {
            for (ServerNode broker : CLUSTER.getTabletServerNodes("KAFKA")) {
                // Missing tables must not hide aliases or valid topics in the same response.
                MetadataResponse response =
                        query(
                                broker,
                                named(
                                        version,
                                        "events",
                                        "fluss.events",
                                        "sales.events",
                                        "fluss.missing",
                                        "missing_db.events"));
                assertThat(response.brokers()).hasSize(2);
                assertThat(response.brokers()).extracting(Node::port).contains(broker.port());
                for (String name : Arrays.asList("fluss.events", "sales.events")) {
                    MetadataResponseTopic topic = response.data().topics().find(name);
                    assertThat(topic.errorCode()).isEqualTo(Errors.NONE.code());
                    assertThat(topic.partitions()).hasSize(2);
                }
                assertThat(response.data().topics().find("sales.events").topicId())
                        .isEqualTo(
                                version >= 10
                                        ? new Uuid(0x466c757373000000L, other)
                                        : Uuid.ZERO_UUID);
                MetadataResponseTopic alias = response.data().topics().find("events");
                MetadataResponseTopic canonical = response.data().topics().find("fluss.events");
                assertThat(alias.errorCode()).isEqualTo(Errors.NONE.code());
                assertThat(alias.partitions()).isEqualTo(canonical.partitions()).hasSize(2);
                assertThat(alias.partitions())
                        .allSatisfy(
                                partition -> {
                                    assertThat(partition.leaderId()).isIn(0, 1);
                                    assertThat(partition.replicaNodes())
                                            .containsExactlyInAnyOrder(0, 1);
                                    assertThat(partition.isrNodes())
                                            .containsExactlyInAnyOrder(0, 1);
                                });
                assertThat(alias.topicId())
                        .isEqualTo(
                                version >= 10
                                        ? new Uuid(0x466c757373410000L, tableId)
                                        : Uuid.ZERO_UUID);
                if (version >= 10) {
                    assertThat(canonical.topicId())
                            .isEqualTo(new Uuid(0x466c757373000000L, tableId))
                            .isNotEqualTo(alias.topicId())
                            .isNotEqualTo(response.data().topics().find("sales.events").topicId());
                }
                for (String missing : Arrays.asList("fluss.missing", "missing_db.events")) {
                    assertThat(response.data().topics().find(missing).errorCode())
                            .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
                }
                MetadataResponse all = query(broker, allTopics(version));
                assertThat(all.data().topics())
                        .extracting(MetadataResponseTopic::name)
                        .containsExactly("fluss.events", "sales.events");
                assertThat(all.errorCounts()).containsOnlyKeys(Errors.NONE);
            }
        }
    }

    @Test
    public void testPersistedEnablementChangesAreVisibleThroughBothNames() throws Exception {
        TablePath path = TablePath.of("fluss", "events");
        TableDescriptor disabled =
                TableDescriptor.builder(topicDescriptor())
                        .property(ConfigOptions.TABLE_KAFKA_ENABLED, false)
                        .build();
        TableDescriptor unset =
                TableDescriptor.builder()
                        .schema(topicDescriptor().getSchema())
                        .distributedBy(2)
                        .property(
                                ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE,
                                KafkaValueConversionType.BINARY)
                        .build();
        long tableId = createTable(CLUSTER, path, disabled);
        long unsetId = createTable(CLUSTER, TablePath.of("sales", "unset"), unset);
        CLUSTER.waitUntilTableReady(tableId);
        CLUSTER.waitUntilTableReady(unsetId);
        ServerNode node = CLUSTER.getTabletServerInfos().get(0).node("KAFKA");
        assertExposure(node, false, tableId);
        setEnabled(CLUSTER, path, true);
        assertExposure(node, true, tableId);
        setEnabled(CLUSTER, path, false);
        assertExposure(node, false, tableId);
        setEnabled(CLUSTER, path, true);
        assertExposure(node, true, tableId);
        CLUSTER.newCoordinatorClient()
                .alterTable(
                        newAlterTableRequest(
                                path,
                                Collections.emptyMap(),
                                Collections.singletonList(ConfigOptions.TABLE_KAFKA_ENABLED.key()),
                                Collections.emptyList(),
                                false))
                .get();
        assertExposure(node, false, tableId);
    }

    @Test
    public void testCustomDefaultDatabaseAliasAndNoRecreationByMetadata() throws Exception {
        Configuration config = clusterConfiguration();
        config.set(ConfigOptions.KAFKA_DEFAULT_DATABASE, "custom_default");
        config.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 1);
        FlussClusterExtension cluster =
                FlussClusterExtension.builder()
                        .setNumOfTabletServers(1)
                        .setClusterConf(config)
                        .setTabletServerListeners("FLUSS://localhost:0,KAFKA://localhost:0")
                        .build();
        cluster.start();
        try {
            long nativeDefaultId =
                    createTable(cluster, TablePath.of("fluss", "events"), topicDescriptor());
            cluster.waitUntilTableReady(nativeDefaultId);
            ServerNode node = cluster.getTabletServerInfos().get(0).node("KAFKA");
            // PR03 creates the configured database at startup, before any Metadata request.
            assertThat(
                            cluster.newCoordinatorClient()
                                    .databaseExists(newDatabaseExistsRequest("custom_default"))
                                    .get()
                                    .isExists())
                    .isTrue();
            // Replica readiness does not imply that the Kafka metadata cache has caught up.
            awaitTopicsReady(node, "fluss.events");
            for (short version = 0; version <= 11; version++) {
                MetadataResponse response = query(node, named(version, "events", "fluss.events"));
                assertThat(response.data().topics().find("events").errorCode())
                        .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
                assertThat(response.data().topics().find("fluss.events").errorCode())
                        .isEqualTo(Errors.NONE.code());
            }
            long customId =
                    createTable(
                            cluster, TablePath.of("custom_default", "events"), topicDescriptor());
            cluster.waitUntilTableReady(customId);
            awaitTopicsReady(node, "events", "custom_default.events", "fluss.events");
            for (short version = 0; version <= 11; version++) {
                MetadataResponse response =
                        query(
                                node,
                                named(version, "events", "custom_default.events", "fluss.events"));
                assertThat(response.errorCounts()).containsOnlyKeys(Errors.NONE);
                MetadataResponseTopic alias = response.data().topics().find("events");
                assertThat(alias.partitions())
                        .isEqualTo(
                                response.data()
                                        .topics()
                                        .find("custom_default.events")
                                        .partitions());
                assertThat(alias.topicId())
                        .isEqualTo(
                                version >= 10
                                        ? new Uuid(0x466c757373410000L, customId)
                                        : Uuid.ZERO_UUID);
                if (version >= 10) {
                    assertThat(response.data().topics().find("custom_default.events").topicId())
                            .isEqualTo(new Uuid(0x466c757373000000L, customId))
                            .isNotEqualTo(alias.topicId())
                            .isNotEqualTo(response.data().topics().find("fluss.events").topicId());
                }
                assertThat(query(node, allTopics(version)).data().topics())
                        .extracting(MetadataResponseTopic::name)
                        .containsExactly("custom_default.events", "fluss.events");
            }
            dropDatabase(cluster, "custom_default");
            awaitTopicError(node, "events", Errors.UNKNOWN_TOPIC_OR_PARTITION);
            MetadataResponse deleted = query(node, named((short) 11, "events", "fluss.events"));
            assertThat(deleted.data().topics().find("events").errorCode())
                    .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
            assertThat(deleted.data().topics().find("fluss.events").errorCode())
                    .isEqualTo(Errors.NONE.code());
            assertThat(
                            cluster.newCoordinatorClient()
                                    .databaseExists(newDatabaseExistsRequest("custom_default"))
                                    .get()
                                    .isExists())
                    .isFalse();
        } finally {
            cluster.close();
        }
    }

    @Test
    public void testMetadataCacheIsRestoredAfterServerRestarts() throws Exception {
        FlussClusterExtension cluster =
                FlussClusterExtension.builder()
                        .setNumOfTabletServers(2)
                        .setClusterConf(clusterConfiguration())
                        .setTabletServerListeners("FLUSS://localhost:0,KAFKA://localhost:0")
                        .build();
        cluster.start();
        try {
            long tableId = createTable(cluster, TablePath.of("fluss", "events"), topicDescriptor());
            cluster.waitUntilTableReady(tableId);
            cluster.startTabletServer(2);
            ServerNode joined =
                    cluster.getTabletServerInfos().stream()
                            .filter(info -> info.id() == 2)
                            .findFirst()
                            .get()
                            .node("KAFKA");
            awaitTopicError(joined, "events", Errors.NONE);
            assertThat(
                            query(joined, allTopics((short) 11))
                                    .data()
                                    .topics()
                                    .find("fluss.events")
                                    .topicId())
                    .isEqualTo(new Uuid(0x466c757373000000L, tableId));
            cluster.restartTabletServer(0, new Configuration());
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        ServerNode node = cluster.getTabletServerInfos().get(0).node("KAFKA");
                        MetadataResponse all = query(node, allTopics((short) 11));
                        assertThat(all.data().topics().find("fluss.events")).isNotNull();
                        assertThat(all.data().topics().find("fluss.events").topicId())
                                .isEqualTo(new Uuid(0x466c757373000000L, tableId));
                        assertThat(all.data().topics().find("fluss.events").partitions())
                                .hasSize(2);
                    });
            cluster.stopCoordinatorServer();
            cluster.getTabletServers()
                    .forEach(server -> server.getMetadataCache().clearTableMetadata());
            cluster.startCoordinatorServer();
            cluster.waitUntilTableReady(tableId);
            for (ServerNode node : cluster.getTabletServerNodes("KAFKA")) {
                awaitTopicError(node, "events", Errors.NONE);
                assertThat(
                                query(node, allTopics((short) 11))
                                        .data()
                                        .topics()
                                        .find("fluss.events")
                                        .topicId())
                        .isEqualTo(new Uuid(0x466c757373000000L, tableId));
            }
        } finally {
            cluster.close();
        }
    }

    private static void awaitTopicsReady(ServerNode node, String... topics) throws Exception {
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(query(node, named((short) 11, topics)).errorCounts())
                                .containsOnlyKeys(Errors.NONE));
    }

    private static void awaitTopicError(ServerNode node, String topic, Errors error)
            throws Exception {
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(
                                        query(node, named((short) 11, topic))
                                                .data()
                                                .topics()
                                                .find(topic)
                                                .errorCode())
                                .isEqualTo(error.code()));
    }

    private static void assertExposure(ServerNode node, boolean enabled, long tableId)
            throws Exception {
        short expected = enabled ? Errors.NONE.code() : Errors.UNKNOWN_TOPIC_OR_PARTITION.code();
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(
                                        query(node, named((short) 11, "events"))
                                                .data()
                                                .topics()
                                                .find("events")
                                                .errorCode())
                                .isEqualTo(expected));
        for (short version = 0; version <= 11; version++) {
            MetadataResponse response =
                    query(node, named(version, "events", "fluss.events", "sales.unset"));
            for (String name : Arrays.asList("events", "fluss.events")) {
                MetadataResponseTopic topic = response.data().topics().find(name);
                assertThat(topic.errorCode()).isEqualTo(expected);
                assertThat(topic.topicId())
                        .isEqualTo(
                                enabled && version >= 10
                                        ? new Uuid(
                                                name.indexOf('.') >= 0
                                                        ? 0x466c757373000000L
                                                        : 0x466c757373410000L,
                                                tableId)
                                        : Uuid.ZERO_UUID);
            }
            assertThat(response.data().topics().find("sales.unset").errorCode())
                    .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
            if (enabled) {
                assertThat(query(node, allTopics(version)).data().topics())
                        .extracting(MetadataResponseTopic::name)
                        .containsExactly("fluss.events");
            } else {
                assertThat(query(node, allTopics(version)).data().topics()).isEmpty();
            }
        }
    }

    private static void setEnabled(FlussClusterExtension cluster, TablePath path, boolean enabled)
            throws Exception {
        cluster.newCoordinatorClient()
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

    private static MetadataRequest named(short version, String... names) {
        return new MetadataRequest(
                new MetadataRequestData()
                        .setTopics(
                                MetadataRequest.convertToMetadataRequestTopic(
                                        Arrays.asList(names))),
                version);
    }

    private static MetadataRequest allTopics(short version) {
        return new MetadataRequest(
                new MetadataRequestData().setTopics(version == 0 ? Collections.emptyList() : null),
                version);
    }

    private static TableDescriptor topicDescriptor() {
        return TableDescriptor.builder()
                .schema(Schema.newBuilder().column("_kafka_value", DataTypes.BYTES()).build())
                .distributedBy(2)
                .logFormat(LogFormat.ARROW)
                .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                .property(
                        ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE,
                        KafkaValueConversionType.BINARY)
                .build();
    }

    private static Configuration clusterConfiguration() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 2);
        conf.set(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS, 3);
        return conf;
    }

    private static MetadataResponse query(ServerNode node, MetadataRequest request)
            throws Exception {
        RequestHeader header =
                new RequestHeader(ApiKeys.METADATA, request.version(), "metadata-test", 1);
        ByteBuffer encoded = request.serializeWithHeader(header);
        byte[] requestBytes = new byte[encoded.remaining()];
        encoded.get(requestBytes);
        try (Socket socket = new Socket(node.host(), node.port())) {
            socket.setSoTimeout(10000);
            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            output.writeInt(requestBytes.length);
            output.write(requestBytes);
            output.flush();
            DataInputStream input = new DataInputStream(socket.getInputStream());
            byte[] responseBytes = new byte[input.readInt()];
            input.readFully(responseBytes);
            return (MetadataResponse)
                    AbstractResponse.parseResponse(ByteBuffer.wrap(responseBytes), header);
        }
    }
}

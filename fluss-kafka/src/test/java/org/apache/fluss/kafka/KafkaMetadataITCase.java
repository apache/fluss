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
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

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
import java.util.Arrays;
import java.util.Collections;

import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.dropDatabase;
import static org.assertj.core.api.Assertions.assertThat;

/** Verifies qualified Metadata discovery over a real Kafka listener and TabletServer. */
public class KafkaMetadataITCase {

    @RegisterExtension
    public static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(clusterConfiguration())
                    .setTabletServerListeners("FLUSS://localhost:0,KAFKA://localhost:0")
                    .build();

    @Test
    public void testQualifiedMetadataAcrossDatabases() throws Exception {
        TableDescriptor descriptor = topicDescriptor();
        long first = createTable(CLUSTER, TablePath.of("first_db", "events"), descriptor);
        long second = createTable(CLUSTER, TablePath.of("second_db", "events"), descriptor);
        CLUSTER.waitUntilTableReady(first);
        CLUSTER.waitUntilTableReady(second);
        ServerNode node = CLUSTER.getTabletServerInfos().get(0).node("KAFKA");

        for (short version = 0; version <= 11; version++) {
            MetadataResponse named =
                    query(
                            node,
                            new MetadataRequest(
                                    new MetadataRequestData()
                                            .setTopics(
                                                    MetadataRequest.convertToMetadataRequestTopic(
                                                            Arrays.asList(
                                                                    "first_db.events",
                                                                    "second_db.events",
                                                                    "first_db.missing"))),
                                    version));
            assertThat(named.data().topics().find("first_db.events").partitions()).hasSize(2);
            assertThat(named.data().topics().find("second_db.events").partitions()).hasSize(2);
            assertThat(named.data().topics().find("first_db.missing").errorCode())
                    .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
            assertThat(named.data().topics().find("first_db.events").topicId())
                    .isEqualTo(
                            version >= 10 ? new Uuid(0x466c757373000000L, first) : Uuid.ZERO_UUID);
            assertThat(named.data().topics().find("second_db.events").topicId())
                    .isEqualTo(
                            version >= 10 ? new Uuid(0x466c757373000000L, second) : Uuid.ZERO_UUID);
            assertThat(named.brokers()).hasSize(1);
            assertThat(named.brokers().iterator().next().port()).isEqualTo(node.port());

            MetadataRequestData allTopics = new MetadataRequestData();
            allTopics.setTopics(version == 0 ? Collections.emptyList() : null);
            MetadataResponse all = query(node, new MetadataRequest(allTopics, version));
            assertThat(all.data().topics())
                    .extracting(MetadataResponseTopic::name)
                    .containsExactly("first_db.events", "second_db.events");
            assertThat(all.errorCounts()).containsOnlyKeys(Errors.NONE);
        }
    }

    @Test
    public void testDeletedDatabaseDoesNotHideBrokersOrOtherTopics() throws Exception {
        TableDescriptor descriptor = topicDescriptor();
        long existing = createTable(CLUSTER, TablePath.of("existing_db", "events"), descriptor);
        long deleted = createTable(CLUSTER, TablePath.of("deleted_db", "events"), descriptor);
        CLUSTER.waitUntilTableReady(existing);
        CLUSTER.waitUntilTableReady(deleted);
        dropDatabase(CLUSTER, "deleted_db");
        ServerNode node = CLUSTER.getTabletServerInfos().get(0).node("KAFKA");

        for (short version = 0; version <= 11; version++) {
            MetadataResponse missing =
                    query(
                            node,
                            new MetadataRequest(
                                    new MetadataRequestData()
                                            .setTopics(
                                                    MetadataRequest.convertToMetadataRequestTopic(
                                                            Collections.singletonList(
                                                                    "deleted_db.events"))),
                                    version));
            assertThat(missing.brokers()).hasSize(1);
            assertThat(missing.data().topics().find("deleted_db.events").errorCode())
                    .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());

            MetadataResponse mixed =
                    query(
                            node,
                            new MetadataRequest(
                                    new MetadataRequestData()
                                            .setTopics(
                                                    MetadataRequest.convertToMetadataRequestTopic(
                                                            Arrays.asList(
                                                                    "deleted_db.events",
                                                                    "existing_db.events"))),
                                    version));
            assertThat(mixed.brokers()).hasSize(1);
            assertThat(mixed.brokers().iterator().next().port()).isEqualTo(node.port());
            assertThat(mixed.data().topics().find("deleted_db.events").errorCode())
                    .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
            assertThat(mixed.data().topics().find("existing_db.events").errorCode())
                    .isEqualTo(Errors.NONE.code());
            assertThat(mixed.data().topics().find("existing_db.events").partitions()).hasSize(2);

            MetadataRequestData allTopics = new MetadataRequestData();
            allTopics.setTopics(version == 0 ? Collections.emptyList() : null);
            MetadataResponse all = query(node, new MetadataRequest(allTopics, version));
            assertThat(all.brokers()).hasSize(1);
            assertThat(all.data().topics())
                    .extracting(MetadataResponseTopic::name)
                    .containsExactly("existing_db.events");
            assertThat(all.errorCounts()).containsOnlyKeys(Errors.NONE);
        }
    }

    private static TableDescriptor topicDescriptor() {
        return TableDescriptor.builder()
                .schema(Schema.newBuilder().column("body", DataTypes.BYTES()).build())
                .distributedBy(2)
                .logFormat(LogFormat.ARROW)
                .customProperty("kafka.value.format", "raw")
                .build();
    }

    private static Configuration clusterConfiguration() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
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

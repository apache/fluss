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

import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.messages.ListTablesRequest;
import org.apache.fluss.rpc.messages.ListTablesResponse;
import org.apache.fluss.rpc.messages.PbBucketMetadata;
import org.apache.fluss.rpc.messages.PbServerNode;
import org.apache.fluss.rpc.messages.PbTableMetadata;
import org.apache.fluss.rpc.messages.PbTablePath;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.fluss.types.DataTypes;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Protocol compatibility tests for the Kafka Metadata API. */
public class KafkaMetadataHandlerTest {

    private static final Uuid TOPIC_ID = new Uuid(0x466c757373000000L, 123L);

    @Test
    public void testNamedTopicForEverySupportedVersion() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        for (short version = ApiKeys.METADATA.oldestVersion(); version <= 11; version++) {
            MetadataRequest request =
                    new MetadataRequest(
                            new MetadataRequestData()
                                    .setTopics(
                                            MetadataRequest.convertToMetadataRequestTopic(
                                                    Collections.singletonList("topic"))),
                            version);
            if (version >= 9) {
                request.data()
                        .unknownTaggedFields()
                        .add(new RawTaggedField(100, new byte[] {1, 2, 3}));
                request.data()
                        .topics()
                        .get(0)
                        .unknownTaggedFields()
                        .add(new RawTaggedField(101, new byte[] {4, 5, 6}));
            }
            MetadataResponse response = handle(service, request, version);

            assertThat(response.brokers()).hasSize(2);
            assertThat(response.controller()).isNull();
            Node broker = response.brokers().iterator().next();
            assertThat(broker.host()).isEqualTo("broker-1");
            assertThat(broker.port()).isEqualTo(9092);
            assertThat(broker.rack()).isEqualTo(version >= 1 ? "rack-a" : null);
            MetadataResponseTopic topic = response.data().topics().find("topic");
            assertThat(topic.errorCode()).isEqualTo(Errors.NONE.code());
            assertThat(topic.partitions()).hasSize(2);
            assertThat(topic.topicId()).isEqualTo(version >= 10 ? TOPIC_ID : Uuid.ZERO_UUID);
            MetadataResponsePartition partition = topic.partitions().get(0);
            assertThat(partition.partitionIndex()).isZero();
            assertThat(partition.leaderId()).isEqualTo(1);
            assertThat(partition.replicaNodes()).containsExactly(1, 2);
            assertThat(partition.isrNodes()).containsExactly(1, 2);
            assertThat(partition.offlineReplicas()).isEmpty();
        }
        assertThat(service.lastListenerName).isEqualTo("KAFKA");
    }

    @Test
    public void testAllTopicsForEverySupportedVersion() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        for (short version = ApiKeys.METADATA.oldestVersion(); version <= 11; version++) {
            MetadataRequest request = allTopicsRequest(version);
            if (version >= 9) {
                request.data()
                        .unknownTaggedFields()
                        .add(new RawTaggedField(102, new byte[] {7, 8, 9}));
            }

            MetadataResponse response = handle(service, request, version);

            assertThat(response.data().topics())
                    .extracting(MetadataResponseTopic::name)
                    .containsExactly("other", "topic");
        }
    }

    @Test
    public void testAllTopicsAndMissingAndInvalidTopic() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();

        MetadataResponse allTopics =
                handle(service, MetadataRequest.Builder.allTopics().build((short) 9), (short) 9);
        assertThat(allTopics.data().topics())
                .extracting(MetadataResponseTopic::name)
                .containsExactlyInAnyOrder("other", "topic");

        MetadataRequest requestedTopics =
                new MetadataRequest.Builder(Arrays.asList("missing", "invalid topic"), false)
                        .build((short) 9);
        MetadataResponse errors = handle(service, requestedTopics, (short) 9);
        assertThat(errors.errors())
                .containsEntry("missing", Errors.UNKNOWN_TOPIC_OR_PARTITION)
                .containsEntry("invalid topic", Errors.INVALID_TOPIC_EXCEPTION);
    }

    @Test
    public void testV10AndV11IgnoreRequestTopicIdAndLookupByName() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        for (short version = 10; version <= 11; version++) {
            MetadataRequest request =
                    new MetadataRequest(
                            new MetadataRequestData()
                                    .setTopics(
                                            Collections.singletonList(
                                                    new MetadataRequestTopic()
                                                            .setName("topic")
                                                            .setTopicId(
                                                                    new Uuid(
                                                                            0x466c757373000000L,
                                                                            999L)))),
                            version);

            MetadataResponse response = handle(service, request, version);

            assertThat(response.errorCounts()).containsOnlyKeys(Errors.NONE);
            assertThat(response.data().topics().find("topic").topicId()).isEqualTo(TOPIC_ID);
        }
    }

    @Test
    public void testV10AndV11RejectTopicIdOnlyLookup() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        Uuid requestedTopicId = new Uuid(0x466c757373000000L, 999L);
        for (short version = 10; version <= 11; version++) {
            MetadataRequest request =
                    new MetadataRequest(
                            new MetadataRequestData()
                                    .setTopics(
                                            Collections.singletonList(
                                                    new MetadataRequestTopic()
                                                            .setName(null)
                                                            .setTopicId(requestedTopicId))),
                            version);

            MetadataResponse response = handle(service, request, version);

            assertThat(response.data().topics()).hasSize(1);
            MetadataResponseTopic responseTopic = response.data().topics().iterator().next();
            assertThat(responseTopic.name()).isEmpty();
            assertThat(responseTopic.topicId()).isEqualTo(requestedTopicId);
            assertThat(responseTopic.errorCode()).isEqualTo(Errors.INVALID_REQUEST.code());
        }
    }

    @Test
    public void testTopicIdentityAcrossDeleteAndRecreate() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();

        MetadataResponse initial = handle(service, namedTopicRequest("topic"), (short) 11);
        assertThat(initial.data().topics().find("topic").topicId()).isEqualTo(TOPIC_ID);

        service.removeTable("topic");
        MetadataResponse deleted = handle(service, namedTopicRequest("topic"), (short) 11);
        assertThat(deleted.errorCounts())
                .containsExactlyEntriesOf(
                        Collections.singletonMap(Errors.UNKNOWN_TOPIC_OR_PARTITION, 1));

        service.putTable("topic", 223L);
        Uuid recreatedTopicId = new Uuid(0x466c757373000000L, 223L);
        MetadataResponse recreatedByName =
                handle(
                        service,
                        new MetadataRequest.Builder(Collections.singletonList("topic"), false)
                                .build((short) 11),
                        (short) 11);
        assertThat(recreatedByName.data().topics().find("topic").topicId())
                .isEqualTo(recreatedTopicId)
                .isNotEqualTo(TOPIC_ID);
    }

    @Test
    public void testDeleteRaceBecomesUnknownTopicResult() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        service.removeTable("topic");
        service.failNextMetadataAsMissing = true;

        MetadataResponse response = handle(service, namedTopicRequest("topic"), (short) 11);

        assertThat(response.errorCounts())
                .containsExactlyEntriesOf(
                        Collections.singletonMap(Errors.UNKNOWN_TOPIC_OR_PARTITION, 1));
    }

    @Test
    public void testUnavailableLeaderUsesPartitionError() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        service.topicLeaderAvailable = false;
        MetadataRequest request =
                new MetadataRequest.Builder(Collections.singletonList("topic"), false)
                        .build((short) 11);

        MetadataResponse response = handle(service, request, (short) 11);

        MetadataResponseTopic topic = response.data().topics().find("topic");
        assertThat(topic.errorCode()).isEqualTo(Errors.NONE.code());
        MetadataResponsePartition partition = topic.partitions().get(0);
        assertThat(partition.errorCode()).isEqualTo(Errors.LEADER_NOT_AVAILABLE.code());
        assertThat(partition.leaderId()).isEqualTo(-1);
        assertThat(partition.replicaNodes()).containsExactly(1, 2, 3);
        assertThat(partition.isrNodes()).containsExactly(1, 2);
        assertThat(partition.offlineReplicas()).containsExactly(3);
    }

    @Test
    public void testAliveReplicaOutsideIsrIsNotReportedAsInSync() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        service.topicIsr = new int[] {1};
        MetadataRequest request =
                new MetadataRequest.Builder(Collections.singletonList("topic"), false)
                        .build((short) 11);

        MetadataResponse response = handle(service, request, (short) 11);

        MetadataResponsePartition partition =
                response.data().topics().find("topic").partitions().get(0);
        assertThat(partition.replicaNodes()).containsExactly(1, 2);
        assertThat(partition.isrNodes()).containsExactly(1);
        assertThat(partition.offlineReplicas()).isEmpty();
    }

    @Test
    public void testLegacyMetadataUsesLeaderOnlyIsr() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        service.topicBucketEpoch = null;

        MetadataResponse response = handle(service, namedTopicRequest("topic"), (short) 11);

        MetadataResponsePartition partition =
                response.data().topics().find("topic").partitions().get(0);
        assertThat(partition.leaderId()).isEqualTo(1);
        assertThat(partition.replicaNodes()).containsExactly(1, 2);
        assertThat(partition.isrNodes()).containsExactly(1);
        assertThat(partition.offlineReplicas()).isEmpty();
    }

    @Test
    public void testLegacyMetadataWithoutAvailableLeaderHasEmptyIsr() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        service.topicBucketEpoch = null;
        service.topicLeaderAvailable = false;

        MetadataResponse response = handle(service, namedTopicRequest("topic"), (short) 11);

        MetadataResponsePartition partition =
                response.data().topics().find("topic").partitions().get(0);
        assertThat(partition.errorCode()).isEqualTo(Errors.LEADER_NOT_AVAILABLE.code());
        assertThat(partition.isrNodes()).isEmpty();
    }

    @Test
    public void testAuthoritativeEmptyIsrDoesNotUseLegacyFallback() {
        for (int bucketEpoch : new int[] {7, -1}) {
            TestingMetadataGatewayService service = new TestingMetadataGatewayService();
            service.topicBucketEpoch = bucketEpoch;
            service.topicIsr = new int[0];

            MetadataResponse response = handle(service, namedTopicRequest("topic"), (short) 11);

            MetadataResponsePartition partition =
                    response.data().topics().find("topic").partitions().get(0);
            assertThat(partition.isrNodes()).isEmpty();
        }
    }

    @Test
    public void testUnexpectedGatewayFailureUsesRequestErrorResponse() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        service.failMetadata = true;
        MetadataRequest request =
                new MetadataRequest.Builder(Collections.singletonList("topic"), false)
                        .build((short) 11);

        MetadataResponse response = handle(service, request, (short) 11);

        assertThat(response.errorCounts())
                .containsExactlyEntriesOf(Collections.singletonMap(Errors.UNKNOWN_SERVER_ERROR, 1));
        assertThat(response.brokers()).isEmpty();
    }

    @Test
    public void testMetadataUsesDdlContractForEveryVersion() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        service.putTable(
                "no_mapping",
                125L,
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("body", DataTypes.BYTES()).build())
                        .distributedBy(2)
                        .build());
        service.putTable(
                "bad_format",
                126L,
                TableDescriptor.builder(defaultDescriptor())
                        .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "json")
                        .build());
        service.putTable(
                "primary_key",
                127L,
                TableDescriptor.builder(defaultDescriptor())
                        .schema(
                                Schema.newBuilder()
                                        .column("body", DataTypes.BYTES().copy(false))
                                        .primaryKey("body")
                                        .build())
                        .build());
        service.putTable(
                "partitioned",
                128L,
                TableDescriptor.builder(defaultDescriptor()).partitionedBy("body").build());
        service.putTable(
                "indexed",
                129L,
                TableDescriptor.builder(defaultDescriptor()).logFormat(LogFormat.INDEXED).build());
        service.putTable("invalid topic", 130L);
        List<String> invalidMappings =
                Arrays.asList("no_mapping", "bad_format", "primary_key", "partitioned", "indexed");
        for (short version = 0; version <= 11; version++) {
            MetadataResponse all = handle(service, allTopicsRequest(version), version);
            assertThat(all.data().topics())
                    .extracting(MetadataResponseTopic::name)
                    .containsExactly("other", "topic");
            List<String> requested = new ArrayList<>(invalidMappings);
            requested.add("topic");
            requested.add("missing");
            MetadataResponse named =
                    handle(
                            service,
                            new MetadataRequest(
                                    new MetadataRequestData()
                                            .setTopics(
                                                    MetadataRequest.convertToMetadataRequestTopic(
                                                            requested)),
                                    version),
                            version);
            for (String name : invalidMappings) {
                MetadataResponseTopic topic = named.data().topics().find(name);
                assertThat(topic.errorCode()).isEqualTo(Errors.INVALID_TOPIC_EXCEPTION.code());
                assertThat(topic.partitions()).isEmpty();
            }
            assertThat(named.data().topics().find("topic").errorCode())
                    .isEqualTo(Errors.NONE.code());
            assertThat(named.data().topics().find("missing").errorCode())
                    .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
        }
    }

    @Test
    public void testMetadataReflectsMappingChangesWithoutChangingTopicIdentity() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        service.putTable(
                "topic",
                123L,
                TableDescriptor.builder(defaultDescriptor())
                        .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "string")
                        .build());
        MetadataResponse invalid = handle(service, namedTopicRequest("topic"), (short) 11);
        assertThat(invalid.data().topics().find("topic").errorCode())
                .isEqualTo(Errors.INVALID_TOPIC_EXCEPTION.code());
        service.putTable("topic", 123L);
        MetadataResponse valid = handle(service, namedTopicRequest("topic"), (short) 11);
        assertThat(valid.data().topics().find("topic").errorCode()).isEqualTo(Errors.NONE.code());
        assertThat(valid.data().topics().find("topic").topicId()).isEqualTo(TOPIC_ID);
    }

    private static TableDescriptor defaultDescriptor() {
        return TableDescriptor.builder()
                .schema(Schema.newBuilder().column("body", DataTypes.BYTES()).build())
                .distributedBy(2)
                .logFormat(LogFormat.ARROW)
                .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "raw")
                .build();
    }

    private static MetadataRequest namedTopicRequest(String topicName) {
        return new MetadataRequest(
                new MetadataRequestData()
                        .setTopics(
                                Collections.singletonList(
                                        new MetadataRequestTopic()
                                                .setName(topicName)
                                                .setTopicId(Uuid.ZERO_UUID))),
                (short) 11);
    }

    private static MetadataRequest allTopicsRequest(short version) {
        MetadataRequestData data = new MetadataRequestData();
        data.setTopics(version == 0 ? Collections.emptyList() : null);
        return new MetadataRequest(data, version);
    }

    private static MetadataResponse handle(
            TestingMetadataGatewayService service, MetadataRequest requestBody, short version) {
        KafkaRequestHandler handler = new KafkaRequestHandler(service, service, "kafka");
        ByteBuf requestBuffer = ByteBufAllocator.DEFAULT.buffer();
        KafkaRequest request;
        try {
            request =
                    new KafkaRequest(
                            ApiKeys.METADATA,
                            version,
                            new RequestHeader(ApiKeys.METADATA, version, "client-id", 1),
                            requestBody,
                            "KAFKA",
                            requestBuffer,
                            new TestingChannelHandlerContext(),
                            new CompletableFuture<>());
        } finally {
            // Mirror KafkaCommandDecoder's ownership transfer to KafkaRequest.
            requestBuffer.release();
        }
        handler.processRequest(request);
        ByteBuf responseBuffer = request.responseBuffer();
        try {
            return (MetadataResponse)
                    AbstractResponse.parseResponse(responseBuffer.nioBuffer(), request.header());
        } finally {
            responseBuffer.release();
            assertThat(requestBuffer.refCnt()).isZero();
        }
    }

    private static final class TestingMetadataGatewayService extends TestingTabletGatewayService {

        private final Map<String, Long> tables = new LinkedHashMap<>();
        private final Map<String, TableDescriptor> descriptors = new LinkedHashMap<>();
        private String lastListenerName;
        private boolean topicLeaderAvailable = true;
        private int[] topicIsr = new int[] {1, 2};
        private Integer topicBucketEpoch = 7;
        private boolean failMetadata;
        private boolean failNextMetadataAsMissing;

        private TestingMetadataGatewayService() {
            putTable("topic", 123L);
            putTable("other", 124L);
        }

        @Override
        public CompletableFuture<ListTablesResponse> listTables(ListTablesRequest request) {
            assertThat(request.getDatabaseName()).isEqualTo("kafka");
            return CompletableFuture.completedFuture(
                    new ListTablesResponse().addAllTableNames(new ArrayList<>(tables.keySet())));
        }

        @Override
        public CompletableFuture<org.apache.fluss.rpc.messages.MetadataResponse> metadata(
                org.apache.fluss.rpc.messages.MetadataRequest request) {
            lastListenerName = currentListenerName();
            if (failMetadata) {
                CompletableFuture<org.apache.fluss.rpc.messages.MetadataResponse> failure =
                        new CompletableFuture<>();
                failure.completeExceptionally(new IllegalStateException("metadata unavailable"));
                return failure;
            }
            if (failNextMetadataAsMissing) {
                failNextMetadataAsMissing = false;
                throw new TableNotExistException("table was deleted");
            }
            List<PbTableMetadata> topics = new ArrayList<>();
            for (PbTablePath tablePath : request.getTablePathsList()) {
                Long tableId = tables.get(tablePath.getTableName());
                if (tableId != null) {
                    topics.add(
                            tableMetadata(
                                            tablePath.getTableName(),
                                            tableId,
                                            !"topic".equals(tablePath.getTableName())
                                                    || topicLeaderAvailable,
                                            "topic".equals(tablePath.getTableName())
                                                    ? topicIsr
                                                    : new int[] {1, 2},
                                            "topic".equals(tablePath.getTableName())
                                                    ? topicBucketEpoch
                                                    : Integer.valueOf(7))
                                    .setTableJson(
                                            descriptors
                                                    .get(tablePath.getTableName())
                                                    .toJsonBytes()));
                }
            }
            return CompletableFuture.completedFuture(
                    new org.apache.fluss.rpc.messages.MetadataResponse()
                            .addAllTabletServers(
                                    Arrays.asList(
                                            new PbServerNode()
                                                    .setNodeId(1)
                                                    .setHost("broker-1")
                                                    .setPort(9092)
                                                    .setRack("rack-a"),
                                            new PbServerNode()
                                                    .setNodeId(2)
                                                    .setHost("broker-2")
                                                    .setPort(9093)))
                            .addAllTableMetadatas(topics));
        }

        private void putTable(String topic, long tableId) {
            putTable(topic, tableId, defaultDescriptor());
        }

        private void putTable(String topic, long tableId, TableDescriptor descriptor) {
            tables.put(topic, tableId);
            descriptors.put(topic, descriptor);
        }

        private void removeTable(String topic) {
            tables.remove(topic);
            descriptors.remove(topic);
        }

        private static PbTableMetadata tableMetadata(
                String topic,
                long tableId,
                boolean leaderAvailable,
                int[] isr,
                Integer bucketEpoch) {
            PbTableMetadata table =
                    new PbTableMetadata()
                            .setTablePath(
                                    new PbTablePath().setDatabaseName("kafka").setTableName(topic))
                            .setTableId(tableId)
                            .addAllBucketMetadatas(
                                    Arrays.asList(
                                            new PbBucketMetadata()
                                                    .setBucketId(0)
                                                    .setLeaderId(leaderAvailable ? 1 : 3)
                                                    .setLeaderEpoch(5)
                                                    .setReplicaIds(
                                                            leaderAvailable
                                                                    ? new int[] {1, 2}
                                                                    : new int[] {1, 2, 3})
                                                    .setIsrs(
                                                            bucketEpoch == null ? new int[0] : isr),
                                            new PbBucketMetadata()
                                                    .setBucketId(1)
                                                    .setLeaderId(2)
                                                    .setLeaderEpoch(6)
                                                    .setReplicaIds(new int[] {1, 2})
                                                    .setIsrs(
                                                            bucketEpoch == null
                                                                    ? new int[0]
                                                                    : new int[] {1, 2})));
            if (bucketEpoch != null) {
                for (PbBucketMetadata bucket : table.getBucketMetadatasList()) {
                    bucket.setBucketEpoch(bucketEpoch);
                }
            }
            return table;
        }
    }
}

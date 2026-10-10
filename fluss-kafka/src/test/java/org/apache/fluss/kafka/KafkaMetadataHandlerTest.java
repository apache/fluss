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

import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.messages.ListDatabasesRequest;
import org.apache.fluss.rpc.messages.ListDatabasesResponse;
import org.apache.fluss.rpc.messages.ListTablesRequest;
import org.apache.fluss.rpc.messages.ListTablesResponse;
import org.apache.fluss.rpc.messages.PbBucketMetadata;
import org.apache.fluss.rpc.messages.PbServerNode;
import org.apache.fluss.rpc.messages.PbTableMetadata;
import org.apache.fluss.rpc.messages.PbTablePath;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.concurrent.FutureUtils;

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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

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
                                                    Collections.singletonList("kafka.topic"))),
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
            MetadataResponseTopic topic = response.data().topics().find("kafka.topic");
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
                    .containsExactly("kafka.other", "kafka.topic");
        }
    }

    @Test
    public void testAllTopicsAndMissingAndInvalidTopic() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();

        MetadataResponse allTopics =
                handle(service, MetadataRequest.Builder.allTopics().build((short) 9), (short) 9);
        assertThat(allTopics.data().topics())
                .extracting(MetadataResponseTopic::name)
                .containsExactlyInAnyOrder("kafka.other", "kafka.topic");

        MetadataRequest requestedTopics =
                new MetadataRequest.Builder(
                                Arrays.asList("kafka.missing", "kafka.invalid topic"), false)
                        .build((short) 9);
        MetadataResponse errors = handle(service, requestedTopics, (short) 9);
        assertThat(errors.errors())
                .containsEntry("kafka.missing", Errors.UNKNOWN_TOPIC_OR_PARTITION)
                .containsEntry("kafka.invalid topic", Errors.INVALID_TOPIC_EXCEPTION);
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
                                                            .setName("kafka.topic")
                                                            .setTopicId(
                                                                    new Uuid(
                                                                            0x466c757373000000L,
                                                                            999L)))),
                            version);

            MetadataResponse response = handle(service, request, version);

            assertThat(response.errorCounts()).containsOnlyKeys(Errors.NONE);
            assertThat(response.data().topics().find("kafka.topic").topicId()).isEqualTo(TOPIC_ID);
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

        MetadataResponse initial = handle(service, namedTopicRequest("kafka.topic"), (short) 11);
        assertThat(initial.data().topics().find("kafka.topic").topicId()).isEqualTo(TOPIC_ID);

        service.removeTable("kafka.topic");
        MetadataResponse deleted = handle(service, namedTopicRequest("kafka.topic"), (short) 11);
        assertThat(deleted.errorCounts())
                .containsExactlyEntriesOf(
                        Collections.singletonMap(Errors.UNKNOWN_TOPIC_OR_PARTITION, 1));

        service.putTable("kafka.topic", 223L);
        Uuid recreatedTopicId = new Uuid(0x466c757373000000L, 223L);
        MetadataResponse recreatedByName =
                handle(
                        service,
                        new MetadataRequest.Builder(Collections.singletonList("kafka.topic"), false)
                                .build((short) 11),
                        (short) 11);
        assertThat(recreatedByName.data().topics().find("kafka.topic").topicId())
                .isEqualTo(recreatedTopicId)
                .isNotEqualTo(TOPIC_ID);
    }

    @Test
    public void testDeleteRaceBecomesUnknownTopicResult() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        service.removeTable("kafka.topic");
        service.failNextMetadataAsMissing = true;

        MetadataResponse response = handle(service, namedTopicRequest("kafka.topic"), (short) 11);

        assertThat(response.errorCounts())
                .containsExactlyEntriesOf(
                        Collections.singletonMap(Errors.UNKNOWN_TOPIC_OR_PARTITION, 1));
    }

    @Test
    public void testUnavailableLeaderUsesPartitionError() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        service.topicLeaderAvailable = false;
        MetadataRequest request =
                new MetadataRequest.Builder(Collections.singletonList("kafka.topic"), false)
                        .build((short) 11);

        MetadataResponse response = handle(service, request, (short) 11);

        MetadataResponseTopic topic = response.data().topics().find("kafka.topic");
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
                new MetadataRequest.Builder(Collections.singletonList("kafka.topic"), false)
                        .build((short) 11);

        MetadataResponse response = handle(service, request, (short) 11);

        MetadataResponsePartition partition =
                response.data().topics().find("kafka.topic").partitions().get(0);
        assertThat(partition.replicaNodes()).containsExactly(1, 2);
        assertThat(partition.isrNodes()).containsExactly(1);
        assertThat(partition.offlineReplicas()).isEmpty();
    }

    @Test
    public void testLegacyMetadataUsesLeaderOnlyIsr() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        service.topicBucketEpoch = null;

        MetadataResponse response = handle(service, namedTopicRequest("kafka.topic"), (short) 11);

        MetadataResponsePartition partition =
                response.data().topics().find("kafka.topic").partitions().get(0);
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

        MetadataResponse response = handle(service, namedTopicRequest("kafka.topic"), (short) 11);

        MetadataResponsePartition partition =
                response.data().topics().find("kafka.topic").partitions().get(0);
        assertThat(partition.errorCode()).isEqualTo(Errors.LEADER_NOT_AVAILABLE.code());
        assertThat(partition.isrNodes()).isEmpty();
    }

    @Test
    public void testAuthoritativeEmptyIsrDoesNotUseLegacyFallback() {
        for (int bucketEpoch : new int[] {7, -1}) {
            TestingMetadataGatewayService service = new TestingMetadataGatewayService();
            service.topicBucketEpoch = bucketEpoch;
            service.topicIsr = new int[0];

            MetadataResponse response =
                    handle(service, namedTopicRequest("kafka.topic"), (short) 11);

            MetadataResponsePartition partition =
                    response.data().topics().find("kafka.topic").partitions().get(0);
            assertThat(partition.isrNodes()).isEmpty();
        }
    }

    @Test
    public void testUnexpectedGatewayFailureUsesRequestErrorResponse() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        service.failMetadata = true;
        MetadataRequest request =
                new MetadataRequest.Builder(Collections.singletonList("kafka.topic"), false)
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
                "kafka.no_mapping",
                125L,
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("body", DataTypes.BYTES()).build())
                        .distributedBy(2)
                        .build());
        service.putTable(
                "kafka.bad_format",
                126L,
                TableDescriptor.builder(defaultDescriptor())
                        .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "json")
                        .build());
        service.putTable(
                "kafka.primary_key",
                127L,
                TableDescriptor.builder(defaultDescriptor())
                        .schema(
                                Schema.newBuilder()
                                        .column("body", DataTypes.BYTES().copy(false))
                                        .primaryKey("body")
                                        .build())
                        .build());
        service.putTable(
                "kafka.partitioned",
                128L,
                TableDescriptor.builder(defaultDescriptor()).partitionedBy("body").build());
        service.putTable(
                "kafka.indexed",
                129L,
                TableDescriptor.builder(defaultDescriptor()).logFormat(LogFormat.INDEXED).build());
        service.putTable("kafka.invalid topic", 130L);
        List<String> invalidMappings =
                Arrays.asList(
                        "kafka.no_mapping",
                        "kafka.bad_format",
                        "kafka.primary_key",
                        "kafka.partitioned",
                        "kafka.indexed");
        for (short version = 0; version <= 11; version++) {
            MetadataResponse all = handle(service, allTopicsRequest(version), version);
            assertThat(all.data().topics())
                    .extracting(MetadataResponseTopic::name)
                    .containsExactly("kafka.other", "kafka.topic");
            List<String> requested = new ArrayList<>(invalidMappings);
            requested.add("kafka.topic");
            requested.add("kafka.missing");
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
            assertThat(named.data().topics().find("kafka.topic").errorCode())
                    .isEqualTo(Errors.NONE.code());
            assertThat(named.data().topics().find("kafka.missing").errorCode())
                    .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
        }
    }

    @Test
    public void testMetadataReflectsMappingChangesWithoutChangingTopicIdentity() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        service.putTable(
                "kafka.topic",
                123L,
                TableDescriptor.builder(defaultDescriptor())
                        .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "string")
                        .build());
        MetadataResponse invalid = handle(service, namedTopicRequest("kafka.topic"), (short) 11);
        assertThat(invalid.data().topics().find("kafka.topic").errorCode())
                .isEqualTo(Errors.INVALID_TOPIC_EXCEPTION.code());
        service.putTable("kafka.topic", 123L);
        MetadataResponse valid = handle(service, namedTopicRequest("kafka.topic"), (short) 11);
        assertThat(valid.data().topics().find("kafka.topic").errorCode())
                .isEqualTo(Errors.NONE.code());
        assertThat(valid.data().topics().find("kafka.topic").topicId()).isEqualTo(TOPIC_ID);
    }

    @Test
    public void testSameTableNameInDifferentDatabasesForEveryVersion() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        service.putTable("sales.topic", 223L);
        for (short version = 0; version <= 11; version++) {
            MetadataResponse all = handle(service, allTopicsRequest(version), version);
            assertThat(all.data().topics())
                    .extracting(MetadataResponseTopic::name)
                    .containsExactly("kafka.other", "kafka.topic", "sales.topic");
            MetadataResponse named =
                    handle(
                            service,
                            new MetadataRequest(
                                    new MetadataRequestData()
                                            .setTopics(
                                                    MetadataRequest.convertToMetadataRequestTopic(
                                                            Arrays.asList(
                                                                    "kafka.topic", "sales.topic"))),
                                    version),
                            version);
            assertThat(named.data().topics()).hasSize(2);
            assertThat(named.errorCounts()).containsOnlyKeys(Errors.NONE);
            if (version >= 10) {
                assertThat(named.data().topics().find("kafka.topic").topicId()).isEqualTo(TOPIC_ID);
                assertThat(named.data().topics().find("sales.topic").topicId())
                        .isEqualTo(new Uuid(0x466c757373000000L, 223L));
            }
        }
    }

    @Test
    public void testRejectUnqualifiedAndMalformedNamesWithoutHidingValidTopics() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        List<String> invalidNames = Arrays.asList("topic", ".topic", "kafka.", "kafka.topic.extra");
        List<String> names = new ArrayList<>(invalidNames);
        names.add("kafka.topic");
        MetadataResponse response =
                handle(
                        service,
                        new MetadataRequest.Builder(names, false).build((short) 11),
                        (short) 11);
        for (String name : invalidNames) {
            assertThat(response.data().topics().find(name).errorCode())
                    .isEqualTo(Errors.INVALID_TOPIC_EXCEPTION.code());
        }
        assertThat(response.data().topics().find("kafka.topic").errorCode())
                .isEqualTo(Errors.NONE.code());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testMissingDatabaseRetainsBrokersAndValidTopics(boolean failedFuture) {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        service.failListTablesAsFuture = failedFuture;
        for (short version = 0; version <= 11; version++) {
            for (List<String> names :
                    Arrays.asList(
                            Collections.singletonList("missing_db.topic"),
                            Arrays.asList("missing_db.topic", "kafka.topic"))) {
                MetadataResponse response =
                        handle(
                                service,
                                new MetadataRequest(
                                        new MetadataRequestData()
                                                .setTopics(
                                                        MetadataRequest
                                                                .convertToMetadataRequestTopic(
                                                                        names)),
                                        version),
                                version);
                assertThat(response.brokers()).hasSize(2);
                assertThat(response.data().topics()).hasSize(names.size());
                assertThat(response.data().topics().find("missing_db.topic").errorCode())
                        .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
                if (names.contains("kafka.topic")) {
                    MetadataResponseTopic valid = response.data().topics().find("kafka.topic");
                    assertThat(valid.errorCode()).isEqualTo(Errors.NONE.code());
                    assertThat(valid.partitions()).hasSize(2);
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testDatabaseDeletedDuringDiscoveryRetainsOtherTopics(boolean failedFuture) {
        for (short version = 0; version <= 11; version++) {
            TestingMetadataGatewayService service = new TestingMetadataGatewayService();
            service.putTable("deleted_db.topic", 223L);
            service.deleteDatabaseBeforeListing = "deleted_db";
            service.failListTablesAsFuture = failedFuture;

            MetadataResponse response = handle(service, allTopicsRequest(version), version);

            assertThat(response.brokers()).hasSize(2);
            assertThat(response.data().topics())
                    .extracting(MetadataResponseTopic::name)
                    .containsExactly("kafka.other", "kafka.topic");
            assertThat(response.errorCounts()).containsOnlyKeys(Errors.NONE);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testUnexpectedListTablesFailureIsNotTreatedAsMissingDatabase(boolean failedFuture) {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        service.failNextMetadataAsMissing = true;
        service.listTablesFailure = new IllegalStateException("metadata unavailable");
        service.failListTablesAsFuture = failedFuture;

        MetadataResponse response = handle(service, namedTopicRequest("kafka.topic"), (short) 11);

        assertThat(response.data().topics().find("kafka.topic").errorCode())
                .isEqualTo(Errors.UNKNOWN_SERVER_ERROR.code());
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
        KafkaRequestHandler handler = new KafkaRequestHandler(service, service);
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

        private final Set<String> databases = new LinkedHashSet<>();
        private final Map<String, Long> tables = new LinkedHashMap<>();
        private final Map<String, TableDescriptor> descriptors = new LinkedHashMap<>();
        private String lastListenerName;
        private boolean topicLeaderAvailable = true;
        private int[] topicIsr = new int[] {1, 2};
        private Integer topicBucketEpoch = 7;
        private boolean failMetadata;
        private boolean failNextMetadataAsMissing;
        private boolean failListTablesAsFuture;
        private String deleteDatabaseBeforeListing;
        private RuntimeException listTablesFailure;

        private TestingMetadataGatewayService() {
            putTable("kafka.topic", 123L);
            putTable("kafka.other", 124L);
        }

        @Override
        public CompletableFuture<ListDatabasesResponse> listDatabases(
                ListDatabasesRequest request) {
            assertThat(currentListenerName()).isEqualTo("KAFKA");
            return CompletableFuture.completedFuture(
                    new ListDatabasesResponse().addAllDatabaseNames(databases));
        }

        @Override
        public CompletableFuture<ListTablesResponse> listTables(ListTablesRequest request) {
            assertThat(currentListenerName()).isEqualTo("KAFKA");
            String database = request.getDatabaseName();
            if (database.equals(deleteDatabaseBeforeListing)) {
                databases.remove(database);
                tables.keySet().removeIf(name -> name.startsWith(database + "."));
            }
            RuntimeException failure = listTablesFailure;
            if (failure == null && !databases.contains(database)) {
                failure = new DatabaseNotExistException("Database does not exist.");
            }
            if (failure != null) {
                if (failListTablesAsFuture) {
                    return FutureUtils.completedExceptionally(new CompletionException(failure));
                }
                throw failure;
            }
            String prefix = request.getDatabaseName() + ".";
            List<String> names =
                    tables.keySet().stream()
                            .filter(name -> name.startsWith(prefix))
                            .map(name -> name.substring(prefix.length()))
                            .collect(Collectors.toList());
            return CompletableFuture.completedFuture(
                    new ListTablesResponse().addAllTableNames(names));
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
                String topicName = tablePath.getDatabaseName() + "." + tablePath.getTableName();
                Long tableId = tables.get(topicName);
                if (tableId == null) {
                    throw new TableNotExistException("Table does not exist: " + topicName);
                }
                topics.add(
                        tableMetadata(
                                        topicName,
                                        tableId,
                                        !"kafka.topic".equals(topicName) || topicLeaderAvailable,
                                        "kafka.topic".equals(topicName)
                                                ? topicIsr
                                                : new int[] {1, 2},
                                        "kafka.topic".equals(topicName)
                                                ? topicBucketEpoch
                                                : Integer.valueOf(7))
                                .setTableJson(descriptors.get(topicName).toJsonBytes()));
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
            databases.add(topic.substring(0, topic.indexOf('.')));
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
                                    new PbTablePath()
                                            .setDatabaseName(topic.substring(0, topic.indexOf('.')))
                                            .setTableName(topic.substring(topic.indexOf('.') + 1)))
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

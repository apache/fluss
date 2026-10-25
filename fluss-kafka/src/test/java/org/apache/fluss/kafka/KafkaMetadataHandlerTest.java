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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.KafkaValueConversionType;
import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.kafka.backend.metadata.GatewayKafkaMetadataBackend;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult;
import org.apache.fluss.kafka.mapping.KafkaTopicMapper;
import org.apache.fluss.kafka.security.KafkaSaslConnection;
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
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.ClusterResourceListeners;
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
import org.apache.kafka.common.utils.LogContext;
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
import static org.assertj.core.api.Assertions.entry;

/** Protocol compatibility tests for the Kafka Metadata API. */
public class KafkaMetadataHandlerTest {

    private static final Uuid TOPIC_ID = new Uuid(0x466c757373000000L, 123L);

    @Test
    public void testNamedTopicForEverySupportedVersion() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        for (short version = ApiKeys.METADATA.oldestVersion(); version <= 11; version++) {
            MetadataRequest request = topicsRequest(version, "kafka.topic");
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testMetadataFailureDoesNotRetryOrRediscover(boolean failedFuture) {
        for (MetadataRequest request :
                Arrays.asList(
                        topicsRequest((short) 11, "kafka.topic", "kafka.other"),
                        allTopicsRequest((short) 11))) {
            TestingMetadataGatewayService service = new TestingMetadataGatewayService();
            service.metadataFailure =
                    new FlussRuntimeException(
                            "Failed to get table", new TableNotExistException("table was deleted"));
            service.failMetadataAsFuture = failedFuture;

            MetadataResponse response = handle(service, request, (short) 11);

            assertThat(response.brokers()).isEmpty();
            if (request.isAllTopics()) {
                assertThat(response.data().topics()).isEmpty();
            } else {
                assertThat(response.errors())
                        .containsOnly(
                                entry("kafka.topic", Errors.UNKNOWN_SERVER_ERROR),
                                entry("kafka.other", Errors.UNKNOWN_SERVER_ERROR));
            }
            assertThat(service.metadataCalls).isOne();
            assertThat(service.listDatabasesCalls).isEqualTo(request.isAllTopics() ? 1 : 0);
            assertThat(service.listTablesCalls).isEqualTo(request.isAllTopics() ? 1 : 0);
        }
    }

    @Test
    public void testUnavailableLeaderUsesPartitionError() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        service.topicLeaderAvailable = false;
        MetadataRequest request = namedTopicRequest("kafka.topic");

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
        MetadataRequest request = namedTopicRequest("kafka.topic");

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
        service.metadataFailure = new IllegalStateException("metadata unavailable");
        MetadataRequest request = namedTopicRequest("kafka.topic");

        MetadataResponse response = handle(service, request, (short) 11);

        assertThat(response.errorCounts())
                .containsExactlyEntriesOf(Collections.singletonMap(Errors.UNKNOWN_SERVER_ERROR, 1));
        assertThat(response.brokers()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testEnabledTablesDoNotRevalidateColumnContract(boolean legacyOffset) {
        RowType metadataType =
                DataTypes.ROW(
                        DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(3)),
                        DataTypes.FIELD("timestamp_type", DataTypes.INT()),
                        DataTypes.FIELD("partition", DataTypes.INT()));
        if (legacyOffset) {
            metadataType =
                    DataTypes.ROW(
                            DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(3)),
                            DataTypes.FIELD("timestamp_type", DataTypes.INT()),
                            DataTypes.FIELD("partition", DataTypes.INT()),
                            DataTypes.FIELD("offset", DataTypes.BIGINT()));
        }
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        // Native DDL owns schema validation. Metadata only checks the persisted enablement flag,
        // even for a historical descriptor that current DDL would reject.
        service.putTable(
                "fluss.metadata_events",
                777L,
                TableDescriptor.builder(defaultDescriptor())
                        .schema(
                                Schema.newBuilder()
                                        .column("_kafka_value", DataTypes.BYTES())
                                        .column("_kafka_metadata", metadataType)
                                        .build())
                        .build());
        for (short version = 0; version <= 11; version++) {
            MetadataResponse named =
                    handle(
                            service,
                            topicsRequest(
                                    version,
                                    "metadata_events",
                                    "fluss.metadata_events",
                                    "kafka.topic"),
                            version);
            for (String name : Arrays.asList("metadata_events", "fluss.metadata_events")) {
                MetadataResponseTopic topic = named.data().topics().find(name);
                assertThat(topic.errorCode()).isEqualTo(Errors.NONE.code());
                assertThat(topic.partitions()).hasSize(2);
            }
            assertThat(named.data().topics().find("kafka.topic").errorCode())
                    .isEqualTo(Errors.NONE.code());

            MetadataResponse all = handle(service, allTopicsRequest(version), version);
            assertThat(all.data().topics().find("fluss.metadata_events").errorCode())
                    .isEqualTo(Errors.NONE.code());
            assertThat(all.data().topics().find("kafka.topic").errorCode())
                    .isEqualTo(Errors.NONE.code());
            assertThat(all.data().topics())
                    .extracting(MetadataResponseTopic::name)
                    .doesNotContain("metadata_events");
        }
    }

    @Test
    public void testRejectMalformedNamesWithoutHidingValidTopics() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        List<String> invalidNames = Arrays.asList(".topic", "kafka.", "kafka.topic.extra");
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

    @Test
    public void testMetadataV12IsRejectedBeforeCallingGateway() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        MetadataRequest request =
                new MetadataRequest.Builder(Collections.singletonList("kafka.topic"), false)
                        .build((short) 12);

        MetadataResponse response = handle(service, request, (short) 12);

        assertThat(response.data().topics().find("kafka.topic").errorCode())
                .isEqualTo(Errors.UNSUPPORTED_VERSION.code());
        assertThat(service.gatewayCalls).isZero();
    }

    @Test
    public void testMissingDatabaseFailsBatchWithoutDiscovery() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        service.putTable("fluss.topic", 777L);
        for (short version = 0; version <= 11; version++) {
            MetadataResponse response =
                    handle(
                            service,
                            topicsRequest(
                                    version,
                                    "topic",
                                    "fluss.topic",
                                    "kafka.topic",
                                    "missing_db.topic"),
                            version);
            assertThat(response.brokers()).isEmpty();
            assertThat(response.errorCounts())
                    .containsExactlyEntriesOf(
                            Collections.singletonMap(Errors.UNKNOWN_SERVER_ERROR, 4));
        }
        assertThat(service.metadataCalls).isEqualTo(12);
        assertThat(service.listDatabasesCalls).isZero();
        assertThat(service.listTablesCalls).isZero();
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
        service.listTablesFailure = new IllegalStateException("metadata unavailable");
        service.failListTablesAsFuture = failedFuture;

        MetadataResponse response = handle(service, allTopicsRequest((short) 11), (short) 11);

        assertThat(response.brokers()).isEmpty();
        assertThat(response.data().topics()).isEmpty();
        assertThat(service.listDatabasesCalls).isOne();
        assertThat(service.listTablesCalls).isOne();
    }

    @ParameterizedTest
    @ValueSource(strings = {"fluss", "custom_db"})
    public void testAliasesPreserveRequestedNamesAndShareIdentity(String defaultDatabase) {
        Configuration config = new Configuration();
        if (!"fluss".equals(defaultDatabase)) {
            config.set(ConfigOptions.KAFKA_DEFAULT_DATABASE, defaultDatabase);
        }
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        String canonical = defaultDatabase + ".topic";
        service.putTable(canonical, 777L);
        service.putTable("sales.topic", 778L);
        for (short version = 0; version <= 11; version++) {
            MetadataResponse named =
                    handle(
                            service,
                            topicsRequest(version, "topic", canonical, "sales.topic"),
                            version,
                            config);
            assertThat(named.data().topics())
                    .extracting(MetadataResponseTopic::name)
                    .containsExactly("topic", canonical, "sales.topic");
            assertThat(named.errorCounts()).containsOnlyKeys(Errors.NONE);
            assertThat(service.metadataCalls).isEqualTo(version * 2 + 1);
            MetadataResponseTopic alias = named.data().topics().find("topic");
            MetadataResponseTopic qualified = named.data().topics().find(canonical);
            assertThat(alias.partitions()).isEqualTo(qualified.partitions());
            assertThat(alias.topicId())
                    .isEqualTo(
                            version >= 10 ? new Uuid(0x466c757373410000L, 777L) : Uuid.ZERO_UUID);
            if (version >= 10) {
                assertThat(qualified.topicId())
                        .isEqualTo(new Uuid(0x466c757373000000L, 777L))
                        .isNotEqualTo(alias.topicId())
                        .isNotEqualTo(named.data().topics().find("sales.topic").topicId());
            }
            MetadataResponse all = handle(service, allTopicsRequest(version), version, config);
            assertThat(service.metadataCalls).isEqualTo(version * 2 + 2);
            assertThat(all.data().topics())
                    .extracting(MetadataResponseTopic::name)
                    .contains(canonical, "sales.topic")
                    .doesNotContain("topic")
                    .doesNotHaveDuplicates();
        }
    }

    @Test
    public void testMissingDefaultDatabaseNeverSearchesOtherDatabases() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        Configuration config = new Configuration();
        config.set(ConfigOptions.KAFKA_DEFAULT_DATABASE, "missing_default");
        MetadataResponse response =
                handle(
                        service,
                        topicsRequest((short) 11, "kafka.topic", "topic"),
                        (short) 11,
                        config);
        assertThat(response.brokers()).isEmpty();
        assertThat(response.errorCounts())
                .containsExactlyEntriesOf(Collections.singletonMap(Errors.UNKNOWN_SERVER_ERROR, 2));
        assertThat(service.metadataCalls).isOne();
        assertThat(service.listDatabasesCalls).isZero();
        assertThat(service.requestedTables)
                .containsExactlyInAnyOrder("missing_default.topic", "kafka.topic");
        assertThat(service.listTablesCalls).isZero();
    }

    @Test
    public void testUnauthorizedAliasesRemainHidden() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        service.putTable("fluss.topic", 777L);
        service.hiddenTopics.add("fluss.topic");
        service.putTable("sales.topic", 778L);
        for (short version = 0; version <= 11; version++) {
            MetadataResponse response =
                    handle(
                            service,
                            topicsRequest(version, "topic", "fluss.topic", "sales.topic"),
                            version);
            assertThat(response.brokers()).hasSize(2);
            assertThat(response.data().topics()).hasSize(3);
            for (String hidden : Arrays.asList("topic", "fluss.topic")) {
                MetadataResponseTopic topic = response.data().topics().find(hidden);
                assertThat(topic.errorCode()).isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
                assertThat(topic.topicId()).isEqualTo(Uuid.ZERO_UUID);
                assertThat(topic.partitions()).isEmpty();
            }
            MetadataResponseTopic visible = response.data().topics().find("sales.topic");
            assertThat(visible.errorCode()).isEqualTo(Errors.NONE.code());
            assertThat(visible.partitions()).hasSize(2);
            MetadataResponse all = handle(service, allTopicsRequest(version), version);
            assertThat(all.brokers()).hasSize(2);
            assertThat(all.data().topics())
                    .extracting(MetadataResponseTopic::name)
                    .contains("sales.topic")
                    .doesNotContain("topic", "fluss.topic");
        }
    }

    @Test
    public void testPersistedEnablementControlsBothAliasesAndDiscovery() {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        TableDescriptor missingFlag =
                TableDescriptor.builder()
                        .schema(defaultDescriptor().getSchema())
                        .distributedBy(2)
                        .property(
                                ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE,
                                KafkaValueConversionType.BINARY)
                        .build();
        TableDescriptor disabledInvalid =
                TableDescriptor.builder(defaultDescriptor())
                        .property(ConfigOptions.TABLE_KAFKA_ENABLED, false)
                        .property(
                                ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE.key(), "unimplemented")
                        .build();
        for (TableDescriptor disabled : Arrays.asList(missingFlag, disabledInvalid)) {
            service.putTable("fluss.topic", 777L, disabled);
            for (short version = 0; version <= 11; version++) {
                MetadataResponse named =
                        handle(service, topicsRequest(version, "topic", "fluss.topic"), version);
                assertThat(named.errors())
                        .containsEntry("topic", Errors.UNKNOWN_TOPIC_OR_PARTITION)
                        .containsEntry("fluss.topic", Errors.UNKNOWN_TOPIC_OR_PARTITION);
                assertThat(named.data().topics())
                        .allSatisfy(
                                topic -> {
                                    assertThat(topic.topicId()).isEqualTo(Uuid.ZERO_UUID);
                                    assertThat(topic.partitions()).isEmpty();
                                });
                assertThat(handle(service, allTopicsRequest(version), version).data().topics())
                        .extracting(MetadataResponseTopic::name)
                        .doesNotContain("topic", "fluss.topic");
            }
        }
        service.putTable("fluss.topic", 777L);
        MetadataResponse enabled =
                handle(service, topicsRequest((short) 11, "topic", "fluss.topic"), (short) 11);
        assertThat(enabled.errorCounts()).containsOnlyKeys(Errors.NONE);
        assertThat(enabled.data().topics().find("topic").topicId())
                .isEqualTo(new Uuid(0x466c757373410000L, 777L));
        assertThat(enabled.data().topics().find("fluss.topic").topicId())
                .isEqualTo(new Uuid(0x466c757373000000L, 777L));
        service.putTable("fluss.topic", 777L, disabledInvalid);
        assertThat(handle(service, namedTopicRequest("topic"), (short) 11).errors())
                .containsEntry("topic", Errors.UNKNOWN_TOPIC_OR_PARTITION);
    }

    @Test
    public void testResolvedAliasNameLimitDoesNotHideValidTopic() {
        String database = String.join("", Collections.nCopies(125, "d"));
        String table = String.join("", Collections.nCopies(125, "t"));
        Configuration config = new Configuration();
        config.set(ConfigOptions.KAFKA_DEFAULT_DATABASE, database);
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        MetadataResponse response =
                handle(
                        service,
                        topicsRequest((short) 11, table, "kafka.topic"),
                        (short) 11,
                        config);
        assertThat(response.errors()).containsEntry(table, Errors.INVALID_TOPIC_EXCEPTION);
        assertThat(response.data().topics().find("kafka.topic").errorCode())
                .isEqualTo(Errors.NONE.code());
        assertThat(service.requestedTables).containsExactly("kafka.topic");

        // A historical enabled table with an unrepresentable canonical name must be visible as
        // an error during discovery rather than silently classified as disabled.
        service.putTable(database + "." + table, 777L);
        assertThat(handle(service, allTopicsRequest((short) 11), (short) 11, config).errors())
                .containsEntry(database + "." + table, Errors.INVALID_TOPIC_EXCEPTION);
    }

    @ParameterizedTest
    @ValueSource(shorts = {10, 11})
    public void testKafkaClientCanCacheCanonicalNameAndAliasTogether(short version) {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        service.putTable("fluss.topic", 777L);
        Metadata client =
                new Metadata(0L, 0L, 60000L, new LogContext(), new ClusterResourceListeners());
        client.updateWithCurrentRequestVersion(
                handle(service, topicsRequest(version, "fluss.topic"), version), false, 1L);
        // A producer discovers the alias after already caching the canonical name. Nonzero
        // duplicate IDs would make Kafka's MetadataSnapshot throw while building topicNames.
        client.updateWithCurrentRequestVersion(
                handle(service, topicsRequest(version, "topic"), version), true, 2L);
        assertThat(client.fetch().partitionsForTopic("topic")).hasSize(2);
        assertThat(client.fetch().partitionsForTopic("fluss.topic")).hasSize(2);
        client.updateWithCurrentRequestVersion(
                handle(service, topicsRequest(version, "topic", "fluss.topic"), version),
                false,
                3L);
        assertThat(client.fetch().partitionsForTopic("topic")).hasSize(2);
        assertThat(client.fetch().partitionsForTopic("fluss.topic")).hasSize(2);
        assertThat(client.topicIds()).containsOnlyKeys("topic", "fluss.topic");
        assertThat(client.topicNames())
                .containsEntry(new Uuid(0x466c757373000000L, 777L), "fluss.topic")
                .containsEntry(new Uuid(0x466c757373410000L, 777L), "topic");
        client.close();
    }

    @ParameterizedTest
    @ValueSource(shorts = {10, 11})
    public void testKafkaClientRefreshesRecreatedAliasWithLowerLeaderEpoch(short version) {
        TestingMetadataGatewayService service = new TestingMetadataGatewayService();
        service.putTable("fluss.topic", 777L);
        Metadata client =
                new Metadata(0L, 0L, 60000L, new LogContext(), new ClusterResourceListeners());
        MetadataResponse original =
                handle(service, topicsRequest(version, "topic", "fluss.topic"), version);
        original.data()
                .topics()
                .forEach(
                        topic ->
                                topic.partitions()
                                        .forEach(p -> p.setLeaderId(1).setLeaderEpoch(5)));
        client.updateWithCurrentRequestVersion(original, false, 1L);
        for (String name : Arrays.asList("topic", "fluss.topic")) {
            assertThat(client.fetch().leaderFor(new TopicPartition(name, 0)).id()).isEqualTo(1);
            assertThat(client.lastSeenLeaderEpoch(new TopicPartition(name, 0))).contains(5);
        }

        // The client never observes the missing table between deletion and recreation.
        service.removeTable("fluss.topic");
        service.putTable("fluss.topic", 778L);
        MetadataResponse recreated =
                handle(service, topicsRequest(version, "topic", "fluss.topic"), version);
        recreated
                .data()
                .topics()
                .forEach(
                        topic ->
                                topic.partitions()
                                        .forEach(p -> p.setLeaderId(2).setLeaderEpoch(0)));
        client.updateWithCurrentRequestVersion(recreated, false, 2L);
        for (String name : Arrays.asList("fluss.topic", "topic")) {
            assertThat(client.fetch().leaderFor(new TopicPartition(name, 0)).id())
                    .as("Leader after recreating %s", name)
                    .isEqualTo(2);
            assertThat(client.lastSeenLeaderEpoch(new TopicPartition(name, 0))).contains(0);
        }
        assertThat(client.topicNames())
                .containsEntry(new Uuid(0x466c757373000000L, 778L), "fluss.topic")
                .containsEntry(new Uuid(0x466c757373410000L, 778L), "topic")
                .doesNotContainKeys(
                        new Uuid(0x466c757373000000L, 777L), new Uuid(0x466c757373410000L, 777L));
        client.close();
    }

    private static MetadataRequest topicsRequest(short version, String... names) {
        return new MetadataRequest(
                new MetadataRequestData()
                        .setTopics(
                                MetadataRequest.convertToMetadataRequestTopic(
                                        Arrays.asList(names))),
                version);
    }

    private static TableDescriptor defaultDescriptor() {
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

    private static MetadataRequest namedTopicRequest(String topicName) {
        return topicsRequest((short) 11, topicName);
    }

    private static MetadataRequest allTopicsRequest(short version) {
        MetadataRequestData data = new MetadataRequestData();
        data.setTopics(version == 0 ? Collections.emptyList() : null);
        return new MetadataRequest(data, version);
    }

    private static MetadataResponse handle(
            TestingMetadataGatewayService service, MetadataRequest requestBody, short version) {
        return handle(service, requestBody, version, new Configuration());
    }

    private static MetadataResponse handle(
            TestingMetadataGatewayService service,
            MetadataRequest requestBody,
            short version,
            Configuration config) {
        KafkaTopicMapper mapper = new KafkaTopicMapper(config);
        KafkaRequestHandler handler =
                new KafkaRequestHandler(
                        mapper,
                        new GatewayKafkaMetadataBackend(service, mapper),
                        command ->
                                CompletableFuture.completedFuture(
                                        new KafkaProduceResult(Collections.emptyList())));
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
                            KafkaSaslConnection.plaintext(),
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
        private final Set<String> hiddenTopics = new LinkedHashSet<>();
        private final Map<String, Long> tables = new LinkedHashMap<>();
        private final Map<String, TableDescriptor> descriptors = new LinkedHashMap<>();
        private int gatewayCalls;
        private int listDatabasesCalls;
        private int listTablesCalls;
        private int metadataCalls;
        private final Set<String> requestedTables = new LinkedHashSet<>();
        private String lastListenerName;
        private boolean topicLeaderAvailable = true;
        private int[] topicIsr = new int[] {1, 2};
        private Integer topicBucketEpoch = 7;
        private RuntimeException metadataFailure;
        private boolean failMetadataAsFuture;
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
            gatewayCalls++;
            listDatabasesCalls++;
            assertThat(currentListenerName()).isEqualTo("KAFKA");
            return CompletableFuture.completedFuture(
                    new ListDatabasesResponse().addAllDatabaseNames(databases));
        }

        @Override
        public CompletableFuture<ListTablesResponse> listTables(ListTablesRequest request) {
            gatewayCalls++;
            listTablesCalls++;
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
                    return FutureUtils.completedExceptionally(
                            new CompletionException(new CompletionException(failure)));
                }
                throw failure;
            }
            String prefix = request.getDatabaseName() + ".";
            List<String> names =
                    tables.keySet().stream()
                            .filter(name -> !hiddenTopics.contains(name))
                            .filter(name -> name.startsWith(prefix))
                            .map(name -> name.substring(prefix.length()))
                            .collect(Collectors.toList());
            return CompletableFuture.completedFuture(
                    new ListTablesResponse().addAllTableNames(names));
        }

        @Override
        public CompletableFuture<org.apache.fluss.rpc.messages.MetadataResponse> metadata(
                org.apache.fluss.rpc.messages.MetadataRequest request) {
            gatewayCalls++;
            metadataCalls++;
            lastListenerName = currentListenerName();
            if (metadataFailure != null) {
                if (failMetadataAsFuture) {
                    return FutureUtils.completedExceptionally(
                            new CompletionException(new CompletionException(metadataFailure)));
                }
                throw metadataFailure;
            }
            List<PbTableMetadata> topics = new ArrayList<>();
            for (PbTablePath tablePath : request.getTablePathsList()) {
                String topicName = tablePath.getDatabaseName() + "." + tablePath.getTableName();
                requestedTables.add(topicName);
                // Native Metadata and listTables omit tables without DESCRIBE permission.
                if (hiddenTopics.contains(topicName)) {
                    continue;
                }
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

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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.gateway.AdminGateway;
import org.apache.fluss.rpc.messages.CreateTableRequest;
import org.apache.fluss.rpc.messages.CreateTableResponse;
import org.apache.fluss.rpc.messages.DropTableRequest;
import org.apache.fluss.rpc.messages.DropTableResponse;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.fluss.types.DataTypes;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DeleteTopicsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests the Kafka topic lifecycle mapping to Fluss tables. */
public class KafkaTopicAdminHandlerTest {

    @Test
    public void testCreateTopicCreatesArrowTable() {
        TestingTabletGatewayService service = new TestingTabletGatewayService();
        AdminGateway adminGateway = mock(AdminGateway.class);
        when(adminGateway.createTable(any(CreateTableRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(new CreateTableResponse()));
        when(adminGateway.getTableInfo(any(GetTableInfoRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new GetTableInfoResponse().setTableId(123L)));
        short version = ApiKeys.CREATE_TOPICS.latestVersion();
        CreateTopicsRequest requestBody = createTopicsRequest(version);
        KafkaRequest request = kafkaRequest(ApiKeys.CREATE_TOPICS, requestBody, version);

        new KafkaRequestHandler(service, service, adminGateway, "kafka").processRequest(request);

        CreateTopicsResponse response = (CreateTopicsResponse) parseResponse(request);
        CreateTopicsResponseData.CreatableTopicResult result =
                response.data().topics().find("topic");
        assertThat(result.errorCode()).isEqualTo(Errors.NONE.code());
        assertThat(result.topicId()).isNotEqualTo(Uuid.ZERO_UUID);
        ArgumentCaptor<CreateTableRequest> captor =
                ArgumentCaptor.forClass(CreateTableRequest.class);
        verify(adminGateway).createTable(captor.capture());
        CreateTableRequest flussRequest = captor.getValue();
        assertThat(flussRequest.getTablePath().getDatabaseName()).isEqualTo("kafka");
        assertThat(flussRequest.getTablePath().getTableName()).isEqualTo("topic");
        TableDescriptor descriptor = TableDescriptor.fromJsonBytes(flussRequest.getTableJson());
        assertThat(descriptor.getSchema().getColumnNames())
                .containsExactly("record_key", "payload", "event_time", "headers");
        assertThat(descriptor.getSchema().getRowType().getTypeAt(0)).isEqualTo(DataTypes.BYTES());
        assertThat(descriptor.getSchema().getRowType().getTypeAt(1)).isEqualTo(DataTypes.BYTES());
        assertThat(descriptor.getSchema().getRowType().getTypeAt(2))
                .isEqualTo(DataTypes.TIMESTAMP_LTZ(3).copy(false));
        assertThat(descriptor.getSchema().getRowType().getTypeAt(3))
                .isEqualTo(
                        DataTypes.ARRAY(
                                DataTypes.ROW(
                                        DataTypes.FIELD("name", DataTypes.STRING().copy(false)),
                                        DataTypes.FIELD("value", DataTypes.BYTES()))));
        assertThat(descriptor.getTableDistribution().get().getBucketCount().get()).isEqualTo(3);
        assertThat(descriptor.getProperties())
                .containsEntry(ConfigOptions.TABLE_LOG_FORMAT.key(), LogFormat.ARROW.toString())
                .containsEntry(ConfigOptions.TABLE_REPLICATION_FACTOR.key(), "2");
        assertThat(descriptor.getCustomProperties())
                .containsEntry(KafkaDataFormat.KEY_FORMAT_CONFIG, "raw")
                .containsEntry(KafkaDataFormat.VALUE_FORMAT_CONFIG, "raw");
    }

    @Test
    public void testCreateTopicSupportsIndependentStringFormats() {
        TestingTabletGatewayService service = new TestingTabletGatewayService();
        AdminGateway adminGateway = mock(AdminGateway.class);
        when(adminGateway.createTable(any(CreateTableRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(new CreateTableResponse()));
        when(adminGateway.getTableInfo(any(GetTableInfoRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new GetTableInfoResponse().setTableId(123L)));
        Map<String, String> configs = new LinkedHashMap<>();
        configs.put(KafkaDataFormat.KEY_FORMAT_CONFIG, "string");
        configs.put(KafkaDataFormat.VALUE_FORMAT_CONFIG, "raw");
        short version = ApiKeys.CREATE_TOPICS.latestVersion();
        KafkaRequest request =
                kafkaRequest(ApiKeys.CREATE_TOPICS, createTopicsRequest(version, configs), version);

        new KafkaRequestHandler(service, service, adminGateway, "kafka").processRequest(request);

        assertThat(((CreateTopicsResponse) parseResponse(request)).errorCounts())
                .containsOnlyKeys(Errors.NONE);
        ArgumentCaptor<CreateTableRequest> captor =
                ArgumentCaptor.forClass(CreateTableRequest.class);
        verify(adminGateway).createTable(captor.capture());
        TableDescriptor descriptor =
                TableDescriptor.fromJsonBytes(captor.getValue().getTableJson());
        assertThat(descriptor.getSchema().getRowType().getTypeAt(0)).isEqualTo(DataTypes.STRING());
        assertThat(descriptor.getSchema().getRowType().getTypeAt(1)).isEqualTo(DataTypes.BYTES());
        assertThat(descriptor.getCustomProperties()).containsAllEntriesOf(configs);
    }

    @Test
    public void testCreateTopicUsesConfiguredDefaultFormats() {
        TestingTabletGatewayService service = new TestingTabletGatewayService();
        AdminGateway adminGateway = mock(AdminGateway.class);
        when(adminGateway.createTable(any(CreateTableRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(new CreateTableResponse()));
        when(adminGateway.getTableInfo(any(GetTableInfoRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new GetTableInfoResponse().setTableId(123L)));
        short version = ApiKeys.CREATE_TOPICS.latestVersion();
        KafkaRequest request =
                kafkaRequest(ApiKeys.CREATE_TOPICS, createTopicsRequest(version), version);

        new KafkaRequestHandler(
                        service,
                        service,
                        adminGateway,
                        "kafka",
                        KafkaDataFormat.STRING,
                        KafkaDataFormat.STRING)
                .processRequest(request);

        assertThat(((CreateTopicsResponse) parseResponse(request)).errorCounts())
                .containsOnlyKeys(Errors.NONE);
        ArgumentCaptor<CreateTableRequest> captor =
                ArgumentCaptor.forClass(CreateTableRequest.class);
        verify(adminGateway).createTable(captor.capture());
        TableDescriptor descriptor =
                TableDescriptor.fromJsonBytes(captor.getValue().getTableJson());
        assertThat(descriptor.getSchema().getRowType().getTypeAt(0)).isEqualTo(DataTypes.STRING());
        assertThat(descriptor.getSchema().getRowType().getTypeAt(1)).isEqualTo(DataTypes.STRING());
        assertThat(descriptor.getCustomProperties())
                .containsEntry(KafkaDataFormat.KEY_FORMAT_CONFIG, "string")
                .containsEntry(KafkaDataFormat.VALUE_FORMAT_CONFIG, "string");
    }

    @Test
    public void testCreateTopicRejectsInvalidFormat() {
        TestingTabletGatewayService service = new TestingTabletGatewayService();
        AdminGateway adminGateway = mock(AdminGateway.class);
        short version = ApiKeys.CREATE_TOPICS.latestVersion();
        KafkaRequest request =
                kafkaRequest(
                        ApiKeys.CREATE_TOPICS,
                        createTopicsRequest(
                                version,
                                Collections.singletonMap(
                                        KafkaDataFormat.VALUE_FORMAT_CONFIG, "json")),
                        version);

        new KafkaRequestHandler(service, service, adminGateway, "kafka").processRequest(request);

        CreateTopicsResponse response = (CreateTopicsResponse) parseResponse(request);
        assertThat(response.errorCounts()).containsEntry(Errors.INVALID_CONFIG, 1);
        assertThat(response.data().topics().find("topic").errorMessage())
                .contains("Expected raw or string");
        verify(adminGateway, never()).createTable(any(CreateTableRequest.class));
    }

    @Test
    public void testCreateTopicMapsAlreadyExists() {
        TestingTabletGatewayService service = new TestingTabletGatewayService();
        AdminGateway adminGateway = mock(AdminGateway.class);
        CompletableFuture<CreateTableResponse> failure = new CompletableFuture<>();
        failure.completeExceptionally(new TableAlreadyExistException("already exists"));
        when(adminGateway.createTable(any(CreateTableRequest.class))).thenReturn(failure);
        short version = ApiKeys.CREATE_TOPICS.latestVersion();
        KafkaRequest request =
                kafkaRequest(ApiKeys.CREATE_TOPICS, createTopicsRequest(version), version);

        new KafkaRequestHandler(service, service, adminGateway, "kafka").processRequest(request);

        CreateTopicsResponse response = (CreateTopicsResponse) parseResponse(request);
        assertThat(response.errorCounts()).containsEntry(Errors.TOPIC_ALREADY_EXISTS, 1);
    }

    @Test
    public void testDeleteTopicDropsTable() {
        TestingTabletGatewayService service = new TestingTabletGatewayService();
        AdminGateway adminGateway = mock(AdminGateway.class);
        when(adminGateway.dropTable(any(DropTableRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(new DropTableResponse()));
        short version = ApiKeys.DELETE_TOPICS.latestVersion();
        DeleteTopicsRequestData data =
                new DeleteTopicsRequestData()
                        .setTimeoutMs(1000)
                        .setTopics(
                                Collections.singletonList(
                                        new DeleteTopicsRequestData.DeleteTopicState()
                                                .setName("topic")
                                                .setTopicId(Uuid.ZERO_UUID)));
        DeleteTopicsRequest requestBody = new DeleteTopicsRequest.Builder(data).build(version);
        KafkaRequest request = kafkaRequest(ApiKeys.DELETE_TOPICS, requestBody, version);

        new KafkaRequestHandler(service, service, adminGateway, "kafka").processRequest(request);

        DeleteTopicsResponse response = (DeleteTopicsResponse) parseResponse(request);
        assertThat(response.errorCounts()).containsOnlyKeys(Errors.NONE);
        ArgumentCaptor<DropTableRequest> captor = ArgumentCaptor.forClass(DropTableRequest.class);
        verify(adminGateway).dropTable(captor.capture());
        assertThat(captor.getValue().getTablePath().getDatabaseName()).isEqualTo("kafka");
        assertThat(captor.getValue().getTablePath().getTableName()).isEqualTo("topic");
    }

    private static CreateTopicsRequest createTopicsRequest(short version) {
        return createTopicsRequest(version, Collections.emptyMap());
    }

    private static CreateTopicsRequest createTopicsRequest(
            short version, Map<String, String> configs) {
        CreateTopicsRequestData.CreatableTopic topic =
                new CreateTopicsRequestData.CreatableTopic()
                        .setName("topic")
                        .setNumPartitions(3)
                        .setReplicationFactor((short) 2);
        for (Map.Entry<String, String> config : configs.entrySet()) {
            topic.configs()
                    .add(
                            new CreateTopicsRequestData.CreatableTopicConfig()
                                    .setName(config.getKey())
                                    .setValue(config.getValue()));
        }
        CreateTopicsRequestData data =
                new CreateTopicsRequestData()
                        .setTimeoutMs(1000)
                        .setTopics(
                                new CreateTopicsRequestData.CreatableTopicCollection(
                                        Collections.singletonList(topic).iterator()));
        return new CreateTopicsRequest.Builder(data).build(version);
    }

    private static KafkaRequest kafkaRequest(
            ApiKeys apiKey, AbstractRequest requestBody, short version) {
        return new KafkaRequest(
                apiKey,
                version,
                new RequestHeader(apiKey, version, "client-id", 1),
                requestBody,
                "KAFKA",
                ByteBufAllocator.DEFAULT.buffer(),
                new TestingChannelHandlerContext(),
                new CompletableFuture<>());
    }

    private static AbstractResponse parseResponse(KafkaRequest request) {
        ByteBuf responseBuffer = request.responseBuffer();
        try {
            return AbstractResponse.parseResponse(responseBuffer.nioBuffer(), request.header());
        } finally {
            responseBuffer.release();
        }
    }
}

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

package org.apache.fluss.kafka.backend.admin;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.kafka.mapping.KafkaTopicMapper;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.gateway.AdminGateway;
import org.apache.fluss.rpc.gateway.AdminOperationAuthorizer;
import org.apache.fluss.rpc.messages.CreateTableRequest;
import org.apache.fluss.rpc.messages.DropTableRequest;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.netty.server.Session;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Uses the TabletServer's existing coordinator gateway for Kafka topic administration. */
@Internal
public final class GatewayKafkaTopicAdminBackend implements KafkaTopicAdminBackend {

    private final RpcGatewayService service;
    private final AdminGateway gateway;
    private final AdminOperationAuthorizer adminOperationAuthorizer;
    private final String databaseName;
    private final KafkaTopicMapper topicMapper;

    /** Creates a topic backend backed by the Fluss coordinator admin gateway. */
    public GatewayKafkaTopicAdminBackend(
            RpcGatewayService service,
            AdminGateway gateway,
            AdminOperationAuthorizer adminOperationAuthorizer,
            String databaseName) {
        this.service = checkNotNull(service);
        this.gateway = checkNotNull(gateway);
        this.adminOperationAuthorizer = checkNotNull(adminOperationAuthorizer);
        this.databaseName = checkNotNull(databaseName);
        this.topicMapper = new KafkaTopicMapper(databaseName);
    }

    @Override
    public CompletableFuture<List<TopicResult>> createTopics(
            List<CreateTopic> topics,
            boolean validateOnly,
            String listenerName,
            @Nullable InetAddress clientAddress) {
        return createTopics(
                topics, validateOnly, listenerName, clientAddress, FlussPrincipal.ANONYMOUS);
    }

    @Override
    public CompletableFuture<List<TopicResult>> createTopics(
            List<CreateTopic> topics,
            boolean validateOnly,
            String listenerName,
            @Nullable InetAddress clientAddress,
            FlussPrincipal principal) {
        List<CompletableFuture<TopicResult>> futures = new ArrayList<>();
        for (CreateTopic topic : topics) {
            futures.add(createTopic(topic, validateOnly, listenerName, clientAddress, principal));
        }
        return collect(futures);
    }

    @Override
    public CompletableFuture<List<TopicResult>> deleteTopics(
            List<DeleteTopic> topics, String listenerName, @Nullable InetAddress clientAddress) {
        return deleteTopics(topics, listenerName, clientAddress, FlussPrincipal.ANONYMOUS);
    }

    @Override
    public CompletableFuture<List<TopicResult>> deleteTopics(
            List<DeleteTopic> topics,
            String listenerName,
            @Nullable InetAddress clientAddress,
            FlussPrincipal principal) {
        List<CompletableFuture<TopicResult>> futures = new ArrayList<>();
        for (DeleteTopic topic : topics) {
            futures.add(deleteTopic(topic, listenerName, clientAddress, principal));
        }
        return collect(futures);
    }

    private CompletableFuture<TopicResult> createTopic(
            CreateTopic topic,
            boolean validateOnly,
            String listenerName,
            @Nullable InetAddress clientAddress,
            FlussPrincipal principal) {
        TableDescriptor descriptor = createDescriptor(topic);
        Session session = clientSession(listenerName, clientAddress, principal);
        try {
            adminOperationAuthorizer.authorize(
                    session, OperationType.CREATE, Resource.database(databaseName));
        } catch (RuntimeException failure) {
            return CompletableFuture.completedFuture(failed(topic.name(), failure));
        }
        if (validateOnly) {
            return CompletableFuture.completedFuture(success(topic, Uuid.ZERO_UUID));
        }

        CreateTableRequest request = new CreateTableRequest();
        request.setTableJson(descriptor.toJsonBytes())
                .setIgnoreIfExists(false)
                .setTablePath()
                .setDatabaseName(databaseName)
                .setTableName(topic.name());
        setCurrentSession(session);
        return gateway.createTable(request)
                .thenCompose(ignored -> getCreatedTopic(topic, session))
                .exceptionally(failure -> failed(topic.name(), failure));
    }

    private CompletableFuture<TopicResult> getCreatedTopic(CreateTopic topic, Session session) {
        GetTableInfoRequest request = new GetTableInfoRequest();
        request.setTablePath().setDatabaseName(databaseName).setTableName(topic.name());
        setCurrentSession(session);
        return gateway.getTableInfo(request)
                .thenApply(
                        response -> success(topic, topicMapper.toTopicId(response.getTableId())));
    }

    private CompletableFuture<TopicResult> deleteTopic(
            DeleteTopic topic,
            String listenerName,
            @Nullable InetAddress clientAddress,
            FlussPrincipal principal) {
        if (topic.name() == null) {
            return CompletableFuture.completedFuture(
                    new TopicResult(
                            null,
                            topic.topicId(),
                            Errors.UNKNOWN_TOPIC_ID,
                            "Deleting a Fluss table by Kafka topic id is not supported.",
                            -1,
                            (short) -1));
        }
        Session session = clientSession(listenerName, clientAddress, principal);
        try {
            adminOperationAuthorizer.authorize(
                    session, OperationType.DROP, Resource.table(databaseName, topic.name()));
        } catch (RuntimeException failure) {
            return CompletableFuture.completedFuture(
                    failed(topic.name(), topic.topicId(), failure));
        }
        DropTableRequest request = new DropTableRequest();
        request.setIgnoreIfNotExists(false)
                .setTablePath()
                .setDatabaseName(databaseName)
                .setTableName(topic.name());
        setCurrentSession(session);
        return gateway.dropTable(request)
                .thenApply(
                        ignored ->
                                new TopicResult(
                                        topic.name(),
                                        topic.topicId(),
                                        Errors.NONE,
                                        null,
                                        -1,
                                        (short) -1))
                .exceptionally(failure -> failed(topic.name(), topic.topicId(), failure));
    }

    private static TableDescriptor createDescriptor(CreateTopic topic) {
        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("record_key", dataType(topic.keyFormat()))
                                        .column("payload", dataType(topic.valueFormat()))
                                        .column(
                                                "event_time",
                                                DataTypes.TIMESTAMP_LTZ(3).copy(false))
                                        .column(
                                                "headers",
                                                DataTypes.ARRAY(
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "name",
                                                                        DataTypes.STRING()
                                                                                .copy(false)),
                                                                DataTypes.FIELD(
                                                                        "value",
                                                                        DataTypes.BYTES()))))
                                        .build())
                        .distributedBy(topic.numPartitions())
                        .property(ConfigOptions.TABLE_LOG_FORMAT, LogFormat.ARROW)
                        .customProperty(
                                KafkaDataFormat.KEY_FORMAT_CONFIG, topic.keyFormat().value())
                        .customProperty(
                                KafkaDataFormat.VALUE_FORMAT_CONFIG, topic.valueFormat().value());
        if (topic.replicationFactor() > 0) {
            builder.property(
                    ConfigOptions.TABLE_REPLICATION_FACTOR, (int) topic.replicationFactor());
        }
        return builder.build();
    }

    private static DataType dataType(KafkaDataFormat format) {
        return format == KafkaDataFormat.RAW ? DataTypes.BYTES() : DataTypes.STRING();
    }

    private static TopicResult success(CreateTopic topic, Uuid topicId) {
        return new TopicResult(
                topic.name(),
                topicId,
                Errors.NONE,
                null,
                topic.numPartitions(),
                topic.replicationFactor());
    }

    private static TopicResult failed(String topicName, Throwable failure) {
        return failed(topicName, Uuid.ZERO_UUID, failure);
    }

    private static TopicResult failed(String topicName, Uuid topicId, Throwable failure) {
        Throwable cause = unwrap(failure);
        return new TopicResult(
                topicName, topicId, toKafkaError(cause), cause.getMessage(), -1, (short) -1);
    }

    private static Errors toKafkaError(Throwable failure) {
        org.apache.fluss.rpc.protocol.Errors error =
                org.apache.fluss.rpc.protocol.Errors.forException(failure);
        switch (error) {
            case TABLE_ALREADY_EXIST:
                return Errors.TOPIC_ALREADY_EXISTS;
            case TABLE_NOT_EXIST:
            case UNKNOWN_TABLE_OR_BUCKET_EXCEPTION:
                return Errors.UNKNOWN_TOPIC_OR_PARTITION;
            case INVALID_TABLE_EXCEPTION:
                return Errors.INVALID_REQUEST;
            case INVALID_REPLICATION_FACTOR:
                return Errors.INVALID_REPLICATION_FACTOR;
            case BUCKET_MAX_NUM_EXCEPTION:
                return Errors.INVALID_PARTITIONS;
            case AUTHORIZATION_EXCEPTION:
                return Errors.TOPIC_AUTHORIZATION_FAILED;
            case DELETION_DISABLED_EXCEPTION:
                return Errors.TOPIC_DELETION_DISABLED;
            case REQUEST_TIME_OUT:
                return Errors.REQUEST_TIMED_OUT;
            case NOT_COORDINATOR_LEADER_EXCEPTION:
                return Errors.NOT_CONTROLLER;
            default:
                return Errors.UNKNOWN_SERVER_ERROR;
        }
    }

    private static Session clientSession(
            String listenerName, @Nullable InetAddress clientAddress, FlussPrincipal principal) {
        return new Session((short) 0, listenerName, false, clientAddress, checkNotNull(principal));
    }

    private void setCurrentSession(Session session) {
        service.setCurrentSession(session);
    }

    private static CompletableFuture<List<TopicResult>> collect(
            List<CompletableFuture<TopicResult>> futures) {
        CompletableFuture<Void> all =
                CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]));
        return all.thenApply(
                ignored -> {
                    List<TopicResult> results = new ArrayList<>();
                    for (CompletableFuture<TopicResult> future : futures) {
                        results.add(future.join());
                    }
                    return results;
                });
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}

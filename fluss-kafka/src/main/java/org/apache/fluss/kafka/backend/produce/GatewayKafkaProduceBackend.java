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

package org.apache.fluss.kafka.backend.produce;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.PartitionWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.TopicWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult.PartitionResult;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult.TopicResult;
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.kafka.transcode.KafkaRecordTranscoder;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.PbProduceLogRespForBucket;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.rpc.netty.server.Session;
import org.apache.fluss.security.acl.FlussPrincipal;

import org.apache.kafka.common.protocol.Errors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Adapts the local TabletServer write gateway to the Kafka Produce backend contract. */
@Internal
public final class GatewayKafkaProduceBackend implements KafkaProduceBackend {

    private final RpcGatewayService service;
    private final TabletServerGateway gateway;
    private final String databaseName;
    private final KafkaRecordTranscoder transcoder;

    /** Creates a Produce backend backed by the local TabletServer gateway. */
    public GatewayKafkaProduceBackend(
            RpcGatewayService service,
            TabletServerGateway gateway,
            String databaseName,
            KafkaRecordTranscoder transcoder) {
        this.service = checkNotNull(service);
        this.gateway = checkNotNull(gateway);
        this.databaseName = checkNotNull(databaseName);
        this.transcoder = checkNotNull(transcoder);
    }

    @Override
    public CompletableFuture<KafkaProduceResult> write(KafkaProduceCommand command) {
        List<CompletableFuture<TopicResult>> futures = new ArrayList<>();
        for (TopicWrite topic : command.topics()) {
            futures.add(writeTopic(command, topic));
        }
        CompletableFuture<Void> all =
                CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]));
        return all.thenApply(
                ignored -> {
                    List<TopicResult> results = new ArrayList<>();
                    for (CompletableFuture<TopicResult> future : futures) {
                        results.add(future.join());
                    }
                    return new KafkaProduceResult(results);
                });
    }

    private CompletableFuture<TopicResult> writeTopic(
            KafkaProduceCommand command, TopicWrite topic) {
        setCurrentSession(command);
        GetTableInfoRequest request = new GetTableInfoRequest();
        request.setTablePath().setDatabaseName(databaseName).setTableName(topic.topicName());
        return gateway.getTableInfo(request)
                .thenCompose(response -> produceTopic(command, topic, toTableInfo(topic, response)))
                .exceptionally(failure -> failedTopic(topic, failure));
    }

    private CompletableFuture<TopicResult> produceTopic(
            KafkaProduceCommand command, TopicWrite topic, TableInfo tableInfo) {
        ProduceLogRequest request =
                new ProduceLogRequest()
                        .setTableId(tableInfo.getTableId())
                        .setAcks(command.acks())
                        .setTimeoutMs(command.timeoutMs());
        List<BytesView> retainedRecords = new ArrayList<>();
        try {
            for (PartitionWrite partition : topic.partitions()) {
                BytesView records = transcoder.transcode(partition.records(), tableInfo);
                retainedRecords.add(records);
                request.addBucketsReq()
                        .setBucketId(partition.partitionId())
                        .setRecordsBytesView(records);
            }
        } catch (Exception e) {
            CompletableFuture<TopicResult> failure = new CompletableFuture<>();
            failure.completeExceptionally(e);
            return failure;
        }

        setCurrentSession(command);
        return gateway.produceLog(request)
                .thenApply(
                        response -> {
                            // Keep the native buffers reachable until the asynchronous append has
                            // completed.
                            retainedRecords.size();
                            return toTopicResult(topic, response);
                        });
    }

    private TableInfo toTableInfo(TopicWrite topic, GetTableInfoResponse response) {
        return TableInfo.of(
                TablePath.of(databaseName, topic.topicName()),
                response.getTableId(),
                response.getSchemaId(),
                TableDescriptor.fromJsonBytes(response.getTableJson()),
                response.hasRemoteDataDir() ? response.getRemoteDataDir() : null,
                response.getCreatedTime(),
                response.getModifiedTime());
    }

    private static TopicResult toTopicResult(TopicWrite topic, ProduceLogResponse response) {
        Map<Integer, PbProduceLogRespForBucket> responses = new HashMap<>();
        for (PbProduceLogRespForBucket bucket : response.getBucketsRespsList()) {
            responses.put(bucket.getBucketId(), bucket);
        }
        List<PartitionResult> partitions = new ArrayList<>();
        for (PartitionWrite partition : topic.partitions()) {
            PbProduceLogRespForBucket bucket = responses.get(partition.partitionId());
            if (bucket == null) {
                partitions.add(
                        new PartitionResult(
                                partition.partitionId(),
                                Errors.UNKNOWN_SERVER_ERROR,
                                -1L,
                                "Fluss Produce response omitted this bucket."));
            } else if (bucket.hasErrorCode()) {
                partitions.add(
                        new PartitionResult(
                                partition.partitionId(),
                                toKafkaError(
                                        org.apache.fluss.rpc.protocol.Errors.forCode(
                                                bucket.getErrorCode())),
                                -1L,
                                bucket.hasErrorMessage() ? bucket.getErrorMessage() : null));
            } else {
                partitions.add(
                        new PartitionResult(
                                partition.partitionId(),
                                Errors.NONE,
                                bucket.hasBaseOffset() ? bucket.getBaseOffset() : -1L,
                                null));
            }
        }
        return new TopicResult(topic.topicName(), partitions);
    }

    private static TopicResult failedTopic(TopicWrite topic, Throwable failure) {
        Throwable cause = unwrap(failure);
        Errors kafkaError =
                cause instanceof KafkaRecordEncodingException
                        ? Errors.CORRUPT_MESSAGE
                        : cause instanceof IllegalArgumentException
                                ? Errors.INVALID_REQUEST
                                : toKafkaError(
                                        org.apache.fluss.rpc.protocol.Errors.forException(cause));
        List<PartitionResult> partitions = new ArrayList<>();
        for (PartitionWrite partition : topic.partitions()) {
            partitions.add(
                    new PartitionResult(
                            partition.partitionId(), kafkaError, -1L, cause.getMessage()));
        }
        return new TopicResult(topic.topicName(), partitions);
    }

    private static Errors toKafkaError(org.apache.fluss.rpc.protocol.Errors error) {
        switch (error) {
            case NONE:
                return Errors.NONE;
            case TABLE_NOT_EXIST:
            case UNKNOWN_TABLE_OR_BUCKET_EXCEPTION:
                return Errors.UNKNOWN_TOPIC_OR_PARTITION;
            case NOT_LEADER_OR_FOLLOWER:
                return Errors.NOT_LEADER_OR_FOLLOWER;
            case LEADER_NOT_AVAILABLE_EXCEPTION:
                return Errors.LEADER_NOT_AVAILABLE;
            case RECORD_TOO_LARGE_EXCEPTION:
                return Errors.MESSAGE_TOO_LARGE;
            case CORRUPT_MESSAGE:
            case CORRUPT_RECORD_EXCEPTION:
                return Errors.CORRUPT_MESSAGE;
            case INVALID_REQUIRED_ACKS:
                return Errors.INVALID_REQUIRED_ACKS;
            case REQUEST_TIME_OUT:
                return Errors.REQUEST_TIMED_OUT;
            case NOT_ENOUGH_REPLICAS_EXCEPTION:
                return Errors.NOT_ENOUGH_REPLICAS;
            case NOT_ENOUGH_REPLICAS_AFTER_APPEND_EXCEPTION:
                return Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND;
            case AUTHORIZATION_EXCEPTION:
                return Errors.TOPIC_AUTHORIZATION_FAILED;
            case LOG_STORAGE_EXCEPTION:
            case STORAGE_EXCEPTION:
            case DISK_WRITE_LOCKED:
                return Errors.KAFKA_STORAGE_ERROR;
            default:
                return Errors.UNKNOWN_SERVER_ERROR;
        }
    }

    private void setCurrentSession(KafkaProduceCommand command) {
        service.setCurrentSession(
                new Session(
                        (short) 0,
                        command.listenerName(),
                        false,
                        command.clientAddress(),
                        FlussPrincipal.ANONYMOUS));
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}

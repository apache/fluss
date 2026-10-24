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
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.PartitionWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.TopicWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult.PartitionResult;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult.TopicResult;
import org.apache.fluss.kafka.mapping.KafkaTopicMapper;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaException;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaResolver;
import org.apache.fluss.kafka.transcode.EncodedRecords;
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.kafka.transcode.KafkaRecordTranscoder;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.bytesview.MemorySegmentBytesView;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.PbProduceLogRespForBucket;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.rpc.netty.server.Session;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.protocol.Errors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Adapts the local TabletServer write gateway to the Kafka Produce backend contract. */
@Internal
public final class GatewayKafkaProduceBackend implements KafkaProduceBackend {

    private final RpcGatewayService service;
    private final TabletServerGateway gateway;
    private final KafkaTopicMapper topicMapper;
    private final KafkaTopicSchemaResolver schemaResolver = new KafkaTopicSchemaResolver();
    private final KafkaRecordTranscoder transcoder;
    private final KafkaTableInfoCache tableInfoCache = new KafkaTableInfoCache();

    /** Creates a Produce backend that runs on the calling RPC request processor. */
    public GatewayKafkaProduceBackend(
            RpcGatewayService service,
            TabletServerGateway gateway,
            KafkaTopicMapper topicMapper,
            KafkaRecordTranscoder transcoder) {
        this.service = checkNotNull(service);
        this.gateway = checkNotNull(gateway);
        this.topicMapper = checkNotNull(topicMapper);
        this.transcoder = checkNotNull(transcoder);
    }

    @Override
    public CompletableFuture<KafkaProduceResult> write(KafkaProduceCommand command) {
        // Consume borrowed payloads and submit every append before returning to the RPC worker.
        // Only the acknowledgements remain asynchronous, as in the native Produce path.
        Map<TablePath, TableInfo> metadataByTable = new HashMap<>();
        List<CompletableFuture<TopicResult>> topics = new ArrayList<>();
        for (TopicWrite topic : command.topics()) {
            topics.add(writeTopic(command, topic, metadataByTable));
        }
        return FutureUtils.combineAll(topics)
                .thenApply(results -> new KafkaProduceResult(new ArrayList<>(results)));
    }

    private CompletableFuture<TopicResult> writeTopic(
            KafkaProduceCommand command,
            TopicWrite topic,
            Map<TablePath, TableInfo> metadataByTable) {
        try {
            TablePath path = topicMapper.toTablePath(topic.topicName());
            TableInfo tableInfo = metadataByTable.get(path);
            if (tableInfo == null) {
                tableInfo = loadTableInfo(command, path);
                metadataByTable.put(path, tableInfo);
            }
            List<CompletableFuture<PartitionResult>> partitions = new ArrayList<>();
            for (PartitionWrite partition : topic.partitions()) {
                partitions.add(producePartition(command, partition, tableInfo));
            }
            // Do not retain the topic's borrowed records while waiting for acknowledgements.
            String topicName = topic.topicName();
            return FutureUtils.combineAll(partitions)
                    .thenApply(results -> new TopicResult(topicName, new ArrayList<>(results)));
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            return CompletableFuture.completedFuture(failedTopic(topic, failure));
        } catch (Exception failure) {
            return CompletableFuture.completedFuture(failedTopic(topic, failure));
        }
    }

    private TableInfo loadTableInfo(KafkaProduceCommand command, TablePath path) throws Exception {
        setCurrentSession(command);
        GetTableInfoRequest request = new GetTableInfoRequest();
        request.setTablePath()
                .setDatabaseName(path.getDatabaseName())
                .setTableName(path.getTableName());
        GetTableInfoResponse response =
                gateway.getTableInfo(request)
                        .get(Math.max(0, command.timeoutMs()), TimeUnit.MILLISECONDS);
        return toTableInfo(path, response);
    }

    private CompletableFuture<PartitionResult> producePartition(
            KafkaProduceCommand command, PartitionWrite partition, TableInfo tableInfo) {
        int partitionId = partition.partitionId();
        if (partitionId < 0 || partitionId >= tableInfo.getNumBuckets()) {
            return CompletableFuture.completedFuture(
                    new PartitionResult(
                            partitionId,
                            Errors.UNKNOWN_TOPIC_OR_PARTITION,
                            -1L,
                            "Partition is outside the table bucket range."));
        }
        try (EncodedRecords converted =
                transcoder.transcode(partitionId, partition.records(), tableInfo)) {
            MemoryLogRecords records = converted.records();
            ProduceLogRequest request =
                    new ProduceLogRequest()
                            .setTableId(tableInfo.getTableId())
                            .setAcks(command.acks())
                            .setTimeoutMs(command.timeoutMs());
            request.addBucketsReq()
                    .setBucketId(partitionId)
                    .setRecordsBytesView(
                            new MemorySegmentBytesView(
                                    records.getMemorySegment(),
                                    records.getPosition(),
                                    records.sizeInBytes()));
            setCurrentSession(command);
            CompletableFuture<ProduceLogResponse> appended = gateway.produceLog(request);
            // The local gateway consumes the records before returning; its future only waits
            // for acknowledgements. Return pooled memory even while replication is pending.
            return appended.handle(
                    (response, failure) ->
                            failure == null
                                    ? toPartitionResult(partitionId, response)
                                    : failedPartition(partitionId, failure));
        } catch (Exception failure) {
            return CompletableFuture.completedFuture(failedPartition(partitionId, failure));
        }
    }

    private TableInfo toTableInfo(TablePath path, GetTableInfoResponse response) {
        return tableInfoCache.getOrLoad(
                path,
                response,
                () -> {
                    TableInfo tableInfo = parseTableInfo(path, response);
                    TableDescriptor descriptor = tableInfo.toTableDescriptor();
                    if (!schemaResolver.isEnabled(descriptor)) {
                        throw new TableNotExistException(
                                "Table " + path + " is not enabled for Kafka.");
                    }
                    schemaResolver.resolve(descriptor);
                    return tableInfo;
                });
    }

    private static TableInfo parseTableInfo(TablePath path, GetTableInfoResponse response) {
        return TableInfo.of(
                path,
                response.getTableId(),
                response.getSchemaId(),
                TableDescriptor.fromJsonBytes(response.getTableJson()),
                response.hasRemoteDataDir() ? response.getRemoteDataDir() : null,
                response.getCreatedTime(),
                response.getModifiedTime(),
                response.hasBucketCountEpoch() ? response.getBucketCountEpoch() : 0L);
    }

    private static PartitionResult toPartitionResult(int partitionId, ProduceLogResponse response) {
        for (PbProduceLogRespForBucket bucket : response.getBucketsRespsList()) {
            if (bucket.getBucketId() != partitionId) {
                continue;
            }
            if (bucket.hasErrorCode() && bucket.getErrorCode() != 0) {
                return new PartitionResult(
                        partitionId,
                        toKafkaError(
                                org.apache.fluss.rpc.protocol.Errors.forCode(
                                        bucket.getErrorCode())),
                        -1L,
                        bucket.hasErrorMessage() ? bucket.getErrorMessage() : null);
            }
            return new PartitionResult(
                    partitionId,
                    Errors.NONE,
                    bucket.hasBaseOffset() ? bucket.getBaseOffset() : -1L,
                    null);
        }
        return new PartitionResult(
                partitionId,
                Errors.UNKNOWN_SERVER_ERROR,
                -1L,
                "Fluss Produce response omitted this bucket.");
    }

    private static TopicResult failedTopic(TopicWrite topic, Throwable failure) {
        List<PartitionResult> partitions = new ArrayList<>();
        for (PartitionWrite partition : topic.partitions()) {
            partitions.add(failedPartition(partition.partitionId(), failure));
        }
        return new TopicResult(topic.topicName(), partitions);
    }

    private static PartitionResult failedPartition(int partitionId, Throwable failure) {
        Throwable cause = unwrap(failure);
        Errors kafkaError =
                cause instanceof RejectedExecutionException
                                || cause instanceof TimeoutException
                                || cause instanceof InterruptedException
                                || cause instanceof CancellationException
                        ? Errors.REQUEST_TIMED_OUT
                        : cause instanceof KafkaTopicSchemaException
                                        || cause instanceof InvalidTopicException
                                ? Errors.INVALID_TOPIC_EXCEPTION
                                : cause instanceof KafkaRecordEncodingException
                                        ? Errors.INVALID_RECORD
                                        : cause instanceof IllegalArgumentException
                                                ? Errors.INVALID_REQUEST
                                                : toKafkaError(
                                                        org.apache.fluss.rpc.protocol.Errors
                                                                .forException(cause));
        return new PartitionResult(partitionId, kafkaError, -1L, cause.getMessage());
    }

    private static Errors toKafkaError(org.apache.fluss.rpc.protocol.Errors error) {
        switch (error) {
            case NONE:
                return Errors.NONE;
            case TABLE_NOT_EXIST:
            case DATABASE_NOT_EXIST:
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
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}

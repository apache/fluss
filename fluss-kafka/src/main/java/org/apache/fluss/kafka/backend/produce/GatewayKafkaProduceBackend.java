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
import org.apache.fluss.kafka.error.KafkaErrorMapper;
import org.apache.fluss.kafka.mapping.KafkaTopicMapper;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaException;
import org.apache.fluss.kafka.transcode.ArrowKafkaRecordTranscoder;
import org.apache.fluss.kafka.transcode.EncodedRecords;
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.kafka.transcode.KafkaRecordTranscoder;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.entity.ProduceLogResultForBucket;
import org.apache.fluss.rpc.netty.server.Session;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.server.entity.UserContext;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.tablet.TabletService;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.protocol.Errors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

import static org.apache.fluss.kafka.error.KafkaErrorMapper.unwrap;
import static org.apache.fluss.security.acl.OperationType.WRITE;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Adapts local replicas to the Kafka Produce backend contract. */
@Internal
public final class GatewayKafkaProduceBackend implements KafkaProduceBackend {

    private final TabletService service;
    private final TabletServerMetadataCache metadataCache;
    private final ReplicaManager replicaManager;
    private final KafkaTopicMapper topicMapper;
    private final KafkaRecordTranscoder transcoder;

    /** Creates a Produce backend using the TabletServer's shared conversion memory. */
    public GatewayKafkaProduceBackend(TabletService service, KafkaTopicMapper topicMapper) {
        this.service = checkNotNull(service);
        this.metadataCache = service.getMetadataCache();
        this.replicaManager = service.getReplicaManager();
        this.topicMapper = checkNotNull(topicMapper);
        this.transcoder =
                new ArrowKafkaRecordTranscoder(
                        replicaManager.getArrowBufferAllocator(),
                        replicaManager.getMemorySegmentPool());
    }

    @Override
    public CompletableFuture<KafkaProduceResult> write(KafkaProduceCommand command) {
        setCurrentSession(command);
        List<CompletableFuture<TopicResult>> topics = new ArrayList<>();
        try (AppendBatch batch = new AppendBatch(command.timeoutMs(), command.acks())) {
            for (TopicWrite topic : command.topics()) {
                List<CompletableFuture<PartitionResult>> partitions = new ArrayList<>();
                for (PartitionWrite partition : topic.partitions()) {
                    CompletableFuture<PartitionResult> result = new CompletableFuture<>();
                    partitions.add(result);
                    TablePath path;
                    TableBucket bucket;
                    TableInfo tableInfo;
                    try {
                        path = topicMapper.toTablePath(topic.topicName());
                        long tableId =
                                metadataCache
                                        .getTableId(path)
                                        .orElseThrow(
                                                () ->
                                                        new TableNotExistException(
                                                                "Unknown table: " + path));
                        bucket = new TableBucket(tableId, partition.partitionId());
                        tableInfo = replicaManager.getReplicaOrException(bucket).getTableInfo();
                        if (!KafkaTableSchema.isEnabled(tableInfo.toTableDescriptor())) {
                            throw new TableNotExistException(
                                    "Table " + path + " is not enabled for Kafka.");
                        }
                    } catch (Exception failure) {
                        result.complete(
                                new PartitionResult(
                                        partition.partitionId(),
                                        Errors.UNKNOWN_TOPIC_OR_PARTITION,
                                        -1L,
                                        failure.getMessage()));
                        continue;
                    }
                    try {
                        service.authorizeTable(WRITE, path);
                        EncodedRecords converted =
                                transcoder.transcode(
                                        partition.partitionId(), partition.records(), tableInfo);
                        batch.convertedRecords.add(converted);
                        batch.entriesPerBucket.put(bucket, converted.records());
                        batch.results.put(bucket, result);
                    } catch (Exception failure) {
                        result.complete(failedPartition(partition.partitionId(), failure));
                    }
                }
                String topicName = topic.topicName();
                topics.add(
                        FutureUtils.combineAll(partitions)
                                .thenApply(
                                        results ->
                                                new TopicResult(
                                                        topicName, new ArrayList<>(results))));
            }
            batch.appendLogs();
        }
        return FutureUtils.combineAll(topics)
                .thenApply(results -> new KafkaProduceResult(new ArrayList<>(results)));
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

    /** Closes cached writers; shared memory remains owned by the TabletServer. */
    @Override
    public void close() throws Exception {
        transcoder.close();
    }

    private final class AppendBatch implements AutoCloseable {
        private final int timeoutMs;
        private final short acks;
        private final Map<TableBucket, MemoryLogRecords> entriesPerBucket = new HashMap<>();
        private final Map<TableBucket, CompletableFuture<PartitionResult>> results =
                new HashMap<>();
        private final List<EncodedRecords> convertedRecords = new ArrayList<>();

        private AppendBatch(int timeoutMs, short acks) {
            this.timeoutMs = timeoutMs;
            this.acks = acks;
        }

        private void appendLogs() {
            if (entriesPerBucket.isEmpty()) {
                return;
            }
            // The callback retains only bucket identities and result futures, never the input
            // payloads or converted records. Local append consumes all records synchronously.
            Map<TableBucket, CompletableFuture<PartitionResult>> pending = new HashMap<>(results);
            try {
                replicaManager.appendRecordsToLog(
                        timeoutMs,
                        acks,
                        entriesPerBucket,
                        new UserContext(FlussPrincipal.ANONYMOUS),
                        responses -> completeResults(pending, responses));
            } catch (Exception failure) {
                pending.forEach(
                        (bucket, result) ->
                                result.complete(failedPartition(bucket.getBucket(), failure)));
            } finally {
                close();
            }
        }

        @Override
        public void close() {
            convertedRecords.forEach(EncodedRecords::close);
            convertedRecords.clear();
            entriesPerBucket.clear();
        }
    }

    private static void completeResults(
            Map<TableBucket, CompletableFuture<PartitionResult>> pending,
            List<ProduceLogResultForBucket> responses) {
        for (ProduceLogResultForBucket response : responses) {
            CompletableFuture<PartitionResult> result = pending.remove(response.getTableBucket());
            if (result != null) {
                result.complete(
                        new PartitionResult(
                                response.getTableBucket().getBucket(),
                                KafkaErrorMapper.toKafkaError(response.getError().error()),
                                response.failed() ? -1L : response.getBaseOffset(),
                                response.getError().message()));
            }
        }
        pending.forEach(
                (bucket, result) ->
                        result.complete(
                                new PartitionResult(
                                        bucket.getBucket(),
                                        Errors.UNKNOWN_SERVER_ERROR,
                                        -1L,
                                        "Fluss Produce response omitted this bucket.")));
    }

    private static PartitionResult failedPartition(int partitionId, Throwable failure) {
        Throwable cause = unwrap(failure);
        Errors kafkaError;
        if (cause instanceof RejectedExecutionException
                || cause instanceof TimeoutException
                || cause instanceof InterruptedException
                || cause instanceof CancellationException) {
            kafkaError = Errors.REQUEST_TIMED_OUT;
        } else if (cause instanceof KafkaTopicSchemaException
                || cause instanceof InvalidTopicException) {
            kafkaError = Errors.INVALID_TOPIC_EXCEPTION;
        } else if (cause instanceof KafkaRecordEncodingException) {
            kafkaError = Errors.INVALID_RECORD;
        } else if (cause instanceof IllegalArgumentException) {
            kafkaError = Errors.INVALID_REQUEST;
        } else {
            kafkaError =
                    KafkaErrorMapper.toKafkaError(
                            org.apache.fluss.rpc.protocol.Errors.forException(cause));
        }
        return new PartitionResult(partitionId, kafkaError, -1L, cause.getMessage());
    }
}

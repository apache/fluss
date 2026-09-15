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

package org.apache.fluss.kafka.api.produce;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.KafkaRequestContext;
import org.apache.fluss.kafka.backend.produce.KafkaProduceBackend;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.PartitionWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.RecordHeader;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.TopicWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult.PartitionResult;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult.TopicResult;
import org.apache.fluss.kafka.dispatcher.KafkaApiHandler;
import org.apache.fluss.kafka.dispatcher.KafkaApiSpec;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidRequiredAcksException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Implements non-idempotent Kafka Produce versions 3 through 11. */
@Internal
public final class ProduceHandler implements KafkaApiHandler<ProduceRequest> {

    private static final short MIN_SUPPORTED_VERSION = 3;
    private static final KafkaApiSpec API_SPEC =
            new KafkaApiSpec(ApiKeys.PRODUCE, MIN_SUPPORTED_VERSION, (short) 11, true);

    private final KafkaProduceBackend backend;

    /** Creates a non-idempotent Produce handler. */
    public ProduceHandler(KafkaProduceBackend backend) {
        this.backend = checkNotNull(backend);
    }

    @Override
    public KafkaApiSpec apiSpec() {
        return API_SPEC;
    }

    @Override
    public CompletableFuture<? extends AbstractResponse> handle(
            KafkaRequestContext context, ProduceRequest request) {
        validateRequest(request);
        List<TopicWrite> topics = new ArrayList<>();
        Map<TopicPartition, PartitionResult> failures = new HashMap<>();
        for (TopicProduceData topic : request.data().topicData()) {
            List<PartitionWrite> partitions = new ArrayList<>();
            for (PartitionProduceData partition : topic.partitionData()) {
                try {
                    if (!Topic.isValid(topic.name())) {
                        throw new InvalidTopicException("Invalid Kafka topic name " + topic.name());
                    }
                    if (partition.index() < 0) {
                        throw new InvalidRequestException("Negative Kafka partition ID.");
                    }
                    partitions.add(
                            new PartitionWrite(
                                    partition.index(),
                                    copyRecords(request.version(), partition.records())));
                } catch (RuntimeException failure) {
                    failures.put(
                            new TopicPartition(topic.name(), partition.index()),
                            failedPartition(partition.index(), failure));
                }
            }
            if (!partitions.isEmpty()) {
                topics.add(new TopicWrite(topic.name(), partitions));
            }
        }
        KafkaProduceCommand command =
                new KafkaProduceCommand(
                        request.acks(),
                        request.timeout(),
                        topics,
                        context.listenerName(),
                        clientAddress(context.remoteAddress()));
        CompletableFuture<KafkaProduceResult> result;
        try {
            result =
                    topics.isEmpty()
                            ? CompletableFuture.completedFuture(
                                    new KafkaProduceResult(Collections.emptyList()))
                            : checkNotNull(backend.write(command));
        } catch (RuntimeException failure) {
            result = new CompletableFuture<>();
            result.completeExceptionally(failure);
        }
        return result.handle(
                (written, failure) -> {
                    Map<TopicPartition, PartitionResult> results = new HashMap<>(failures);
                    if (failure == null && written != null) {
                        for (TopicResult topic : written.topics()) {
                            for (PartitionResult partition : topic.partitions()) {
                                results.putIfAbsent(
                                        new TopicPartition(
                                                topic.topicName(), partition.partitionId()),
                                        partition);
                            }
                        }
                    }
                    List<TopicResult> ordered = new ArrayList<>();
                    for (TopicProduceData topic : request.data().topicData()) {
                        List<PartitionResult> partitions = new ArrayList<>();
                        for (PartitionProduceData partition : topic.partitionData()) {
                            TopicPartition key =
                                    new TopicPartition(topic.name(), partition.index());
                            PartitionResult value = results.get(key);
                            if (value == null) {
                                value =
                                        failedPartition(
                                                partition.index(),
                                                failure == null
                                                        ? new IllegalStateException(
                                                                "Produce backend omitted this partition.")
                                                        : failure);
                            }
                            partitions.add(value);
                        }
                        ordered.add(new TopicResult(topic.name(), partitions));
                    }
                    return toResponse(new KafkaProduceResult(ordered));
                });
    }

    private static PartitionResult failedPartition(int partitionId, Throwable failure) {
        while (failure instanceof CompletionException && failure.getCause() != null) {
            failure = failure.getCause();
        }
        return new PartitionResult(
                partitionId, Errors.forException(failure), -1L, failure.getMessage());
    }

    private static void validateRequest(ProduceRequest request) {
        Set<String> names = new HashSet<>();
        for (TopicProduceData topic : request.data().topicData()) {
            if (!names.add(topic.name())) {
                throw new InvalidRequestException("Duplicate Kafka topic in Produce request.");
            }
            Set<Integer> partitions = new HashSet<>();
            for (PartitionProduceData partition : topic.partitionData()) {
                if (!partitions.add(partition.index())) {
                    throw new InvalidRequestException(
                            "Duplicate Kafka partition in Produce request.");
                }
            }
        }
        if (request.transactionalId() != null) {
            throw new InvalidRequestException(
                    "Transactional Produce is not supported by the Fluss Kafka compatibility layer.");
        }
        if (request.acks() != -1 && request.acks() != 0 && request.acks() != 1) {
            throw new InvalidRequiredAcksException("Invalid required acks " + request.acks());
        }
    }

    private static List<KafkaProduceCommand.Record> copyRecords(
            short version, BaseRecords baseRecords) {
        if (!(baseRecords instanceof Records)) {
            throw new InvalidRequestException("Unsupported Kafka records representation.");
        }
        ProduceRequest.validateRecords(version, baseRecords);
        Records records = (Records) baseRecords;
        if (records.sizeInBytes() == 0) {
            throw new InvalidRequestException("Empty or truncated Kafka record batch.");
        }
        List<KafkaProduceCommand.Record> copied = new ArrayList<>();
        int validBytes = 0;
        for (RecordBatch batch : records.batches()) {
            validBytes += batch.sizeInBytes();
            batch.ensureValid();
            if (batch.hasProducerId() || batch.isTransactional() || batch.isControlBatch()) {
                throw new InvalidRequestException(
                        "Idempotent, transactional, and control record batches are not supported.");
            }
            for (org.apache.kafka.common.record.Record record : batch) {
                record.ensureValid();
                copied.add(
                        new KafkaProduceCommand.Record(
                                record.timestamp(),
                                copyBuffer(record.hasKey() ? record.key() : null),
                                copyBuffer(record.hasValue() ? record.value() : null),
                                copyHeaders(record.headers())));
            }
        }
        if (copied.isEmpty() || validBytes != records.sizeInBytes()) {
            throw new InvalidRequestException("Empty or truncated Kafka record batch.");
        }
        return copied;
    }

    private static ProduceResponse toResponse(KafkaProduceResult result) {
        ProduceResponseData data = new ProduceResponseData().setThrottleTimeMs(0);
        for (TopicResult topic : result.topics()) {
            ProduceResponseData.TopicProduceResponse topicResponse =
                    new ProduceResponseData.TopicProduceResponse().setName(topic.topicName());
            for (PartitionResult partition : topic.partitions()) {
                topicResponse
                        .partitionResponses()
                        .add(
                                new ProduceResponseData.PartitionProduceResponse()
                                        .setIndex(partition.partitionId())
                                        .setErrorCode(partition.error().code())
                                        .setBaseOffset(partition.baseOffset())
                                        .setLogAppendTimeMs(-1L)
                                        .setLogStartOffset(-1L)
                                        .setErrorMessage(partition.errorMessage()));
            }
            data.responses().add(topicResponse);
        }
        return new ProduceResponse(data);
    }

    private static List<RecordHeader> copyHeaders(Header[] headers) {
        List<RecordHeader> copied = new ArrayList<>(headers.length);
        for (Header header : headers) {
            copied.add(new RecordHeader(header.key(), header.value()));
        }
        return copied;
    }

    private static byte[] copyBuffer(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        ByteBuffer duplicate = buffer.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }

    private static InetAddress clientAddress(SocketAddress remoteAddress) {
        if (remoteAddress instanceof InetSocketAddress) {
            return ((InetSocketAddress) remoteAddress).getAddress();
        }
        return null;
    }
}

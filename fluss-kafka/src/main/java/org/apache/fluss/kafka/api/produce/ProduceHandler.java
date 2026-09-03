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

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidRequiredAcksException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
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
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Implements non-idempotent Kafka Produce versions 3 through 11. */
@Internal
public final class ProduceHandler implements KafkaApiHandler<ProduceRequest> {

    private static final short MIN_SUPPORTED_VERSION = 3;
    private static final KafkaApiSpec API_SPEC =
            new KafkaApiSpec(
                    ApiKeys.PRODUCE, MIN_SUPPORTED_VERSION, ApiKeys.PRODUCE.latestVersion(), true);

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
        for (TopicProduceData topic : request.data().topicData()) {
            if (!Topic.isValid(topic.name())) {
                throw new InvalidTopicException("Invalid Kafka topic name " + topic.name());
            }
            List<PartitionWrite> partitions = new ArrayList<>();
            for (PartitionProduceData partition : topic.partitionData()) {
                partitions.add(
                        new PartitionWrite(
                                partition.index(),
                                copyRecords(request.version(), partition.records())));
            }
            topics.add(new TopicWrite(topic.name(), partitions));
        }
        KafkaProduceCommand command =
                new KafkaProduceCommand(
                        request.acks(),
                        request.timeout(),
                        topics,
                        context.listenerName(),
                        clientAddress(context.remoteAddress()));
        return backend.write(command).thenApply(ProduceHandler::toResponse);
    }

    private static void validateRequest(ProduceRequest request) {
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
        List<KafkaProduceCommand.Record> copied = new ArrayList<>();
        for (RecordBatch batch : records.batches()) {
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

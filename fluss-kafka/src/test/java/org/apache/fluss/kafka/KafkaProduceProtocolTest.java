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

import org.apache.fluss.kafka.api.produce.ProduceHandler;
import org.apache.fluss.kafka.backend.produce.KafkaProduceBackend;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult.PartitionResult;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult.TopicResult;
import org.apache.fluss.kafka.dispatcher.KafkaApiRegistry;
import org.apache.fluss.kafka.dispatcher.KafkaRequestDispatcher;
import org.apache.fluss.kafka.error.KafkaErrorMapper;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.requests.ResponseHeader;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Protocol tests independent of the Fluss append and record conversion backends. */
class KafkaProduceProtocolTest {

    @Test
    void testSupportedVersionsCopyRecordsAndForwardContext() {
        for (short version = 3; version <= 11; version++) {
            AtomicReference<KafkaProduceCommand> copied = new AtomicReference<>();
            ProduceResponse response =
                    dispatch(
                            request(version, (short) -1, partition(0, records())),
                            command -> {
                                copied.set(command);
                                return successful(command);
                            });
            assertThat(response.errorCounts()).containsOnlyKeys(Errors.NONE);
            assertThat(
                            response.data()
                                    .responses()
                                    .find("topic")
                                    .partitionResponses()
                                    .get(0)
                                    .baseOffset())
                    .isEqualTo(42L);
            KafkaProduceCommand command = copied.get();
            assertThat(command.acks()).isEqualTo((short) -1);
            assertThat(command.timeoutMs()).isEqualTo(4321);
            assertThat(command.listenerName()).isEqualTo("KAFKA");
            KafkaProduceCommand.Record record =
                    command.topics().get(0).partitions().get(0).records().get(0);
            assertThat(record.timestamp()).isEqualTo(123L);
            assertThat(record.key()).containsExactly((byte) 1);
            assertThat(record.value()).containsExactly((byte) 2);
            assertThat(record.headers()).hasSize(1);
            assertThat(record.headers().get(0).value()).isNull();
            byte[] key = record.key();
            key[0] = 9;
            assertThat(record.key()).containsExactly((byte) 1);
        }
    }

    @Test
    void testCorruptPartitionDoesNotSuppressValidPartition() {
        MemoryRecords corrupt = records();
        corrupt.buffer().put(corrupt.sizeInBytes() - 1, (byte) 99);
        ProduceResponse response =
                dispatch(
                        request(
                                (short) 11,
                                (short) 1,
                                partition(0, corrupt),
                                partition(1, records())),
                        command -> {
                            assertThat(command.topics().get(0).partitions()).hasSize(1);
                            assertThat(command.topics().get(0).partitions().get(0).partitionId())
                                    .isEqualTo(1);
                            return successful(command);
                        });
        assertErrors(response, Errors.CORRUPT_MESSAGE, Errors.NONE);
    }

    @Test
    void testInvalidTopicIsIsolated() {
        ProduceRequest request = request((short) 11, (short) 1, partition(0, records()));
        request.data()
                .topicData()
                .add(
                        new TopicProduceData()
                                .setName("bad/name")
                                .setPartitionData(
                                        Collections.singletonList(partition(0, records()))));
        ProduceResponse response =
                dispatch(
                        request,
                        command -> {
                            assertThat(command.topics()).hasSize(1);
                            return successful(command);
                        });
        assertThat(response.errorCounts())
                .containsEntry(Errors.NONE, 1)
                .containsEntry(Errors.INVALID_TOPIC_EXCEPTION, 1);
    }

    @Test
    void testEmptyNegativeAndIdempotentPartitionsAreRejectedLocally() {
        MemoryRecords idempotent =
                MemoryRecords.withIdempotentRecords(
                        Compression.NONE, 10L, (short) 0, 0, new SimpleRecord(new byte[] {1}));
        ProduceResponse response =
                dispatch(
                        request(
                                (short) 11,
                                (short) 1,
                                partition(0, MemoryRecords.EMPTY),
                                partition(-1, records()),
                                partition(2, idempotent),
                                partition(3, records())),
                        command -> {
                            assertThat(command.topics().get(0).partitions()).hasSize(1);
                            return successful(command);
                        });
        assertErrors(
                response,
                Errors.INVALID_RECORD,
                Errors.INVALID_REQUEST,
                Errors.INVALID_REQUEST,
                Errors.NONE);
    }

    @Test
    void testRequestValidationNeverCallsBackend() {
        KafkaProduceBackend unused =
                command -> {
                    throw new AssertionError("Backend must not be called");
                };
        assertErrors(
                dispatch(request((short) 11, (short) 2, partition(0, records())), unused),
                Errors.INVALID_REQUIRED_ACKS);
        ProduceRequest transactional = request((short) 11, (short) 1, partition(0, records()));
        transactional.data().setTransactionalId("transaction");
        assertErrors(
                dispatch(new ProduceRequest(transactional.data(), transactional.version()), unused),
                Errors.INVALID_REQUEST);
        assertErrors(
                dispatch(
                        request(
                                (short) 11,
                                (short) 1,
                                partition(0, records()),
                                partition(0, records())),
                        unused),
                Errors.INVALID_REQUEST);
        assertErrors(
                dispatch(request((short) 2, (short) 1, partition(0, records())), unused),
                Errors.UNSUPPORTED_VERSION);
    }

    @Test
    void testBackendFailuresAndMissingResultsPreserveValidationErrors() {
        for (boolean synchronous : Arrays.asList(true, false)) {
            ProduceResponse response =
                    dispatch(
                            request(
                                    (short) 11,
                                    (short) 1,
                                    partition(0, MemoryRecords.EMPTY),
                                    partition(1, records())),
                            command -> {
                                if (synchronous) {
                                    throw new TimeoutException("append timed out");
                                }
                                CompletableFuture<KafkaProduceResult> failed =
                                        new CompletableFuture<>();
                                failed.completeExceptionally(
                                        new TimeoutException("append timed out"));
                                return failed;
                            });
            assertErrors(response, Errors.INVALID_RECORD, Errors.REQUEST_TIMED_OUT);
        }
        assertErrors(
                dispatch(
                        request((short) 11, (short) 1, partition(0, records())),
                        command ->
                                CompletableFuture.completedFuture(
                                        new KafkaProduceResult(Collections.emptyList()))),
                Errors.UNKNOWN_SERVER_ERROR);
    }

    @Test
    void testAcksZeroSuccessAndFailureReleaseBufferWithoutSendingResponse() {
        for (boolean failure : Arrays.asList(false, true)) {
            RequestChannel requests = new RequestChannel(100);
            EmbeddedChannel channel =
                    new EmbeddedChannel(
                            new KafkaCommandDecoder(new RequestChannel[] {requests}, "KAFKA"));
            ProduceRequest body = request((short) 11, (short) 0, partition(0, records()));
            RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, (short) 11, "producer", 1);
            ByteBuf buffer =
                    Unpooled.wrappedBuffer(
                            RequestUtils.serialize(
                                    header.data(),
                                    header.headerVersion(),
                                    body.data(),
                                    body.version()));
            CompletableFuture<KafkaProduceResult> pending = new CompletableFuture<>();
            try {
                channel.writeInbound(buffer);
                KafkaRequest parsed = (KafkaRequest) requests.pollRequest(1000);
                dispatcher(command -> pending)
                        .dispatch(parsed)
                        .whenComplete(
                                (response, error) -> {
                                    if (error == null) {
                                        parsed.complete(response);
                                    } else {
                                        parsed.fail(error);
                                    }
                                });
                assertThat(parsed.future()).isNotDone();
                if (failure) {
                    pending.completeExceptionally(new TimeoutException("failure"));
                } else {
                    pending.complete(
                            new KafkaProduceResult(
                                    Collections.singletonList(
                                            new TopicResult(
                                                    "topic",
                                                    Collections.singletonList(
                                                            new PartitionResult(
                                                                    0, Errors.NONE, 42L, null))))));
                }
                channel.runPendingTasks();
                assertThat(parsed.future()).isDone();
                assertThat((Object) channel.readOutbound()).isNull();
                assertThat(buffer.refCnt()).isZero();
            } finally {
                channel.finishAndReleaseAll();
            }
        }
    }

    private static ProduceResponse dispatch(ProduceRequest body, KafkaProduceBackend backend) {
        ByteBuf buffer = Unpooled.buffer(1);
        RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, body.version(), "producer", 1);
        KafkaRequest request =
                new KafkaRequest(
                        ApiKeys.PRODUCE,
                        body.version(),
                        header,
                        body,
                        "KAFKA",
                        buffer,
                        new TestingChannelHandlerContext(),
                        new CompletableFuture<>());
        buffer.release();
        request.complete(dispatcher(backend).dispatch(request).join());
        ByteBuf response = request.responseBuffer();
        try {
            ByteBuffer bytes = response.nioBuffer();
            ResponseHeader.parse(bytes, header.toResponseHeader().headerVersion());
            return (ProduceResponse)
                    AbstractResponse.parseResponse(ApiKeys.PRODUCE, bytes, body.version());
        } finally {
            response.release();
        }
    }

    private static KafkaRequestDispatcher dispatcher(KafkaProduceBackend backend) {
        KafkaApiRegistry registry = new KafkaApiRegistry();
        registry.register(new ProduceHandler(backend));
        registry.freeze();
        return new KafkaRequestDispatcher(registry, new KafkaErrorMapper());
    }

    private static CompletableFuture<KafkaProduceResult> successful(KafkaProduceCommand command) {
        List<TopicResult> topics = new ArrayList<>();
        for (KafkaProduceCommand.TopicWrite topic : command.topics()) {
            List<PartitionResult> partitions = new ArrayList<>();
            for (KafkaProduceCommand.PartitionWrite partition : topic.partitions()) {
                partitions.add(
                        new PartitionResult(partition.partitionId(), Errors.NONE, 42L, null));
            }
            topics.add(new TopicResult(topic.topicName(), partitions));
        }
        return CompletableFuture.completedFuture(new KafkaProduceResult(topics));
    }

    private static ProduceRequest request(
            short version, short acks, PartitionProduceData... partitions) {
        TopicProduceData topic =
                new TopicProduceData().setName("topic").setPartitionData(Arrays.asList(partitions));
        return new ProduceRequest(
                new ProduceRequestData()
                        .setAcks(acks)
                        .setTimeoutMs(4321)
                        .setTopicData(
                                new ProduceRequestData.TopicProduceDataCollection(
                                        Collections.singletonList(topic).iterator())),
                version);
    }

    private static PartitionProduceData partition(int index, MemoryRecords records) {
        return new PartitionProduceData().setIndex(index).setRecords(records);
    }

    private static MemoryRecords records() {
        return MemoryRecords.withRecords(
                RecordBatch.MAGIC_VALUE_V2,
                0L,
                Compression.NONE,
                new SimpleRecord(
                        123L,
                        new byte[] {1},
                        new byte[] {2},
                        new Header[] {new RecordHeader("header", null)}));
    }

    private static void assertErrors(ProduceResponse response, Errors... errors) {
        assertThat(response.data().responses().find("topic").partitionResponses())
                .extracting(PartitionProduceResponse::errorCode)
                .containsExactly(Arrays.stream(errors).map(Errors::code).toArray(Short[]::new));
    }
}

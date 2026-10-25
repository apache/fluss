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
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.async.ByteBufferFeeder;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufUtil;
import org.apache.fluss.shaded.netty4.io.netty.buffer.PooledByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.requests.ResponseHeader;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests borrowed Produce payloads across decoding, worker cleanup and response completion. */
class KafkaProduceBufferLifecycleTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @ParameterizedTest
    @CsvSource({
        "3, false, 0, false",
        "3, true, 1, true",
        "9, false, -1, false",
        "9, true, 0, true",
        "11, false, 1, false",
        "11, true, -1, true"
    })
    void testZeroCopyDecodeReleasesBufferBeforeAsyncWriteCompletes(
            short version, boolean direct, short acks, boolean failWrite) {
        RequestChannel requests = new RequestChannel(100);
        EmbeddedChannel channel = new EmbeddedChannel(new KafkaCommandDecoder(requests, "KAFKA"));
        ByteBuf buffer = requestBuffer(version, acks, direct, "{\"value\":2,\"text\":\"中文\"}");
        CompletableFuture<KafkaProduceResult> pending = new CompletableFuture<>();
        AtomicReference<JsonNode> decodedKey = new AtomicReference<>();
        AtomicReference<JsonNode> decodedValue = new AtomicReference<>();
        KafkaRequest parsed = null;
        try {
            channel.writeInbound(buffer);
            parsed = (KafkaRequest) requests.pollRequest(1000);
            assertThat(parsed).isNotNull();
            // The decoder released its reference; the queued request still owns the input.
            assertThat(buffer.refCnt()).isOne();
            processRequest(
                    parsed,
                    command -> {
                        assertThat(buffer.refCnt()).isOne();
                        KafkaProduceCommand.Record record =
                                command.topics().get(0).partitions().get(0).records().get(0);
                        assertThat(record.key().isDirect()).isEqualTo(direct);
                        assertThat(record.value().isDirect()).isEqualTo(direct);
                        assertThat(record.key().isReadOnly()).isTrue();
                        assertThat(record.value().isReadOnly()).isTrue();

                        // Change the original frame after validation and command construction.
                        // A copied payload would still decode the old values.
                        replaceLastByte(buffer, "\"id\":1", (byte) '3');
                        replaceLastByte(buffer, "\"value\":2", (byte) '7');
                        decodedKey.set(decodeJson(record.key()));
                        decodedValue.set(decodeJson(record.value()));
                        assertThat(decodedKey.get().get("id").asInt()).isEqualTo(3);
                        assertThat(decodedValue.get().get("value").asInt()).isEqualTo(7);

                        // Materialized JSON must survive destruction of the input bytes.
                        buffer.setZero(buffer.readerIndex(), buffer.readableBytes());
                        return pending;
                    });
            assertThat(buffer.refCnt()).isZero();
            assertThat(pending).isNotDone();
            assertThat(parsed.future()).isNotDone();
            assertThat((Object) channel.readOutbound()).isNull();
            assertThat(decodedKey.get().get("id").asInt()).isEqualTo(3);
            assertThat(decodedValue.get().get("value").asInt()).isEqualTo(7);
            assertThat(decodedValue.get().get("text").asText()).isEqualTo("中文");

            if (failWrite) {
                pending.completeExceptionally(new TimeoutException("write failed"));
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
            assertResponse(channel, parsed, failWrite ? Errors.REQUEST_TIMED_OUT : Errors.NONE);
            // Response cleanup must not release the request's ownership a second time.
            parsed.releaseBuffer();
            assertThat(buffer.refCnt()).isZero();
            channel.checkException();
        } finally {
            if (parsed != null) {
                parsed.releaseBuffer();
            }
            channel.finishAndReleaseAll();
        }
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    void testSynchronousDecodeFailureReleasesBuffer(short acks) {
        RequestChannel requests = new RequestChannel(100);
        EmbeddedChannel channel = new EmbeddedChannel(new KafkaCommandDecoder(requests, "KAFKA"));
        ByteBuf buffer = requestBuffer((short) 11, acks, true, "{\"value\":");
        KafkaRequest parsed = null;
        try {
            channel.writeInbound(buffer);
            parsed = (KafkaRequest) requests.pollRequest(1000);
            assertThat(parsed).isNotNull();
            processRequest(
                    parsed,
                    command -> {
                        assertThat(buffer.refCnt()).isOne();
                        decodeJson(
                                command.topics()
                                        .get(0)
                                        .partitions()
                                        .get(0)
                                        .records()
                                        .get(0)
                                        .value());
                        throw new AssertionError(
                                "Malformed JSON must fail before starting a write.");
                    });
            assertThat(buffer.refCnt()).isZero();
            channel.runPendingTasks();
            assertResponse(channel, parsed, Errors.INVALID_RECORD);
            assertThat(buffer.refCnt()).isZero();
            channel.checkException();
        } finally {
            if (parsed != null) {
                parsed.releaseBuffer();
            }
            channel.finishAndReleaseAll();
        }
    }

    private static void processRequest(KafkaRequest request, KafkaProduceBackend backend) {
        KafkaRequestDispatcher dispatcher =
                new KafkaRequestDispatcher(
                        new KafkaApiRegistry(
                                Collections.singletonList(new ProduceHandler(backend))),
                        new KafkaErrorMapper());
        try {
            dispatcher
                    .dispatch(request)
                    .whenComplete(
                            (response, failure) -> {
                                if (failure == null) {
                                    request.complete(response);
                                } else {
                                    request.fail(failure);
                                }
                            });
        } finally {
            // Mirror RequestProcessor's cleanup immediately after handler invocation, without
            // waiting for the write future or letting response cleanup mask a missing release.
            request.releaseBuffer();
        }
    }

    private static ByteBuf requestBuffer(
            short version, short acks, boolean direct, String jsonValue) {
        MemoryRecords records =
                MemoryRecords.withRecords(
                        Compression.NONE,
                        new SimpleRecord(
                                "{\"id\":1}".getBytes(StandardCharsets.UTF_8),
                                jsonValue.getBytes(StandardCharsets.UTF_8)));
        ProduceRequestData data = new ProduceRequestData().setAcks(acks).setTimeoutMs(1000);
        data.topicData()
                .add(
                        new TopicProduceData()
                                .setName("topic")
                                .setPartitionData(
                                        Collections.singletonList(
                                                new PartitionProduceData()
                                                        .setIndex(0)
                                                        .setRecords(records))));
        ProduceRequest body = new ProduceRequest(data, version);
        RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, version, "producer", 1);
        ByteBuffer serialized =
                RequestUtils.serialize(header.data(), header.headerVersion(), body.data(), version);
        int prefix = 7;
        ByteBuf buffer =
                direct
                        ? PooledByteBufAllocator.DEFAULT.directBuffer(
                                prefix + serialized.remaining())
                        : PooledByteBufAllocator.DEFAULT.heapBuffer(
                                prefix + serialized.remaining());
        buffer.writeZero(prefix).writeBytes(serialized).readerIndex(prefix);
        return buffer;
    }

    private static JsonNode decodeJson(ByteBuffer input) {
        try (JsonParser parser = MAPPER.getFactory().createNonBlockingByteBufferParser()) {
            ByteBufferFeeder feeder = (ByteBufferFeeder) parser.getNonBlockingInputFeeder();
            feeder.feedInput(input.asReadOnlyBuffer());
            feeder.endOfInput();
            JsonNode json = MAPPER.readTree(parser);
            if (json == null || !json.isObject() || parser.nextToken() != null) {
                throw new InvalidRecordException("Expected exactly one JSON object.");
            }
            return json;
        } catch (IOException e) {
            throw new InvalidRecordException("Invalid JSON payload.", e);
        }
    }

    private static void replaceLastByte(ByteBuf buffer, String pattern, byte replacement) {
        ByteBuf needle = Unpooled.wrappedBuffer(pattern.getBytes(StandardCharsets.UTF_8));
        try {
            int index = ByteBufUtil.indexOf(needle, buffer);
            assertThat(index).isGreaterThanOrEqualTo(buffer.readerIndex());
            buffer.setByte(index + needle.readableBytes() - 1, replacement);
        } finally {
            needle.release();
        }
    }

    private static void assertResponse(
            EmbeddedChannel channel, KafkaRequest request, Errors expectedError) {
        ProduceResponse response = (ProduceResponse) request.future().join();
        assertThat(response.errorCounts()).containsOnlyKeys(expectedError);
        ProduceRequest body = request.request();
        ByteBuf outbound = channel.readOutbound();
        if (body.acks() == 0) {
            assertThat(outbound).isNull();
        } else {
            assertThat(outbound).isNotNull();
            try {
                ByteBuffer bytes = outbound.nioBuffer();
                ResponseHeader header =
                        ResponseHeader.parse(
                                bytes, request.header().toResponseHeader().headerVersion());
                assertThat(header.correlationId()).isEqualTo(request.header().correlationId());
                AbstractResponse wireResponse =
                        AbstractResponse.parseResponse(ApiKeys.PRODUCE, bytes, body.version());
                assertThat(wireResponse.errorCounts()).containsOnlyKeys(expectedError);
            } finally {
                outbound.release();
            }
        }
        assertThat((Object) channel.readOutbound()).isNull();
    }
}

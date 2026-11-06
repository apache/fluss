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
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.TimestampType;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult.PartitionResult;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult.TopicResult;
import org.apache.fluss.kafka.dispatcher.KafkaApiRegistry;
import org.apache.fluss.kafka.dispatcher.KafkaRequestDispatcher;
import org.apache.fluss.kafka.error.KafkaErrorMapper;
import org.apache.fluss.kafka.security.KafkaSaslConnection;
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
import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.utils.Crc32C;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

/** Protocol tests independent of the Fluss append and record conversion backends. */
class KafkaProduceProtocolTest {

    @ParameterizedTest
    @EnumSource(
            value = CompressionType.class,
            names = {"GZIP", "SNAPPY", "LZ4", "ZSTD"})
    void testCorruptCompressedStreamDoesNotSuppressValidPartition(CompressionType codec) {
        byte[] value = new byte[1024];
        Arrays.fill(value, (byte) 7);
        MemoryRecords malformed =
                MemoryRecords.withRecords(
                        RecordBatch.MAGIC_VALUE_V2,
                        0L,
                        Compression.of(codec).build(),
                        new SimpleRecord(value));
        ByteBuffer bytes = malformed.buffer();
        // Preserve a valid outer batch CRC to exercise the decompressor, not just the CRC guard.
        for (int i = DefaultRecordBatch.RECORD_BATCH_OVERHEAD; i < bytes.limit(); i++) {
            bytes.put(i, (byte) 0);
        }
        updateBatchChecksum(malformed);
        malformed.batches().iterator().next().ensureValid();
        ProduceResponse response =
                dispatch(
                        request(
                                (short) 11,
                                (short) 1,
                                partition(0, malformed),
                                partition(1, records())),
                        command -> {
                            assertThat(command.topics().get(0).partitions())
                                    .extracting(KafkaProduceCommand.PartitionWrite::partitionId)
                                    .containsExactly(1);
                            return successful(command);
                        });
        assertThat(
                        response.data()
                                .responses()
                                .find("topic")
                                .partitionResponses()
                                .get(0)
                                .errorCode())
                .isIn(Errors.CORRUPT_MESSAGE.code(), Errors.INVALID_RECORD.code());
        if (codec == CompressionType.GZIP) {
            PartitionProduceResponse failed =
                    response.data().responses().find("topic").partitionResponses().get(0);
            assertThat(failed.errorCode()).isEqualTo(Errors.INVALID_RECORD.code());
            assertThat(failed.errorMessage())
                    .contains("java.util.zip.ZipException", "Not in GZIP format");
        }
        assertThat(
                        response.data()
                                .responses()
                                .find("topic")
                                .partitionResponses()
                                .get(1)
                                .errorCode())
                .isEqualTo(Errors.NONE.code());
        assertErrors(
                dispatch(
                        request((short) 11, (short) 1, partition(0, records())),
                        KafkaProduceProtocolTest::successful),
                Errors.NONE);
    }

    @ParameterizedTest
    @EnumSource(
            value = CompressionType.class,
            names = {"GZIP", "SNAPPY", "LZ4", "ZSTD"})
    void testTruncatedCompressedStreamsDoNotSuppressValidPartition(CompressionType codec) {
        byte[] value = new byte[1024];
        Arrays.fill(value, (byte) 7);
        MemoryRecords complete =
                MemoryRecords.withRecords(
                        RecordBatch.MAGIC_VALUE_V2,
                        0L,
                        Compression.of(codec).build(),
                        new SimpleRecord(value));
        for (int size = DefaultRecordBatch.RECORD_BATCH_OVERHEAD;
                size < complete.sizeInBytes();
                size++) {
            ByteBuffer bytes = ByteBuffer.allocate(size);
            ByteBuffer source = complete.buffer().duplicate();
            source.limit(size);
            bytes.put(source).flip();
            // The batch length excludes the base offset and the length field itself.
            bytes.putInt(Long.BYTES, size - Long.BYTES - Integer.BYTES);
            MemoryRecords truncated = MemoryRecords.readableRecords(bytes);
            updateBatchChecksum(truncated);
            truncated.batches().iterator().next().ensureValid();
            MemoryRecords valid =
                    timestampedRecords(
                            codec,
                            org.apache.kafka.common.record.TimestampType.LOG_APPEND_TIME,
                            123L,
                            456L,
                            789L);
            ProduceResponse response =
                    dispatch(
                            request(
                                    (short) 11,
                                    (short) 1,
                                    partition(0, truncated),
                                    partition(1, valid)),
                            command -> {
                                assertThat(command.topics().get(0).partitions())
                                        .extracting(KafkaProduceCommand.PartitionWrite::partitionId)
                                        .containsExactly(1);
                                assertThat(command.topics().get(0).partitions().get(0).records())
                                        .allSatisfy(
                                                record -> {
                                                    assertThat(record.timestamp()).isEqualTo(123L);
                                                    assertThat(record.timestampType())
                                                            .isEqualTo(
                                                                    TimestampType.LOG_APPEND_TIME);
                                                    assertThat(record.key())
                                                            .isEqualTo(
                                                                    ByteBuffer.wrap(
                                                                            new byte[] {1}));
                                                    assertThat(record.value())
                                                            .isEqualTo(
                                                                    ByteBuffer.wrap(
                                                                            new byte[] {2}));
                                                });
                                return successful(command);
                            });
            assertErrors(response, Errors.INVALID_RECORD, Errors.NONE);
        }
    }

    @Test
    void testSupportedVersionsExposeRecordsAndForwardContext() {
        for (short version = 3; version <= 11; version++) {
            ProduceResponse response =
                    dispatch(
                            request(version, (short) -1, partition(0, records())),
                            command -> {
                                assertThat(command.acks()).isEqualTo((short) -1);
                                assertThat(command.timeoutMs()).isEqualTo(4321);
                                assertThat(command.listenerName()).isEqualTo("KAFKA");
                                KafkaProduceCommand.Record record =
                                        command.topics()
                                                .get(0)
                                                .partitions()
                                                .get(0)
                                                .records()
                                                .get(0);
                                assertThat(record.timestamp()).isEqualTo(123L);
                                assertThat(record.timestampType())
                                        .isEqualTo(TimestampType.CREATE_TIME);
                                assertThat(record.hasTimestamp()).isTrue();
                                assertThat(record.key()).isEqualTo(ByteBuffer.wrap(new byte[] {1}));
                                assertThat(record.value())
                                        .isEqualTo(ByteBuffer.wrap(new byte[] {2}));
                                assertThat(record.headers()).hasSize(1);
                                assertThat(record.headers().get(0).value()).isNull();
                                ByteBuffer key = record.key();
                                assertThatThrownBy(() -> key.put((byte) 9))
                                        .isInstanceOf(ReadOnlyBufferException.class);
                                assertThat(key.get()).isEqualTo((byte) 1);
                                assertThat(record.key()).isEqualTo(ByteBuffer.wrap(new byte[] {1}));
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
        }
    }

    @ParameterizedTest
    @ValueSource(shorts = {3, 4, 5, 6, 7, 8, 9, 10, 11})
    void testBareAndQualifiedTopicNamesRemainUnchanged(short version) {
        MemoryRecords nullableValues =
                MemoryRecords.withRecords(
                        Compression.NONE,
                        new SimpleRecord(123L, new byte[] {1}, null),
                        new SimpleRecord(456L, null, new byte[0]));
        ProduceRequest request =
                requestForTopics(
                        version,
                        (short) 1,
                        topic("orders", partition(0, nullableValues)),
                        topic("fluss.orders", partition(0, records())),
                        topic("sales.orders", partition(0, records())));
        ProduceResponse response =
                dispatch(
                        request,
                        command -> {
                            assertThat(command.topics())
                                    .extracting(KafkaProduceCommand.TopicWrite::topicName)
                                    .containsExactly("orders", "fluss.orders", "sales.orders");
                            for (KafkaProduceCommand.TopicWrite topic : command.topics()) {
                                assertThat(topic.partitions())
                                        .extracting(KafkaProduceCommand.PartitionWrite::partitionId)
                                        .containsExactly(0);
                            }
                            List<KafkaProduceCommand.Record> recordViews =
                                    command.topics().get(0).partitions().get(0).records();
                            assertThat(recordViews).hasSize(2);
                            // The protocol command preserves null and empty values for the adapter.
                            assertThat(recordViews.get(0).value()).isNull();
                            assertThat(recordViews.get(1).value())
                                    .isEqualTo(ByteBuffer.allocate(0));
                            List<TopicResult> results =
                                    new ArrayList<>(successful(command).join().topics());
                            Collections.reverse(results);
                            return CompletableFuture.completedFuture(
                                    new KafkaProduceResult(results));
                        });

        assertThat(response.data().responses())
                .extracting(TopicProduceResponse::name)
                .containsExactly("orders", "fluss.orders", "sales.orders");
        for (TopicProduceResponse topic : response.data().responses()) {
            assertThat(topic.partitionResponses())
                    .extracting(
                            PartitionProduceResponse::index,
                            PartitionProduceResponse::errorCode,
                            PartitionProduceResponse::baseOffset)
                    .containsExactly(tuple(0, Errors.NONE.code(), 42L));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"orders", "fluss.orders"})
    void testLocalAndBackendErrorsAreIsolatedByRequestTopicName(String invalidTopic) {
        String validTopic = invalidTopic.equals("orders") ? "fluss.orders" : "orders";
        ProduceResponse response =
                dispatch(
                        requestForTopics(
                                (short) 11,
                                (short) 1,
                                topic(invalidTopic, partition(0, MemoryRecords.EMPTY)),
                                topic(validTopic, partition(0, records())),
                                topic("sales.orders", partition(0, records()))),
                        command -> {
                            assertThat(command.topics())
                                    .extracting(KafkaProduceCommand.TopicWrite::topicName)
                                    .containsExactly(validTopic, "sales.orders");
                            return CompletableFuture.completedFuture(
                                    new KafkaProduceResult(
                                            Arrays.asList(
                                                    new TopicResult(
                                                            "sales.orders",
                                                            Collections.singletonList(
                                                                    new PartitionResult(
                                                                            0,
                                                                            Errors
                                                                                    .TOPIC_AUTHORIZATION_FAILED,
                                                                            -1L,
                                                                            null))),
                                                    new TopicResult(
                                                            validTopic,
                                                            Collections.singletonList(
                                                                    new PartitionResult(
                                                                            0,
                                                                            Errors.NONE,
                                                                            42L,
                                                                            null))))));
                        });

        assertThat(response.data().responses())
                .extracting(TopicProduceResponse::name)
                .containsExactly(invalidTopic, validTopic, "sales.orders");
        assertTopicError(response, invalidTopic, Errors.INVALID_RECORD);
        assertTopicError(response, validTopic, Errors.NONE);
        assertTopicError(response, "sales.orders", Errors.TOPIC_AUTHORIZATION_FAILED);
    }

    @ParameterizedTest
    @ValueSource(shorts = {3, 4, 5, 6, 7, 8, 9, 10, 11})
    void testRequestAndBackendFailuresPreserveTopicNames(short version) {
        for (boolean invalidAcks : Arrays.asList(false, true)) {
            ProduceResponse response =
                    dispatch(
                            requestForTopics(
                                    version,
                                    invalidAcks ? (short) 2 : (short) 1,
                                    topic("orders", partition(0, records())),
                                    topic("fluss.orders", partition(0, records())),
                                    topic("sales.orders", partition(0, records()))),
                            command -> {
                                assertThat(invalidAcks).isFalse();
                                CompletableFuture<KafkaProduceResult> failed =
                                        new CompletableFuture<>();
                                failed.completeExceptionally(new TimeoutException("append failed"));
                                return failed;
                            });
            assertThat(response.data().responses())
                    .extracting(TopicProduceResponse::name)
                    .containsExactlyInAnyOrder("orders", "fluss.orders", "sales.orders");
            Errors error = invalidAcks ? Errors.INVALID_REQUIRED_ACKS : Errors.REQUEST_TIMED_OUT;
            for (String name : Arrays.asList("orders", "fluss.orders", "sales.orders")) {
                assertTopicError(response, name, error);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void testTimestampTypesRemainLocalToTheirBatches(CompressionType compressionType) {
        MemoryRecords createTime =
                timestampedRecords(
                        compressionType,
                        org.apache.kafka.common.record.TimestampType.CREATE_TIME,
                        RecordBatch.NO_TIMESTAMP,
                        123L,
                        456L);
        MemoryRecords appendTime =
                timestampedRecords(
                        compressionType,
                        org.apache.kafka.common.record.TimestampType.LOG_APPEND_TIME,
                        123L,
                        789L,
                        987L);
        MemoryRecords missingCreateTime =
                timestampedRecords(
                        compressionType,
                        org.apache.kafka.common.record.TimestampType.CREATE_TIME,
                        RecordBatch.NO_TIMESTAMP,
                        RecordBatch.NO_TIMESTAMP,
                        RecordBatch.NO_TIMESTAMP);
        MemoryRecords missingAppendTime =
                timestampedRecords(
                        compressionType,
                        org.apache.kafka.common.record.TimestampType.LOG_APPEND_TIME,
                        RecordBatch.NO_TIMESTAMP,
                        RecordBatch.NO_TIMESTAMP,
                        RecordBatch.NO_TIMESTAMP);
        ProduceResponse response =
                dispatch(
                        request(
                                (short) 11,
                                (short) 1,
                                partition(0, createTime),
                                partition(1, appendTime),
                                partition(2, missingCreateTime),
                                partition(3, missingAppendTime)),
                        command -> {
                            List<KafkaProduceCommand.PartitionWrite> partitions =
                                    command.topics().get(0).partitions();
                            assertThat(partitions).hasSize(4);
                            assertThat(partitions.get(0).records())
                                    .extracting(KafkaProduceCommand.Record::timestamp)
                                    .containsExactly(123L, 456L);
                            // The same numeric timestamp can have a different batch type.
                            assertThat(partitions.get(1).records())
                                    .extracting(KafkaProduceCommand.Record::timestamp)
                                    .containsExactly(123L, 123L);
                            for (int i = 0; i < partitions.size(); i++) {
                                TimestampType expectedType =
                                        i % 2 == 0
                                                ? TimestampType.CREATE_TIME
                                                : TimestampType.LOG_APPEND_TIME;
                                assertThat(partitions.get(i).records())
                                        .extracting(KafkaProduceCommand.Record::timestampType)
                                        .containsExactly(expectedType, expectedType);
                                assertThat(partitions.get(i).records())
                                        .extracting(KafkaProduceCommand.Record::hasTimestamp)
                                        .containsExactly(i < 2, i < 2);
                                if (i >= 2) {
                                    assertThat(partitions.get(i).records())
                                            .extracting(KafkaProduceCommand.Record::timestamp)
                                            .containsExactly(
                                                    RecordBatch.NO_TIMESTAMP,
                                                    RecordBatch.NO_TIMESTAMP);
                                }
                            }
                            return successful(command);
                        });
        assertErrors(response, Errors.NONE, Errors.NONE, Errors.NONE, Errors.NONE);
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void testMultipleBatchesPerPartitionRemainInvalid(CompressionType compressionType) {
        MemoryRecords first =
                timestampedRecords(
                        compressionType,
                        org.apache.kafka.common.record.TimestampType.CREATE_TIME,
                        RecordBatch.NO_TIMESTAMP,
                        123L);
        MemoryRecords second =
                timestampedRecords(
                        compressionType,
                        org.apache.kafka.common.record.TimestampType.LOG_APPEND_TIME,
                        456L,
                        123L);
        ByteBuffer combined = ByteBuffer.allocate(first.sizeInBytes() + second.sizeInBytes());
        combined.put(first.buffer().duplicate()).put(second.buffer().duplicate()).flip();
        ProduceResponse response =
                dispatch(
                        request(
                                (short) 11,
                                (short) 1,
                                partition(0, MemoryRecords.readableRecords(combined)),
                                partition(1, records())),
                        command -> {
                            assertThat(command.topics().get(0).partitions())
                                    .extracting(KafkaProduceCommand.PartitionWrite::partitionId)
                                    .containsExactly(1);
                            return successful(command);
                        });
        assertErrors(response, Errors.INVALID_RECORD, Errors.NONE);
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void testStreamedRecordsAreReadableDuringBackendInvocation(CompressionType compressionType) {
        byte[] value = new byte[32 * 1024];
        Arrays.fill(value, (byte) 7);
        MemoryRecords records =
                MemoryRecords.withRecords(
                        RecordBatch.MAGIC_VALUE_V2,
                        0L,
                        Compression.of(compressionType).build(),
                        new SimpleRecord(
                                123L,
                                new byte[] {1},
                                value,
                                new Header[] {
                                    new RecordHeader("header", new byte[] {2, 3}),
                                    new RecordHeader("header", null)
                                }),
                        new SimpleRecord(456L, null, new byte[0]));

        ProduceResponse response =
                dispatch(
                        request((short) 11, (short) 1, partition(0, records)),
                        command -> {
                            List<KafkaProduceCommand.Record> result =
                                    command.topics().get(0).partitions().get(0).records();
                            assertThat(result).hasSize(2);
                            assertThat(result.get(0).timestamp()).isEqualTo(123L);
                            assertThat(result.get(0).timestampType())
                                    .isEqualTo(TimestampType.CREATE_TIME);
                            assertThat(result.get(0).key())
                                    .isEqualTo(ByteBuffer.wrap(new byte[] {1}));
                            assertThat(result.get(0).value()).isEqualTo(ByteBuffer.wrap(value));
                            assertThat(result.get(0).key().isReadOnly()).isTrue();
                            assertThat(result.get(0).value().isReadOnly()).isTrue();
                            assertThat(result.get(0).headers())
                                    .extracting(KafkaProduceCommand.RecordHeader::name)
                                    .containsExactly("header", "header");
                            assertThat(result.get(0).headers().get(0).value())
                                    .containsExactly((byte) 2, (byte) 3);
                            assertThat(result.get(0).headers().get(1).value()).isNull();
                            assertThat(result.get(1).timestamp()).isEqualTo(456L);
                            assertThat(result.get(1).timestampType())
                                    .isEqualTo(TimestampType.CREATE_TIME);
                            assertThat(result.get(1).key()).isNull();
                            assertThat(result.get(1).value()).isEqualTo(ByteBuffer.allocate(0));
                            return successful(command);
                        });
        assertErrors(response, Errors.NONE);
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void testIncorrectRecordCountDoesNotSuppressValidPartition(CompressionType compressionType) {
        MemoryRecords malformed =
                MemoryRecords.withRecords(
                        RecordBatch.MAGIC_VALUE_V2,
                        0L,
                        Compression.of(compressionType).build(),
                        new SimpleRecord(new byte[] {1}));
        ByteBuffer bytes = malformed.buffer();
        bytes.putInt(DefaultRecordBatch.RECORDS_COUNT_OFFSET, 32);
        bytes.putInt(DefaultRecordBatch.LAST_OFFSET_DELTA_OFFSET, 31);
        updateBatchChecksum(malformed);

        ProduceResponse response =
                dispatch(
                        request(
                                (short) 11,
                                (short) 1,
                                partition(0, malformed),
                                partition(1, records())),
                        command -> {
                            assertThat(command.topics().get(0).partitions())
                                    .extracting(KafkaProduceCommand.PartitionWrite::partitionId)
                                    .containsExactly(1);
                            return successful(command);
                        });
        assertErrors(response, Errors.INVALID_RECORD, Errors.NONE);
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void testInvalidBatchOffsetRangeDoesNotSuppressValidPartition(CompressionType compressionType) {
        for (int[] invalidHeader :
                new int[][] {{1, -1}, {1, 9}, {1, Integer.MAX_VALUE}, {0, 0}, {-1, 0}}) {
            MemoryRecords malformed =
                    MemoryRecords.withRecords(
                            RecordBatch.MAGIC_VALUE_V2,
                            0L,
                            Compression.of(compressionType).build(),
                            new SimpleRecord(new byte[] {1}));
            int recordCount = invalidHeader[0];
            int lastOffsetDelta = invalidHeader[1];
            malformed.buffer().putInt(DefaultRecordBatch.RECORDS_COUNT_OFFSET, recordCount);
            malformed.buffer().putInt(DefaultRecordBatch.LAST_OFFSET_DELTA_OFFSET, lastOffsetDelta);
            updateBatchChecksum(malformed);

            ProduceResponse response =
                    dispatch(
                            request(
                                    (short) 11,
                                    (short) 1,
                                    partition(0, malformed),
                                    partition(1, records())),
                            command -> {
                                assertThat(command.topics().get(0).partitions())
                                        .extracting(KafkaProduceCommand.PartitionWrite::partitionId)
                                        .containsExactly(1);
                                return successful(command);
                            });
            assertErrors(response, Errors.INVALID_RECORD, Errors.NONE);
        }
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void testValidBatchUsesOffsetAssignedByBackend(CompressionType compressionType) {
        MemoryRecords records =
                MemoryRecords.withRecords(
                        RecordBatch.MAGIC_VALUE_V2,
                        0L,
                        Compression.of(compressionType).build(),
                        new SimpleRecord(new byte[] {1}),
                        new SimpleRecord(new byte[] {2}),
                        new SimpleRecord(new byte[] {3}));
        long backendBaseOffset = 1L << 40;
        ProduceResponse response =
                dispatch(
                        request((short) 11, (short) 1, partition(0, records)),
                        command -> {
                            assertThat(command.topics().get(0).partitions().get(0).records())
                                    .hasSize(3);
                            return CompletableFuture.completedFuture(
                                    new KafkaProduceResult(
                                            Collections.singletonList(
                                                    new TopicResult(
                                                            "topic",
                                                            Collections.singletonList(
                                                                    new PartitionResult(
                                                                            0,
                                                                            Errors.NONE,
                                                                            backendBaseOffset,
                                                                            null))))));
                        });
        assertErrors(response, Errors.NONE);
        assertThat(
                        response.data()
                                .responses()
                                .find("topic")
                                .partitionResponses()
                                .get(0)
                                .baseOffset())
                .isEqualTo(backendBaseOffset);
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
                    new EmbeddedChannel(new KafkaCommandDecoder(requests, "KAFKA"));
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
                dispatcher(
                                command -> {
                                    assertThat(
                                                    command.topics()
                                                            .get(0)
                                                            .partitions()
                                                            .get(0)
                                                            .records()
                                                            .get(0)
                                                            .timestampType())
                                            .isEqualTo(TimestampType.CREATE_TIME);
                                    return pending;
                                })
                        .dispatch(parsed)
                        .whenComplete(
                                (response, error) -> {
                                    if (error == null) {
                                        parsed.complete(response);
                                    } else {
                                        parsed.fail(error);
                                    }
                                });
                // This test polls RequestChannel directly, so mirror RequestProcessor cleanup.
                parsed.releaseBuffer();
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
                        KafkaSaslConnection.plaintext(),
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
        KafkaApiRegistry registry =
                new KafkaApiRegistry(Collections.singletonList(new ProduceHandler(backend)));
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
        return requestForTopics(version, acks, topic("topic", partitions));
    }

    private static ProduceRequest requestForTopics(
            short version, short acks, TopicProduceData... topics) {
        return new ProduceRequest(
                new ProduceRequestData()
                        .setAcks(acks)
                        .setTimeoutMs(4321)
                        .setTopicData(
                                new ProduceRequestData.TopicProduceDataCollection(
                                        Arrays.asList(topics).iterator())),
                version);
    }

    private static TopicProduceData topic(String name, PartitionProduceData... partitions) {
        return new TopicProduceData().setName(name).setPartitionData(Arrays.asList(partitions));
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

    private static MemoryRecords timestampedRecords(
            CompressionType compressionType,
            org.apache.kafka.common.record.TimestampType timestampType,
            long logAppendTime,
            long... timestamps) {
        try (MemoryRecordsBuilder builder =
                MemoryRecords.builder(
                        ByteBuffer.allocate(1024),
                        RecordBatch.MAGIC_VALUE_V2,
                        Compression.of(compressionType).build(),
                        timestampType,
                        0L,
                        logAppendTime)) {
            for (long timestamp : timestamps) {
                builder.append(new SimpleRecord(timestamp, new byte[] {1}, new byte[] {2}));
            }
            return builder.build();
        }
    }

    private static void assertErrors(ProduceResponse response, Errors... errors) {
        assertThat(response.data().responses().find("topic").partitionResponses())
                .extracting(PartitionProduceResponse::errorCode)
                .containsExactly(Arrays.stream(errors).map(Errors::code).toArray(Short[]::new));
    }

    private static void assertTopicError(ProduceResponse response, String topic, Errors error) {
        assertThat(response.data().responses().find(topic).partitionResponses())
                .extracting(PartitionProduceResponse::index, PartitionProduceResponse::errorCode)
                .containsExactly(tuple(0, error.code()));
    }

    private static void updateBatchChecksum(MemoryRecords records) {
        ByteBuffer bytes = records.buffer();
        int checksumStart = DefaultRecordBatch.CRC_OFFSET + Integer.BYTES;
        bytes.putInt(
                DefaultRecordBatch.CRC_OFFSET,
                (int) Crc32C.compute(bytes, checksumStart, bytes.limit() - checksumStart));
    }
}

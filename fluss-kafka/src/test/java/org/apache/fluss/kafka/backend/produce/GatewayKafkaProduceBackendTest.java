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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.KafkaValueConversionType;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.PartitionWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.TopicWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult.PartitionResult;
import org.apache.fluss.kafka.mapping.KafkaTopicMapper;
import org.apache.fluss.kafka.transcode.ArrowKafkaRecordTranscoder;
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.kafka.transcode.KafkaRecordTranscoder;
import org.apache.fluss.memory.LazyMemorySegmentPool;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.PbProduceLogRespForBucket;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocatorUtil;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IOUtils;

import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests synchronous append submission and asynchronous acknowledgements. */
class GatewayKafkaProduceBackendTest {

    private final BufferAllocator allocator = BufferAllocatorUtil.createBufferAllocator(null);
    private final LazyMemorySegmentPool memorySegmentPool = createMemoryPool();
    private final List<ArrowKafkaRecordTranscoder> transcoders = new ArrayList<>();
    private final List<TestingProduceService> services = new ArrayList<>();

    private static LazyMemorySegmentPool createMemoryPool() {
        Configuration conf = new Configuration();
        MemorySize pageSize = MemorySize.parse("128kb");
        conf.set(ConfigOptions.SERVER_BUFFER_MEMORY_SIZE, pageSize);
        conf.set(ConfigOptions.SERVER_BUFFER_PAGE_SIZE, pageSize);
        conf.set(ConfigOptions.SERVER_BUFFER_PER_REQUEST_MEMORY_SIZE, pageSize);
        conf.set(ConfigOptions.SERVER_BUFFER_POOL_WAIT_TIMEOUT, Duration.ofMillis(500));
        return LazyMemorySegmentPool.createServerBufferPool(conf);
    }

    private ArrowKafkaRecordTranscoder createTranscoder() {
        ArrowKafkaRecordTranscoder transcoder =
                new ArrowKafkaRecordTranscoder(allocator, memorySegmentPool);
        transcoders.add(transcoder);
        return transcoder;
    }

    @AfterEach
    void closeTranscoders() throws Exception {
        try {
            for (TestingProduceService service : services) {
                if (service.pendingAppend != null) {
                    service.pendingAppend.completeExceptionally(
                            new IllegalStateException("test cleanup"));
                }
            }
            assertThat(memorySegmentPool.usedPages()).isZero();
        } finally {
            List<AutoCloseable> resources = new ArrayList<>(transcoders);
            resources.add(allocator);
            resources.add(memorySegmentPool);
            IOUtils.closeAll(resources);
        }
    }

    @Test
    void testUsesRequestWorkerForAllPartitions() throws Exception {
        Thread requestWorker = Thread.currentThread();
        TestingProduceService service = service();
        List<Thread> conversionThreads = Collections.synchronizedList(new ArrayList<>());
        ArrowKafkaRecordTranscoder arrow = createTranscoder();
        GatewayKafkaProduceBackend backend =
                backend(
                        service,
                        (TestingTranscoder)
                                (partition, records, table) -> {
                                    conversionThreads.add(Thread.currentThread());
                                    return arrow.transcode(partition, records, table);
                                });

        assertErrors(
                backend.write(command((short) 1, topic("topic", good(0), good(1))))
                        .get(10, TimeUnit.SECONDS),
                Errors.NONE,
                Errors.NONE);
        assertThat(service.metadataThread).isSameAs(requestWorker);
        assertThat(service.appendThread).isSameAs(requestWorker);
        assertThat(conversionThreads)
                .hasSize(2)
                .allSatisfy(thread -> assertThat(thread).isSameAs(requestWorker));
    }

    @Test
    void testRevokedWritePermissionIsCheckedOnEveryAppend() throws Exception {
        TestingProduceService service = service();
        GatewayKafkaProduceBackend backend = backend(service);
        KafkaProduceCommand command = command((short) 1, topic("topic", nullValue(0)));
        assertErrors(backend.write(command).get(10, TimeUnit.SECONDS), Errors.NONE);
        service.denyWrite = true;
        assertErrors(
                backend.write(command).get(10, TimeUnit.SECONDS),
                Errors.TOPIC_AUTHORIZATION_FAILED);
        assertThat(service.writeChecks).isEqualTo(2);
        assertThat(service.appends).hasSize(1);
    }

    @Test
    void testSharesMetadataAcrossPartitionsAndAliasesWithinEachRequest() throws Exception {
        TestingProduceService service = service();
        GatewayKafkaProduceBackend backend = backend(service);
        KafkaProduceCommand command =
                command(
                        (short) 1,
                        topic("topic", good(0), good(1)),
                        topic("kafka.topic", good(2)),
                        topic("other", good(0)));

        for (int request = 0; request < 2; request++) {
            KafkaProduceResult result = backend.write(command).get(10, TimeUnit.SECONDS);
            assertErrors(result, Errors.NONE, Errors.NONE);
            assertThat(result.topics().get(1).partitions().get(0).error()).isEqualTo(Errors.NONE);
            assertThat(result.topics().get(2).partitions().get(0).error()).isEqualTo(Errors.NONE);
            assertThat(service.metadataPaths)
                    .filteredOn(path -> path.equals(TablePath.of("kafka", "topic")))
                    .hasSize(request + 1);
            assertThat(service.metadataPaths)
                    .filteredOn(path -> path.equals(TablePath.of("kafka", "other")))
                    .hasSize(request + 1);
        }
        assertThat(service.appends).hasSize(8);
    }

    @Test
    void testRevokedMetadataPermissionIsCheckedAfterCacheHit() throws Exception {
        TestingProduceService service = service();
        GatewayKafkaProduceBackend backend = backend(service);
        KafkaProduceCommand command = command((short) 1, topic("topic", good(0), good(1)));

        assertErrors(backend.write(command).get(10, TimeUnit.SECONDS), Errors.NONE, Errors.NONE);
        service.denyMetadata = true;
        assertErrors(
                backend.write(command).get(10, TimeUnit.SECONDS),
                Errors.TOPIC_AUTHORIZATION_FAILED,
                Errors.TOPIC_AUTHORIZATION_FAILED);
        assertThat(service.metadataPaths).hasSize(2);
        assertThat(service.appends).hasSize(2);
    }

    @Test
    void testPropertyChangesInvalidateCachedAdmissionWithoutSchemaChange() throws Exception {
        TestingProduceService service = service();
        GatewayKafkaProduceBackend backend = backend(service);
        KafkaProduceCommand command = command((short) 1, topic("topic", good(0), good(1)));

        assertErrors(backend.write(command).get(10, TimeUnit.SECONDS), Errors.NONE, Errors.NONE);
        service.metadataResponse = metadata(true);
        assertErrors(
                backend.write(command).get(10, TimeUnit.SECONDS),
                Errors.INVALID_TOPIC_EXCEPTION,
                Errors.INVALID_TOPIC_EXCEPTION);
        service.metadataResponse =
                metadata(
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("_kafka_value", DataTypes.STRING())
                                                .build())
                                .distributedBy(3)
                                .logFormat(LogFormat.ARROW)
                                .property(ConfigOptions.TABLE_KAFKA_ENABLED, false)
                                .property(
                                        ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE,
                                        KafkaValueConversionType.STRING)
                                .build());
        assertErrors(
                backend.write(command).get(10, TimeUnit.SECONDS),
                Errors.UNKNOWN_TOPIC_OR_PARTITION,
                Errors.UNKNOWN_TOPIC_OR_PARTITION);
        service.metadataResponse = metadata(false);
        assertErrors(backend.write(command).get(10, TimeUnit.SECONDS), Errors.NONE, Errors.NONE);
        assertThat(service.metadataPaths).hasSize(4);
        assertThat(service.appends).hasSize(4);
    }

    @ParameterizedTest
    @ValueSource(strings = {"success", "failure", "cancel"})
    void testReleasesConvertedRecordsBeforeAcknowledgement(String outcome) throws Exception {
        TestingProduceService service = service();
        service.pendingAppend = new CompletableFuture<>();
        CompletableFuture<KafkaProduceResult> result =
                backend(service).write(command((short) -1, topic("topic", good(0), good(1))));

        // Both partitions must be submitted using the same single-page pool before any ACK.
        assertThat(service.appends).hasSize(2);
        assertThat(service.append.getAcks()).isEqualTo(-1);
        assertThat(service.append.getTimeoutMs()).isEqualTo(4321);
        assertThat(service.append.getTableId()).isEqualTo(42L);
        assertThat(memorySegmentPool.usedPages()).isZero();
        assertThat(result).isNotDone();
        if (outcome.equals("cancel")) {
            result.cancel(false);
            assertThat(service.pendingAppend).isNotCancelled();
        }
        if (outcome.equals("failure")) {
            service.pendingAppend.completeExceptionally(new IllegalStateException("append failed"));
            assertErrors(
                    result.get(10, TimeUnit.SECONDS),
                    Errors.UNKNOWN_SERVER_ERROR,
                    Errors.UNKNOWN_SERVER_ERROR);
        } else {
            service.pendingAppend.complete(
                    success(0).addAllBucketsResps(success(1).getBucketsRespsList()));
            if (!outcome.equals("cancel")) {
                KafkaProduceResult completed = result.get(10, TimeUnit.SECONDS);
                assertErrors(completed, Errors.NONE, Errors.NONE);
                assertThat(completed.topics().get(0).partitions().get(0).baseOffset())
                        .isEqualTo(17L);
            }
        }
    }

    @Test
    void testConversionAndBucketErrorsAreIsolatedFromValidWrites() throws Exception {
        TestingProduceService service = service();
        ArrowKafkaRecordTranscoder arrow = createTranscoder();
        GatewayKafkaProduceBackend backend =
                backend(
                        service,
                        (TestingTranscoder)
                                (partitionId, records, table) -> {
                                    if (partitionId == 0) {
                                        throw new KafkaRecordEncodingException("invalid record");
                                    }
                                    return arrow.transcode(partitionId, records, table);
                                });
        KafkaProduceResult result =
                backend.write(command((short) 1, topic("topic", good(0), good(8), good(1)))).join();
        assertErrors(result, Errors.INVALID_RECORD, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.NONE);
        assertThat(service.append.getBucketsReqsList())
                .extracting(bucket -> bucket.getBucketId())
                .containsExactly(1);
    }

    @Test
    void testInvalidAndMissingTablesDoNotSuppressOtherTopics() throws Exception {
        TestingProduceService service = service();
        KafkaProduceResult result =
                backend(service)
                        .write(
                                command(
                                        (short) 1,
                                        topic("kafka.invalid", nullValue(0)),
                                        topic("kafka.missing", good(0)),
                                        topic("kafka.topic", good(0))))
                        .join();
        assertThat(result.topics())
                .extracting(topic -> topic.partitions().get(0).error())
                .containsExactly(
                        Errors.INVALID_TOPIC_EXCEPTION,
                        Errors.UNKNOWN_TOPIC_OR_PARTITION,
                        Errors.NONE);
        assertThat(service.appends).hasSize(1);
        assertThat(service.writeChecks).isEqualTo(1);
    }

    @Test
    void testMapsReplicationFailuresAndMissingResponsesPerPartition() throws Exception {
        org.apache.fluss.rpc.protocol.Errors flussError =
                org.apache.fluss.rpc.protocol.Errors.NOT_ENOUGH_REPLICAS_EXCEPTION;
        TestingProduceService service = service();
        service.pendingAppend =
                CompletableFuture.completedFuture(
                        success(1)
                                .addAllBucketsResps(
                                        Collections.singletonList(
                                                new PbProduceLogRespForBucket()
                                                        .setBucketId(0)
                                                        .setErrorCode(flussError.code()))));
        KafkaProduceResult result =
                backend(service)
                        .write(command((short) -1, topic("kafka.topic", good(0), good(1), good(2))))
                        .join();
        assertErrors(result, Errors.NOT_ENOUGH_REPLICAS, Errors.NONE, Errors.UNKNOWN_SERVER_ERROR);
        assertThat(result.topics().get(0).partitions().get(1).baseOffset()).isEqualTo(17L);
    }

    private static PartitionWrite nullValue(int partition) {
        return new PartitionWrite(
                partition,
                Collections.singletonList(
                        new Record(
                                -1L,
                                ByteBuffer.wrap(new byte[] {(byte) 0xff}),
                                null,
                                Collections.emptyList())));
    }

    private interface TestingTranscoder extends KafkaRecordTranscoder {
        @Override
        default void close() {}
    }

    private static KafkaTopicMapper mapper() {
        Configuration config = new Configuration();
        config.set(ConfigOptions.KAFKA_DEFAULT_DATABASE, "kafka");
        return new KafkaTopicMapper(config);
    }

    private GatewayKafkaProduceBackend backend(TestingProduceService service) {
        return backend(service, createTranscoder());
    }

    private GatewayKafkaProduceBackend backend(
            TestingProduceService service, KafkaRecordTranscoder transcoder) {
        return new GatewayKafkaProduceBackend(service, service, mapper(), transcoder);
    }

    private TestingProduceService service() {
        TestingProduceService service = new TestingProduceService();
        services.add(service);
        return service;
    }

    private static KafkaProduceCommand command(short acks, TopicWrite... topics) throws Exception {
        return new KafkaProduceCommand(
                acks, 4321, Arrays.asList(topics), "KAFKA", InetAddress.getLoopbackAddress());
    }

    private static TopicWrite topic(String name, PartitionWrite... partitions) {
        return new TopicWrite(name, Arrays.asList(partitions));
    }

    private static PartitionWrite good(int id) {
        return good(id, 65);
    }

    private static PartitionWrite good(int id, int value) {
        return new PartitionWrite(
                id,
                Collections.singletonList(
                        new Record(
                                123L,
                                null,
                                ByteBuffer.wrap(new byte[] {(byte) value}),
                                Collections.emptyList())));
    }

    private static GetTableInfoResponse metadata(boolean invalid) {
        TableDescriptor descriptor =
                descriptor(
                        Schema.newBuilder().column("_kafka_value", DataTypes.STRING()).build(),
                        invalid
                                ? KafkaValueConversionType.BINARY
                                : KafkaValueConversionType.STRING);
        return metadata(descriptor);
    }

    private static GetTableInfoResponse metadata(TableDescriptor descriptor) {
        return new GetTableInfoResponse()
                .setTableId(42L)
                .setSchemaId(1)
                .setTableJson(descriptor.toJsonBytes())
                .setCreatedTime(1L)
                .setModifiedTime(1L);
    }

    private static TableDescriptor descriptor(Schema schema, KafkaValueConversionType format) {
        return TableDescriptor.builder()
                .schema(schema)
                .distributedBy(3)
                .logFormat(LogFormat.ARROW)
                .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE, format)
                .build();
    }

    private static ProduceLogResponse success(int id) {
        return new ProduceLogResponse()
                .addAllBucketsResps(
                        Collections.singletonList(
                                new PbProduceLogRespForBucket()
                                        .setBucketId(id)
                                        .setBaseOffset(17L)));
    }

    private static void assertErrors(KafkaProduceResult result, Errors... errors) {
        List<PartitionResult> partitions = result.topics().get(0).partitions();
        assertThat(partitions).extracting(PartitionResult::error).containsExactly(errors);
        for (PartitionResult partition : partitions) {
            if (partition.error() != Errors.NONE) {
                assertThat(partition.baseOffset()).isEqualTo(-1L);
            }
        }
    }

    private static class TestingProduceService extends TestingTabletGatewayService {
        private final List<TablePath> metadataPaths = new ArrayList<>();
        private final List<ProduceLogRequest> appends = new ArrayList<>();
        private ProduceLogRequest append;
        private CompletableFuture<ProduceLogResponse> pendingAppend;
        private GetTableInfoResponse metadataResponse;
        private int writeChecks;
        private boolean denyWrite;
        private boolean denyMetadata;
        private Thread metadataThread;
        private Thread appendThread;

        @Override
        public CompletableFuture<GetTableInfoResponse> getTableInfo(GetTableInfoRequest request) {
            assertThat(currentListenerName()).isEqualTo("KAFKA");
            String database = request.getTablePath().getDatabaseName();
            String name = request.getTablePath().getTableName();
            metadataPaths.add(TablePath.of(database, name));
            metadataThread = Thread.currentThread();
            if (name.equals("missing")) {
                throw new TableNotExistException("missing");
            }
            if (denyMetadata) {
                throw new AuthorizationException("Describe denied");
            }
            return CompletableFuture.completedFuture(
                    metadataResponse == null
                            ? GatewayKafkaProduceBackendTest.metadata(name.equals("invalid"))
                            : metadataResponse);
        }

        private void checkWriteAuthorization(long tableId) {
            assertThat(tableId).isPositive();
            assertThat(currentListenerName()).isEqualTo("KAFKA");
            assertThat(currentSession().getInetAddress())
                    .isEqualTo(InetAddress.getLoopbackAddress());
            writeChecks++;
            if (denyWrite) {
                throw new AuthorizationException("Write denied");
            }
        }

        @Override
        public CompletableFuture<ProduceLogResponse> produceLog(ProduceLogRequest request) {
            checkWriteAuthorization(request.getTableId());
            assertThat(currentListenerName()).isEqualTo("KAFKA");
            assertThat(currentSession().getInetAddress())
                    .isEqualTo(InetAddress.getLoopbackAddress());
            ProduceLogRequest copiedRequest = new ProduceLogRequest().copyFrom(request);
            append = copiedRequest;
            appends.add(copiedRequest);
            appendThread = Thread.currentThread();
            return pendingAppend == null
                    ? CompletableFuture.completedFuture(
                            success(request.getBucketsReqsList().get(0).getBucketId()))
                    : pendingAppend;
        }
    }
}

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
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.PartitionWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.TopicWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult.PartitionResult;
import org.apache.fluss.kafka.mapping.KafkaTopicMapper;
import org.apache.fluss.kafka.transcode.ArrowKafkaRecordTranscoder;
import org.apache.fluss.kafka.transcode.KafkaRecordTranscoder;
import org.apache.fluss.kafka.transcode.KafkaTopicWritePlan;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.PbProduceLogRespForBucket;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.types.DataTypes;

import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests append admission, error isolation, session propagation and asynchronous acknowledgements.
 */
class GatewayKafkaProduceBackendTest {

    @Test
    void testAllNullValuesUseNativeAppendAndAuthorization() throws Exception {
        TestingProduceService service = new TestingProduceService();
        KafkaProduceResult result =
                backend(service)
                        .write(command((short) 1, topic("kafka.topic", nullValue(0), nullValue(8))))
                        .join();
        assertErrors(result, Errors.NONE, Errors.UNKNOWN_TOPIC_OR_PARTITION);
        assertThat(result.topics().get(0).partitions().get(0).baseOffset()).isEqualTo(17L);
        assertThat(service.writeChecks).isEqualTo(1);
        assertThat(service.appends).hasSize(1);
        assertThat(service.append.getBucketsReqsList()).hasSize(1);
        assertThat(service.append.getBucketsReqsList().get(0).getBucketId()).isZero();
        service.denyWrite = true;
        assertErrors(
                backend(service)
                        .write(command((short) 1, topic("kafka.topic", nullValue(0))))
                        .join(),
                Errors.TOPIC_AUTHORIZATION_FAILED);
        assertThat(service.appends).hasSize(1);
        assertThat(service.writeChecks).isEqualTo(2);
    }

    @ParameterizedTest
    @ValueSource(strings = {"non-null", "all-null", "mixed"})
    void testCachedMetadataDoesNotBypassRevokedWritePermission(String recordsKind)
            throws Exception {
        TestingProduceService service = new TestingProduceService();
        KafkaProduceConversionExecutor executor = new KafkaProduceConversionExecutor(1, 2);
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(
                        service, service, mapper(), new ArrowKafkaRecordTranscoder(), executor);
        PartitionWrite partition =
                recordsKind.equals("non-null")
                        ? good(0)
                        : recordsKind.equals("all-null")
                                ? nullValue(0)
                                : new PartitionWrite(
                                        0,
                                        Arrays.asList(
                                                nullValue(0).records().get(0),
                                                good(0).records().get(0)));
        KafkaProduceCommand command = command((short) 1, topic("kafka.topic", partition));
        try {
            assertErrors(backend.write(command).get(10, TimeUnit.SECONDS), Errors.NONE);
            int appendedBeforeRevocation = service.appends.size();
            service.denyWrite = true;
            assertErrors(
                    backend.write(command).get(10, TimeUnit.SECONDS),
                    Errors.TOPIC_AUTHORIZATION_FAILED);
            assertThat(service.metadataPaths).hasSize(2);
            assertThat(service.writeChecks).isEqualTo(2);
            assertThat(service.appends).hasSize(appendedBeforeRevocation);
            assertThat(service.metadataThread).isNotSameAs(Thread.currentThread());
        } finally {
            executor.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void testNullValuesCannotBypassTableAndSchemaValidation() throws Exception {
        TestingProduceService service = new TestingProduceService();
        for (String name : new String[] {"invalid", "missing"}) {
            assertErrors(
                    backend(service)
                            .write(command((short) 1, topic("kafka." + name, nullValue(0))))
                            .join(),
                    name.equals("invalid")
                            ? Errors.INVALID_TOPIC_EXCEPTION
                            : Errors.UNKNOWN_TOPIC_OR_PARTITION);
        }
        assertThat(service.writeChecks).isZero();
        assertThat(service.append).isNull();
    }

    @Test
    void testMixedNullValuesPreserveRecordOrderAndNativeOffsets() throws Exception {
        TestingProduceService service = new TestingProduceService();
        List<Record> seen = new ArrayList<>();
        ArrowKafkaRecordTranscoder transcoder = new ArrowKafkaRecordTranscoder();
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(
                        service,
                        service,
                        mapper(),
                        (partitionId, records, table) -> {
                            seen.addAll(records);
                            return transcoder.transcode(partitionId, records, table);
                        });
        Record nullRecord = nullValue(0).records().get(0);
        Record empty = new Record(1L, null, new byte[0], Collections.emptyList());
        Record valid = good(0).records().get(0);
        KafkaProduceResult result =
                backend.write(
                                command(
                                        (short) 1,
                                        topic(
                                                "kafka.topic",
                                                new PartitionWrite(
                                                        0,
                                                        Arrays.asList(nullRecord, empty, valid)))))
                        .join();
        assertErrors(result, Errors.NONE);
        assertThat(seen).containsExactly(nullRecord, empty, valid);
        assertThat(result.topics().get(0).partitions().get(0).baseOffset()).isEqualTo(17L);
    }

    private static PartitionWrite nullValue(int partition) {
        return new PartitionWrite(
                partition,
                Collections.singletonList(
                        new Record(-1L, new byte[] {(byte) 0xff}, null, Collections.emptyList())));
    }

    @Test
    void testAcksAllWaitsForNativeAppendResponse() throws Exception {
        TestingProduceService service = new TestingProduceService();
        service.pendingAppend = new CompletableFuture<>();
        CompletableFuture<KafkaProduceResult> result =
                backend(service).write(command((short) -1, topic("kafka.topic", good(0))));
        assertThat(service.append.getAcks()).isEqualTo(-1);
        assertThat(service.append.getTimeoutMs()).isEqualTo(4321);
        assertThat(service.append.getTableId()).isEqualTo(42L);
        assertThat(result).isNotDone();
        assertThat(service.pendingAppend).isNotDone();
        service.pendingAppend.complete(success(0));
        assertThat(result.join().topics().get(0).partitions().get(0).baseOffset()).isEqualTo(17L);
    }

    @Test
    void testMapsSynchronousAppendFailure() throws Exception {
        TestingProduceService service = new TestingProduceService();
        service.throwAppend = true;
        assertErrors(
                backend(service).write(command((short) 1, topic("kafka.topic", good(0)))).join(),
                Errors.UNKNOWN_SERVER_ERROR);
    }

    @Test
    void testConversionAndBucketErrorsAreIsolatedFromValidWrites() throws Exception {
        TestingProduceService service = new TestingProduceService();
        PartitionWrite malformed =
                new PartitionWrite(
                        0,
                        Arrays.asList(
                                good(0).records().get(0),
                                new Record(
                                        1L,
                                        null,
                                        new byte[] {(byte) 0xff},
                                        Collections.emptyList())));
        KafkaProduceResult result =
                backend(service)
                        .write(
                                command(
                                        (short) 1,
                                        topic("kafka.topic", malformed, good(1), good(8))))
                        .join();
        assertErrors(result, Errors.INVALID_RECORD, Errors.NONE, Errors.UNKNOWN_TOPIC_OR_PARTITION);
        assertThat(service.append.getBucketsReqsList()).hasSize(1);
        assertThat(service.append.getBucketsReqsList().get(0).getBucketId()).isEqualTo(1);
    }

    @Test
    void testInvalidAndMissingTablesDoNotSuppressOtherTopics() throws Exception {
        for (boolean async : new boolean[] {false, true}) {
            TestingProduceService service = new TestingProduceService();
            service.asyncMissing = async;
            KafkaProduceResult result =
                    backend(service)
                            .write(
                                    command(
                                            (short) 1,
                                            topic("kafka.invalid", good(0)),
                                            topic("kafka.missing", good(0)),
                                            topic("kafka.topic", good(0))))
                            .join();
            assertThat(result.topics()).hasSize(3);
            assertThat(result.topics().get(0).partitions().get(0).error())
                    .isEqualTo(Errors.INVALID_TOPIC_EXCEPTION);
            assertThat(result.topics().get(1).partitions().get(0).error())
                    .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION);
            assertThat(result.topics().get(2).partitions().get(0).error()).isEqualTo(Errors.NONE);
        }
    }

    @Test
    void testNoAppendWhenAllPartitionsFailConversion() throws Exception {
        TestingProduceService service = new TestingProduceService();
        KafkaProduceResult result =
                backend(service).write(command((short) 1, topic("kafka.topic", good(8)))).join();
        assertErrors(result, Errors.UNKNOWN_TOPIC_OR_PARTITION);
        assertThat(service.append).isNull();
    }

    @Test
    void testMapsReplicationFailuresAndMissingResponsesPerPartition() throws Exception {
        org.apache.fluss.rpc.protocol.Errors[] flussErrors = {
            org.apache.fluss.rpc.protocol.Errors.NOT_ENOUGH_REPLICAS_EXCEPTION,
            org.apache.fluss.rpc.protocol.Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND_EXCEPTION,
            org.apache.fluss.rpc.protocol.Errors.REQUEST_TIME_OUT,
            org.apache.fluss.rpc.protocol.Errors.NOT_LEADER_OR_FOLLOWER,
            org.apache.fluss.rpc.protocol.Errors.CORRUPT_RECORD_EXCEPTION
        };
        Errors[] kafkaErrors = {
            Errors.NOT_ENOUGH_REPLICAS,
            Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND,
            Errors.REQUEST_TIMED_OUT,
            Errors.NOT_LEADER_OR_FOLLOWER,
            Errors.CORRUPT_MESSAGE
        };
        for (int i = 0; i < flussErrors.length; i++) {
            TestingProduceService service = new TestingProduceService();
            service.pendingAppend =
                    CompletableFuture.completedFuture(
                            new ProduceLogResponse()
                                    .addAllBucketsResps(
                                            Arrays.asList(
                                                    new PbProduceLogRespForBucket()
                                                            .setBucketId(0)
                                                            .setErrorCode(flussErrors[i].code()),
                                                    new PbProduceLogRespForBucket()
                                                            .setBucketId(1)
                                                            .setErrorCode(0)
                                                            .setBaseOffset(17L))));
            KafkaProduceResult result =
                    backend(service)
                            .write(
                                    command(
                                            (short) -1,
                                            topic("kafka.topic", good(0), good(1), good(2))))
                            .join();
            assertErrors(result, kafkaErrors[i], Errors.NONE, Errors.UNKNOWN_SERVER_ERROR);
            assertThat(result.topics().get(0).partitions().get(1).baseOffset()).isEqualTo(17L);
        }
    }

    @Test
    void testAsyncMetadataCompletionRestoresSessionAndAcksZero() throws Exception {
        TestingProduceService service = new TestingProduceService();
        service.pendingMetadata = new CompletableFuture<>();
        CompletableFuture<KafkaProduceResult> result =
                backend(service).write(command((short) 0, topic("kafka.topic", good(0))));
        CompletableFuture.runAsync(() -> service.pendingMetadata.complete(metadata(false))).join();
        assertErrors(result.join(), Errors.NONE);
        assertThat(service.append.getAcks()).isZero();
    }

    @Test
    void testQualifiedTopicsPreserveDatabaseAndTranscoderTablePath() throws Exception {
        TestingProduceService service = new TestingProduceService();
        List<TablePath> transcodedPaths = new ArrayList<>();
        ArrowKafkaRecordTranscoder transcoder = new ArrowKafkaRecordTranscoder();
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(
                        service,
                        service,
                        mapper(),
                        (partitionId, records, tableInfo) -> {
                            transcodedPaths.add(tableInfo.getTablePath());
                            return transcoder.transcode(partitionId, records, tableInfo);
                        });

        KafkaProduceResult result =
                backend.write(
                                command(
                                        (short) 1,
                                        topic("kafka.topic", good(0)),
                                        topic("other.topic", good(0))))
                        .join();

        assertThat(service.metadataPaths)
                .containsExactly(TablePath.of("kafka", "topic"), TablePath.of("other", "topic"));
        assertThat(transcodedPaths).containsExactlyElementsOf(service.metadataPaths);
        assertThat(service.appends)
                .extracting(ProduceLogRequest::getTableId)
                .containsExactly(42L, 43L);
        assertThat(result.topics())
                .extracting(KafkaProduceResult.TopicResult::topicName)
                .containsExactly("kafka.topic", "other.topic");
        assertThat(result.topics())
                .allSatisfy(
                        topic ->
                                assertThat(topic.partitions().get(0).error())
                                        .isEqualTo(Errors.NONE));
    }

    @Test
    void testInvalidQualifiedNamesDoNotReachGatewayOrSuppressValidTopic() throws Exception {
        TestingProduceService service = new TestingProduceService();
        KafkaProduceResult result =
                backend(service)
                        .write(
                                command(
                                        (short) 1,
                                        topic(".", good(0)),
                                        topic("kafka.topic.extra", good(0)),
                                        topic("kafka.__internal", good(0)),
                                        topic("kafka.topic", good(0))))
                        .join();

        assertThat(result.topics())
                .extracting(topic -> topic.partitions().get(0).error())
                .containsExactly(
                        Errors.INVALID_TOPIC_EXCEPTION,
                        Errors.INVALID_TOPIC_EXCEPTION,
                        Errors.INVALID_TOPIC_EXCEPTION,
                        Errors.NONE);
        assertThat(service.metadataPaths).containsExactly(TablePath.of("kafka", "topic"));
        assertThat(service.appends).hasSize(1);
    }

    @Test
    void testReusesPlansButAlwaysRefreshesMetadataAndValidatesChangedContract() throws Exception {
        TestingProduceService service = new TestingProduceService();
        ArrowKafkaRecordTranscoder arrow = new ArrowKafkaRecordTranscoder();
        List<KafkaTopicWritePlan> prepared = new ArrayList<>();
        List<KafkaTopicWritePlan> converted = new ArrayList<>();
        List<Integer> convertedPartitions = new ArrayList<>();
        KafkaRecordTranscoder transcoder =
                new KafkaRecordTranscoder() {
                    @Override
                    public KafkaTopicWritePlan prepare(TableInfo tableInfo) {
                        KafkaTopicWritePlan plan = arrow.prepare(tableInfo);
                        prepared.add(plan);
                        return plan;
                    }

                    @Override
                    public BytesView transcode(
                            int partitionId, List<Record> records, TableInfo tableInfo)
                            throws Exception {
                        return arrow.transcode(partitionId, records, tableInfo);
                    }

                    @Override
                    public BytesView transcode(
                            int partitionId, List<Record> records, KafkaTopicWritePlan plan)
                            throws Exception {
                        converted.add(plan);
                        convertedPartitions.add(partitionId);
                        return arrow.transcode(partitionId, records, plan);
                    }
                };
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(service, service, mapper(), transcoder);
        backend.write(command((short) 1, topic("kafka.topic", good(0), good(1)))).join();
        backend.write(command((short) 1, topic("kafka.topic", good(0)))).join();
        assertThat(service.metadataPaths).hasSize(2);
        assertThat(prepared).hasSize(2);
        assertThat(prepared.get(1)).isSameAs(prepared.get(0));
        assertThat(converted).containsExactly(prepared.get(0), prepared.get(0), prepared.get(0));
        assertThat(convertedPartitions).containsExactly(0, 1, 0);

        service.pendingMetadata = CompletableFuture.completedFuture(metadata(true));
        assertErrors(
                backend.write(command((short) 1, topic("kafka.topic", good(0)))).join(),
                Errors.INVALID_TOPIC_EXCEPTION);
        assertThat(service.metadataPaths).hasSize(3);
        assertThat(service.appends).hasSize(2);
        service.pendingMetadata = new CompletableFuture<>();
        service.pendingMetadata.completeExceptionally(
                new org.apache.fluss.exception.AuthorizationException("revoked"));
        assertErrors(
                backend.write(command((short) 1, topic("kafka.topic", good(0)))).join(),
                Errors.TOPIC_AUTHORIZATION_FAILED);
        assertThat(service.metadataPaths).hasSize(4);
        assertThat(service.appends).hasSize(2);
    }

    @Test
    void testOffloadedRequestsAppendInOrderWithoutWaitingForAcknowledgement() throws Exception {
        KafkaProduceConversionExecutor executor = new KafkaProduceConversionExecutor(2, 4);
        TestingProduceService service = new TestingProduceService();
        service.pendingMetadata = new CompletableFuture<>();
        service.pendingAppend = new CompletableFuture<>();
        service.metadataEntered = new CountDownLatch(1);
        service.appended = new CountDownLatch(2);
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(
                        service, service, mapper(), new ArrowKafkaRecordTranscoder(), executor);
        try {
            CompletableFuture<KafkaProduceResult> first =
                    backend.write(command((short) -1, topic("kafka.topic", good(0))));
            assertThat(service.metadataEntered.await(10, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<KafkaProduceResult> second =
                    backend.write(command((short) -1, topic("kafka.topic", good(1))));
            assertThat(service.metadataThread).isNotSameAs(Thread.currentThread());
            CompletableFuture.runAsync(() -> service.pendingMetadata.complete(metadata(false)))
                    .join();
            assertThat(service.appended.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(service.appends)
                    .extracting(request -> request.getBucketsReqsList().get(0).getBucketId())
                    .containsExactly(0, 1);
            assertThat(first).isNotDone();
            assertThat(second).isNotDone();
            service.pendingAppend.complete(
                    new ProduceLogResponse()
                            .addAllBucketsResps(
                                    Arrays.asList(
                                            new PbProduceLogRespForBucket()
                                                    .setBucketId(0)
                                                    .setBaseOffset(17L),
                                            new PbProduceLogRespForBucket()
                                                    .setBucketId(1)
                                                    .setBaseOffset(18L))));
            assertErrors(first.get(10, TimeUnit.SECONDS), Errors.NONE);
            assertErrors(second.get(10, TimeUnit.SECONDS), Errors.NONE);
        } finally {
            executor.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void testOffloadedMetadataRefreshUsesNewTableIdentityAndSchema() throws Exception {
        TestingProduceService service = new TestingProduceService();
        KafkaProduceConversionExecutor executor = new KafkaProduceConversionExecutor(1, 2);
        ArrowKafkaRecordTranscoder transcoder = new ArrowKafkaRecordTranscoder();
        List<TableInfo> snapshots = new ArrayList<>();
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(
                        service,
                        service,
                        mapper(),
                        (partitionId, records, table) -> {
                            snapshots.add(table);
                            return transcoder.transcode(partitionId, records, table);
                        },
                        executor);
        KafkaProduceCommand command = command((short) 1, topic("kafka.topic", good(0)));
        try {
            assertErrors(backend.write(command).get(10, TimeUnit.SECONDS), Errors.NONE);
            assertErrors(backend.write(command).get(10, TimeUnit.SECONDS), Errors.NONE);
            TableDescriptor changedSchema =
                    TableDescriptor.builder()
                            .schema(
                                    Schema.newBuilder()
                                            .column("_kafka_value", DataTypes.BYTES())
                                            .build())
                            .distributedBy(3)
                            .logFormat(LogFormat.ARROW)
                            .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                            .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE, "binary")
                            .build();
            service.pendingMetadata =
                    CompletableFuture.completedFuture(
                            metadata(false)
                                    .setTableId(84L)
                                    .setSchemaId(2)
                                    .setBucketCountEpoch(7L)
                                    .setTableJson(changedSchema.toJsonBytes()));
            assertErrors(backend.write(command).get(10, TimeUnit.SECONDS), Errors.NONE);
            assertThat(snapshots).hasSize(3);
            assertThat(snapshots.get(1)).isSameAs(snapshots.get(0));
            assertThat(snapshots.get(2)).isNotSameAs(snapshots.get(0));
            assertThat(snapshots.get(2).getTableId()).isEqualTo(84L);
            assertThat(snapshots.get(2).getSchemaId()).isEqualTo(2);
            assertThat(snapshots.get(2).getRowType().getTypeAt(0)).isEqualTo(DataTypes.BYTES());
            assertThat(snapshots.get(2).getBucketCountEpoch()).isEqualTo(7L);
            assertThat(service.appends)
                    .extracting(ProduceLogRequest::getTableId)
                    .containsExactly(42L, 42L, 84L);
            service.pendingMetadata = new CompletableFuture<>();
            service.pendingMetadata.completeExceptionally(new TableNotExistException("deleted"));
            assertErrors(
                    backend.write(command).get(10, TimeUnit.SECONDS),
                    Errors.UNKNOWN_TOPIC_OR_PARTITION);
            assertThat(service.metadataPaths).hasSize(4);
            assertThat(service.appends).hasSize(3);
        } finally {
            executor.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void testShutdownPreservesAlreadySubmittedAppendResult() throws Exception {
        TestingProduceService service = new TestingProduceService();
        service.pendingAppend = new CompletableFuture<>();
        service.appended = new CountDownLatch(1);
        KafkaProduceConversionExecutor executor = new KafkaProduceConversionExecutor(1, 1);
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(
                        service, service, mapper(), new ArrowKafkaRecordTranscoder(), executor);
        try {
            CompletableFuture<KafkaProduceResult> result =
                    backend.write(command((short) -1, topic("kafka.topic", good(0))));
            assertThat(service.appended.await(10, TimeUnit.SECONDS)).isTrue();
            executor.closeAsync().get(10, TimeUnit.SECONDS);
            assertThat(result).isNotDone();
            assertThat(service.pendingAppend).isNotCancelled();
            service.pendingAppend.complete(success(0));
            KafkaProduceResult completed = result.get(10, TimeUnit.SECONDS);
            assertErrors(completed, Errors.NONE);
            assertThat(completed.topics().get(0).partitions().get(0).baseOffset()).isEqualTo(17L);
            assertThat(service.appends).hasSize(1);
        } finally {
            executor.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void testSaturationAndShutdownReturnRetriableErrorsWithoutAppend() throws Exception {
        KafkaProduceConversionExecutor executor = new KafkaProduceConversionExecutor(1, 1);
        TestingProduceService service = new TestingProduceService();
        service.pendingMetadata = new CompletableFuture<>();
        service.metadataEntered = new CountDownLatch(1);
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(
                        service, service, mapper(), new ArrowKafkaRecordTranscoder(), executor);
        KafkaProduceCommand command = command((short) 1, topic("kafka.topic", good(0)));
        try {
            CompletableFuture<KafkaProduceResult> running = backend.write(command);
            assertThat(service.metadataEntered.await(10, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<KafkaProduceResult> queued = backend.write(command);
            assertErrors(
                    backend.write(command).get(10, TimeUnit.SECONDS), Errors.REQUEST_TIMED_OUT);
            executor.closeAsync().get(10, TimeUnit.SECONDS);
            assertErrors(running.get(10, TimeUnit.SECONDS), Errors.REQUEST_TIMED_OUT);
            assertErrors(queued.get(10, TimeUnit.SECONDS), Errors.REQUEST_TIMED_OUT);
            assertErrors(
                    backend.write(command).get(10, TimeUnit.SECONDS), Errors.REQUEST_TIMED_OUT);
            service.pendingMetadata.complete(metadata(false));
            assertThat(service.appends).isEmpty();
            assertThat(service.metadataPaths).hasSize(1);
        } finally {
            executor.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void testAliasesSharePlansAndAuthorizationButRetainRequestNames() throws Exception {
        TestingProduceService service = new TestingProduceService();
        RecordingTranscoder transcoder = new RecordingTranscoder();
        KafkaProduceConversionExecutor executor = new KafkaProduceConversionExecutor(1, 4);
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(service, service, mapper(), transcoder, executor);
        try {
            KafkaProduceResult result =
                    backend.write(
                                    command(
                                            (short) 1,
                                            topic("topic", good(0)),
                                            topic("kafka.topic", good(0))))
                            .get(10, TimeUnit.SECONDS);
            assertThat(result.topics())
                    .extracting(KafkaProduceResult.TopicResult::topicName)
                    .containsExactly("topic", "kafka.topic");
            assertThat(result.topics())
                    .allSatisfy(
                            topic ->
                                    assertThat(topic.partitions().get(0).error())
                                            .isEqualTo(Errors.NONE));
            assertThat(service.metadataPaths)
                    .containsExactly(
                            TablePath.of("kafka", "topic"), TablePath.of("kafka", "topic"));
            assertThat(service.appends)
                    .extracting(ProduceLogRequest::getTableId)
                    .containsExactly(42L, 42L);
            assertThat(service.writeChecks).isEqualTo(2);
            assertThat(transcoder.prepared).hasSize(2);
            assertThat(transcoder.prepared.get(0)).isSameAs(transcoder.prepared.get(1));
        } finally {
            executor.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDisabledAndResetFlagsRejectBothNamesBeforePrepare(boolean allNull) throws Exception {
        TestingProduceService service = new TestingProduceService();
        RecordingTranscoder transcoder = new RecordingTranscoder();
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(service, service, mapper(), transcoder);
        PartitionWrite partition = allNull ? nullValue(0) : good(0);
        assertErrors(
                backend.write(command((short) 1, topic("topic", partition))).join(), Errors.NONE);
        for (boolean reset : new boolean[] {false, true}) {
            TableDescriptor enabled = TableDescriptor.fromJsonBytes(metadata(false).getTableJson());
            java.util.Map<String, String> properties =
                    new java.util.HashMap<>(enabled.getProperties());
            if (reset) {
                properties.remove(ConfigOptions.TABLE_KAFKA_ENABLED.key());
            } else {
                properties.put(ConfigOptions.TABLE_KAFKA_ENABLED.key(), "false");
            }
            service.pendingMetadata =
                    CompletableFuture.completedFuture(
                            metadata(false)
                                    .setTableJson(
                                            enabled.withProperties(properties).toJsonBytes()));
            int prepared = transcoder.prepared.size();
            int appended = service.appends.size();
            int authorized = service.writeChecks;
            for (String name : Arrays.asList("topic", "kafka.topic")) {
                assertErrors(
                        backend.write(command((short) 1, topic(name, partition))).join(),
                        Errors.UNKNOWN_TOPIC_OR_PARTITION);
            }
            assertThat(transcoder.prepared).hasSize(prepared);
            assertThat(service.appends).hasSize(appended);
            assertThat(service.writeChecks).isEqualTo(authorized);
            service.pendingMetadata = CompletableFuture.completedFuture(metadata(false));
            assertErrors(
                    backend.write(command((short) 1, topic("kafka.topic", partition))).join(),
                    Errors.NONE);
            assertThat(transcoder.prepared.get(prepared))
                    .isNotSameAs(transcoder.prepared.get(prepared - 1));
        }
        assertThat(service.metadataPaths).hasSize(7);
    }

    @Test
    void testPropertyOnlyChangesRejectInvalidContractsAndRecompileRecoveredPlan() throws Exception {
        TestingProduceService service = new TestingProduceService();
        RecordingTranscoder transcoder = new RecordingTranscoder();
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(service, service, mapper(), transcoder);
        assertErrors(
                backend.write(command((short) 1, topic("topic", good(0)))).join(), Errors.NONE);
        TableDescriptor original = TableDescriptor.fromJsonBytes(metadata(false).getTableJson());
        for (TableDescriptor invalid :
                Arrays.asList(
                        TableDescriptor.builder(original)
                                .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE, "binary")
                                .build(),
                        TableDescriptor.builder(original)
                                .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT, "nested")
                                .build(),
                        TableDescriptor.builder(original)
                                .property(ConfigOptions.TABLE_KAFKA_CONVERT_KEY_TYPE, "string")
                                .build())) {
            service.pendingMetadata =
                    CompletableFuture.completedFuture(
                            metadata(false).setTableJson(invalid.toJsonBytes()));
            for (String name : Arrays.asList("topic", "kafka.topic")) {
                assertErrors(
                        backend.write(command((short) 1, topic(name, nullValue(0)))).join(),
                        Errors.INVALID_TOPIC_EXCEPTION);
            }
        }
        assertThat(service.appends).hasSize(1);
        assertThat(transcoder.prepared).hasSize(1);
        service.pendingMetadata =
                CompletableFuture.completedFuture(
                        metadata(false)
                                .setTableJson(
                                        TableDescriptor.builder(original)
                                                .property(
                                                        ConfigOptions
                                                                .TABLE_KAFKA_CONVERT_VALUE_TYPE,
                                                        "STRING")
                                                .build()
                                                .toJsonBytes()));
        assertErrors(
                backend.write(command((short) 1, topic("kafka.topic", good(0)))).join(),
                Errors.NONE);
        assertThat(transcoder.prepared).hasSize(2);
        assertThat(transcoder.prepared.get(1)).isNotSameAs(transcoder.prepared.get(0));
        assertThat(transcoder.prepared.get(1).tableInfo().getSchemaId()).isEqualTo(1);
        assertThat(service.appends).hasSize(2);
        assertThat(service.metadataPaths).hasSize(8);
    }

    @Test
    void testMissingDefaultAndExplicitDatabasesDoNotFallback() throws Exception {
        TestingProduceService service = new TestingProduceService();
        Configuration config = new Configuration();
        config.set(ConfigOptions.KAFKA_DEFAULT_DATABASE, "missing_db");
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(
                        service,
                        service,
                        new KafkaTopicMapper(config),
                        new ArrowKafkaRecordTranscoder());
        KafkaProduceResult result =
                backend.write(
                                command(
                                        (short) 1,
                                        topic("topic", good(0)),
                                        topic("missing_db.topic", good(0)),
                                        topic("other.topic", good(0))))
                        .join();
        assertThat(result.topics())
                .extracting(topic -> topic.partitions().get(0).error())
                .containsExactly(
                        Errors.UNKNOWN_TOPIC_OR_PARTITION,
                        Errors.UNKNOWN_TOPIC_OR_PARTITION,
                        Errors.NONE);
        assertThat(service.metadataPaths)
                .containsExactly(
                        TablePath.of("missing_db", "topic"),
                        TablePath.of("missing_db", "topic"),
                        TablePath.of("other", "topic"));
        assertThat(service.appends).extracting(ProduceLogRequest::getTableId).containsExactly(43L);
    }

    @Test
    void testRecordNullabilityFailureDoesNotAppendValidPrefix() throws Exception {
        TestingProduceService service = new TestingProduceService();
        TableDescriptor original = TableDescriptor.fromJsonBytes(metadata(false).getTableJson());
        service.pendingMetadata =
                CompletableFuture.completedFuture(
                        metadata(false)
                                .setTableJson(
                                        TableDescriptor.builder(original)
                                                .schema(
                                                        Schema.newBuilder()
                                                                .column(
                                                                        "_kafka_value",
                                                                        DataTypes.STRING())
                                                                .column(
                                                                        "_kafka_key",
                                                                        DataTypes.BYTES()
                                                                                .copy(false))
                                                                .build())
                                                .build()
                                                .toJsonBytes()));
        Record valid = new Record(1L, new byte[] {1}, new byte[] {65}, Collections.emptyList());
        assertErrors(
                backend(service)
                        .write(
                                command(
                                        (short) 1,
                                        topic(
                                                "topic",
                                                new PartitionWrite(
                                                        0,
                                                        Arrays.asList(
                                                                valid, good(0).records().get(0))))))
                        .join(),
                Errors.INVALID_RECORD);
        assertThat(service.appends).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"binary", "string"})
    void testNullValueViolationRejectsWholePartitionWithoutAppendingPrefix(String format)
            throws Exception {
        TestingProduceService service = new TestingProduceService();
        TableDescriptor original = TableDescriptor.fromJsonBytes(metadata(false).getTableJson());
        TableDescriptor notNull =
                TableDescriptor.builder(original)
                        .schema(
                                Schema.newBuilder()
                                        .column(
                                                "_kafka_value",
                                                (format.equals("binary")
                                                                ? DataTypes.BYTES()
                                                                : DataTypes.STRING())
                                                        .copy(false))
                                        .build())
                        .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE, format)
                        .build();
        service.pendingMetadata =
                CompletableFuture.completedFuture(
                        metadata(false).setTableJson(notNull.toJsonBytes()));
        GatewayKafkaProduceBackend backend = backend(service);
        assertErrors(
                backend.write(command((short) 1, topic("topic", nullValue(0)))).join(),
                Errors.INVALID_RECORD);
        assertThat(service.appends).isEmpty();

        Record valid = good(0).records().get(0);
        PartitionWrite invalid =
                new PartitionWrite(0, Arrays.asList(valid, nullValue(0).records().get(0), valid));
        assertErrors(
                backend.write(command((short) 1, topic("topic", invalid, good(1)))).join(),
                Errors.INVALID_RECORD,
                Errors.NONE);
        assertThat(service.appends).hasSize(1);
        assertThat(service.append.getBucketsReqsList()).hasSize(1);
        assertThat(service.append.getBucketsReqsList().get(0).getBucketId()).isEqualTo(1);
    }

    @Test
    void testMetadataOffsetsAndJsonCannotBeActivatedByNullValues() throws Exception {
        TestingProduceService service = new TestingProduceService();
        TableDescriptor original = TableDescriptor.fromJsonBytes(metadata(false).getTableJson());
        TableDescriptor unsupportedMetadata =
                TableDescriptor.builder(original)
                        .schema(
                                Schema.newBuilder()
                                        .column("_kafka_value", DataTypes.STRING())
                                        .column(
                                                "_kafka_metadata",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "timestamp",
                                                                DataTypes.TIMESTAMP_LTZ(3)),
                                                        DataTypes.FIELD(
                                                                "timestamp_type", DataTypes.INT()),
                                                        DataTypes.FIELD(
                                                                "partition", DataTypes.INT()),
                                                        DataTypes.FIELD(
                                                                "offset", DataTypes.BIGINT())))
                                        .build())
                        .build();
        service.pendingMetadata =
                CompletableFuture.completedFuture(
                        metadata(false).setTableJson(unsupportedMetadata.toJsonBytes()));
        GatewayKafkaProduceBackend backend = backend(service);
        assertErrors(
                backend.write(command((short) 1, topic("topic", nullValue(0)))).join(),
                Errors.INVALID_TOPIC_EXCEPTION);
        TableDescriptor json =
                TableDescriptor.builder(original)
                        .schema(Schema.newBuilder().column("business", DataTypes.STRING()).build())
                        .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE, "parse_json")
                        .build();
        service.pendingMetadata =
                CompletableFuture.completedFuture(metadata(false).setTableJson(json.toJsonBytes()));
        assertErrors(
                backend.write(command((short) 1, topic("topic", nullValue(0)))).join(),
                Errors.INVALID_RECORD);
        assertThat(service.appends).isEmpty();
        assertThat(service.writeChecks).isZero();
    }

    private static final class RecordingTranscoder implements KafkaRecordTranscoder {
        private final ArrowKafkaRecordTranscoder arrow = new ArrowKafkaRecordTranscoder();
        private final List<KafkaTopicWritePlan> prepared = new ArrayList<>();

        @Override
        public KafkaTopicWritePlan prepare(TableInfo table) {
            KafkaTopicWritePlan plan = arrow.prepare(table);
            prepared.add(plan);
            return plan;
        }

        @Override
        public BytesView transcode(int partitionId, List<Record> records, TableInfo table)
                throws Exception {
            return arrow.transcode(partitionId, records, table);
        }
    }

    private static KafkaTopicMapper mapper() {
        Configuration config = new Configuration();
        config.set(ConfigOptions.KAFKA_DEFAULT_DATABASE, "kafka");
        return new KafkaTopicMapper(config);
    }

    private static GatewayKafkaProduceBackend backend(TestingProduceService service) {
        return new GatewayKafkaProduceBackend(
                service, service, mapper(), new ArrowKafkaRecordTranscoder());
    }

    private static KafkaProduceCommand command(short acks, TopicWrite... topics) throws Exception {
        return new KafkaProduceCommand(
                acks, 4321, Arrays.asList(topics), "KAFKA", InetAddress.getLoopbackAddress());
    }

    private static TopicWrite topic(String name, PartitionWrite... partitions) {
        return new TopicWrite(name, Arrays.asList(partitions));
    }

    private static PartitionWrite good(int id) {
        return new PartitionWrite(
                id,
                Collections.singletonList(
                        new Record(123L, null, new byte[] {65}, Collections.emptyList())));
    }

    private static GetTableInfoResponse metadata(boolean invalid) {
        TableDescriptor.Builder descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("_kafka_value", DataTypes.STRING())
                                        .build())
                        .distributedBy(3)
                        .logFormat(LogFormat.ARROW)
                        .property(ConfigOptions.TABLE_KAFKA_ENABLED, true);
        if (!invalid) {
            descriptor.property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE, "string");
        }
        return new GetTableInfoResponse()
                .setTableId(42L)
                .setSchemaId(1)
                .setTableJson(descriptor.build().toJsonBytes())
                .setCreatedTime(1L)
                .setModifiedTime(1L);
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
        private CompletableFuture<GetTableInfoResponse> pendingMetadata;
        private boolean throwAppend;
        private boolean asyncMissing;
        private int writeChecks;
        private boolean denyWrite;
        private CountDownLatch metadataEntered;
        private CountDownLatch appended;
        private Thread metadataThread;

        @Override
        public CompletableFuture<GetTableInfoResponse> getTableInfo(GetTableInfoRequest request) {
            assertThat(currentListenerName()).isEqualTo("KAFKA");
            String database = request.getTablePath().getDatabaseName();
            String name = request.getTablePath().getTableName();
            metadataPaths.add(TablePath.of(database, name));
            metadataThread = Thread.currentThread();
            if (metadataEntered != null) {
                metadataEntered.countDown();
            }
            if (database.equals("missing_db")) {
                throw new DatabaseNotExistException("missing_db");
            }
            if (name.equals("missing")) {
                if (!asyncMissing) {
                    throw new TableNotExistException("missing");
                }
                CompletableFuture<GetTableInfoResponse> failed = new CompletableFuture<>();
                failed.completeExceptionally(new TableNotExistException("missing"));
                return failed;
            }
            return pendingMetadata == null
                    ? CompletableFuture.completedFuture(
                            GatewayKafkaProduceBackendTest.metadata(name.equals("invalid"))
                                    .setTableId(database.equals("other") ? 43L : 42L))
                    : pendingMetadata;
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
            append = request;
            appends.add(request);
            if (throwAppend) {
                throw new IllegalStateException("append failed");
            }
            if (appended != null) {
                appended.countDown();
            }
            return pendingAppend == null
                    ? CompletableFuture.completedFuture(
                            success(request.getBucketsReqsList().get(0).getBucketId()))
                    : pendingAppend;
        }
    }
}

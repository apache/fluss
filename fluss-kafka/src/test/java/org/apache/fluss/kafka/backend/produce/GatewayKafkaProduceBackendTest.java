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

import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.PartitionWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.TopicWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult.PartitionResult;
import org.apache.fluss.kafka.format.KafkaDataFormat;
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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests append admission, error isolation, session propagation and delayed-fetch completion. */
class GatewayKafkaProduceBackendTest {

    @Test
    void testDrainsImmediatelyWhileAcksAllResponseIsPending() throws Exception {
        TestingProduceService service = new TestingProduceService();
        service.pendingAppend = new CompletableFuture<>();
        CompletableFuture<KafkaProduceResult> result =
                backend(service).write(command((short) -1, topic("kafka.topic", good(0))));
        assertThat(service.append.getAcks()).isEqualTo(-1);
        assertThat(service.append.getTimeoutMs()).isEqualTo(4321);
        assertThat(service.append.getTableId()).isEqualTo(42L);
        assertThat(service.drains).isEqualTo(1);
        assertThat(result).isNotDone();
        assertThat(service.pendingAppend).isNotDone();
        service.pendingAppend.complete(success(0));
        assertThat(result.join().topics().get(0).partitions().get(0).baseOffset()).isEqualTo(17L);
        assertThat(service.drains).isEqualTo(1);
    }

    @Test
    void testDrainsAfterSynchronousAppendFailure() throws Exception {
        TestingProduceService service = new TestingProduceService();
        service.throwAppend = true;
        assertErrors(
                backend(service).write(command((short) 1, topic("kafka.topic", good(0)))).join(),
                Errors.UNKNOWN_SERVER_ERROR);
        assertThat(service.drains).isEqualTo(1);
    }

    @Test
    void testConversionAndBucketErrorsAreIsolatedFromValidWrites() throws Exception {
        TestingProduceService service = new TestingProduceService();
        PartitionWrite malformed =
                new PartitionWrite(
                        0,
                        Collections.singletonList(
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
        assertErrors(
                result, Errors.CORRUPT_MESSAGE, Errors.NONE, Errors.UNKNOWN_TOPIC_OR_PARTITION);
        assertThat(service.append.getBucketsReqsList()).hasSize(1);
        assertThat(service.append.getBucketsReqsList().get(0).getBucketId()).isEqualTo(1);
        assertThat(service.drains).isEqualTo(1);
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
            assertThat(service.drains).isEqualTo(1);
        }
    }

    @Test
    void testNoAppendOrDrainWhenAllPartitionsFailConversion() throws Exception {
        TestingProduceService service = new TestingProduceService();
        KafkaProduceResult result =
                backend(service).write(command((short) 1, topic("kafka.topic", good(8)))).join();
        assertErrors(result, Errors.UNKNOWN_TOPIC_OR_PARTITION);
        assertThat(service.append).isNull();
        assertThat(service.drains).isZero();
    }

    @Test
    void testMapsReplicationFailuresAndMissingResponsesPerPartition() throws Exception {
        org.apache.fluss.rpc.protocol.Errors[] flussErrors = {
            org.apache.fluss.rpc.protocol.Errors.NOT_ENOUGH_REPLICAS_EXCEPTION,
            org.apache.fluss.rpc.protocol.Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND_EXCEPTION,
            org.apache.fluss.rpc.protocol.Errors.REQUEST_TIME_OUT,
            org.apache.fluss.rpc.protocol.Errors.NOT_LEADER_OR_FOLLOWER
        };
        Errors[] kafkaErrors = {
            Errors.NOT_ENOUGH_REPLICAS,
            Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND,
            Errors.REQUEST_TIMED_OUT,
            Errors.NOT_LEADER_OR_FOLLOWER
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
        assertThat(service.drains).isEqualTo(1);
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
                        (records, tableInfo) -> {
                            transcodedPaths.add(tableInfo.getTablePath());
                            return transcoder.transcode(records, tableInfo);
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
                                        topic("topic", good(0)),
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
        KafkaRecordTranscoder transcoder =
                new KafkaRecordTranscoder() {
                    @Override
                    public KafkaTopicWritePlan prepare(TableInfo tableInfo) {
                        KafkaTopicWritePlan plan = arrow.prepare(tableInfo);
                        prepared.add(plan);
                        return plan;
                    }

                    @Override
                    public BytesView transcode(List<Record> records, TableInfo tableInfo)
                            throws Exception {
                        return arrow.transcode(records, tableInfo);
                    }

                    @Override
                    public BytesView transcode(List<Record> records, KafkaTopicWritePlan plan)
                            throws Exception {
                        converted.add(plan);
                        return arrow.transcode(records, plan);
                    }
                };
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(service, service, transcoder);
        backend.write(command((short) 1, topic("kafka.topic", good(0), good(1)))).join();
        backend.write(command((short) 1, topic("kafka.topic", good(0)))).join();
        assertThat(service.metadataPaths).hasSize(2);
        assertThat(prepared).hasSize(2);
        assertThat(prepared.get(1)).isSameAs(prepared.get(0));
        assertThat(converted).containsExactly(prepared.get(0), prepared.get(0), prepared.get(0));

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
                        service, service, new ArrowKafkaRecordTranscoder(), executor);
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
    void testSaturationAndShutdownReturnRetriableErrorsWithoutAppend() throws Exception {
        KafkaProduceConversionExecutor executor = new KafkaProduceConversionExecutor(1, 1);
        TestingProduceService service = new TestingProduceService();
        service.pendingMetadata = new CompletableFuture<>();
        service.metadataEntered = new CountDownLatch(1);
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(
                        service, service, new ArrowKafkaRecordTranscoder(), executor);
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

    private static GatewayKafkaProduceBackend backend(TestingProduceService service) {
        return new GatewayKafkaProduceBackend(service, service, new ArrowKafkaRecordTranscoder());
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
                        .schema(Schema.newBuilder().column("value", DataTypes.STRING()).build())
                        .distributedBy(3)
                        .logFormat(LogFormat.ARROW);
        if (!invalid) {
            descriptor.customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "string");
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
        private int drains;
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

        @Override
        public CompletableFuture<ProduceLogResponse> produceLog(ProduceLogRequest request) {
            assertThat(currentListenerName()).isEqualTo("KAFKA");
            assertThat(currentSession().getInetAddress())
                    .isEqualTo(InetAddress.getLoopbackAddress());
            append = request;
            appends.add(request);
            if (throwAppend) {
                throw new IllegalStateException("append failed");
            }
            return pendingAppend == null
                    ? CompletableFuture.completedFuture(
                            success(request.getBucketsReqsList().get(0).getBucketId()))
                    : pendingAppend;
        }

        @Override
        public void tryCompleteActions() {
            assertThat(append).isNotNull();
            drains++;
            if (appended != null) {
                appended.countDown();
            }
        }
    }
}

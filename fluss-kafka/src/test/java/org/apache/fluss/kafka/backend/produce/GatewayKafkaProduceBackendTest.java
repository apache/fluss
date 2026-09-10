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
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests append admission, error isolation, session propagation and delayed-fetch completion. */
class GatewayKafkaProduceBackendTest {

    @Test
    void testDrainsImmediatelyWhileAcksAllResponseIsPending() throws Exception {
        TestingProduceService service = new TestingProduceService();
        service.pendingAppend = new CompletableFuture<>();
        CompletableFuture<KafkaProduceResult> result =
                backend(service).write(command((short) -1, topic("topic", good(0))));
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
                backend(service).write(command((short) 1, topic("topic", good(0)))).join(),
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
                        .write(command((short) 1, topic("topic", malformed, good(1), good(8))))
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
                                            topic("invalid", good(0)),
                                            topic("missing", good(0)),
                                            topic("topic", good(0))))
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
                backend(service).write(command((short) 1, topic("topic", good(8)))).join();
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
                            .write(command((short) -1, topic("topic", good(0), good(1), good(2))))
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
                backend(service).write(command((short) 0, topic("topic", good(0))));
        CompletableFuture.runAsync(() -> service.pendingMetadata.complete(metadata(false))).join();
        assertErrors(result.join(), Errors.NONE);
        assertThat(service.append.getAcks()).isZero();
        assertThat(service.drains).isEqualTo(1);
    }

    private static GatewayKafkaProduceBackend backend(TestingProduceService service) {
        return new GatewayKafkaProduceBackend(
                service, service, "kafka", new ArrowKafkaRecordTranscoder());
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
        private ProduceLogRequest append;
        private CompletableFuture<ProduceLogResponse> pendingAppend;
        private CompletableFuture<GetTableInfoResponse> pendingMetadata;
        private boolean throwAppend;
        private boolean asyncMissing;
        private int drains;

        @Override
        public CompletableFuture<GetTableInfoResponse> getTableInfo(GetTableInfoRequest request) {
            assertThat(currentListenerName()).isEqualTo("KAFKA");
            assertThat(request.getTablePath().getDatabaseName()).isEqualTo("kafka");
            String name = request.getTablePath().getTableName();
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
                            GatewayKafkaProduceBackendTest.metadata(name.equals("invalid")))
                    : pendingMetadata;
        }

        @Override
        public CompletableFuture<ProduceLogResponse> produceLog(ProduceLogRequest request) {
            assertThat(currentListenerName()).isEqualTo("KAFKA");
            assertThat(currentSession().getInetAddress())
                    .isEqualTo(InetAddress.getLoopbackAddress());
            append = request;
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
        }
    }
}

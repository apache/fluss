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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.PbProduceLogRespForBucket;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.server.utils.ServerRpcMessageUtils;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Protocol and native-record tests for the minimal Kafka Produce implementation. */
public class KafkaProduceHandlerTest {

    private static final int SCHEMA_ID = 1;
    private static final long TABLE_ID = 123L;
    private static final long TIMESTAMP = 123456L;

    @Test
    public void testAuthenticatedPrincipalPropagatesToProduceGatewaySessions() {
        TestingProduceGatewayService service = new TestingProduceGatewayService();
        FlussPrincipal principal = new FlussPrincipal("kafka-user", "User");
        short version = ApiKeys.PRODUCE.latestVersion();
        ProduceRequest requestBody = produceRequest(version, (short) 1);
        KafkaRequest request =
                new KafkaRequest(
                        ApiKeys.PRODUCE,
                        version,
                        new RequestHeader(ApiKeys.PRODUCE, version, "client-id", 1),
                        requestBody,
                        "KAFKA",
                        org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator.DEFAULT
                                .buffer(),
                        new TestingChannelHandlerContext(),
                        new CompletableFuture<>()) {
                    @Override
                    public FlussPrincipal principal() {
                        return principal;
                    }
                };

        new KafkaRequestHandler(service, service, "kafka").processRequest(request);

        assertThat(parseResponse(request).errorCounts()).containsOnlyKeys(Errors.NONE);
        assertThat(service.getTableInfoPrincipal).isEqualTo(principal);
        assertThat(service.producePrincipal).isEqualTo(principal);
    }

    @Test
    public void testProduceTranscodesAndWritesKafkaRecord() throws Exception {
        TestingProduceGatewayService service = new TestingProduceGatewayService();
        short version = ApiKeys.PRODUCE.latestVersion();
        ProduceRequest requestBody = produceRequest(version, (short) 1);
        KafkaRequest request = kafkaRequest(requestBody, version);

        new KafkaRequestHandler(service, service, "kafka").processRequest(request);

        ProduceResponse response = parseResponse(request);
        assertThat(response.errorCounts()).containsOnlyKeys(Errors.NONE);
        ProduceResponseData.PartitionProduceResponse partitionResponse =
                response.data().responses().find("topic").partitionResponses().get(0);
        assertThat(partitionResponse.baseOffset()).isEqualTo(42L);
        assertThat(service.lastProduceRequest.getTableId()).isEqualTo(TABLE_ID);
        assertThat(service.lastProduceRequest.getAcks()).isEqualTo(1);
        assertThat(service.lastProduceRequest.getTimeoutMs()).isEqualTo(1000);

        MemoryLogRecords records =
                ServerRpcMessageUtils.getProduceLogData(service.lastProduceRequest)
                        .values()
                        .iterator()
                        .next();
        LogRecordBatch batch = records.batches().iterator().next();
        batch.ensureValid();
        assertThat(batch.schemaId()).isEqualTo((short) SCHEMA_ID);
        assertThat(batch.getRecordCount()).isEqualTo(1);
        try (LogRecordReadContext context =
                        LogRecordReadContext.createArrowReadContext(
                                service.schema.getRowType(),
                                SCHEMA_ID,
                                new TestingSchemaGetter(
                                        new SchemaInfo(service.schema, SCHEMA_ID)));
                CloseableIterator<LogRecord> iterator = batch.records(context)) {
            LogRecord record = iterator.next();
            InternalRow row = record.getRow();
            assertThat(row.getBytes(0)).isEqualTo(bytes("key"));
            assertThat(row.getBytes(1)).isEqualTo(bytes("value"));
            assertThat(row.getTimestampLtz(2, 3).getEpochMillisecond()).isEqualTo(TIMESTAMP);
            InternalArray headers = row.getArray(3);
            assertThat(headers.size()).isEqualTo(1);
            InternalRow header = headers.getRow(0, 2);
            assertThat(header.getString(0).toString()).isEqualTo("header");
            assertThat(header.getBytes(1)).isEqualTo(bytes("header-value"));
            assertThat(iterator.hasNext()).isFalse();
        }
    }

    @Test
    public void testAcksZeroCompletesWithoutChangingWriteSemantics() {
        TestingProduceGatewayService service = new TestingProduceGatewayService();
        short version = ApiKeys.PRODUCE.latestVersion();
        KafkaRequest request = kafkaRequest(produceRequest(version, (short) 0), version);

        new KafkaRequestHandler(service, service, "kafka").processRequest(request);

        assertThat(request.future()).isCompleted();
        assertThat(service.lastProduceRequest.getAcks()).isZero();
    }

    @Test
    public void testAcksAllAndTimeoutAreForwardedUnchanged() {
        TestingProduceGatewayService service = new TestingProduceGatewayService();
        short version = ApiKeys.PRODUCE.latestVersion();
        KafkaRequest request = kafkaRequest(produceRequest(version, (short) -1, 4321), version);

        new KafkaRequestHandler(service, service, "kafka").processRequest(request);

        assertThat(parseResponse(request).errorCounts()).containsOnlyKeys(Errors.NONE);
        assertThat(service.lastProduceRequest.getAcks()).isEqualTo(-1);
        assertThat(service.lastProduceRequest.getTimeoutMs()).isEqualTo(4321);
    }

    @Test
    public void testInvalidAcksReturnsInvalidRequiredAcksWithoutCallingBackend() {
        TestingProduceGatewayService service = new TestingProduceGatewayService();
        short version = ApiKeys.PRODUCE.latestVersion();
        KafkaRequest request = kafkaRequest(produceRequest(version, (short) 2), version);

        new KafkaRequestHandler(service, service, "kafka").processRequest(request);

        ProduceResponseData.PartitionProduceResponse partition =
                parseResponse(request).data().responses().find("topic").partitionResponses().get(0);
        assertThat(Errors.forCode(partition.errorCode())).isEqualTo(Errors.INVALID_REQUIRED_ACKS);
        assertThat(partition.baseOffset()).isEqualTo(-1L);
        assertThat(service.lastProduceRequest).isNull();
    }

    @Test
    public void testProduceTranscodesStringKeyAndValue() throws Exception {
        TestingProduceGatewayService service =
                new TestingProduceGatewayService(KafkaDataFormat.STRING, KafkaDataFormat.STRING);
        short version = ApiKeys.PRODUCE.latestVersion();
        KafkaRequest request = kafkaRequest(produceRequest(version, (short) 1), version);

        new KafkaRequestHandler(service, service, "kafka").processRequest(request);

        assertThat(parseResponse(request).errorCounts()).containsOnlyKeys(Errors.NONE);
        MemoryLogRecords records =
                ServerRpcMessageUtils.getProduceLogData(service.lastProduceRequest)
                        .values()
                        .iterator()
                        .next();
        LogRecordBatch batch = records.batches().iterator().next();
        try (LogRecordReadContext context =
                        LogRecordReadContext.createArrowReadContext(
                                service.schema.getRowType(),
                                SCHEMA_ID,
                                new TestingSchemaGetter(
                                        new SchemaInfo(service.schema, SCHEMA_ID)));
                CloseableIterator<LogRecord> iterator = batch.records(context)) {
            InternalRow row = iterator.next().getRow();
            assertThat(row.getString(0).toString()).isEqualTo("key");
            assertThat(row.getString(1).toString()).isEqualTo("value");
        }
    }

    @Test
    public void testProduceRejectsInvalidUtf8ForStringFormat() {
        TestingProduceGatewayService service =
                new TestingProduceGatewayService(KafkaDataFormat.STRING, KafkaDataFormat.RAW);
        short version = ApiKeys.PRODUCE.latestVersion();
        KafkaRequest request =
                kafkaRequest(
                        produceRequest(
                                version, (short) 1, new byte[] {(byte) 0xc3, 0x28}, bytes("value")),
                        version);

        new KafkaRequestHandler(service, service, "kafka").processRequest(request);

        ProduceResponse response = parseResponse(request);
        assertThat(response.errorCounts()).containsEntry(Errors.CORRUPT_MESSAGE, 1);
        assertThat(service.lastProduceRequest).isNull();
    }

    @Test
    public void testMapsAcksAllOutcomesToPartitionResponses() {
        assertAcksAllOutcome(org.apache.fluss.rpc.protocol.Errors.NONE, Errors.NONE, 42L);
        assertAcksAllOutcome(
                org.apache.fluss.rpc.protocol.Errors.NOT_ENOUGH_REPLICAS_EXCEPTION,
                Errors.NOT_ENOUGH_REPLICAS,
                -1L);
        assertAcksAllOutcome(
                org.apache.fluss.rpc.protocol.Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND_EXCEPTION,
                Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND,
                -1L);
        assertAcksAllOutcome(
                org.apache.fluss.rpc.protocol.Errors.REQUEST_TIME_OUT,
                Errors.REQUEST_TIMED_OUT,
                -1L);
        assertAcksAllOutcome(
                org.apache.fluss.rpc.protocol.Errors.NOT_LEADER_OR_FOLLOWER,
                Errors.NOT_LEADER_OR_FOLLOWER,
                -1L);
    }

    @Test
    public void testMapsMixedPartitionResultsIndependently() {
        TestingProduceGatewayService service = new TestingProduceGatewayService();
        service.produceResponse =
                new ProduceLogResponse()
                        .addAllBucketsResps(
                                Arrays.asList(
                                        new PbProduceLogRespForBucket()
                                                .setBucketId(0)
                                                .setBaseOffset(42L),
                                        new PbProduceLogRespForBucket()
                                                .setBucketId(1)
                                                .setErrorCode(
                                                        org.apache.fluss.rpc.protocol.Errors
                                                                .NOT_ENOUGH_REPLICAS_EXCEPTION
                                                                .code())));
        short version = ApiKeys.PRODUCE.latestVersion();
        KafkaRequest request = kafkaRequest(twoPartitionProduceRequest(version), version);

        new KafkaRequestHandler(service, service, "kafka").processRequest(request);

        ProduceResponse response = parseResponse(request);
        ProduceResponseData.PartitionProduceResponse successful =
                response.data().responses().find("topic").partitionResponses().get(0);
        ProduceResponseData.PartitionProduceResponse failed =
                response.data().responses().find("topic").partitionResponses().get(1);
        assertThat(Errors.forCode(successful.errorCode())).isEqualTo(Errors.NONE);
        assertThat(successful.baseOffset()).isEqualTo(42L);
        assertThat(Errors.forCode(failed.errorCode())).isEqualTo(Errors.NOT_ENOUGH_REPLICAS);
        assertThat(failed.baseOffset()).isEqualTo(-1L);
    }

    @Test
    public void testMissingBucketResponseFailsOnlyThatPartition() {
        TestingProduceGatewayService service = new TestingProduceGatewayService();
        service.produceResponse =
                new ProduceLogResponse()
                        .addAllBucketsResps(
                                Collections.singletonList(
                                        new PbProduceLogRespForBucket()
                                                .setBucketId(0)
                                                .setBaseOffset(42L)));
        short version = ApiKeys.PRODUCE.latestVersion();
        KafkaRequest request = kafkaRequest(twoPartitionProduceRequest(version), version);

        new KafkaRequestHandler(service, service, "kafka").processRequest(request);

        ProduceResponse response = parseResponse(request);
        ProduceResponseData.PartitionProduceResponse successful =
                response.data().responses().find("topic").partitionResponses().get(0);
        ProduceResponseData.PartitionProduceResponse missing =
                response.data().responses().find("topic").partitionResponses().get(1);
        assertThat(Errors.forCode(successful.errorCode())).isEqualTo(Errors.NONE);
        assertThat(successful.baseOffset()).isEqualTo(42L);
        assertThat(Errors.forCode(missing.errorCode())).isEqualTo(Errors.UNKNOWN_SERVER_ERROR);
        assertThat(missing.baseOffset()).isEqualTo(-1L);
        assertThat(missing.errorMessage()).contains("omitted this bucket");
    }

    @Test
    public void testEveryAdvertisedProduceVersion() {
        for (short version = 3; version <= ApiKeys.PRODUCE.latestVersion(); version++) {
            TestingProduceGatewayService service = new TestingProduceGatewayService();
            KafkaRequest request = kafkaRequest(produceRequest(version, (short) 1), version);

            new KafkaRequestHandler(service, service, "kafka").processRequest(request);

            assertThat(parseResponse(request).errorCounts()).containsOnlyKeys(Errors.NONE);
        }
    }

    private static ProduceRequest produceRequest(short version, short acks) {
        return produceRequest(version, acks, 1000);
    }

    private static ProduceRequest produceRequest(short version, short acks, int timeoutMs) {
        return produceRequest(version, acks, timeoutMs, bytes("key"), bytes("value"));
    }

    private static ProduceRequest produceRequest(
            short version, short acks, byte[] key, byte[] value) {
        return produceRequest(version, acks, 1000, key, value);
    }

    private static ProduceRequest produceRequest(
            short version, short acks, int timeoutMs, byte[] key, byte[] value) {
        Header[] headers = {new RecordHeader("header", bytes("header-value"))};
        MemoryRecords records =
                MemoryRecords.withRecords(
                        org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2,
                        TIMESTAMP,
                        Compression.NONE,
                        new SimpleRecord(TIMESTAMP, key, value, headers));
        TopicProduceData topic =
                new TopicProduceData()
                        .setName("topic")
                        .setPartitionData(
                                Collections.singletonList(
                                        new PartitionProduceData()
                                                .setIndex(0)
                                                .setRecords(records)));
        ProduceRequestData data =
                new ProduceRequestData()
                        .setAcks(acks)
                        .setTimeoutMs(timeoutMs)
                        .setTopicData(
                                new ProduceRequestData.TopicProduceDataCollection(
                                        Collections.singletonList(topic).iterator()));
        return new ProduceRequest(data, version);
    }

    private static ProduceRequest twoPartitionProduceRequest(short version) {
        TopicProduceData topic =
                new TopicProduceData()
                        .setName("topic")
                        .setPartitionData(
                                Arrays.asList(
                                        new PartitionProduceData()
                                                .setIndex(0)
                                                .setRecords(memoryRecords()),
                                        new PartitionProduceData()
                                                .setIndex(1)
                                                .setRecords(memoryRecords())));
        ProduceRequestData data =
                new ProduceRequestData()
                        .setAcks((short) -1)
                        .setTimeoutMs(4321)
                        .setTopicData(
                                new ProduceRequestData.TopicProduceDataCollection(
                                        Collections.singletonList(topic).iterator()));
        return new ProduceRequest(data, version);
    }

    private static MemoryRecords memoryRecords() {
        return MemoryRecords.withRecords(
                org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2,
                TIMESTAMP,
                Compression.NONE,
                new SimpleRecord(TIMESTAMP, bytes("key"), bytes("value")));
    }

    private static void assertAcksAllOutcome(
            org.apache.fluss.rpc.protocol.Errors flussError,
            Errors expectedKafkaError,
            long expectedBaseOffset) {
        TestingProduceGatewayService service = new TestingProduceGatewayService();
        service.produceError = flussError;
        short version = ApiKeys.PRODUCE.latestVersion();
        KafkaRequest request = kafkaRequest(produceRequest(version, (short) -1, 4321), version);

        new KafkaRequestHandler(service, service, "kafka").processRequest(request);

        ProduceResponseData.PartitionProduceResponse partition =
                parseResponse(request).data().responses().find("topic").partitionResponses().get(0);
        assertThat(Errors.forCode(partition.errorCode())).isEqualTo(expectedKafkaError);
        assertThat(partition.baseOffset()).isEqualTo(expectedBaseOffset);
        assertThat(service.lastProduceRequest.getAcks()).isEqualTo(-1);
        assertThat(service.lastProduceRequest.getTimeoutMs()).isEqualTo(4321);
    }

    private static KafkaRequest kafkaRequest(ProduceRequest requestBody, short version) {
        return new KafkaRequest(
                ApiKeys.PRODUCE,
                version,
                new RequestHeader(ApiKeys.PRODUCE, version, "client-id", 1),
                requestBody,
                "KAFKA",
                org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator.DEFAULT.buffer(),
                new TestingChannelHandlerContext(),
                new CompletableFuture<>());
    }

    private static ProduceResponse parseResponse(KafkaRequest request) {
        org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf responseBuffer =
                request.responseBuffer();
        try {
            return (ProduceResponse)
                    AbstractResponse.parseResponse(responseBuffer.nioBuffer(), request.header());
        } finally {
            responseBuffer.release();
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static final class TestingProduceGatewayService extends TestingTabletGatewayService {
        private final Schema schema;
        private final TableDescriptor tableDescriptor;
        private ProduceLogRequest lastProduceRequest;
        private ProduceLogResponse produceResponse;
        private FlussPrincipal getTableInfoPrincipal;
        private FlussPrincipal producePrincipal;
        private org.apache.fluss.rpc.protocol.Errors produceError =
                org.apache.fluss.rpc.protocol.Errors.NONE;

        private TestingProduceGatewayService() {
            this(KafkaDataFormat.RAW, KafkaDataFormat.RAW);
        }

        private TestingProduceGatewayService(
                KafkaDataFormat keyFormat, KafkaDataFormat valueFormat) {
            schema =
                    Schema.newBuilder()
                            .column("record_key", dataType(keyFormat))
                            .column("payload", dataType(valueFormat))
                            .column("event_time", DataTypes.TIMESTAMP_LTZ(3).copy(false))
                            .column(
                                    "headers",
                                    DataTypes.ARRAY(
                                            DataTypes.ROW(
                                                    DataTypes.FIELD(
                                                            "name", DataTypes.STRING().copy(false)),
                                                    DataTypes.FIELD("value", DataTypes.BYTES()))))
                            .build();
            tableDescriptor =
                    TableDescriptor.builder()
                            .schema(schema)
                            .distributedBy(1)
                            .property(ConfigOptions.TABLE_LOG_FORMAT, LogFormat.ARROW)
                            .customProperty(KafkaDataFormat.KEY_FORMAT_CONFIG, keyFormat.value())
                            .customProperty(
                                    KafkaDataFormat.VALUE_FORMAT_CONFIG, valueFormat.value())
                            .build();
        }

        @Override
        public CompletableFuture<GetTableInfoResponse> getTableInfo(GetTableInfoRequest request) {
            getTableInfoPrincipal = currentSession().getPrincipal();
            return CompletableFuture.completedFuture(
                    new GetTableInfoResponse()
                            .setTableId(TABLE_ID)
                            .setSchemaId(SCHEMA_ID)
                            .setTableJson(tableDescriptor.toJsonBytes())
                            .setCreatedTime(1L)
                            .setModifiedTime(1L));
        }

        @Override
        public CompletableFuture<ProduceLogResponse> produceLog(ProduceLogRequest request) {
            producePrincipal = currentSession().getPrincipal();
            lastProduceRequest = request;
            if (produceResponse != null) {
                return CompletableFuture.completedFuture(produceResponse);
            }
            PbProduceLogRespForBucket bucket =
                    new PbProduceLogRespForBucket().setBucketId(0).setBaseOffset(42L);
            if (produceError != org.apache.fluss.rpc.protocol.Errors.NONE) {
                bucket.clearBaseOffset().setErrorCode(produceError.code());
            }
            return CompletableFuture.completedFuture(
                    new ProduceLogResponse().addAllBucketsResps(Collections.singletonList(bucket)));
        }

        private static DataType dataType(KafkaDataFormat format) {
            return format == KafkaDataFormat.RAW ? DataTypes.BYTES() : DataTypes.STRING();
        }
    }
}

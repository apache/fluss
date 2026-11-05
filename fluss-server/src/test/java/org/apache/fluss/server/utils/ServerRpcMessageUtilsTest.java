/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.fluss.server.utils;

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.metadata.ColumnPath;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.remote.RemoteLogFetchInfo;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.entity.LookupResultForBucket;
import org.apache.fluss.rpc.entity.ProduceLogResultForBucket;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.messages.AlterTableRequest;
import org.apache.fluss.rpc.messages.FetchLogResponse;
import org.apache.fluss.rpc.messages.LookupResponse;
import org.apache.fluss.rpc.messages.PbAddColumn;
import org.apache.fluss.rpc.messages.PbAddNestedField;
import org.apache.fluss.rpc.messages.PbBucketMetadata;
import org.apache.fluss.rpc.messages.PbColumnPathSegment;
import org.apache.fluss.rpc.messages.PbFetchLogRespForBucket;
import org.apache.fluss.rpc.messages.PbPartitionMetadata;
import org.apache.fluss.rpc.messages.PbProduceLogReqForBucket;
import org.apache.fluss.rpc.messages.PbProduceLogRespForBucket;
import org.apache.fluss.rpc.messages.PbPutKvReqForBucket;
import org.apache.fluss.rpc.messages.PbPutKvRespForBucket;
import org.apache.fluss.rpc.messages.PbTableMetadata;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.rpc.util.CommonRpcMessageUtils;
import org.apache.fluss.server.entity.ProduceLogDataForBucket;
import org.apache.fluss.server.entity.PutKvDataForBucket;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.ClusterMetadata;
import org.apache.fluss.server.metadata.PartitionMetadata;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.json.DataTypeJsonSerde;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA_1_WITH_KEY_AND_VALUE;
import static org.apache.fluss.server.metadata.BucketMetadata.NO_LEADER_ISR_STATE_EPOCH;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getUpdateMetadataRequestData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeUpdateMetadataRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ServerRpcMessageUtils}. */
class ServerRpcMessageUtilsTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFetchLogResponsePreservesRemoteLogTabletDir(boolean partitioned) {
        TablePath tablePath = TablePath.of("db", "table");
        PhysicalTablePath physicalPath =
                PhysicalTablePath.of(tablePath, partitioned ? "dt=2026-10-10" : null);
        TableBucket bucket = new TableBucket(1L, partitioned ? 2L : null, 0);
        for (String root :
                Arrays.asList(
                        "file:/tmp/remote/log", "s3://bucket/log", "s3://bucket/nested/log")) {
            FsPath remoteLogDir = new FsPath(root);
            FsPath tabletDir = FlussPaths.remoteLogTabletDir(remoteLogDir, physicalPath, bucket);
            assertThat(FlussPaths.remoteLogDirFromTabletDir(tabletDir, bucket))
                    .isEqualTo(remoteLogDir);
            RemoteLogSegment segment =
                    RemoteLogSegment.Builder.builder()
                            .physicalTablePath(physicalPath)
                            .tableBucket(bucket)
                            .remoteLogSegmentId(UUID.randomUUID())
                            .remoteLogStartOffset(0L)
                            .remoteLogEndOffset(10L)
                            .maxTimestamp(100L)
                            .segmentSizeInBytes(1024)
                            .build();
            FetchLogResponse response =
                    ServerRpcMessageUtils.makeFetchLogResponse(
                            Collections.singletonMap(
                                    bucket,
                                    FetchLogResultForBucket.remote(
                                            bucket,
                                            new RemoteLogFetchInfo(
                                                    tabletDir.toString(),
                                                    physicalPath.getPartitionName(),
                                                    Collections.singletonList(segment),
                                                    0),
                                            10L)));
            RemoteLogFetchInfo decoded =
                    CommonRpcMessageUtils.getFetchLogResultForBucket(
                                    bucket,
                                    tablePath,
                                    response.getTablesRespsList()
                                            .get(0)
                                            .getBucketsRespsList()
                                            .get(0))
                            .remoteLogFetchInfo();
            assertThat(decoded.remoteLogTabletDir()).isEqualTo(tabletDir.toString());
            assertThat(decoded.remoteLogSegmentList()).containsExactly(segment);
        }
    }

    @Test
    void testNestedFieldChangesRoundTripPreservesAggregationAndBucketCount() {
        PbAddNestedField field = nestedFieldMessage();
        field.setComment("field comment");
        field.addAllParentPaths(
                Arrays.asList(
                        new PbColumnPathSegment().setSegmentType(0).setName("profile"),
                        new PbColumnPathSegment().setSegmentType(0).setName("address")));
        PbAddColumn column =
                new PbAddColumn()
                        .setColumnName("total")
                        .setDataTypeJson(
                                JsonSerdeUtils.writeValueAsBytes(
                                        DataTypes.INT(), DataTypeJsonSerde.INSTANCE))
                        .setColumnPositionType(0)
                        .setAggFunctionType("sum");
        AlterTableRequest request = new AlterTableRequest().setIgnoreIfNotExists(false);
        request.setTablePath().setDatabaseName("db").setTableName("table");
        request.setModifyBucketCount().setNewBucketCount(8);
        request.addAllAddColumns(Collections.singletonList(column));
        request.addAllAddNestedFields(Collections.singletonList(field));
        ByteBuf buffer = Unpooled.buffer();
        try {
            request.writeTo(buffer);
            AlterTableRequest decoded = new AlterTableRequest();
            decoded.parseFrom(buffer, buffer.readableBytes());
            List<TableChange> changes = ServerRpcMessageUtils.toAlterTableSchemaChanges(decoded);
            assertThat(changes).hasSize(2);
            TableChange.AddColumn addColumn = (TableChange.AddColumn) changes.get(0);
            assertThat(addColumn.getAggFunction()).hasValue(AggFunctions.SUM());
            TableChange.AddNestedField nested = (TableChange.AddNestedField) changes.get(1);
            assertThat(nested.getParentPath()).isEqualTo(ColumnPath.of("profile", "address"));
            assertThat(nested.getFieldName()).isEqualTo("country");
            assertThat(nested.getDataType()).isEqualTo(DataTypes.STRING());
            assertThat(nested.getComment()).isEqualTo("field comment");
            assertThat(ServerRpcMessageUtils.toAlterTableDistributionChanges(decoded))
                    .containsExactly(TableChange.modifyBucketCount(8));
        } finally {
            buffer.release();
        }
    }

    @Test
    void testNestedFieldSpecialSegmentsAndMissingComment() {
        PbAddNestedField field = nestedFieldMessage();
        field.addAllParentPaths(
                Arrays.asList(
                        new PbColumnPathSegment().setSegmentType(0).setName("items"),
                        new PbColumnPathSegment().setSegmentType(1),
                        new PbColumnPathSegment().setSegmentType(2)));
        List<TableChange> changes =
                ServerRpcMessageUtils.toAddNestedFields(Arrays.asList(null, field));
        assertThat(changes).hasSize(1);
        TableChange.AddNestedField nested = (TableChange.AddNestedField) changes.get(0);
        assertThat(nested.getParentPath())
                .isEqualTo(
                        ColumnPath.of(
                                ColumnPath.Segment.field("items"),
                                ColumnPath.Segment.arrayElement(),
                                ColumnPath.Segment.mapValue()));
        assertThat(nested.getComment()).isNull();
        assertThat(ServerRpcMessageUtils.toAlterTableSchemaChanges(new AlterTableRequest()))
                .isEmpty();
    }

    @Test
    void testRejectMalformedNestedFieldPaths() {
        assertThatThrownBy(
                        () ->
                                ServerRpcMessageUtils.toAddNestedFields(
                                        Collections.singletonList(nestedFieldMessage())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one segment");
        PbAddNestedField missingName = nestedFieldMessage();
        missingName.addAllParentPaths(
                Collections.singletonList(new PbColumnPathSegment().setSegmentType(0)));
        assertThatThrownBy(
                        () ->
                                ServerRpcMessageUtils.toAddNestedFields(
                                        Collections.singletonList(missingName)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("FIELD segment must have name");
        PbAddNestedField unknownType = nestedFieldMessage();
        unknownType.addAllParentPaths(
                Collections.singletonList(new PbColumnPathSegment().setSegmentType(99)));
        assertThatThrownBy(
                        () ->
                                ServerRpcMessageUtils.toAddNestedFields(
                                        Collections.singletonList(unknownType)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported column path segment type: 99");
    }

    private static PbAddNestedField nestedFieldMessage() {
        return new PbAddNestedField()
                .setFieldName("country")
                .setDataTypeJson(
                        JsonSerdeUtils.writeValueAsBytes(
                                DataTypes.STRING(), DataTypeJsonSerde.INSTANCE));
    }

    @Test
    void testAlterTableDistributionChanges() {
        AlterTableRequest request = new AlterTableRequest();
        assertThat(ServerRpcMessageUtils.toAlterTableDistributionChanges(request)).isEmpty();

        request.setModifyBucketCount().setNewBucketCount(8);
        assertThat(ServerRpcMessageUtils.toAlterTableDistributionChanges(request))
                .containsExactly(TableChange.modifyBucketCount(8));
    }

    @Test
    void testFetchLogResponseContainsMinRetainOffset() {
        TableBucket tableBucket = new TableBucket(1L, 0);
        FetchLogResultForBucket result =
                FetchLogResultForBucket.records(tableBucket, MemoryLogRecords.EMPTY, 10L, -1L, 8L);

        FetchLogResponse response =
                ServerRpcMessageUtils.makeFetchLogResponse(
                        Collections.singletonMap(tableBucket, result));

        PbFetchLogRespForBucket bucketResponse =
                response.getTablesRespsList().get(0).getBucketsRespsList().get(0);
        assertThat(bucketResponse.hasMinRetainOffset()).isTrue();
        assertThat(bucketResponse.getMinRetainOffset()).isEqualTo(8L);

        FetchLogResultForBucket decodedResult =
                CommonRpcMessageUtils.getFetchLogResultForBucket(
                        tableBucket, TablePath.of("db", "table"), bucketResponse);
        assertThat(decodedResult.hasMinRetainOffset()).isTrue();
        assertThat(decodedResult.getMinRetainOffset()).isEqualTo(8L);

        FetchLogResponse responseWithoutMinRetainOffset =
                ServerRpcMessageUtils.makeFetchLogResponse(
                        Collections.singletonMap(
                                tableBucket,
                                FetchLogResultForBucket.records(
                                        tableBucket, MemoryLogRecords.EMPTY, 10L, -1L, -1L)));
        PbFetchLogRespForBucket bucketResponseWithoutMinRetainOffset =
                responseWithoutMinRetainOffset
                        .getTablesRespsList()
                        .get(0)
                        .getBucketsRespsList()
                        .get(0);
        assertThat(bucketResponseWithoutMinRetainOffset.hasMinRetainOffset()).isFalse();
        assertThat(
                        CommonRpcMessageUtils.getFetchLogResultForBucket(
                                        tableBucket,
                                        TablePath.of("db", "table"),
                                        bucketResponseWithoutMinRetainOffset)
                                .hasMinRetainOffset())
                .isFalse();
    }

    @Test
    void testLookupResponseSupportsMixedValueLayouts() {
        byte[] plainValue = new byte[] {1, 0, 11, 12};
        byte[] taggedValue = new byte[] {0, 0, 0, 0, 0, 0, 0, 7, 1, 0, 21, 22};
        LookupResultForBucket result =
                new LookupResultForBucket(
                        new TableBucket(1L, 0),
                        Arrays.asList(
                                KvValueLayout.PLAIN.toValueBodySlice(plainValue),
                                KvValueLayout.TAGGED.toValueBodySlice(taggedValue)),
                        "dt=2025-01-01");

        LookupResponse response =
                ServerRpcMessageUtils.makeLookupResponse(Collections.singletonList(result));

        assertThat(response.getBucketsRespAt(0).getValuesList())
                .extracting(value -> value.getValues())
                .containsExactly(new byte[] {1, 0, 11, 12}, new byte[] {1, 0, 21, 22});
    }

    @Test
    void testHistoricalProduceLogRequestAndResponsePreserveOriginalPartitions() {
        long tableId = 1L;
        long partitionId = 2L;
        PbProduceLogReqForBucket firstBucketRequest =
                new PbProduceLogReqForBucket()
                        .setPartitionId(partitionId)
                        .setBucketId(0)
                        .setRecords(new byte[0])
                        .setOriginalPartitionName("dt=2025-01-01");
        ProduceLogRequest request =
                new ProduceLogRequest().setTableId(tableId).setAcks(1).setTimeoutMs(10_000);
        request.addAllBucketsReqs(
                Arrays.asList(
                        firstBucketRequest,
                        new PbProduceLogReqForBucket()
                                .copyFrom(firstBucketRequest)
                                .setOriginalPartitionName("dt=2025-01-02")));

        TableBucket tableBucket = new TableBucket(tableId, partitionId, 0);
        List<ProduceLogDataForBucket> decoded =
                ServerRpcMessageUtils.toProduceLogDataForBuckets(request);
        assertThat(decoded)
                .extracting(ProduceLogDataForBucket::tableBucket)
                .containsOnly(tableBucket);
        assertThat(decoded)
                .extracting(ProduceLogDataForBucket::originalPartitionName)
                .containsExactly("dt=2025-01-01", "dt=2025-01-02");

        ProduceLogResponse response =
                ServerRpcMessageUtils.makeProduceLogResponse(
                        Arrays.asList(
                                ProduceLogResultForBucket.historicalSuccess(
                                        tableBucket, 0L, 1L, "dt=2025-01-01"),
                                ProduceLogResultForBucket.historicalSuccess(
                                        tableBucket, 1L, 2L, "dt=2025-01-02")));
        assertThat(response.getBucketsRespsList())
                .extracting(PbProduceLogRespForBucket::getOriginalPartitionName)
                .containsExactly("dt=2025-01-01", "dt=2025-01-02");

        request.addAllBucketsReqs(Collections.singletonList(firstBucketRequest));
        assertThatThrownBy(() -> ServerRpcMessageUtils.toProduceLogDataForBuckets(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate table bucket");
    }

    @Test
    void testHistoricalPutKvRequestAndResponsePreserveOriginalPartitions() throws Exception {
        long tableId = 1L;
        long partitionId = 2L;
        KvRecordBatch records = genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE);
        PutKvRequest request = newPutKvRequest(tableId, 0, 1, records);
        PbPutKvReqForBucket bucketRequest = request.getBucketsReqsList().get(0);
        bucketRequest.setPartitionId(partitionId).setOriginalPartitionName("dt=2025-01-01");
        request.addAllBucketsReqs(
                Collections.singletonList(
                        new PbPutKvReqForBucket()
                                .copyFrom(bucketRequest)
                                .setOriginalPartitionName("dt=2025-01-02")));

        TableBucket tableBucket = new TableBucket(tableId, partitionId, 0);
        List<PutKvDataForBucket> decoded = ServerRpcMessageUtils.toPutKvDataForBuckets(request);
        assertThat(decoded).extracting(PutKvDataForBucket::tableBucket).containsOnly(tableBucket);
        assertThat(decoded)
                .extracting(PutKvDataForBucket::originalPartitionName)
                .containsExactly("dt=2025-01-01", "dt=2025-01-02");
        assertThat(decoded).extracting(PutKvDataForBucket::records).containsOnly(records);

        PutKvResponse response =
                ServerRpcMessageUtils.makePutKvResponse(
                        Arrays.asList(
                                PutKvResultForBucket.historicalSuccess(
                                        tableBucket, 1L, "dt=2025-01-01"),
                                PutKvResultForBucket.historicalSuccess(
                                        tableBucket, 2L, "dt=2025-01-02")));
        assertThat(response.getBucketsRespsList())
                .extracting(PbPutKvRespForBucket::getOriginalPartitionName)
                .containsExactly("dt=2025-01-01", "dt=2025-01-02");

        request.addAllBucketsReqs(
                Collections.singletonList(new PbPutKvReqForBucket().copyFrom(bucketRequest)));
        assertThatThrownBy(() -> ServerRpcMessageUtils.toPutKvDataForBuckets(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate table bucket");
    }

    @Test
    void testAuthoritativeBucketMetadataTableAndPartitionRoundTrip() {
        List<BucketMetadata> bucketMetadata =
                Arrays.asList(
                        new BucketMetadata(0, 1, 2, Arrays.asList(1, 2, 3), Arrays.asList(1, 2), 3),
                        new BucketMetadata(
                                1,
                                null,
                                null,
                                Arrays.asList(1, 2, 3),
                                Collections.emptyList(),
                                NO_LEADER_ISR_STATE_EPOCH),
                        new BucketMetadata(
                                2, 2, 4, Arrays.asList(2, 3), Collections.emptyList(), 5));
        TableMetadata tableMetadata = new TableMetadata(DATA1_TABLE_INFO, bucketMetadata);
        PartitionMetadata partitionMetadata =
                new PartitionMetadata(DATA1_TABLE_INFO.getTableId(), "p1", 10L, bucketMetadata);

        UpdateMetadataRequest request =
                makeUpdateMetadataRequest(
                        null,
                        null,
                        Collections.emptySet(),
                        Collections.singletonList(tableMetadata),
                        Collections.singletonList(partitionMetadata));

        PbTableMetadata pbTableMetadata = request.getTableMetadatasList().get(0);
        assertAuthoritativeWireMetadata(pbTableMetadata.getBucketMetadatasList());
        PbPartitionMetadata pbPartitionMetadata = request.getPartitionMetadatasList().get(0);
        assertAuthoritativeWireMetadata(pbPartitionMetadata.getBucketMetadatasList());

        ClusterMetadata decoded = getUpdateMetadataRequestData(request);
        assertThat(decoded.getTableMetadataList().get(0).getBucketMetadataList())
                .containsExactlyElementsOf(bucketMetadata);
        assertThat(decoded.getPartitionMetadataList().get(0).getBucketMetadataList())
                .containsExactlyElementsOf(bucketMetadata);
    }

    @Test
    void testLegacyBucketMetadataRoundTripOmitsBucketEpoch() {
        BucketMetadata legacy = new BucketMetadata(0, 1, 2, Arrays.asList(1, 2, 3));
        UpdateMetadataRequest request =
                makeUpdateMetadataRequest(
                        null,
                        null,
                        Collections.emptySet(),
                        Collections.singletonList(
                                new TableMetadata(
                                        DATA1_TABLE_INFO, Collections.singletonList(legacy))),
                        Collections.emptyList());

        PbBucketMetadata pbBucketMetadata =
                request.getTableMetadatasList().get(0).getBucketMetadatasList().get(0);
        assertThat(pbBucketMetadata.hasBucketEpoch()).isFalse();
        assertThat(pbBucketMetadata.getIsrs()).isEmpty();

        BucketMetadata decoded =
                getUpdateMetadataRequestData(request)
                        .getTableMetadataList()
                        .get(0)
                        .getBucketMetadataList()
                        .get(0);
        assertThat(decoded).isEqualTo(legacy);
        assertThat(decoded.getBucketEpoch()).isNull();
    }

    private static void assertAuthoritativeWireMetadata(List<PbBucketMetadata> bucketMetadata) {
        assertThat(bucketMetadata.get(0).getBucketId()).isEqualTo(0);
        assertThat(bucketMetadata.get(0).getLeaderId()).isEqualTo(1);
        assertThat(bucketMetadata.get(0).getLeaderEpoch()).isEqualTo(2);
        assertThat(bucketMetadata.get(0).getReplicaIds()).containsExactly(1, 2, 3);
        assertThat(bucketMetadata.get(0).hasBucketEpoch()).isTrue();
        assertThat(bucketMetadata.get(0).getBucketEpoch()).isEqualTo(3);
        assertThat(bucketMetadata.get(0).getIsrs()).containsExactly(1, 2);

        assertThat(bucketMetadata.get(1).hasBucketEpoch()).isTrue();
        assertThat(bucketMetadata.get(1).getBucketEpoch()).isEqualTo(NO_LEADER_ISR_STATE_EPOCH);
        assertThat(bucketMetadata.get(1).getIsrs()).isEmpty();

        assertThat(bucketMetadata.get(2).hasBucketEpoch()).isTrue();
        assertThat(bucketMetadata.get(2).getBucketEpoch()).isEqualTo(5);
        assertThat(bucketMetadata.get(2).getIsrs()).isEmpty();
    }
}

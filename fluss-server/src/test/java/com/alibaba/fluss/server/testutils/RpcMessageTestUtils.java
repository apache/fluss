/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.testutils;

import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.record.DefaultKvRecordBatch;
import com.alibaba.fluss.record.DefaultValueRecordBatch;
import com.alibaba.fluss.record.KvRecordBatch;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.record.bytesview.MemorySegmentBytesView;
import com.alibaba.fluss.rpc.gateway.CoordinatorGateway;
import com.alibaba.fluss.rpc.messages.CreateDatabaseRequest;
import com.alibaba.fluss.rpc.messages.CreateTableRequest;
import com.alibaba.fluss.rpc.messages.DatabaseExistsRequest;
import com.alibaba.fluss.rpc.messages.DropDatabaseRequest;
import com.alibaba.fluss.rpc.messages.DropTableRequest;
import com.alibaba.fluss.rpc.messages.FetchLogRequest;
import com.alibaba.fluss.rpc.messages.FetchLogResponse;
import com.alibaba.fluss.rpc.messages.GetTableInfoRequest;
import com.alibaba.fluss.rpc.messages.GetTableInfoResponse;
import com.alibaba.fluss.rpc.messages.LimitScanRequest;
import com.alibaba.fluss.rpc.messages.LimitScanResponse;
import com.alibaba.fluss.rpc.messages.ListOffsetsRequest;
import com.alibaba.fluss.rpc.messages.ListTablesRequest;
import com.alibaba.fluss.rpc.messages.LookupRequest;
import com.alibaba.fluss.rpc.messages.MetadataRequest;
import com.alibaba.fluss.rpc.messages.PbFetchLogReqForBucket;
import com.alibaba.fluss.rpc.messages.PbFetchLogReqForTable;
import com.alibaba.fluss.rpc.messages.PbFetchLogRespForBucket;
import com.alibaba.fluss.rpc.messages.PbFetchLogRespForTable;
import com.alibaba.fluss.rpc.messages.PbLookupReqForBucket;
import com.alibaba.fluss.rpc.messages.PbPrefixLookupReqForBucket;
import com.alibaba.fluss.rpc.messages.PbProduceLogReqForBucket;
import com.alibaba.fluss.rpc.messages.PbProduceLogRespForBucket;
import com.alibaba.fluss.rpc.messages.PbPutKvReqForBucket;
import com.alibaba.fluss.rpc.messages.PbTablePath;
import com.alibaba.fluss.rpc.messages.PrefixLookupRequest;
import com.alibaba.fluss.rpc.messages.ProduceLogRequest;
import com.alibaba.fluss.rpc.messages.ProduceLogResponse;
import com.alibaba.fluss.rpc.messages.PutKvRequest;
import com.alibaba.fluss.rpc.messages.TableExistsRequest;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.types.Tuple2;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.testutils.DataTestUtils.assertMemoryRecordsEqualsWithRowKind;
import static org.assertj.core.api.Assertions.assertThat;

/** Test utils for rpc message. */
public class RpcMessageTestUtils {
    public static DropTableRequest newDropTableRequest(
            String db, String tb, boolean ignoreIfNotExists) {
        DropTableRequest dropTableRequest = new DropTableRequest();
        dropTableRequest
                .setIgnoreIfNotExists(ignoreIfNotExists)
                .setTablePath()
                .setDatabaseName(db)
                .setTableName(tb);
        return dropTableRequest;
    }

    public static GetTableInfoRequest newGetTableInfoRequest(TablePath tablePath) {
        GetTableInfoRequest request = new GetTableInfoRequest();
        request.setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        return request;
    }

    public static DatabaseExistsRequest newDatabaseExistsRequest(String db) {
        return new DatabaseExistsRequest().setDatabaseName(db);
    }

    public static CreateDatabaseRequest newCreateDatabaseRequest(
            String db, boolean ignoreIfExists) {
        return new CreateDatabaseRequest().setDatabaseName(db).setIgnoreIfExists(ignoreIfExists);
    }

    public static DropDatabaseRequest newDropDatabaseRequest(
            String databaseName, boolean ignoreIfNotExists, boolean cascade) {
        return new DropDatabaseRequest()
                .setDatabaseName(databaseName)
                .setIgnoreIfNotExists(ignoreIfNotExists)
                .setCascade(cascade);
    }

    public static ListTablesRequest newListTablesRequest(String databaseName) {
        return new ListTablesRequest().setDatabaseName(databaseName);
    }

    public static TableExistsRequest newTableExistsRequest(TablePath tablePath) {
        TableExistsRequest tableExistsRequest = new TableExistsRequest();
        tableExistsRequest
                .setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        return tableExistsRequest;
    }

    public static CreateTableRequest newCreateTableRequest(
            TablePath tablePath, TableDescriptor tableDescriptor, boolean ignoreIfExists) {
        CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest
                .setIgnoreIfExists(ignoreIfExists)
                .setTableJson(tableDescriptor.toJsonBytes())
                .setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        return createTableRequest;
    }

    public static MetadataRequest newMetadataRequest(List<TablePath> tablePaths) {
        MetadataRequest metadataRequest = new MetadataRequest();
        metadataRequest.addAllTablePaths(
                tablePaths.stream()
                        .map(
                                tablePath ->
                                        new PbTablePath()
                                                .setDatabaseName(tablePath.getDatabaseName())
                                                .setTableName(tablePath.getTableName()))
                        .collect(Collectors.toList()));
        return metadataRequest;
    }

    public static ProduceLogRequest newProduceLogRequest(
            long tableId, int bucketId, int acks, MemoryLogRecords records) {
        ProduceLogRequest produceRequest = new ProduceLogRequest();
        produceRequest.setTableId(tableId).setAcks(acks).setTimeoutMs(10000);
        PbProduceLogReqForBucket pbProduceLogReqForBucket = new PbProduceLogReqForBucket();
        pbProduceLogReqForBucket
                .setBucketId(bucketId)
                .setRecordsBytesView(
                        new MemorySegmentBytesView(
                                records.getMemorySegment(),
                                records.getPosition(),
                                records.sizeInBytes()));
        produceRequest.addAllBucketsReqs(Collections.singletonList(pbProduceLogReqForBucket));
        return produceRequest;
    }

    public static PutKvRequest newPutKvRequest(
            long tableId, int bucketId, int acks, KvRecordBatch kvRecordBatch) {
        PutKvRequest putKvRequest = new PutKvRequest();
        putKvRequest.setTableId(tableId).setAcks(acks).setTimeoutMs(10000);
        PbPutKvReqForBucket pbPutKvReqForBucket = new PbPutKvReqForBucket();
        pbPutKvReqForBucket.setBucketId(bucketId);
        if (kvRecordBatch instanceof DefaultKvRecordBatch) {
            DefaultKvRecordBatch batch = (DefaultKvRecordBatch) kvRecordBatch;
            pbPutKvReqForBucket.setRecords(
                    batch.getMemorySegment(), batch.getPosition(), batch.sizeInBytes());
        } else {
            throw new IllegalArgumentException(
                    "Unsupported KvRecordBatch type: " + kvRecordBatch.getClass().getName());
        }
        putKvRequest.addAllBucketsReqs(Collections.singletonList(pbPutKvReqForBucket));
        return putKvRequest;
    }

    public static FetchLogRequest newFetchLogRequest(
            int followerId, long tableId, int bucketId, long fetchOffset) {
        return newFetchLogRequest(followerId, tableId, bucketId, fetchOffset, null);
    }

    public static FetchLogRequest newFetchLogRequest(
            int followerId, long tableId, int bucketId, long fetchOffset, int[] selectedFields) {
        return newFetchLogRequest(
                followerId,
                tableId,
                bucketId,
                fetchOffset,
                selectedFields,
                -1,
                Integer.MAX_VALUE,
                -1);
    }

    public static FetchLogRequest newFetchLogRequest(
            int followerId,
            long tableId,
            int bucketId,
            long fetchOffset,
            int[] selectedFields,
            int minFetchBytes,
            int maxFetchBytes,
            int maxWaitMs) {
        FetchLogRequest fetchLogRequest =
                new FetchLogRequest().setFollowerServerId(followerId).setMaxBytes(maxFetchBytes);
        if (minFetchBytes > 0) {
            fetchLogRequest.setMinBytes(minFetchBytes).setMaxWaitMs(maxWaitMs);
        }

        PbFetchLogReqForTable fetchLogReqForTable = new PbFetchLogReqForTable().setTableId(tableId);
        if (selectedFields != null) {
            fetchLogReqForTable
                    .setProjectionPushdownEnabled(true)
                    .setProjectedFields(selectedFields);
        } else {
            fetchLogReqForTable.setProjectionPushdownEnabled(false);
        }
        // TODO make the max fetch bytes configurable.
        PbFetchLogReqForBucket fetchLogReqForBucket =
                new PbFetchLogReqForBucket()
                        .setBucketId(bucketId)
                        .setFetchOffset(fetchOffset)
                        .setMaxFetchBytes(1024 * 1024);
        fetchLogReqForTable.addAllBucketsReqs(Collections.singletonList(fetchLogReqForBucket));
        fetchLogRequest.addAllTablesReqs(Collections.singletonList(fetchLogReqForTable));
        return fetchLogRequest;
    }

    public static LookupRequest newLookupRequest(long tableId, int bucketId, byte[] key) {
        LookupRequest lookupRequest = new LookupRequest().setTableId(tableId);
        PbLookupReqForBucket pbLookupReqForBucket = lookupRequest.addBucketsReq();
        pbLookupReqForBucket.setBucketId(bucketId).addKey(key);
        return lookupRequest;
    }

    public static PrefixLookupRequest newPrefixLookupRequest(
            long tableId, int bucketId, List<byte[]> prefixKeys) {
        PrefixLookupRequest prefixLookupRequest = new PrefixLookupRequest().setTableId(tableId);
        PbPrefixLookupReqForBucket pbPrefixLookupReqForBucket = prefixLookupRequest.addBucketsReq();
        pbPrefixLookupReqForBucket.setBucketId(bucketId);
        for (byte[] prefixKey : prefixKeys) {
            pbPrefixLookupReqForBucket.addKey(prefixKey);
        }
        return prefixLookupRequest;
    }

    public static LimitScanRequest newLimitScanRequest(long tableId, int bucketId, int limit) {
        return new LimitScanRequest().setTableId(tableId).setBucketId(bucketId).setLimit(limit);
    }

    public static ListOffsetsRequest newListOffsetsRequest(
            int followerServerId, int offsetType, long tableId, int bucketId) {
        ListOffsetsRequest listOffsetsRequest =
                new ListOffsetsRequest()
                        .setFollowerServerId(followerServerId)
                        .setOffsetType(offsetType)
                        .setTableId(tableId);
        listOffsetsRequest.addBucketId(bucketId);
        return listOffsetsRequest;
    }

    public static long createTable(
            FlussClusterExtension extension, TablePath tablePath, TableDescriptor tableDescriptor)
            throws Exception {
        CoordinatorGateway coordinatorGateway = extension.newCoordinatorClient();
        coordinatorGateway
                .createDatabase(newCreateDatabaseRequest(tablePath.getDatabaseName(), true))
                .get();
        coordinatorGateway
                .createTable(newCreateTableRequest(tablePath, tableDescriptor, false))
                .get();
        GetTableInfoResponse response =
                coordinatorGateway.getTableInfo(newGetTableInfoRequest(tablePath)).get();
        return response.getTableId();
    }

    public static void assertProduceLogResponse(
            ProduceLogResponse produceLogResponse, int bucketId, Long baseOffset) {
        assertThat(produceLogResponse.getBucketsRespsCount()).isEqualTo(1);
        PbProduceLogRespForBucket produceLogRespForBucket =
                produceLogResponse.getBucketsRespsList().get(0);
        assertThat(produceLogRespForBucket.getBucketId()).isEqualTo(bucketId);
        assertThat(produceLogRespForBucket.hasErrorMessage()).isFalse();
        assertThat(produceLogRespForBucket.hasErrorCode()).isFalse();
        assertThat(produceLogRespForBucket.hasBaseOffset()).isTrue();
        assertThat(produceLogRespForBucket.getBaseOffset()).isEqualTo(baseOffset);
    }

    public static void assertFetchLogResponse(
            FetchLogResponse response,
            long tableId,
            long bucketId,
            Long highWatermark,
            List<Object[]> expectedRecords) {
        assertFetchLogResponse(
                response, DATA1_ROW_TYPE, tableId, bucketId, highWatermark, expectedRecords);
    }

    public static void assertFetchLogResponse(
            FetchLogResponse response,
            RowType rowType,
            long tableId,
            long bucketId,
            Long highWatermark,
            List<Object[]> expectedRecords) {
        List<Tuple2<ChangeType, Object[]>> expectedFieldAndRowKind =
                expectedRecords.stream()
                        .map(val -> Tuple2.of(ChangeType.APPEND_ONLY, val))
                        .collect(Collectors.toList());
        assertFetchLogResponse(
                response,
                rowType,
                tableId,
                bucketId,
                highWatermark,
                expectedFieldAndRowKind,
                null,
                null);
    }

    public static void assertFetchLogResponseWithRowKind(
            FetchLogResponse response,
            long tableId,
            long bucketId,
            Long highWatermark,
            List<Tuple2<ChangeType, Object[]>> expectedFieldAndRowKind) {
        assertFetchLogResponse(
                response,
                DATA1_ROW_TYPE,
                tableId,
                bucketId,
                highWatermark,
                expectedFieldAndRowKind,
                null,
                null);
    }

    public static void assertFetchLogResponse(
            FetchLogResponse response,
            long tableId,
            long bucketId,
            Integer errorCode,
            @Nullable String errorMessage) {
        assertFetchLogResponse(
                response,
                DATA1_ROW_TYPE,
                tableId,
                bucketId,
                -1L,
                Collections.emptyList(),
                errorCode,
                errorMessage);
    }

    private static void assertFetchLogResponse(
            FetchLogResponse response,
            RowType rowType,
            long tableId,
            long bucketId,
            Long highWatermark,
            List<Tuple2<ChangeType, Object[]>> expectedRecords,
            Integer errorCode,
            @Nullable String errorMessage) {
        assertThat(response.getTablesRespsCount()).isEqualTo(1);
        PbFetchLogRespForTable fetchLogRespForTable = response.getTablesRespsList().get(0);
        assertThat(fetchLogRespForTable.getTableId()).isEqualTo(tableId);
        assertThat(fetchLogRespForTable.getBucketsRespsCount()).isEqualTo(1);
        PbFetchLogRespForBucket protoFetchedBucket =
                fetchLogRespForTable.getBucketsRespsList().get(0);
        assertThat(protoFetchedBucket.getBucketId()).isEqualTo(bucketId);
        if (errorCode != null) {
            assertThat(protoFetchedBucket.getErrorCode()).isEqualTo(errorCode);
            assertThat(protoFetchedBucket.getErrorMessage()).contains(errorMessage);
        } else {
            ApiError error = ApiError.fromErrorMessage(protoFetchedBucket);
            assertThat(error.isSuccess()).as(error.toString()).isTrue();
            assertThat(protoFetchedBucket.getHighWatermark()).isEqualTo(highWatermark);
            MemoryLogRecords resultRecords =
                    MemoryLogRecords.pointToBytes(protoFetchedBucket.getRecords());
            assertMemoryRecordsEqualsWithRowKind(
                    rowType, resultRecords, Collections.singletonList(expectedRecords));
        }
    }

    public static void assertLimitScanResponse(
            LimitScanResponse limitScanResponse, RowType rowType, List<Object[]> expectedRecords) {
        List<Tuple2<ChangeType, Object[]>> expectedFieldAndRowKind =
                expectedRecords.stream()
                        .map(val -> Tuple2.of(ChangeType.APPEND_ONLY, val))
                        .collect(Collectors.toList());
        MemoryLogRecords resultRecords =
                MemoryLogRecords.pointToBytes(limitScanResponse.getRecords());
        assertMemoryRecordsEqualsWithRowKind(
                rowType, resultRecords, Collections.singletonList(expectedFieldAndRowKind));
    }

    public static void assertLimitScanResponse(
            LimitScanResponse limitScanResponse, @Nullable DefaultValueRecordBatch expected) {
        if (limitScanResponse.hasErrorCode()) {
            throw new AssertionError(
                    "Error code: "
                            + limitScanResponse.getErrorCode()
                            + ", error message: "
                            + limitScanResponse.getErrorMessage());
        }
        DefaultValueRecordBatch actual =
                DefaultValueRecordBatch.pointToBytes(limitScanResponse.getRecords());
        assertThat(actual).isEqualTo(expected);
    }
}

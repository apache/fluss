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

package com.alibaba.fluss.server.replica;

import com.alibaba.fluss.cluster.Endpoint;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.exception.InvalidCoordinatorException;
import com.alibaba.fluss.exception.InvalidRequiredAcksException;
import com.alibaba.fluss.exception.PartitionNotExistException;
import com.alibaba.fluss.exception.TableNotExistException;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.record.DefaultValueRecordBatch;
import com.alibaba.fluss.record.KvRecord;
import com.alibaba.fluss.record.KvRecordBatch;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.record.LogRecordBatch;
import com.alibaba.fluss.record.LogRecordReadContext;
import com.alibaba.fluss.record.LogRecords;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.row.encode.CompactedKeyEncoder;
import com.alibaba.fluss.row.encode.ValueEncoder;
import com.alibaba.fluss.rpc.entity.FetchLogResultForBucket;
import com.alibaba.fluss.rpc.entity.LimitScanResultForBucket;
import com.alibaba.fluss.rpc.entity.ListOffsetsResultForBucket;
import com.alibaba.fluss.rpc.entity.LookupResultForBucket;
import com.alibaba.fluss.rpc.entity.PrefixLookupResultForBucket;
import com.alibaba.fluss.rpc.entity.ProduceLogResultForBucket;
import com.alibaba.fluss.rpc.entity.PutKvResultForBucket;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.rpc.protocol.Errors;
import com.alibaba.fluss.server.entity.FetchReqInfo;
import com.alibaba.fluss.server.entity.NotifyLeaderAndIsrData;
import com.alibaba.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import com.alibaba.fluss.server.entity.StopReplicaData;
import com.alibaba.fluss.server.entity.StopReplicaResultForBucket;
import com.alibaba.fluss.server.kv.rocksdb.RocksDBKv;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshot;
import com.alibaba.fluss.server.log.FetchParams;
import com.alibaba.fluss.server.log.ListOffsetsParam;
import com.alibaba.fluss.server.metadata.ClusterMetadata;
import com.alibaba.fluss.server.metadata.PartitionMetadata;
import com.alibaba.fluss.server.metadata.ServerInfo;
import com.alibaba.fluss.server.metadata.TableMetadata;
import com.alibaba.fluss.server.testutils.KvTestUtils;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;
import com.alibaba.fluss.server.zk.data.TableRegistration;
import com.alibaba.fluss.testutils.DataTestUtils;
import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.CloseableIterator;
import com.alibaba.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.alibaba.fluss.record.LogRecordReadContext.createArrowReadContext;
import static com.alibaba.fluss.record.TestData.ANOTHER_DATA1;
import static com.alibaba.fluss.record.TestData.DATA1;
import static com.alibaba.fluss.record.TestData.DATA1_KEY_TYPE;
import static com.alibaba.fluss.record.TestData.DATA1_PARTITIONED_TABLE_DESCRIPTOR;
import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.record.TestData.DATA1_SCHEMA;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_ID;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static com.alibaba.fluss.record.TestData.DATA_1_WITH_KEY_AND_VALUE;
import static com.alibaba.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static com.alibaba.fluss.record.TestData.EXPECTED_LOG_RESULTS_FOR_DATA_1_WITH_PK;
import static com.alibaba.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static com.alibaba.fluss.server.metadata.PartitionMetadata.DELETED_PARTITION_ID;
import static com.alibaba.fluss.server.metadata.TableMetadata.DELETED_TABLE_ID;
import static com.alibaba.fluss.server.testutils.PartitionMetadataAssert.assertPartitionMetadata;
import static com.alibaba.fluss.server.testutils.TableMetadataAssert.assertTableMetadata;
import static com.alibaba.fluss.server.zk.data.LeaderAndIsr.INITIAL_BUCKET_EPOCH;
import static com.alibaba.fluss.server.zk.data.LeaderAndIsr.INITIAL_LEADER_EPOCH;
import static com.alibaba.fluss.testutils.DataTestUtils.assertLogRecordBatchEqualsWithRowKind;
import static com.alibaba.fluss.testutils.DataTestUtils.assertLogRecordsEquals;
import static com.alibaba.fluss.testutils.DataTestUtils.assertLogRecordsEqualsWithRowKind;
import static com.alibaba.fluss.testutils.DataTestUtils.assertMemoryRecordsEquals;
import static com.alibaba.fluss.testutils.DataTestUtils.assertMemoryRecordsEqualsWithRowKind;
import static com.alibaba.fluss.testutils.DataTestUtils.compactedRow;
import static com.alibaba.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static com.alibaba.fluss.testutils.DataTestUtils.genKvRecordBatchWithWriterId;
import static com.alibaba.fluss.testutils.DataTestUtils.genKvRecords;
import static com.alibaba.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static com.alibaba.fluss.testutils.DataTestUtils.getKeyValuePairs;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ReplicaManager}. */
class ReplicaManagerTest extends ReplicaTestBase {

    @Test
    void testProduceLog() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());

        // 1. append first batch.
        CompletableFuture<List<ProduceLogResultForBucket>> future = new CompletableFuture<>();
        replicaManager.appendRecordsToLog(
                20000,
                1,
                Collections.singletonMap(tb, genMemoryLogRecordsByObject(DATA1)),
                future::complete);
        assertThat(future.get()).containsOnly(new ProduceLogResultForBucket(tb, 0, 10L));

        // 2. append second batch.
        future = new CompletableFuture<>();
        replicaManager.appendRecordsToLog(
                20000,
                1,
                Collections.singletonMap(tb, genMemoryLogRecordsByObject(DATA1)),
                future::complete);
        assertThat(future.get()).containsOnly(new ProduceLogResultForBucket(tb, 10L, 20L));

        // 3. test append with error acks which will throw exception directly.
        assertThatThrownBy(
                        () ->
                                replicaManager.appendRecordsToLog(
                                        20000,
                                        100,
                                        Collections.singletonMap(
                                                tb, genMemoryLogRecordsByObject(DATA1)),
                                        (result) -> {
                                            // do nothing.
                                        }))
                .isInstanceOf(InvalidRequiredAcksException.class)
                .hasMessageContaining("Invalid required acks");

        // 4. test append with unknown table bucket, which will return error code in the
        // ProduceLogResultForBucket.
        TableBucket unknownBucket = new TableBucket(10001, 0);
        future = new CompletableFuture<>();
        replicaManager.appendRecordsToLog(
                20000,
                1,
                Collections.singletonMap(unknownBucket, genMemoryLogRecordsByObject(DATA1)),
                future::complete);
        assertThat(future.get())
                .containsOnly(
                        new ProduceLogResultForBucket(
                                unknownBucket,
                                new ApiError(
                                        Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION,
                                        "Unknown table or bucket: TableBucket{tableId=10001, bucket=0}")));
    }

    @Test
    void testFetchLog() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());

        // test fetch empty buckets without log segment.
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> emptyFuture =
                new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                buildFetchParams(-1),
                Collections.singletonMap(tb, new FetchReqInfo(tb.getTableId(), 0L, 1024 * 1024)),
                emptyFuture::complete);
        Map<TableBucket, FetchLogResultForBucket> result = emptyFuture.get();
        assertThat(result.size()).isEqualTo(1);
        FetchLogResultForBucket resultForBucket = result.get(tb);
        assertThat(resultForBucket.getTableBucket()).isEqualTo(tb);
        assertThat(resultForBucket.getHighWatermark()).isEqualTo(0L);
        assertThat(resultForBucket.records().sizeInBytes()).isEqualTo(0);

        // produce one batch to this bucket.
        CompletableFuture<List<ProduceLogResultForBucket>> future = new CompletableFuture<>();
        replicaManager.appendRecordsToLog(
                20000,
                1,
                Collections.singletonMap(tb, genMemoryLogRecordsByObject(DATA1)),
                future::complete);
        assertThat(future.get()).containsOnly(new ProduceLogResultForBucket(tb, 0, 10L));

        // fetch from this bucket from offset 0, return data1.
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future1 =
                new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                buildFetchParams(-1),
                Collections.singletonMap(tb, new FetchReqInfo(tb.getTableId(), 0L, 1024 * 1024)),
                future1::complete);
        result = future1.get();
        assertThat(result.size()).isEqualTo(1);
        resultForBucket = result.get(tb);
        assertThat(resultForBucket.getTableBucket()).isEqualTo(tb);
        assertThat(resultForBucket.getHighWatermark()).isEqualTo(10L);
        LogRecords records = resultForBucket.records();
        assertThat(records).isNotNull();
        assertLogRecordsEquals(DATA1_ROW_TYPE, records, DATA1);

        // fetch from this bucket from offset 3, return data1.
        future1 = new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                buildFetchParams(-1),
                Collections.singletonMap(tb, new FetchReqInfo(tb.getTableId(), 3L, 1024 * 1024)),
                future1::complete);
        result = future1.get();
        assertThat(result.size()).isEqualTo(1);
        resultForBucket = result.get(tb);
        assertThat(resultForBucket.getTableBucket()).isEqualTo(tb);
        assertThat(resultForBucket.getHighWatermark()).isEqualTo(10L);
        records = resultForBucket.records();
        assertThat(records).isNotNull();
        assertLogRecordsEquals(DATA1_ROW_TYPE, records, DATA1);

        // append new batch.
        future = new CompletableFuture<>();
        replicaManager.appendRecordsToLog(
                20000,
                1,
                Collections.singletonMap(tb, genMemoryLogRecordsByObject(DATA1)),
                future::complete);
        assertThat(future.get()).containsOnly(new ProduceLogResultForBucket(tb, 10L, 20L));

        // fetch this bucket from offset 10, return data2.
        future1 = new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                buildFetchParams(-1),
                Collections.singletonMap(tb, new FetchReqInfo(tb.getTableId(), 10L, 1024 * 1024)),
                future1::complete);
        result = future1.get();
        assertThat(result.size()).isEqualTo(1);
        resultForBucket = result.get(tb);
        assertThat(resultForBucket.getTableBucket()).isEqualTo(tb);
        assertThat(resultForBucket.getHighWatermark()).isEqualTo(20L);
        records = resultForBucket.records();
        assertThat(records).isNotNull();
        assertLogRecordsEquals(DATA1_ROW_TYPE, records, DATA1);

        // fetch this bucket from offset 100, return error code.
        future1 = new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                buildFetchParams(-1),
                Collections.singletonMap(tb, new FetchReqInfo(tb.getTableId(), 100L, 1024 * 1024)),
                future1::complete);
        result = future1.get();
        assertThat(result.size()).isEqualTo(1);
        resultForBucket = result.get(tb);
        assertThat(resultForBucket.getTableBucket()).isEqualTo(tb);
        assertThat(resultForBucket.getErrorCode())
                .isEqualTo(Errors.LOG_OFFSET_OUT_OF_RANGE_EXCEPTION.code());
        assertThat(resultForBucket.getErrorMessage())
                .contains(
                        "Received request for offset 100 for table bucket "
                                + "TableBucket{tableId=150001, bucket=1}, but we only have log "
                                + "segments from offset 0 up to 20.");

        future1 = new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                buildFetchParams(-1),
                Collections.singletonMap(tb, new FetchReqInfo(tb.getTableId(), 20L, 1024 * 1024)),
                future1::complete);
        result = future1.get();
        assertThat(result.size()).isEqualTo(1);
        LogRecords records1 = result.get(tb).records();
        assertThat(records1).isNotNull();
        assertThat(records1.batches()).hasSize(0);
    }

    @Test
    void testFetchLogWithMaxBytesLimit() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());

        // produce one batch to this bucket.
        MemoryLogRecords records1 = genMemoryLogRecordsByObject(DATA1);
        int batchSize = records1.sizeInBytes();
        int maxFetchBytesSize = batchSize + 10;
        CompletableFuture<List<ProduceLogResultForBucket>> future = new CompletableFuture<>();
        replicaManager.appendRecordsToLog(
                20000, 1, Collections.singletonMap(tb, records1), future::complete);
        assertThat(future.get()).containsOnly(new ProduceLogResultForBucket(tb, 0, 10L));

        // fetch from this bucket from offset 0 with fetch max bytes size bigger that data1 batch
        // size, return data1.
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future1 =
                new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                buildFetchParams(-1, maxFetchBytesSize),
                Collections.singletonMap(
                        tb, new FetchReqInfo(tb.getTableId(), 0L, Integer.MAX_VALUE)),
                future1::complete);
        Map<TableBucket, FetchLogResultForBucket> result = future1.get();
        assertThat(result.size()).isEqualTo(1);
        FetchLogResultForBucket resultForBucket = result.get(tb);
        assertThat(resultForBucket.getTableBucket()).isEqualTo(tb);
        assertThat(resultForBucket.getHighWatermark()).isEqualTo(10L);
        LogRecords records = resultForBucket.records();
        assertThat(records).isNotNull();
        assertThat(records.sizeInBytes()).isLessThan(maxFetchBytesSize);
        assertLogRecordsEquals(DATA1_ROW_TYPE, records, DATA1);

        // append new batch.
        future = new CompletableFuture<>();
        replicaManager.appendRecordsToLog(
                20000,
                1,
                Collections.singletonMap(tb, genMemoryLogRecordsByObject(ANOTHER_DATA1)),
                future::complete);
        assertThat(future.get()).containsOnly(new ProduceLogResultForBucket(tb, 10L, 20L));

        // fetch this bucket from offset 0 without fetch bytes size, return data1 + anotherData1.
        future1 = new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                buildFetchParams(-1),
                Collections.singletonMap(
                        tb, new FetchReqInfo(tb.getTableId(), 0, Integer.MAX_VALUE)),
                future1::complete);
        result = future1.get();
        resultForBucket = result.get(tb);
        assertThat(resultForBucket.getTableBucket()).isEqualTo(tb);
        records = resultForBucket.records();
        assertThat(records).isNotNull();
        assertThat(records.sizeInBytes()).isGreaterThan(maxFetchBytesSize);
        assertMemoryRecordsEquals(DATA1_ROW_TYPE, records, Arrays.asList(DATA1, ANOTHER_DATA1));

        // fetch this bucket from offset 0 with fetch max bytes size bigger than data1 batch size
        // but smaller than data1 + anotherData1 batch size. The data will only return data1, but
        // the returned LogRecords size is equal to maxFetchBytesSize.
        future1 = new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                buildFetchParams(-1, maxFetchBytesSize),
                Collections.singletonMap(
                        tb, new FetchReqInfo(tb.getTableId(), 0, Integer.MAX_VALUE)),
                future1::complete);
        result = future1.get();
        resultForBucket = result.get(tb);
        assertThat(resultForBucket.getTableBucket()).isEqualTo(tb);
        records = resultForBucket.records();
        assertThat(records).isNotNull();
        assertThat(records.sizeInBytes()).isEqualTo(maxFetchBytesSize);
        assertMemoryRecordsEquals(DATA1_ROW_TYPE, records, Collections.singletonList(DATA1));
    }

    @Test
    void testFetchLogWithMaxBytesLimitForMultiTableBucket() throws Exception {
        TableBucket tb1 = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb1.getBucket());
        TableBucket tb2 = new TableBucket(DATA1_TABLE_ID, 2);
        makeLogTableAsLeader(tb2.getBucket());

        // produce one batch to tb1 and tb2.
        CompletableFuture<List<ProduceLogResultForBucket>> future = new CompletableFuture<>();
        Map<TableBucket, MemoryLogRecords> data = new HashMap<>();
        data.put(tb1, genMemoryLogRecordsByObject(DATA1));
        data.put(tb2, genMemoryLogRecordsByObject(DATA1));
        replicaManager.appendRecordsToLog(20000, 1, data, future::complete);
        assertThat(future.get())
                .containsExactlyInAnyOrder(
                        new ProduceLogResultForBucket(tb1, 0, 10L),
                        new ProduceLogResultForBucket(tb2, 0, 10L));

        // produce another batch to tb1 and tb2.
        future = new CompletableFuture<>();
        data = new HashMap<>();
        data.put(tb1, genMemoryLogRecordsByObject(ANOTHER_DATA1));
        data.put(tb2, genMemoryLogRecordsByObject(ANOTHER_DATA1));
        replicaManager.appendRecordsToLog(20000, 1, data, future::complete);
        assertThat(future.get())
                .containsExactlyInAnyOrder(
                        new ProduceLogResultForBucket(tb1, 10L, 20L),
                        new ProduceLogResultForBucket(tb2, 10L, 20L));

        // fetch from tb1 and tb2 from offset 0 with fetch max bytes size 10, return data1 and an
        // empty memory records.
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future1 =
                new CompletableFuture<>();
        Map<TableBucket, FetchReqInfo> newFetchData = new HashMap<>();
        newFetchData.put(tb1, new FetchReqInfo(tb1.getTableId(), 0, Integer.MAX_VALUE));
        newFetchData.put(tb2, new FetchReqInfo(tb2.getTableId(), 0, Integer.MAX_VALUE));
        replicaManager.fetchLogRecords(buildFetchParams(-1, 10), newFetchData, future1::complete);
        Map<TableBucket, FetchLogResultForBucket> result = future1.get();
        assertThat(result.size()).isEqualTo(2);
        List<FetchLogResultForBucket> resultList = new ArrayList<>(result.values());
        assertThat(resultList.get(0).getError()).isEqualTo(ApiError.NONE);
        assertThat(resultList.get(1).getError()).isEqualTo(ApiError.NONE);
        LogRecords records1 = resultList.get(0).records();
        LogRecords records2 = resultList.get(1).records();
        assertThat(records1).isNotNull();
        assertThat(records2).isNotNull();
        if (records1.sizeInBytes() == 0) {
            assertThat(records2.sizeInBytes() > 0).isTrue();
            assertMemoryRecordsEquals(DATA1_ROW_TYPE, records2, Collections.singletonList(DATA1));
        } else {
            assertThat(records2.sizeInBytes()).isEqualTo(0);
            assertMemoryRecordsEquals(DATA1_ROW_TYPE, records1, Collections.singletonList(DATA1));
        }
    }

    @Test
    void testPutKv() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID_PK, 1);
        makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, tb.getBucket());

        // put kv records to kv store.
        CompletableFuture<List<PutKvResultForBucket>> future = new CompletableFuture<>();
        replicaManager.putRecordsToKv(
                20000,
                1,
                Collections.singletonMap(tb, genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE)),
                null,
                future::complete);
        assertThat(future.get()).containsOnly(new PutKvResultForBucket(tb, 8));

        // 2. test put with error acks, will throw exception.
        assertThatThrownBy(
                        () ->
                                replicaManager.putRecordsToKv(
                                        20000,
                                        100,
                                        Collections.singletonMap(
                                                tb, genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE)),
                                        null,
                                        (result) -> {
                                            // do nothing.
                                        }))
                .isInstanceOf(InvalidRequiredAcksException.class)
                .hasMessageContaining("Invalid required acks");

        // 3. test put to unknown table bucket.
        TableBucket unknownTb = new TableBucket(10001, 0);
        future = new CompletableFuture<>();
        replicaManager.putRecordsToKv(
                20000,
                1,
                Collections.singletonMap(unknownTb, genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE)),
                null,
                future::complete);
        assertThat(future.get())
                .containsOnly(
                        new PutKvResultForBucket(
                                unknownTb,
                                new ApiError(
                                        Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION,
                                        "Unknown table or bucket: TableBucket{tableId=10001, bucket=0}")));
    }

    @Test
    void testPutKvWithOutOfBatchSequence() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID_PK, 1);
        makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, tb.getBucket());

        // 1. put kv records to kv store.
        List<Tuple2<Object[], Object[]>> data1 =
                Arrays.asList(
                        Tuple2.of(new Object[] {1}, new Object[] {1, "a"}),
                        Tuple2.of(new Object[] {2}, new Object[] {2, "b"}),
                        Tuple2.of(new Object[] {3}, new Object[] {3, "c"}),
                        Tuple2.of(new Object[] {1}, new Object[] {1, "a1"}));
        CompletableFuture<List<PutKvResultForBucket>> future = new CompletableFuture<>();
        replicaManager.putRecordsToKv(
                20000,
                1,
                Collections.singletonMap(
                        tb,
                        genKvRecordBatchWithWriterId(
                                data1, DATA1_KEY_TYPE, DATA1_ROW_TYPE, 100L, 0)),
                null,
                future::complete);
        assertThat(future.get()).containsOnly(new PutKvResultForBucket(tb, 5));

        // 2. get the cdc-log of this batch (data1).
        List<Tuple2<ChangeType, Object[]>> expectedLogForData1 =
                Arrays.asList(
                        Tuple2.of(ChangeType.INSERT, new Object[] {1, "a"}),
                        Tuple2.of(ChangeType.INSERT, new Object[] {2, "b"}),
                        Tuple2.of(ChangeType.INSERT, new Object[] {3, "c"}),
                        Tuple2.of(ChangeType.UPDATE_BEFORE, new Object[] {1, "a"}),
                        Tuple2.of(ChangeType.UPDATE_AFTER, new Object[] {1, "a1"}));
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future1 =
                new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                buildFetchParams(-1),
                Collections.singletonMap(tb, new FetchReqInfo(tb.getTableId(), 0L, 1024 * 1024)),
                future1::complete);
        FetchLogResultForBucket resultForBucket = future1.get().get(tb);
        assertThat(resultForBucket.getHighWatermark()).isEqualTo(5L);
        LogRecords records = resultForBucket.records();
        assertMemoryRecordsEqualsWithRowKind(
                DATA1_ROW_TYPE, records, Collections.singletonList(expectedLogForData1));

        // 3. append one batch with wrong batchSequence, which will throw
        // OutOfBatchSequenceException.
        List<Tuple2<Object[], Object[]>> data2 =
                Arrays.asList(
                        Tuple2.of(new Object[] {1}, new Object[] {1, "a2"}),
                        Tuple2.of(new Object[] {2}, new Object[] {2, "b2"}),
                        Tuple2.of(new Object[] {3}, new Object[] {3, "c2"}),
                        Tuple2.of(new Object[] {1}, new Object[] {1, "a3"}));
        future = new CompletableFuture<>();
        replicaManager.putRecordsToKv(
                20000,
                1,
                Collections.singletonMap(
                        tb,
                        genKvRecordBatchWithWriterId(
                                data2, DATA1_KEY_TYPE, DATA1_ROW_TYPE, 100L, 3)),
                null,
                future::complete);
        PutKvResultForBucket putKvResultForBucket = future.get().get(0);
        assertThat(putKvResultForBucket.getErrorCode())
                .isEqualTo(Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION.code());
        assertThat(putKvResultForBucket.getErrorMessage())
                .contains(
                        "Out of order batch sequence for writer 100 at offset 12 in table-bucket "
                                + "TableBucket{tableId=150003, bucket=1} : 3 (incoming batch seq.), 0 (current batch seq.)");

        // 4. get the cdc-log, should not change.
        future1 = new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                buildFetchParams(-1),
                Collections.singletonMap(tb, new FetchReqInfo(tb.getTableId(), 0L, 1024 * 1024)),
                future1::complete);
        resultForBucket = future1.get().get(tb);
        assertThat(resultForBucket.getHighWatermark()).isEqualTo(5L);
        records = resultForBucket.records();
        assertMemoryRecordsEqualsWithRowKind(
                DATA1_ROW_TYPE, records, Collections.singletonList(expectedLogForData1));

        // 5. append one new batch with correct batchSequence, the cdc-log will not influence by the
        // previous error batch.
        List<Tuple2<Object[], Object[]>> data3 =
                Arrays.asList(
                        Tuple2.of(new Object[] {2}, new Object[] {2, "b1"}),
                        Tuple2.of(new Object[] {3}, null));
        future = new CompletableFuture<>();
        replicaManager.putRecordsToKv(
                20000,
                1,
                Collections.singletonMap(
                        tb,
                        genKvRecordBatchWithWriterId(
                                data3, DATA1_KEY_TYPE, DATA1_ROW_TYPE, 100L, 1)),
                null,
                future::complete);
        assertThat(future.get()).containsOnly(new PutKvResultForBucket(tb, 8));

        // 6. get the cdc-log of this batch (data2).
        List<Tuple2<ChangeType, Object[]>> expectedLogForData2 =
                Arrays.asList(
                        Tuple2.of(ChangeType.UPDATE_BEFORE, new Object[] {2, "b"}),
                        Tuple2.of(ChangeType.UPDATE_AFTER, new Object[] {2, "b1"}),
                        Tuple2.of(ChangeType.DELETE, new Object[] {3, "c"}));
        future1 = new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                buildFetchParams(-1),
                Collections.singletonMap(tb, new FetchReqInfo(tb.getTableId(), 0L, 1024 * 1024)),
                future1::complete);
        resultForBucket = future1.get().get(tb);
        assertThat(resultForBucket.getHighWatermark()).isEqualTo(8L);
        records = resultForBucket.records();
        assertMemoryRecordsEqualsWithRowKind(
                DATA1_ROW_TYPE, records, Arrays.asList(expectedLogForData1, expectedLogForData2));
    }

    @Test
    void testPutKvWithDeleteNonExistsKey() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID_PK, 1);
        makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, tb.getBucket());

        // put 10 batches delete non-exists key batch to kv store.
        CompletableFuture<List<PutKvResultForBucket>> future;
        List<Tuple2<Object[], Object[]>> deleteList =
                Arrays.asList(
                        Tuple2.of(new Object[] {1}, null),
                        Tuple2.of(new Object[] {2}, null),
                        Tuple2.of(new Object[] {3}, null),
                        Tuple2.of(new Object[] {4}, null));
        for (int i = 0; i < 10; i++) {
            future = new CompletableFuture<>();
            replicaManager.putRecordsToKv(
                    20000,
                    1,
                    Collections.singletonMap(tb, genKvRecordBatch(deleteList)),
                    null,
                    future::complete);
            assertThat(future.get()).containsOnly(new PutKvResultForBucket(tb, i + 1));
        }

        // 2. write a normal batch.
        future = new CompletableFuture<>();
        replicaManager.putRecordsToKv(
                20000,
                1,
                Collections.singletonMap(tb, genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE)),
                null,
                future::complete);
        assertThat(future.get()).containsOnly(new PutKvResultForBucket(tb, 18));

        // 2. get the cdc-log of these batches.
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future1 =
                new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                buildFetchParams(-1),
                Collections.singletonMap(
                        tb, new FetchReqInfo(tb.getTableId(), 0L, Integer.MAX_VALUE)),
                future1::complete);
        FetchLogResultForBucket resultForBucket = future1.get().get(tb);
        assertThat(resultForBucket.getHighWatermark()).isEqualTo(18L);
        LogRecords logRecords = resultForBucket.recordsOrEmpty();
        Iterator<LogRecordBatch> iterator = logRecords.batches().iterator();
        for (int i = 0; i < 10; i++) {
            assertThat(iterator.hasNext()).isTrue();
            LogRecordBatch logRecordBatch = iterator.next();
            assertThat(logRecordBatch.getRecordCount()).isEqualTo(0);
            assertThat(logRecordBatch.baseLogOffset()).isEqualTo(i);
            assertThat(logRecordBatch.lastLogOffset()).isEqualTo(i);
            assertThat(logRecordBatch.nextLogOffset()).isEqualTo(i + 1);
            try (LogRecordReadContext readContext =
                            createArrowReadContext(DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);
                    CloseableIterator<LogRecord> logIterator =
                            logRecordBatch.records(readContext)) {
                assertThat(logIterator.hasNext()).isFalse();
            }
        }

        assertThat(iterator.hasNext()).isTrue();
        LogRecordBatch logRecordBatch = iterator.next();
        assertThat(iterator.hasNext()).isFalse();
        assertLogRecordBatchEqualsWithRowKind(
                DATA1_ROW_TYPE, logRecordBatch, EXPECTED_LOG_RESULTS_FOR_DATA_1_WITH_PK);
    }

    @Test
    void testLookup() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID_PK, 1);
        makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, tb.getBucket());

        // first lookup key without in table, key = 1.
        Object[] key1 = DATA_1_WITH_KEY_AND_VALUE.get(0).f0;
        CompactedKeyEncoder keyEncoder = new CompactedKeyEncoder(DATA1_ROW_TYPE, new int[] {0});
        byte[] key1Bytes = keyEncoder.encodeKey(row(key1));
        verifyLookup(tb, key1Bytes, null);

        // send one batch kv.
        CompletableFuture<List<PutKvResultForBucket>> future1 = new CompletableFuture<>();
        replicaManager.putRecordsToKv(
                20000,
                1,
                Collections.singletonMap(tb, genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE)),
                null,
                future1::complete);
        assertThat(future1.get()).containsOnly(new PutKvResultForBucket(tb, 8));

        // second lookup key in table, key = 1, value = 1, "a1".
        Object[] value1 = DATA_1_WITH_KEY_AND_VALUE.get(3).f1;
        byte[] value1Bytes =
                ValueEncoder.encodeValue(DEFAULT_SCHEMA_ID, compactedRow(DATA1_ROW_TYPE, value1));
        verifyLookup(tb, key1Bytes, value1Bytes);

        // key = 3 is deleted, need return null.
        Object[] key3 = DATA_1_WITH_KEY_AND_VALUE.get(2).f0;
        byte[] key3Bytes = keyEncoder.encodeKey(row(key3));
        verifyLookup(tb, key3Bytes, null);

        // Lookup from none pk table.
        TableBucket tb2 = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb2.getBucket());
        replicaManager.lookups(
                Collections.singletonMap(tb2, Collections.singletonList(key1Bytes)),
                (lookupResultForBuckets) -> {
                    LookupResultForBucket lookupResultForBucket = lookupResultForBuckets.get(tb2);
                    assertThat(lookupResultForBucket.failed()).isTrue();
                    ApiError apiError = lookupResultForBucket.getError();
                    assertThat(apiError.error()).isEqualTo(Errors.NON_PRIMARY_KEY_TABLE_EXCEPTION);
                    assertThat(apiError.message())
                            .isEqualTo("the primary key table not exists for %s", tb2);
                });
    }

    @Test
    void testPrefixLookup() throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "test_prefix_lookup_t1");
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.BIGINT())
                        .column("d", DataTypes.STRING())
                        .primaryKey("a", "b", "c")
                        .build();
        RowType rowType = schema.getRowType();
        RowType keyType =
                DataTypes.ROW(
                        new DataField("a", DataTypes.INT()),
                        new DataField("b", DataTypes.STRING()),
                        new DataField("c", DataTypes.BIGINT()));

        long tableId =
                registerTableInZkClient(
                        tablePath,
                        schema,
                        1998232L,
                        Arrays.asList("a", "b"), // bucket keys equals prefix keys.
                        Collections.emptyMap());
        TableBucket tb = new TableBucket(tableId, 0);
        makeKvTableAsLeader(tableId, tablePath, tb.getBucket());

        List<Tuple2<Object[], Object[]>> data1 =
                Arrays.asList(
                        Tuple2.of(new Object[] {1, "a", 1L}, new Object[] {1, "a", 1L, "value1"}),
                        Tuple2.of(new Object[] {1, "a", 2L}, new Object[] {1, "a", 2L, "value2"}),
                        Tuple2.of(new Object[] {1, "a", 3L}, new Object[] {1, "a", 3L, "value3"}),
                        Tuple2.of(new Object[] {2, "a", 4L}, new Object[] {2, "a", 4L, "value4"}));
        // send one batch kv.
        CompletableFuture<List<PutKvResultForBucket>> future = new CompletableFuture<>();
        replicaManager.putRecordsToKv(
                20000,
                1,
                Collections.singletonMap(tb, genKvRecordBatch(keyType, rowType, data1)),
                null,
                future::complete);
        assertThat(future.get()).containsOnly(new PutKvResultForBucket(tb, 4));
        // second prefix lookup in table, prefix key = (1, "a").
        Object[] prefixKey1 = new Object[] {1, "a"};
        CompactedKeyEncoder keyEncoder = new CompactedKeyEncoder(rowType, new int[] {0, 1});
        byte[] prefixKey1Bytes = keyEncoder.encodeKey(row(prefixKey1));
        List<byte[]> key1ExpectedValues =
                Arrays.asList(
                        ValueEncoder.encodeValue(
                                DEFAULT_SCHEMA_ID,
                                compactedRow(rowType, new Object[] {1, "a", 1L, "value1"})),
                        ValueEncoder.encodeValue(
                                DEFAULT_SCHEMA_ID,
                                compactedRow(rowType, new Object[] {1, "a", 2L, "value2"})),
                        ValueEncoder.encodeValue(
                                DEFAULT_SCHEMA_ID,
                                compactedRow(rowType, new Object[] {1, "a", 3L, "value3"})));
        verifyPrefixLookup(
                tb,
                Collections.singletonList(prefixKey1Bytes),
                Collections.singletonList(key1ExpectedValues));

        // third prefix lookup in table for multi prefix keys, prefix key = (1, "a") and (2, "a").
        Object[] prefixKey2 = new Object[] {2, "a"};
        byte[] prefixKey2Bytes = keyEncoder.encodeKey(row(prefixKey2));
        List<byte[]> key2ExpectedValues =
                Collections.singletonList(
                        ValueEncoder.encodeValue(
                                DEFAULT_SCHEMA_ID,
                                compactedRow(rowType, new Object[] {2, "a", 4L, "value4"})));
        verifyPrefixLookup(
                tb,
                Arrays.asList(prefixKey1Bytes, prefixKey2Bytes),
                Arrays.asList(key1ExpectedValues, key2ExpectedValues));

        // Prefix lookup an unsupported prefixLookup table (a log table).
        tableId =
                registerTableInZkClient(
                        DATA1_TABLE_PATH,
                        DATA1_SCHEMA,
                        2001L,
                        Collections.emptyList(),
                        Collections.emptyMap());
        TableBucket tb3 = new TableBucket(tableId, 0);
        makeLogTableAsLeader(tb3, false);
        replicaManager.prefixLookups(
                Collections.singletonMap(tb3, Collections.singletonList(prefixKey2Bytes)),
                (prefixLookupResultForBuckets) -> {
                    PrefixLookupResultForBucket lookupResultForBucket =
                            prefixLookupResultForBuckets.get(tb3);
                    assertThat(lookupResultForBucket.failed()).isTrue();
                    ApiError apiError = lookupResultForBucket.getError();
                    assertThat(apiError.error()).isEqualTo(Errors.NON_PRIMARY_KEY_TABLE_EXCEPTION);
                    assertThat(apiError.message())
                            .isEqualTo(
                                    "Try to do prefix lookup on a non primary key table: "
                                            + DATA1_TABLE_PATH);
                });
    }

    @Test
    void testLimitScanPrimaryKeyTable() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID_PK, 1);
        makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, tb.getBucket());
        DefaultValueRecordBatch.Builder builder = DefaultValueRecordBatch.builder();

        // first limit scan from an empty table.
        CompletableFuture<LimitScanResultForBucket> future = new CompletableFuture<>();
        replicaManager.limitScan(tb, 1, future::complete);
        assertThat(future.get().getValues()).isEqualTo(builder.build());

        // first, send one batch kv.
        CompletableFuture<List<PutKvResultForBucket>> future1 = new CompletableFuture<>();
        replicaManager.putRecordsToKv(
                20000,
                1,
                Collections.singletonMap(tb, genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE)),
                null,
                future1::complete);
        assertThat(future1.get()).containsOnly(new PutKvResultForBucket(tb, 8));

        // second, limit scan from table with limit
        builder.append(DEFAULT_SCHEMA_ID, compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a1"}));
        future = new CompletableFuture<>();
        replicaManager.limitScan(tb, 1, future::complete);
        assertThat(future.get().getValues()).isEqualTo(builder.build());

        // third, limit scan from table with more limit
        future = new CompletableFuture<>();
        replicaManager.limitScan(tb, 3, future::complete);
        // there is only 2 records in the table bucket after merged
        builder.append(DEFAULT_SCHEMA_ID, compactedRow(DATA1_ROW_TYPE, new Object[] {2, "b1"}));
        assertThat(future.get().getValues()).isEqualTo(builder.build());
    }

    @Test
    void testLimitScanLogTable() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb.getBucket());

        // produce one batch to this bucket.
        CompletableFuture<List<ProduceLogResultForBucket>> future = new CompletableFuture<>();
        replicaManager.appendRecordsToLog(
                20000,
                1,
                Collections.singletonMap(tb, genMemoryLogRecordsByObject(DATA1)),
                future::complete);
        assertThat(future.get()).containsOnly(new ProduceLogResultForBucket(tb, 0, 10L));
        // produce another batch to this bucket.
        future = new CompletableFuture<>();
        replicaManager.appendRecordsToLog(
                20000,
                1,
                Collections.singletonMap(tb, genMemoryLogRecordsByObject(ANOTHER_DATA1)),
                future::complete);
        assertThat(future.get()).containsOnly(new ProduceLogResultForBucket(tb, 10, 20L));

        // get limit 10 records from local.
        CompletableFuture<LimitScanResultForBucket> limitFuture = new CompletableFuture<>();
        replicaManager.limitScan(tb, 10, limitFuture::complete);
        assertMemoryRecordsEquals(
                DATA1_ROW_TYPE,
                limitFuture.get().getRecords(),
                Collections.singletonList(ANOTHER_DATA1));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testListOffsets(boolean isPartitioned) throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, isPartitioned ? 10L : null, 1);
        makeLogTableAsLeader(tb, isPartitioned);

        // produce one batch to this bucket.
        CompletableFuture<List<ProduceLogResultForBucket>> future = new CompletableFuture<>();
        replicaManager.appendRecordsToLog(
                20000,
                1,
                Collections.singletonMap(tb, genMemoryLogRecordsByObject(DATA1)),
                future::complete);
        assertThat(future.get()).containsOnly(new ProduceLogResultForBucket(tb, 0, 10L));

        // list offsets from client.
        CompletableFuture<List<ListOffsetsResultForBucket>> future1 = new CompletableFuture<>();
        replicaManager.listOffsets(
                new ListOffsetsParam(-1, ListOffsetsParam.LATEST_OFFSET_TYPE, null),
                Collections.singleton(tb),
                future1::complete);
        assertThat(future1.get()).containsOnly(new ListOffsetsResultForBucket(tb, 10L));

        // listOffset from tablet server where follower locate in.
        future1 = new CompletableFuture<>();
        replicaManager.listOffsets(
                new ListOffsetsParam(1, ListOffsetsParam.LATEST_OFFSET_TYPE, null),
                Collections.singleton(tb),
                future1::complete);
        assertThat(future1.get()).containsOnly(new ListOffsetsResultForBucket(tb, 10L));

        // list an unknown table bucket.
        TableBucket unknownTb = new TableBucket(10001, 0);
        future1 = new CompletableFuture<>();
        replicaManager.listOffsets(
                new ListOffsetsParam(-1, ListOffsetsParam.LATEST_OFFSET_TYPE, null),
                Collections.singleton(unknownTb),
                future1::complete);
        assertThat(future1.get())
                .containsOnly(
                        new ListOffsetsResultForBucket(
                                unknownTb,
                                new ApiError(
                                        Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION,
                                        "Unknown table or bucket: TableBucket{tableId=10001, bucket=0}")));

        // list log start offset.
        future1 = new CompletableFuture<>();
        replicaManager.listOffsets(
                new ListOffsetsParam(1, ListOffsetsParam.EARLIEST_OFFSET_TYPE, null),
                Collections.singleton(tb),
                future1::complete);
        assertThat(future1.get()).containsOnly(new ListOffsetsResultForBucket(tb, 0L));
    }

    @Test
    void testListOffsetsWithTimestamp() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());

        long startTimestamp = manualClock.milliseconds();
        // append five batches.
        for (int i = 0; i < 5; i++) {
            CompletableFuture<List<ProduceLogResultForBucket>> future = new CompletableFuture<>();
            replicaManager.appendRecordsToLog(
                    20000,
                    1,
                    Collections.singletonMap(tb, genMemoryLogRecordsByObject(DATA1)),
                    future::complete);
            future.get();
            // advance clock to generate different batch commit timestamp.
            manualClock.advanceTime(100, TimeUnit.MILLISECONDS);
        }

        // list offset by the lowest startTimestamp.
        CompletableFuture<List<ListOffsetsResultForBucket>> future1 = new CompletableFuture<>();
        replicaManager.listOffsets(
                new ListOffsetsParam(-1, ListOffsetsParam.TIMESTAMP_OFFSET_TYPE, startTimestamp),
                Collections.singleton(tb),
                future1::complete);
        assertThat(future1.get()).containsOnly(new ListOffsetsResultForBucket(tb, 0L));

        // fetch all bathes and get the batch commit timestamp.
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                buildFetchParams(-1, Integer.MAX_VALUE),
                Collections.singletonMap(
                        tb, new FetchReqInfo(tb.getTableId(), 0L, Integer.MAX_VALUE)),
                future::complete);
        Map<Long, Long> offsetToCommitTimestampMap =
                startOffsetToBatchCommitTimestamp(future.get().get(tb));
        for (Map.Entry<Long, Long> entry : offsetToCommitTimestampMap.entrySet()) {
            Long baseOffset = entry.getKey();
            long commitTimestamp = entry.getValue();
            // fetch offset with start offset equal to batch commit timestamp.
            future1 = new CompletableFuture<>();
            replicaManager.listOffsets(
                    new ListOffsetsParam(
                            -1, ListOffsetsParam.TIMESTAMP_OFFSET_TYPE, commitTimestamp),
                    Collections.singleton(tb),
                    future1::complete);
            assertThat(future1.get()).containsOnly(new ListOffsetsResultForBucket(tb, baseOffset));
        }

        // list offset by a timestamp which higher than max batch commit time but less then current
        // timestamp .
        future1 = new CompletableFuture<>();
        replicaManager.listOffsets(
                new ListOffsetsParam(-1, ListOffsetsParam.LATEST_OFFSET_TYPE, null),
                Collections.singleton(tb),
                future1::complete);
        long latestOffset = future1.get().get(0).getOffset();
        manualClock.advanceTime(100, TimeUnit.MILLISECONDS);
        future1 = new CompletableFuture<>();
        replicaManager.listOffsets(
                new ListOffsetsParam(
                        -1,
                        ListOffsetsParam.TIMESTAMP_OFFSET_TYPE,
                        manualClock.milliseconds() - 50),
                Collections.singleton(tb),
                future1::complete);
        assertThat(future1.get()).containsOnly(new ListOffsetsResultForBucket(tb, latestOffset));

        // list offset by an invalid timestamp which higher than current timestamp.
        future1 = new CompletableFuture<>();
        replicaManager.listOffsets(
                new ListOffsetsParam(
                        -1,
                        ListOffsetsParam.TIMESTAMP_OFFSET_TYPE,
                        manualClock.milliseconds() + 1000),
                Collections.singleton(tb),
                future1::complete);
        assertThat(future1.get())
                .containsOnly(
                        new ListOffsetsResultForBucket(
                                tb,
                                new ApiError(
                                        Errors.INVALID_TIMESTAMP_EXCEPTION,
                                        String.format(
                                                "Get offset error for table bucket "
                                                        + "TableBucket{tableId=150001, bucket=1}, the fetch "
                                                        + "timestamp %s is larger than the current timestamp %s",
                                                manualClock.milliseconds() + 1000,
                                                manualClock.milliseconds()))));
    }

    private Map<Long, Long> startOffsetToBatchCommitTimestamp(FetchLogResultForBucket result) {
        Map<Long, Long> offsetToCommitTimestampMap = new HashMap<>();
        for (LogRecordBatch batch : result.recordsOrEmpty().batches()) {
            offsetToCommitTimestampMap.put(batch.baseLogOffset(), batch.commitTimestamp());
        }
        return offsetToCommitTimestampMap;
    }

    @Test
    void testCompleteDelayProduceLog() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());

        // append records to log as ack = -1, which will generate delayed write operation.
        CompletableFuture<List<ProduceLogResultForBucket>> future = new CompletableFuture<>();
        replicaManager.appendRecordsToLog(
                300000,
                -1,
                Collections.singletonMap(tb, genMemoryLogRecordsByObject(DATA1)),
                future::complete);
        assertThat(future.get()).containsOnly(new ProduceLogResultForBucket(tb, 0, 10L));
    }

    @Test
    void testCompleteDelayPutKv() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID_PK, 1);
        makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, tb.getBucket());

        // put kv records to kv store as ack = -1, which will generate delayed write operation.
        CompletableFuture<List<PutKvResultForBucket>> future = new CompletableFuture<>();
        replicaManager.putRecordsToKv(
                300000,
                -1,
                Collections.singletonMap(tb, genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE)),
                null,
                future::complete);
        assertThat(future.get()).containsOnly(new PutKvResultForBucket(tb, 8));
    }

    @Test
    void becomeLeaderOrFollower() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);

        // make tb as leader.
        CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> future =
                new CompletableFuture<>();
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                PhysicalTablePath.of(DATA1_TABLE_PATH),
                                tb,
                                Arrays.asList(1, 2, 3),
                                new LeaderAndIsr(
                                        TABLET_SERVER_ID,
                                        1,
                                        Arrays.asList(1, 2, 3),
                                        INITIAL_COORDINATOR_EPOCH,
                                        INITIAL_BUCKET_EPOCH))),
                future::complete);
        assertThat(future.get()).containsOnly(new NotifyLeaderAndIsrResultForBucket(tb));
        assertReplicaEpochEquals(
                replicaManager.getReplicaOrException(tb), true, 1, INITIAL_BUCKET_EPOCH);

        // become leader with lower leader epoch, it will throw exception.
        future = new CompletableFuture<>();
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                PhysicalTablePath.of(DATA1_TABLE_PATH),
                                tb,
                                Arrays.asList(1, 2, 3),
                                new LeaderAndIsr(
                                        TABLET_SERVER_ID,
                                        INITIAL_LEADER_EPOCH,
                                        Arrays.asList(1, 2, 3),
                                        INITIAL_COORDINATOR_EPOCH,
                                        INITIAL_BUCKET_EPOCH))),
                future::complete);
        assertThat(future.get())
                .containsOnly(
                        new NotifyLeaderAndIsrResultForBucket(
                                tb,
                                new ApiError(
                                        Errors.FENCED_LEADER_EPOCH_EXCEPTION,
                                        "the leader epoch 0 in request is smaller than "
                                                + "the current leader epoch 1 for table bucket "
                                                + "TableBucket{tableId=150001, bucket=1}")));
        assertReplicaEpochEquals(
                replicaManager.getReplicaOrException(tb), true, 1, INITIAL_BUCKET_EPOCH);
    }

    @Test
    void testStopReplica() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);

        // make tb as leader.
        CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> future =
                new CompletableFuture<>();
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                PhysicalTablePath.of(DATA1_TABLE_PATH),
                                tb,
                                Arrays.asList(1, 2, 3),
                                new LeaderAndIsr(
                                        TABLET_SERVER_ID,
                                        1,
                                        Arrays.asList(1, 2, 3),
                                        INITIAL_COORDINATOR_EPOCH,
                                        INITIAL_BUCKET_EPOCH))),
                future::complete);
        assertThat(future.get()).containsOnly(new NotifyLeaderAndIsrResultForBucket(tb));
        assertReplicaEpochEquals(
                replicaManager.getReplicaOrException(tb), true, 1, INITIAL_BUCKET_EPOCH);

        // stop replica.
        CompletableFuture<List<StopReplicaResultForBucket>> future1 = new CompletableFuture<>();
        replicaManager.stopReplicas(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        new StopReplicaData(tb, true, INITIAL_COORDINATOR_EPOCH, 1)),
                future1::complete);
        assertThat(future1.get()).containsOnly(new StopReplicaResultForBucket(tb));
        ReplicaManager.HostedReplica hostedReplica = replicaManager.getReplica(tb);
        assertThat(hostedReplica).isInstanceOf(ReplicaManager.NoneReplica.class);

        // make tb as leader again.
        future = new CompletableFuture<>();
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                PhysicalTablePath.of(DATA1_TABLE_PATH),
                                tb,
                                Arrays.asList(1, 2, 3),
                                new LeaderAndIsr(
                                        TABLET_SERVER_ID,
                                        2,
                                        Arrays.asList(1, 2, 3),
                                        INITIAL_COORDINATOR_EPOCH,
                                        INITIAL_BUCKET_EPOCH))),
                future::complete);
        assertThat(future.get()).containsOnly(new NotifyLeaderAndIsrResultForBucket(tb));
        assertReplicaEpochEquals(
                replicaManager.getReplicaOrException(tb), true, 2, INITIAL_BUCKET_EPOCH);

        // stop replica with fenced leader epoch.
        future1 = new CompletableFuture<>();
        replicaManager.stopReplicas(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        new StopReplicaData(tb, true, INITIAL_COORDINATOR_EPOCH, 1)),
                future1::complete);
        assertThat(future1.get())
                .containsOnly(
                        new StopReplicaResultForBucket(
                                tb,
                                Errors.FENCED_LEADER_EPOCH_EXCEPTION,
                                "invalid leader epoch 1 in stop replica request, "
                                        + "The latest known leader epoch is 2 for table bucket "
                                        + "TableBucket{tableId=150001, bucket=1}."));
        replicaManager.getReplicaOrException(tb);
    }

    @Test
    void testKvDataVisibility() throws Exception {
        // The CDC log is only visible after the KV has been flushed to RocksDB. In other words,
        // when we can read the CDC log, the associated kv record must have been
        // inserted/updated/deleted in RocksDB. The reason for ensuring this visibility is that we
        // first buffer the data in memory before flushing it to RocksDB. Thus, we need to guarantee
        // visibility.
        TableBucket tb = new TableBucket(DATA1_TABLE_ID_PK, 1);
        makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, tb.getBucket());
        Replica replica = replicaManager.getReplicaOrException(tb);
        RocksDBKv rocksDBKv = replica.getKvTablet().getRocksDBKv();
        long beginTime = System.nanoTime();

        CompactedKeyEncoder keyEncoder = new CompactedKeyEncoder(DATA1_ROW_TYPE, new int[] {0});
        // retry send kv records to kv store, if the highWatermark increased, the kv record must be
        // visible in rocksdb.
        int round = 1000;
        Thread writerThread =
                new Thread(
                        () -> {
                            CompletableFuture<List<PutKvResultForBucket>> future;
                            for (int i = 0; i < round; i++) {
                                future = new CompletableFuture<>();
                                Object[] key = {i};
                                Object[] value = {i, "a"};
                                // don't wait for the result.
                                try {
                                    replicaManager.putRecordsToKv(
                                            20000,
                                            -1,
                                            Collections.singletonMap(
                                                    tb,
                                                    genKvRecordBatch(
                                                            Collections.singletonList(
                                                                    Tuple2.of(key, value)))),
                                            null,
                                            future::complete);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });

        long[] highWatermarkUpdateTimestamps = new long[round];
        Thread readWatermarkThead =
                new Thread(
                        () -> {
                            int count = 0;
                            while (count < round) {
                                if (replica.getLogHighWatermark() >= count + 1) {
                                    highWatermarkUpdateTimestamps[count] = System.nanoTime();
                                    count++;
                                }
                            }
                        });

        long[] lastTimestampForNullValues = new long[round];
        Thread getKvThread =
                new Thread(
                        () -> {
                            int count = 0;
                            long lastTimestampForNullValue = beginTime;
                            while (count < round) {
                                Object[] key = {count};
                                byte[] keyBytes = keyEncoder.encodeKey(row(key));
                                try {
                                    long timestamp = System.nanoTime();
                                    if (rocksDBKv.get(keyBytes) == null) {
                                        lastTimestampForNullValue = timestamp;
                                    } else {
                                        lastTimestampForNullValues[count] =
                                                lastTimestampForNullValue;
                                        count++;
                                    }
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });

        writerThread.start();
        readWatermarkThead.start();
        getKvThread.start();

        writerThread.join();
        readWatermarkThead.join();
        getKvThread.join();

        for (int i = 0; i < round; i++) {
            assertThat(highWatermarkUpdateTimestamps[i])
                    .isGreaterThanOrEqualTo(lastTimestampForNullValues[i]);
        }
    }

    @Test
    void testSnapshotKvReplicas() throws Exception {
        // create multiple kv replicas and all do the snapshot operation
        int nBuckets = 5;
        List<TableBucket> tableBuckets = createTableBuckets(nBuckets);
        makeKvTableAsLeader(tableBuckets, DATA1_TABLE_PATH_PK, 0);
        Map<TableBucket, KvRecordBatch> entriesPerBucket = new HashMap<>();
        for (int i = 0; i < tableBuckets.size(); i++) {
            TableBucket tableBucket = tableBuckets.get(i);
            entriesPerBucket.put(
                    tableBucket,
                    genKvRecordBatch(
                            Tuple2.of(i + "k1", new Object[] {1, "a"}),
                            Tuple2.of(i + "k2", new Object[] {2, "b"})));
        }

        // put one kv record batch for every bucket
        replicaManager.putRecordsToKv(
                300000,
                -1,
                entriesPerBucket,
                null,
                writeResultForBuckets -> {
                    // do nothing
                });

        List<CompletedSnapshot> completedSnapshots = new ArrayList<>();
        // wait until we get completed snapshots for all table buckets.
        for (TableBucket tableBucket : tableBuckets) {
            completedSnapshots.add(snapshotReporter.waitUntilSnapshotComplete(tableBucket, 0));
        }

        // check the snapshots for each table bucket
        List<Tuple2<byte[], byte[]>> expectedKeyValues;
        for (int i = 0; i < tableBuckets.size(); i++) {
            CompletedSnapshot completedSnapshot = completedSnapshots.get(i);
            // check the data in the completed snapshot
            expectedKeyValues =
                    getKeyValuePairs(
                            genKvRecords(
                                    Tuple2.of(i + "k1", new Object[] {1, "a"}),
                                    Tuple2.of(i + "k2", new Object[] {2, "b"})));
            KvTestUtils.checkSnapshot(completedSnapshot, expectedKeyValues, 2);
        }

        // put one kv record batch again for every bucket
        for (int i = 0; i < tableBuckets.size(); i++) {
            TableBucket tableBucket = tableBuckets.get(i);
            // should produce -U,+U,-D
            entriesPerBucket.put(
                    tableBucket,
                    genKvRecordBatch(
                            Tuple2.of(i + "k1", new Object[] {1, "aa"}),
                            Tuple2.of(i + "k2", null)));
        }
        replicaManager.putRecordsToKv(
                300000,
                -1,
                entriesPerBucket,
                null,
                writeResultForBuckets -> {
                    // do nothing
                });

        completedSnapshots.clear();
        // wait until we get completed snapshots for all table buckets.
        for (TableBucket tableBucket : tableBuckets) {
            completedSnapshots.add(snapshotReporter.waitUntilSnapshotComplete(tableBucket, 1));
        }
        // check the snapshots for each table bucket
        for (int i = 0; i < tableBuckets.size(); i++) {
            CompletedSnapshot completedSnapshot = completedSnapshots.get(i);
            // should only remain one key
            expectedKeyValues =
                    getKeyValuePairs(genKvRecords(Tuple2.of(i + "k1", new Object[] {1, "aa"})));
            KvTestUtils.checkSnapshot(completedSnapshot, expectedKeyValues, 5);
        }
    }

    @Test
    void testKvRestore() throws Exception {
        // create multiple kv replicas and all do the snapshot operation
        int nBuckets = 3;
        List<TableBucket> tableBuckets = createTableBuckets(nBuckets);
        makeKvTableAsLeader(tableBuckets, DATA1_TABLE_PATH_PK, 0);

        Map<TableBucket, List<KvRecord>> kvRecordsPerBucket = new HashMap<>();
        Map<TableBucket, KvRecordBatch> kvRecordBatchPerBucket = new HashMap<>();
        for (TableBucket tableBucket : tableBuckets) {
            List<KvRecord> kvRecords =
                    genKvRecords(
                            new Object[] {1, "a" + tableBucket.getBucket()},
                            new Object[] {2, "b" + tableBucket.getBucket()});
            kvRecordsPerBucket.put(tableBucket, kvRecords);
            kvRecordBatchPerBucket.put(tableBucket, DataTestUtils.toKvRecordBatch(kvRecords));
        }
        putRecords(kvRecordBatchPerBucket);

        // make all become leader with a bigger leader epoch, which will cause restore
        makeKvTableAsLeader(tableBuckets, DATA1_TABLE_PATH_PK, 1);

        // let's check the result after restore
        checkKvDataForBuckets(kvRecordsPerBucket);

        // let's put some data again make sure the restore works
        // put one kv record batch again for every bucket
        for (TableBucket tableBucket : tableBuckets) {
            // should produce -U,+U, -U,+U
            List<KvRecord> kvRecords =
                    genKvRecords(
                            new Object[] {1, "aa" + tableBucket.getBucket()},
                            new Object[] {2, "bb" + tableBucket.getBucket()});
            kvRecordsPerBucket.put(tableBucket, kvRecords);
            kvRecordBatchPerBucket.put(tableBucket, DataTestUtils.toKvRecordBatch(kvRecords));
        }

        putRecords(kvRecordBatchPerBucket);

        // check the data now;
        checkKvDataForBuckets(kvRecordsPerBucket);

        // restore and check data again
        makeKvTableAsLeader(tableBuckets, DATA1_TABLE_PATH_PK, 2);
        checkKvDataForBuckets(kvRecordsPerBucket);

        // fetch log, make sure the log is correct after restore
        Map<TableBucket, FetchLogResultForBucket> result = fetchLog(tableBuckets);
        for (Map.Entry<TableBucket, FetchLogResultForBucket> r1 : result.entrySet()) {
            TableBucket tableBucket = r1.getKey();
            int bucketId = tableBucket.getBucket();
            List<Tuple2<ChangeType, Object[]>> expectedLogResults =
                    Arrays.asList(
                            Tuple2.of(ChangeType.INSERT, new Object[] {1, "a" + bucketId}),
                            Tuple2.of(ChangeType.INSERT, new Object[] {2, "b" + bucketId}),
                            Tuple2.of(ChangeType.UPDATE_BEFORE, new Object[] {1, "a" + bucketId}),
                            Tuple2.of(ChangeType.UPDATE_AFTER, new Object[] {1, "aa" + bucketId}),
                            Tuple2.of(ChangeType.UPDATE_BEFORE, new Object[] {2, "b" + bucketId}),
                            Tuple2.of(ChangeType.UPDATE_AFTER, new Object[] {2, "bb" + bucketId}));
            FetchLogResultForBucket r = r1.getValue();
            assertLogRecordsEqualsWithRowKind(DATA1_ROW_TYPE, r.records(), expectedLogResults);
        }
    }

    @Test
    void testUpdateMetadata() throws Exception {
        // check the server metadata before update.
        TablePath nonePartitionTablePath = TablePath.of("test_db_1", "test_update_metadata_table");
        TablePath partitionTablePath =
                TablePath.of("test_db_1", "test_update_metadata_partition_table");
        long nonePartitionTableId = 150004L;
        long partitionTableId = 150002L;
        long partitionId1 = 15L;
        String partitionName1 = "p1";
        PhysicalTablePath physicalTablePath1 =
                PhysicalTablePath.of(partitionTablePath, partitionName1);
        long partitionId2 = 16L;
        String partitionName2 = "p2";
        PhysicalTablePath physicalTablePath2 =
                PhysicalTablePath.of(partitionTablePath, partitionName2);

        Map<String, ServerNode> expectedCoordinatorServer = new HashMap<>();
        expectedCoordinatorServer.put(
                "CLIENT", new ServerNode(0, "localhost", 1234, ServerType.COORDINATOR));
        expectedCoordinatorServer.put("INTERNAL", null);

        Map<Long, TablePath> expectedTablePathById = new HashMap<>();
        expectedTablePathById.put(nonePartitionTableId, null);
        expectedTablePathById.put(partitionTableId, null);

        Map<TablePath, TableMetadata> expectedTableMetadataById = new HashMap<>();
        expectedTableMetadataById.put(nonePartitionTablePath, null);
        expectedTableMetadataById.put(partitionTablePath, null);

        Map<Long, String> expectedPartitionNameById = new HashMap<>();
        expectedPartitionNameById.put(partitionId1, null);
        expectedPartitionNameById.put(partitionId2, null);

        Map<PhysicalTablePath, PartitionMetadata> expectedPartitionMetadataById = new HashMap<>();
        expectedPartitionMetadataById.put(physicalTablePath1, null);
        expectedPartitionMetadataById.put(physicalTablePath2, null);

        assertUpdateMetadataEquals(
                expectedCoordinatorServer,
                3,
                expectedTablePathById,
                expectedTableMetadataById,
                expectedPartitionNameById,
                expectedPartitionMetadataById);

        // 1. test update metadata with coordinatorEpoch = 2, with new coordinatorServer address,
        // with one new tabletServer, with one new table and one new partition table.
        ServerInfo csServerInfo =
                new ServerInfo(
                        0,
                        null,
                        Endpoint.fromListenersString(
                                "CLIENT://localhost:1234,INTERNAL://localhost:1235"),
                        ServerType.COORDINATOR);
        Set<ServerInfo> tsServerInfoList =
                new HashSet<>(
                        Arrays.asList(
                                new ServerInfo(
                                        TABLET_SERVER_ID,
                                        "rack0",
                                        Endpoint.fromListenersString("CLIENT://localhost:90"),
                                        ServerType.TABLET_SERVER),
                                new ServerInfo(
                                        2,
                                        "rack1",
                                        Endpoint.fromListenersString("CLIENT://localhost:91"),
                                        ServerType.TABLET_SERVER),
                                new ServerInfo(
                                        3,
                                        "rack2",
                                        Endpoint.fromListenersString("CLIENT://localhost:92"),
                                        ServerType.TABLET_SERVER),
                                new ServerInfo(
                                        4,
                                        "rack3",
                                        Endpoint.fromListenersString("CLIENT://localhost:93"),
                                        ServerType.TABLET_SERVER)));

        TableInfo nonePartitionTableInfo =
                TableInfo.of(
                        nonePartitionTablePath,
                        nonePartitionTableId,
                        1,
                        DATA1_TABLE_DESCRIPTOR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());
        TableInfo partitionTableInfo =
                TableInfo.of(
                        partitionTablePath,
                        partitionTableId,
                        1,
                        DATA1_PARTITIONED_TABLE_DESCRIPTOR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());
        TableMetadata tableMetadata1 =
                new TableMetadata(nonePartitionTableInfo, Collections.emptyList());
        TableMetadata tableMetadata2 =
                new TableMetadata(partitionTableInfo, Collections.emptyList());

        PartitionMetadata partitionMetadata1 =
                new PartitionMetadata(
                        partitionTableId, partitionName1, partitionId1, Collections.emptyList());
        PartitionMetadata partitionMetadata2 =
                new PartitionMetadata(
                        partitionTableId, partitionName2, partitionId2, Collections.emptyList());
        replicaManager.maybeUpdateMetadataCache(
                2,
                buildClusterMetadata(
                        csServerInfo,
                        tsServerInfoList,
                        Arrays.asList(tableMetadata1, tableMetadata2),
                        Arrays.asList(partitionMetadata1, partitionMetadata2)));

        // register table to zk.
        zkClient.registerTable(
                nonePartitionTablePath,
                TableRegistration.newTable(nonePartitionTableId, DATA1_TABLE_DESCRIPTOR));
        zkClient.registerSchema(nonePartitionTablePath, DATA1_TABLE_DESCRIPTOR.getSchema());
        zkClient.registerTable(
                partitionTablePath,
                TableRegistration.newTable(partitionTableId, DATA1_PARTITIONED_TABLE_DESCRIPTOR));
        zkClient.registerSchema(partitionTablePath, DATA1_PARTITIONED_TABLE_DESCRIPTOR.getSchema());

        expectedCoordinatorServer.put(
                "INTERNAL", new ServerNode(0, "localhost", 1235, ServerType.COORDINATOR));
        expectedTablePathById.put(nonePartitionTableId, nonePartitionTablePath);
        expectedTablePathById.put(partitionTableId, partitionTablePath);

        expectedTableMetadataById.put(nonePartitionTablePath, tableMetadata1);
        expectedTableMetadataById.put(partitionTablePath, tableMetadata2);

        expectedPartitionNameById.put(partitionId1, partitionName1);
        expectedPartitionNameById.put(partitionId2, partitionName2);

        expectedPartitionMetadataById.put(physicalTablePath1, partitionMetadata1);
        expectedPartitionMetadataById.put(physicalTablePath2, partitionMetadata2);

        assertUpdateMetadataEquals(
                expectedCoordinatorServer,
                4,
                expectedTablePathById,
                expectedTableMetadataById,
                expectedPartitionNameById,
                expectedPartitionMetadataById);

        // 3. test drop one table.
        replicaManager.maybeUpdateMetadataCache(
                2,
                buildClusterMetadata(
                        csServerInfo,
                        tsServerInfoList,
                        Collections.singletonList(
                                new TableMetadata(
                                        TableInfo.of(
                                                nonePartitionTablePath,
                                                DELETED_TABLE_ID, // mark as deleted.
                                                1,
                                                DATA1_TABLE_DESCRIPTOR,
                                                System.currentTimeMillis(),
                                                System.currentTimeMillis()),
                                        Collections.emptyList())),
                        Collections.emptyList()));
        zkClient.deleteTable(nonePartitionTablePath);

        expectedTablePathById.put(nonePartitionTableId, null);
        expectedTableMetadataById.put(nonePartitionTablePath, null);
        assertUpdateMetadataEquals(
                expectedCoordinatorServer,
                4,
                expectedTablePathById,
                expectedTableMetadataById,
                expectedPartitionNameById,
                expectedPartitionMetadataById);

        // 4. test drop one partition.
        replicaManager.maybeUpdateMetadataCache(
                2,
                buildClusterMetadata(
                        csServerInfo,
                        tsServerInfoList,
                        Collections.emptyList(),
                        Collections.singletonList(
                                new PartitionMetadata(
                                        partitionTableId,
                                        partitionName1,
                                        DELETED_PARTITION_ID, // mark as deleted.
                                        Collections.emptyList()))));
        expectedPartitionNameById.put(partitionId1, null);
        expectedPartitionMetadataById.put(physicalTablePath1, null);
        assertUpdateMetadataEquals(
                expectedCoordinatorServer,
                4,
                expectedTablePathById,
                expectedTableMetadataById,
                expectedPartitionNameById,
                expectedPartitionMetadataById);

        // 5. check fenced coordinatorEpoch
        assertThatThrownBy(
                        () ->
                                replicaManager.maybeUpdateMetadataCache(
                                        1, new ClusterMetadata(csServerInfo, tsServerInfoList)))
                .isInstanceOf(InvalidCoordinatorException.class)
                .hasMessageContaining(
                        "invalid coordinator epoch 1 in updateMetadataCache request, "
                                + "The latest known coordinator epoch is 2");
    }

    private void assertReplicaEpochEquals(
            Replica replica, boolean isLeader, int leaderEpoch, int bucketEpoch) {
        assertThat(replica.isLeader()).isEqualTo(isLeader);
        assertThat(replica.getLeaderEpoch()).isEqualTo(leaderEpoch);
        assertThat(replica.getBucketEpoch()).isEqualTo(bucketEpoch);
    }

    private List<TableBucket> createTableBuckets(int nBuckets) {
        List<TableBucket> tableBuckets = new ArrayList<>();
        for (int i = 0; i < nBuckets; i++) {
            TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID_PK, i);
            tableBuckets.add(tableBucket);
        }
        return tableBuckets;
    }

    private void makeKvTableAsLeader(
            List<TableBucket> tableBuckets, TablePath tablePath, int leaderEpoch) {
        for (TableBucket tableBucket : tableBuckets) {
            makeKvTableAsLeader(tableBucket, tablePath, leaderEpoch, false);
        }
    }

    private void checkKvDataForBuckets(Map<TableBucket, List<KvRecord>> expectedKvRecordsPerBucket)
            throws Exception {
        for (TableBucket tableBucket : expectedKvRecordsPerBucket.keySet()) {
            List<KvRecord> kvRecords = expectedKvRecordsPerBucket.get(tableBucket);
            List<Tuple2<byte[], byte[]>> expectedKeyValues = getKeyValuePairs(kvRecords);
            for (Tuple2<byte[], byte[]> kv : expectedKeyValues) {
                verifyLookup(tableBucket, kv.f0, kv.f1);
            }
        }
    }

    private void putRecords(Map<TableBucket, KvRecordBatch> kvRecordBatchPerBucket)
            throws Exception {
        CompletableFuture<List<PutKvResultForBucket>> writeFuture = new CompletableFuture<>();
        // put kv record batch for every bucket
        replicaManager.putRecordsToKv(
                300000, -1, kvRecordBatchPerBucket, null, writeFuture::complete);
        // wait the write ack
        writeFuture.get();
    }

    private Map<TableBucket, FetchLogResultForBucket> fetchLog(List<TableBucket> tableBuckets)
            throws Exception {
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> fetchLogFuture =
                new CompletableFuture<>();
        Map<TableBucket, FetchReqInfo> fetchData = new HashMap<>();
        for (TableBucket tb : tableBuckets) {
            fetchData.put(tb, new FetchReqInfo(tb.getTableId(), 0L, 1024 * 1024));
        }
        replicaManager.fetchLogRecords(buildFetchParams(-1), fetchData, fetchLogFuture::complete);
        return fetchLogFuture.get();
    }

    private FetchParams buildFetchParams(int replicaId) {
        return buildFetchParams(replicaId, Integer.MAX_VALUE);
    }

    private FetchParams buildFetchParams(int replicaId, int fetchMaxSize) {
        return new FetchParams(replicaId, fetchMaxSize);
    }

    private void verifyLookup(TableBucket tb, byte[] keyBytes, @Nullable byte[] expectValues)
            throws Exception {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        replicaManager.lookup(tb, keyBytes, future::complete);
        byte[] lookupValues = future.get();
        assertThat(lookupValues).isEqualTo(expectValues);
    }

    private void verifyPrefixLookup(
            TableBucket tb, List<byte[]> prefixKeyBytes, List<List<byte[]>> expectedValues)
            throws Exception {
        Map<TableBucket, List<byte[]>> entriesPerBucket = new HashMap<>();
        entriesPerBucket.put(tb, prefixKeyBytes);

        CompletableFuture<Map<TableBucket, PrefixLookupResultForBucket>> future =
                new CompletableFuture<>();
        replicaManager.prefixLookups(entriesPerBucket, future::complete);
        Map<TableBucket, PrefixLookupResultForBucket> prefixResult = future.get();
        assertThat(prefixResult.size()).isEqualTo(1);
        PrefixLookupResultForBucket resultForBucket = prefixResult.get(tb);
        assertThat(resultForBucket).isNotNull();
        List<List<byte[]>> prefixLookupValues = resultForBucket.prefixLookupValues();
        assertThat(prefixLookupValues.size()).isEqualTo(expectedValues.size());
        for (int i = 0; i < expectedValues.size(); i++) {
            List<byte[]> prefixValueList = prefixLookupValues.get(i);
            List<byte[]> expectedValueList = expectedValues.get(i);
            assertThat(prefixValueList.size()).isEqualTo(expectedValueList.size());
            for (int j = 0; j < expectedValueList.size(); j++) {
                assertThat(prefixValueList.get(j)).isEqualTo(expectedValueList.get(j));
            }
        }
    }

    private ClusterMetadata buildClusterMetadata(
            @Nullable ServerInfo coordinatorServer,
            Set<ServerInfo> aliveTabletServers,
            List<TableMetadata> tableMetadataList,
            List<PartitionMetadata> partitionMetadataList) {
        return new ClusterMetadata(
                coordinatorServer, aliveTabletServers, tableMetadataList, partitionMetadataList);
    }

    private void assertUpdateMetadataEquals(
            Map<String, ServerNode> expectedCoordinatorServer,
            int expectedTabletServerSize,
            Map<Long, TablePath> expectedTablePathById,
            Map<TablePath, TableMetadata> expectedTableMetadataById,
            Map<Long, String> expectedPartitionNameById,
            Map<PhysicalTablePath, PartitionMetadata> expectedPartitionMetadataById) {
        expectedCoordinatorServer.forEach(
                (k, v) -> {
                    if (v != null) {
                        assertThat(serverMetadataCache.getCoordinatorServer(k)).isEqualTo(v);
                    } else {
                        assertThat(serverMetadataCache.getCoordinatorServer(k)).isNull();
                    }
                });
        assertThat(serverMetadataCache.getAliveTabletServerInfos().size())
                .isEqualTo(expectedTabletServerSize);
        expectedTablePathById.forEach(
                (k, v) -> {
                    if (v != null) {
                        assertThat(serverMetadataCache.getTablePath(k).get()).isEqualTo(v);
                    } else {
                        assertThat(serverMetadataCache.getTablePath(k)).isEmpty();
                    }
                });

        expectedTableMetadataById.forEach(
                (k, v) -> {
                    if (v != null) {
                        assertTableMetadata(serverMetadataCache.getTableMetadata(k)).isEqualTo(v);
                    } else {
                        assertThatThrownBy(() -> serverMetadataCache.getTableMetadata(k))
                                .isInstanceOf(TableNotExistException.class)
                                .hasMessageContaining("Table '" + k + "' does not exist.");
                    }
                });

        expectedPartitionNameById.forEach(
                (k, v) -> {
                    if (v != null) {
                        assertThat(
                                        serverMetadataCache
                                                .getPhysicalTablePath(k)
                                                .get()
                                                .getPartitionName())
                                .isEqualTo(v);
                    } else {
                        assertThat(serverMetadataCache.getPhysicalTablePath(k)).isEmpty();
                    }
                });

        expectedPartitionMetadataById.forEach(
                (k, v) -> {
                    if (v != null) {
                        assertPartitionMetadata(serverMetadataCache.getPartitionMetadata(k))
                                .isEqualTo(v);
                    } else {
                        assertThatThrownBy(() -> serverMetadataCache.getPartitionMetadata(k))
                                .isInstanceOf(PartitionNotExistException.class)
                                .hasMessageContaining(
                                        "Table partition '" + k + "' does not exist.");
                    }
                });
    }
}

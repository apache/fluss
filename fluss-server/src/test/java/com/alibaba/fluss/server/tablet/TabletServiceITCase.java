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

package com.alibaba.fluss.server.tablet;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.exception.InvalidRequiredAcksException;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.DefaultKvRecordBatch;
import com.alibaba.fluss.record.DefaultValueRecordBatch;
import com.alibaba.fluss.row.encode.CompactedKeyEncoder;
import com.alibaba.fluss.row.encode.ValueEncoder;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.FetchLogResponse;
import com.alibaba.fluss.rpc.messages.InitWriterRequest;
import com.alibaba.fluss.rpc.messages.InitWriterResponse;
import com.alibaba.fluss.rpc.messages.ListOffsetsResponse;
import com.alibaba.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import com.alibaba.fluss.rpc.messages.NotifyLeaderAndIsrResponse;
import com.alibaba.fluss.rpc.messages.PbFetchLogRespForBucket;
import com.alibaba.fluss.rpc.messages.PbFetchLogRespForTable;
import com.alibaba.fluss.rpc.messages.PbListOffsetsRespForBucket;
import com.alibaba.fluss.rpc.messages.PbLookupRespForBucket;
import com.alibaba.fluss.rpc.messages.PbNotifyLeaderAndIsrReqForBucket;
import com.alibaba.fluss.rpc.messages.PbPrefixLookupRespForBucket;
import com.alibaba.fluss.rpc.messages.PbPutKvRespForBucket;
import com.alibaba.fluss.rpc.messages.ProduceLogResponse;
import com.alibaba.fluss.rpc.messages.PutKvResponse;
import com.alibaba.fluss.rpc.protocol.Errors;
import com.alibaba.fluss.server.entity.NotifyLeaderAndIsrData;
import com.alibaba.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import com.alibaba.fluss.server.log.ListOffsetsParam;
import com.alibaba.fluss.server.metadata.ServerInfo;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.server.utils.ServerRpcMessageUtils;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;
import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.alibaba.fluss.record.TestData.ANOTHER_DATA1;
import static com.alibaba.fluss.record.TestData.DATA1;
import static com.alibaba.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH;
import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.record.TestData.DATA1_SCHEMA;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR_PK;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static com.alibaba.fluss.record.TestData.DATA_1_WITH_KEY_AND_VALUE;
import static com.alibaba.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static com.alibaba.fluss.server.testutils.KvTestUtils.assertLookupResponse;
import static com.alibaba.fluss.server.testutils.KvTestUtils.assertPrefixLookupResponse;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.assertFetchLogResponse;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.assertLimitScanResponse;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.assertProduceLogResponse;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newFetchLogRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newLimitScanRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newListOffsetsRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newLookupRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newPrefixLookupRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getNotifyLeaderAndIsrResponseData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeNotifyBucketLeaderAndIsr;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeUpdateMetadataRequest;
import static com.alibaba.fluss.testutils.DataTestUtils.compactedRow;
import static com.alibaba.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static com.alibaba.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for {@link TabletService}. */
public class TabletServiceITCase {
    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(3).build();

    @Test
    void testProduceLog() throws Exception {
        long tableId =
                createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR);
        TableBucket tb = new TableBucket(tableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        // 1. send first batch.
        assertProduceLogResponse(
                leaderGateWay
                        .produceLog(
                                newProduceLogRequest(
                                        tableId, 0, 1, genMemoryLogRecordsByObject(DATA1)))
                        .get(),
                0,
                0L);

        // 2. send second batch.
        assertProduceLogResponse(
                leaderGateWay
                        .produceLog(
                                newProduceLogRequest(
                                        tableId, 0, 1, genMemoryLogRecordsByObject(DATA1)))
                        .get(),
                0,
                10L);

        // 3. test produce with error acks to check error produce record.
        assertThatThrownBy(
                        () ->
                                leaderGateWay
                                        .produceLog(
                                                newProduceLogRequest(
                                                        tableId,
                                                        0,
                                                        100,
                                                        genMemoryLogRecordsByObject(DATA1)))
                                        .get())
                .cause()
                .isInstanceOf(InvalidRequiredAcksException.class)
                .hasMessageContaining("Invalid required acks");
    }

    @Test
    @Disabled("TODO: add back in https://github.com/alibaba/fluss/issues/771")
    void testProduceLogResponseReturnInOrder() throws Exception {
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        DATA1_TABLE_PATH,
                        DATA1_TABLE_DESCRIPTOR.withReplicationFactor(3));
        TableBucket tb = new TableBucket(tableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        Queue<Integer> responseOrder = new ConcurrentLinkedQueue<>();
        Random random = new Random();
        for (int i = 0; i < 1000; i++) {
            boolean needAck = random.nextBoolean();
            CompletableFuture<ProduceLogResponse> produceLogFuture =
                    leaderGateWay.produceLog(
                            newProduceLogRequest(
                                    tableId,
                                    0,
                                    needAck ? -1 : 0, // 0 means return immediately.
                                    genMemoryLogRecordsByObject(DATA1)));
            final int number = i;
            produceLogFuture.whenComplete((r, e) -> responseOrder.add(number));
        }

        retry(Duration.ofMinutes(1), () -> assertThat(responseOrder.size()).isEqualTo(1000));
        int previousValue = -1;
        while (!responseOrder.isEmpty()) {
            int currentValue = responseOrder.poll();
            assertThat(currentValue).isGreaterThan(previousValue);
            previousValue = currentValue;
        }
    }

    @Test
    void testFetchLog() throws Exception {
        long tableId =
                createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR);
        TableBucket tb = new TableBucket(tableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        // produce one batch to this bucket.
        assertProduceLogResponse(
                leaderGateWay
                        .produceLog(
                                newProduceLogRequest(
                                        tableId, 0, 1, genMemoryLogRecordsByObject(DATA1)))
                        .get(),
                0,
                0L);

        // fetch from this bucket from offset 0, return data1.
        assertFetchLogResponse(
                leaderGateWay.fetchLog(newFetchLogRequest(-1, tableId, 0, 0L)).get(),
                tableId,
                0,
                10L,
                DATA1);

        // fetch from this bucket from offset 3, return data1.
        assertFetchLogResponse(
                leaderGateWay.fetchLog(newFetchLogRequest(-1, tableId, 0, 3L)).get(),
                tableId,
                0,
                10L,
                DATA1);

        // append new batch.
        assertProduceLogResponse(
                leaderGateWay
                        .produceLog(
                                newProduceLogRequest(
                                        tableId, 0, 1, genMemoryLogRecordsByObject(ANOTHER_DATA1)))
                        .get(),
                0,
                10L);

        // fetch this bucket from offset 10, return data2.
        assertFetchLogResponse(
                leaderGateWay.fetchLog(newFetchLogRequest(-1, tableId, 0, 10L)).get(),
                tableId,
                0,
                20L,
                ANOTHER_DATA1);

        // fetch this bucket from offset 100, return error code.
        assertFetchLogResponse(
                leaderGateWay.fetchLog(newFetchLogRequest(-1, tableId, 0, 100L)).get(),
                tableId,
                0,
                Errors.LOG_OFFSET_OUT_OF_RANGE_EXCEPTION.code(),
                "Received request for offset");

        // fetch only first field
        List<Object[]> totalData = new ArrayList<>(DATA1);
        totalData.addAll(ANOTHER_DATA1);
        List<Object[]> expected1 = new ArrayList<>();
        for (int i = 0; i < ANOTHER_DATA1.size(); i++) {
            expected1.add(new Object[] {totalData.get(i)[0]});
        }
        assertFetchLogResponse(
                leaderGateWay
                        .fetchLog(newFetchLogRequest(-1, tableId, 0, 10L, new int[] {0}))
                        .get(),
                DATA1_ROW_TYPE.project(new int[] {0}),
                tableId,
                0,
                20L,
                expected1);

        // fetch only second field, results contains from offset 10 ~ 20, even fetchOffset=15L
        List<Object[]> expected2 = new ArrayList<>();
        for (int i = 10; i < totalData.size(); i++) {
            expected2.add(new Object[] {totalData.get(i)[1]});
        }
        assertFetchLogResponse(
                leaderGateWay
                        .fetchLog(newFetchLogRequest(-1, tableId, 0, 15L, new int[] {1}))
                        .get(),
                DATA1_ROW_TYPE.project(new int[] {1}),
                tableId,
                0,
                20L,
                expected2);

        assertFetchLogResponse(
                leaderGateWay
                        .fetchLog(newFetchLogRequest(-1, tableId, 0, 10L, new int[] {2, 3}))
                        .get(),
                tableId,
                0,
                Errors.INVALID_COLUMN_PROJECTION.code(),
                "Projected fields [2, 3] is out of bound for schema with 2 fields.");
    }

    @Test
    @Disabled("TODO: add back in https://github.com/alibaba/fluss/issues/777")
    void testFetchLogWithMinFetchSizeAndTimeout() throws Exception {
        long tableId =
                createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR);
        TableBucket tb = new TableBucket(tableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        // first send an empty fetch request, without min fetch size, the request will return
        // immediately.
        FetchLogResponse fetchLogResponse =
                leaderGateWay.fetchLog(newFetchLogRequest(-1, tableId, 0, 0L)).get();
        assertThat(fetchLogResponse.getTablesRespsCount()).isEqualTo(1);
        PbFetchLogRespForTable fetchLogRespForTable = fetchLogResponse.getTablesRespsList().get(0);
        assertThat(fetchLogRespForTable.getTableId()).isEqualTo(tableId);
        assertThat(fetchLogRespForTable.getBucketsRespsCount()).isEqualTo(1);
        PbFetchLogRespForBucket protoFetchedBucket =
                fetchLogRespForTable.getBucketsRespsList().get(0);
        assertThat(protoFetchedBucket.getHighWatermark()).isEqualTo(0L);
        assertThat(protoFetchedBucket.getRecordsSize()).isEqualTo(0);

        // second send a fetch request with minFetchSize and small maxFetchWaitMs, the request will
        // also return immediately.
        fetchLogResponse =
                leaderGateWay
                        .fetchLog(
                                newFetchLogRequest(
                                        -1, tableId, 0, 0L, null, 1, Integer.MAX_VALUE, 100))
                        .get();
        assertThat(fetchLogResponse.getTablesRespsCount()).isEqualTo(1);
        fetchLogRespForTable = fetchLogResponse.getTablesRespsList().get(0);
        assertThat(fetchLogRespForTable.getTableId()).isEqualTo(tableId);
        assertThat(fetchLogRespForTable.getBucketsRespsCount()).isEqualTo(1);
        protoFetchedBucket = fetchLogRespForTable.getBucketsRespsList().get(0);
        assertThat(protoFetchedBucket.getHighWatermark()).isEqualTo(0L);
        assertThat(protoFetchedBucket.getRecordsSize()).isEqualTo(0);

        // third send a fetch request with minFetchSize and much bigger maxFetchWaitMs, the request
        // will return after we send a produce log request to this bucket.
        CompletableFuture<FetchLogResponse> fetchResultFuture =
                leaderGateWay.fetchLog(
                        newFetchLogRequest(
                                -1,
                                tableId,
                                0,
                                0L,
                                null,
                                1,
                                Integer.MAX_VALUE,
                                (int) Duration.ofMinutes(5).toMillis()));

        // send a produce log request to trigger delay fetch log finish.
        assertProduceLogResponse(
                leaderGateWay
                        .produceLog(
                                newProduceLogRequest(
                                        tableId, 0, -1, genMemoryLogRecordsByObject(DATA1)))
                        .get(),
                0,
                0L);
        // the delay fetch will be completed.
        assertFetchLogResponse(fetchResultFuture.get(), tableId, 0, 10L, DATA1);

        // return immediately.
        assertFetchLogResponse(
                leaderGateWay
                        .fetchLog(
                                newFetchLogRequest(
                                        -1,
                                        tableId,
                                        0,
                                        0L,
                                        null,
                                        1,
                                        Integer.MAX_VALUE,
                                        (int) Duration.ofMinutes(5).toMillis()))
                        .get(),
                tableId,
                0,
                10L,
                DATA1);
    }

    @Test
    void testInvalidFetchLog() throws Exception {
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        TablePath.of("test_db_1", "test_indexed_table_1"),
                        TableDescriptor.builder()
                                .schema(DATA1_SCHEMA)
                                .logFormat(LogFormat.INDEXED)
                                .distributedBy(3)
                                .build());
        TableBucket tb = new TableBucket(tableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);
        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        assertFetchLogResponse(
                leaderGateWay.fetchLog(newFetchLogRequest(-1, tableId, 0, 0L, new int[] {1})).get(),
                tableId,
                0,
                Errors.INVALID_COLUMN_PROJECTION.code(),
                "Column projection is only supported for ARROW format, "
                        + "but the table test_db_1.test_indexed_table_1 is INDEXED format.");
    }

    @Test
    void testPutKv() throws Exception {
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH_PK, DATA1_TABLE_DESCRIPTOR_PK);
        TableBucket tb = new TableBucket(tableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        // 1. send one batch kv.
        assertPutKvResponse(
                leaderGateWay
                        .putKv(
                                newPutKvRequest(
                                        tableId, 0, 1, genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE)))
                        .get());

        // 2. test put with error acks to check error produce record.
        assertThatThrownBy(
                        () ->
                                leaderGateWay
                                        .putKv(
                                                newPutKvRequest(
                                                        tableId,
                                                        0,
                                                        100,
                                                        genKvRecordBatch(
                                                                DATA_1_WITH_KEY_AND_VALUE)))
                                        .get())
                .cause()
                .isInstanceOf(InvalidRequiredAcksException.class)
                .hasMessageContaining("Invalid required acks");
    }

    @Test
    void testLookup() throws Exception {
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH_PK, DATA1_TABLE_DESCRIPTOR_PK);
        TableBucket tb = new TableBucket(tableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        // first lookup without in table, key = 1.
        Object[] key1 = DATA_1_WITH_KEY_AND_VALUE.get(0).f0;
        CompactedKeyEncoder keyEncoder = new CompactedKeyEncoder(DATA1_ROW_TYPE, new int[] {0});
        byte[] key1Bytes = keyEncoder.encodeKey(row(key1));
        assertLookupResponse(
                leaderGateWay.lookup(newLookupRequest(tableId, 0, key1Bytes)).get(), null);

        // send one batch kv.
        assertPutKvResponse(
                leaderGateWay
                        .putKv(
                                newPutKvRequest(
                                        tableId, 0, 1, genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE)))
                        .get());

        // second lookup in table, key = 1, value = 1, "a1".
        Object[] value1 = DATA_1_WITH_KEY_AND_VALUE.get(3).f1;
        byte[] value1Bytes =
                ValueEncoder.encodeValue(DEFAULT_SCHEMA_ID, compactedRow(DATA1_ROW_TYPE, value1));
        assertLookupResponse(
                leaderGateWay.lookup(newLookupRequest(tableId, 0, key1Bytes)).get(), value1Bytes);

        // key = 3 is deleted, need return null.
        Object[] key3 = DATA_1_WITH_KEY_AND_VALUE.get(2).f0;
        byte[] key3Bytes = keyEncoder.encodeKey(row(key3));
        assertLookupResponse(
                leaderGateWay.lookup(newLookupRequest(tableId, 0, key3Bytes)).get(), null);

        // Lookup from an unknown table-bucket.
        PbLookupRespForBucket pbLookupRespForBucket =
                leaderGateWay
                        .lookup(newLookupRequest(10005L, 6, key3Bytes))
                        .get()
                        .getBucketsRespAt(0);

        verifyLookupBucketError(
                pbLookupRespForBucket,
                Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION,
                "Unknown table or bucket: TableBucket{tableId=10005, bucket=6}");

        // Lookup from a non-pk table.
        long logTableId =
                createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR);
        TableBucket logTableBucket = new TableBucket(logTableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(logTableBucket);

        int logLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(logTableBucket);
        TabletServerGateway logLeaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(logLeader);
        pbLookupRespForBucket =
                logLeaderGateWay
                        .lookup(newLookupRequest(logTableId, 0, key3Bytes))
                        .get()
                        .getBucketsRespAt(0);
        verifyLookupBucketError(
                pbLookupRespForBucket,
                Errors.NON_PRIMARY_KEY_TABLE_EXCEPTION,
                "the primary key table not exists for TableBucket");
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
        RowType primaryKeyType =
                DataTypes.ROW(
                        new DataField("a", DataTypes.INT()),
                        new DataField("b", DataTypes.STRING()),
                        new DataField("c", DataTypes.BIGINT()));

        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "a", "b").build();
        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, tablePath, descriptor);
        TableBucket tb = new TableBucket(tableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        // first prefix lookup without in table, prefix key = (1, "a").
        Object[] prefixKey1 = new Object[] {1, "a"};
        CompactedKeyEncoder keyEncoder = new CompactedKeyEncoder(rowType, new int[] {0, 1});
        byte[] prefixKey1Bytes = keyEncoder.encodeKey(row(prefixKey1));
        assertPrefixLookupResponse(
                leaderGateWay
                        .prefixLookup(
                                newPrefixLookupRequest(
                                        tableId, 0, Collections.singletonList(prefixKey1Bytes)))
                        .get(),
                Collections.singletonList(Collections.emptyList()));

        // send one batch kv.
        List<Tuple2<Object[], Object[]>> data1 =
                Arrays.asList(
                        Tuple2.of(new Object[] {1, "a", 1L}, new Object[] {1, "a", 1L, "value1"}),
                        Tuple2.of(new Object[] {1, "a", 2L}, new Object[] {1, "a", 2L, "value2"}),
                        Tuple2.of(new Object[] {1, "a", 3L}, new Object[] {1, "a", 3L, "value3"}),
                        Tuple2.of(new Object[] {2, "a", 4L}, new Object[] {2, "a", 4L, "value4"}));
        assertPutKvResponse(
                leaderGateWay
                        .putKv(
                                newPutKvRequest(
                                        tableId,
                                        0,
                                        1,
                                        genKvRecordBatch(primaryKeyType, rowType, data1)))
                        .get());

        // second prefix lookup in table, prefix key = (1, "a").
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
        assertPrefixLookupResponse(
                leaderGateWay
                        .prefixLookup(
                                newPrefixLookupRequest(
                                        tableId, 0, Collections.singletonList(prefixKey1Bytes)))
                        .get(),
                Collections.singletonList(key1ExpectedValues));

        // third prefix lookup in table for multi prefix keys, prefix key = (1, "a") and (2, "a").
        Object[] prefixKey2 = new Object[] {2, "a"};
        byte[] prefixKey2Bytes = keyEncoder.encodeKey(row(prefixKey2));
        List<byte[]> key2ExpectedValues =
                Collections.singletonList(
                        ValueEncoder.encodeValue(
                                DEFAULT_SCHEMA_ID,
                                compactedRow(rowType, new Object[] {2, "a", 4L, "value4"})));
        assertPrefixLookupResponse(
                leaderGateWay
                        .prefixLookup(
                                newPrefixLookupRequest(
                                        tableId,
                                        0,
                                        Arrays.asList(prefixKey1Bytes, prefixKey2Bytes)))
                        .get(),
                Arrays.asList(key1ExpectedValues, key2ExpectedValues));

        // Prefix lookup an unsupported prefixLookup table.
        long logTableId =
                createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR);
        tb = new TableBucket(logTableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);
        leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay2 =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        PbPrefixLookupRespForBucket pbPrefixLookupRespForBucket =
                leaderGateWay2
                        .prefixLookup(
                                newPrefixLookupRequest(
                                        logTableId, 0, Collections.singletonList(prefixKey1Bytes)))
                        .get()
                        .getBucketsRespAt(0);
        verifyPrefixLookupBucketError(
                pbPrefixLookupRespForBucket,
                Errors.NON_PRIMARY_KEY_TABLE_EXCEPTION,
                "Try to do prefix lookup on a non primary key table: " + DATA1_TABLE_PATH);
    }

    @Test
    void testLimitScanPrimaryKeyTable() throws Exception {
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH_PK, DATA1_TABLE_DESCRIPTOR_PK);
        TableBucket tb = new TableBucket(tableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        DefaultValueRecordBatch.Builder builder = DefaultValueRecordBatch.builder();

        // first limit scan from empty table.
        assertLimitScanResponse(
                leaderGateWay.limitScan(newLimitScanRequest(tableId, 0, 1)).get(), builder.build());

        // send one batch kv.
        DefaultKvRecordBatch kvRecordBatch =
                (DefaultKvRecordBatch) genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE);
        assertPutKvResponse(
                leaderGateWay.putKv(newPutKvRequest(tableId, 0, 1, kvRecordBatch)).get());
        builder.append(DEFAULT_SCHEMA_ID, compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a1"}));
        // second limit scan from table
        assertLimitScanResponse(
                leaderGateWay.limitScan(newLimitScanRequest(tableId, 0, 1)).get(), builder.build());
        builder.append(DEFAULT_SCHEMA_ID, compactedRow(DATA1_ROW_TYPE, new Object[] {2, "b1"}));
        assertLimitScanResponse(
                leaderGateWay.limitScan(newLimitScanRequest(tableId, 0, 3)).get(), builder.build());
    }

    @Test
    void testLimitScanLogTable() throws Exception {
        long logTableId =
                createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR);
        TableBucket logTableBucket = new TableBucket(logTableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(logTableBucket);
        int logLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(logTableBucket);
        TabletServerGateway logLeaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(logLeader);
        // send first batch.
        assertProduceLogResponse(
                logLeaderGateWay
                        .produceLog(
                                newProduceLogRequest(
                                        logTableId, 0, 1, genMemoryLogRecordsByObject(DATA1)))
                        .get(),
                0,
                0L);

        // append new batch.
        assertProduceLogResponse(
                logLeaderGateWay
                        .produceLog(
                                newProduceLogRequest(
                                        logTableId,
                                        0,
                                        1,
                                        genMemoryLogRecordsByObject(ANOTHER_DATA1)))
                        .get(),
                0,
                10L);

        // fetch only second field, results contains from offset 10 ~ 20, even fetchOffset=15L
        List<Object[]> expected2 = new ArrayList<>(ANOTHER_DATA1);

        // limit log table scan will get the latest limit number of data.
        assertLimitScanResponse(
                logLeaderGateWay.limitScan(newLimitScanRequest(logTableId, 0, 10)).get(),
                DATA1_ROW_TYPE,
                expected2);
    }

    @Test
    void testListOffsets() throws Exception {
        long tableId =
                createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR);
        TableBucket tb = new TableBucket(tableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        // produce one batch to this bucket.
        assertProduceLogResponse(
                leaderGateWay
                        .produceLog(
                                newProduceLogRequest(
                                        tableId, 0, 1, genMemoryLogRecordsByObject(DATA1)))
                        .get(),
                0,
                0L);

        // listOffset from client.
        assertListOffsetsResponse(
                leaderGateWay
                        .listOffsets(
                                newListOffsetsRequest(
                                        -1, ListOffsetsParam.LATEST_OFFSET_TYPE, tableId, 0))
                        .get(),
                10L,
                Errors.NONE.code(),
                null);

        // listOffset from tablet server where follower locate in.
        assertListOffsetsResponse(
                leaderGateWay
                        .listOffsets(
                                newListOffsetsRequest(
                                        1, ListOffsetsParam.LATEST_OFFSET_TYPE, tableId, 0))
                        .get(),
                10L,
                Errors.NONE.code(),
                null);

        // list an unknown table id.
        assertListOffsetsResponse(
                leaderGateWay
                        .listOffsets(
                                newListOffsetsRequest(
                                        1, ListOffsetsParam.LATEST_OFFSET_TYPE, 10005L, 6))
                        .get(),
                null,
                Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION.code(),
                "Unknown table or bucket: TableBucket{tableId=10005, bucket=6}");
    }

    @Test
    void testInitWriterId() throws Exception {
        TabletServerGateway tabletServerGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(0);
        for (int i = 0; i < 100; i++) {
            InitWriterResponse response =
                    tabletServerGateway.initWriter(new InitWriterRequest()).get();
            assertThat(response.getWriterId()).isEqualTo(i);
        }

        FLUSS_CLUSTER_EXTENSION.stopCoordinatorServer();
        // start again.
        FLUSS_CLUSTER_EXTENSION.startCoordinatorServer();

        for (int i = 100; i < 200; i++) {
            InitWriterResponse response =
                    tabletServerGateway.initWriter(new InitWriterRequest()).get();
            assertThat(response.getWriterId()).isEqualTo(i);
        }
    }

    @Test
    void testBecomeLeaderOrFollowerWithOneTabletServerOffline() throws Exception {
        // If one tabletServer offline, and the leader of specify tableBucket is on this
        // tabletServer, one bucket level error NotLeaderOrFollower exception will be thrown.
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        DATA1_TABLE_PATH,
                        TableDescriptor.builder()
                                .schema(DATA1_SCHEMA)
                                .distributedBy(3)
                                .property(ConfigOptions.TABLE_REPLICATION_FACTOR.key(), "3")
                                .build());
        TableBucket tb = new TableBucket(tableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);

        LeaderAndIsr originLeaderAndIsr = FLUSS_CLUSTER_EXTENSION.waitLeaderAndIsrReady(tb);
        int leader = originLeaderAndIsr.leader();
        int follower = getOneFollower(originLeaderAndIsr);
        TabletServerGateway followerGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(follower);

        // 1. first send one NotifyLeaderAndIsr request with same LeaderAndIsr to mock the
        // coordinator is offline and recovery to send NotifyLeaderAndIsr request with same
        // leader but leader epoch plus 1.
        NotifyLeaderAndIsrResponse notifyLeaderAndIsrResponse =
                followerGateway
                        .notifyLeaderAndIsr(
                                makeNotifyLeaderAndIsrRequest(
                                        DATA1_PHYSICAL_TABLE_PATH,
                                        tb,
                                        new LeaderAndIsr(
                                                leader,
                                                1,
                                                originLeaderAndIsr.isr(),
                                                originLeaderAndIsr.coordinatorEpoch(),
                                                originLeaderAndIsr.bucketEpoch())))
                        .get();
        List<NotifyLeaderAndIsrResultForBucket> result =
                getNotifyLeaderAndIsrResponseData(notifyLeaderAndIsrResponse);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getErrorCode()).isEqualTo(Errors.NONE.code());

        // 2. send one UpdateMetadata request to followerGateway to mock the tabletServer where the
        // leader resides is offline.
        ServerInfo coordinatorServerInfo = FLUSS_CLUSTER_EXTENSION.getCoordinatorServerInfo();
        Set<ServerInfo> newTabletServerInfos = new HashSet<>();
        FLUSS_CLUSTER_EXTENSION
                .getTabletServerInfos()
                .forEach(
                        serverNode -> {
                            if (serverNode.id() != leader) {
                                newTabletServerInfos.add(serverNode);
                            }
                        });
        followerGateway
                .updateMetadata(
                        makeUpdateMetadataRequest(
                                coordinatorServerInfo,
                                newTabletServerInfos,
                                Collections.emptyList(),
                                Collections.emptyList()))
                .get();

        // 3. send one NotifyLeaderAndIsr request again with same LeaderAndIsr to mock the
        // coordinator is offline and recovery again to send NotifyLeaderAndIsr request with
        // same leader but leader epoch plus 1. Shouldn't throw any exception
        notifyLeaderAndIsrResponse =
                followerGateway
                        .notifyLeaderAndIsr(
                                makeNotifyLeaderAndIsrRequest(
                                        DATA1_PHYSICAL_TABLE_PATH,
                                        tb,
                                        new LeaderAndIsr(
                                                leader,
                                                2,
                                                originLeaderAndIsr.isr(),
                                                originLeaderAndIsr.coordinatorEpoch(),
                                                originLeaderAndIsr.bucketEpoch())))
                        .get();
        result = getNotifyLeaderAndIsrResponseData(notifyLeaderAndIsrResponse);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getError().error()).isEqualTo(Errors.NONE);
    }

    private static void assertPutKvResponse(PutKvResponse putKvResponse) {
        assertThat(putKvResponse.getBucketsRespsCount()).isEqualTo(1);
        PbPutKvRespForBucket putKvRespForBucket = putKvResponse.getBucketsRespsList().get(0);
        assertThat(putKvRespForBucket.getBucketId()).isEqualTo(0);
    }

    private static void assertListOffsetsResponse(
            ListOffsetsResponse listOffsetsResponse,
            @Nullable Long offset,
            Integer errorCode,
            @Nullable String errorMessage) {
        assertThat(listOffsetsResponse.getBucketsRespsCount()).isEqualTo(1);
        PbListOffsetsRespForBucket respForBucket = listOffsetsResponse.getBucketsRespsList().get(0);
        if (respForBucket.hasErrorCode()) {
            assertThat(respForBucket.getErrorCode()).isEqualTo(errorCode);
            assertThat(respForBucket.getErrorMessage()).contains(errorMessage);
        } else {
            assertThat(respForBucket.getOffset()).isEqualTo(offset);
        }
    }

    private static void verifyLookupBucketError(
            PbLookupRespForBucket lookupRespForBucket,
            Errors expectedError,
            String expectErrMessage) {
        assertThat(lookupRespForBucket.hasErrorCode()).isTrue();
        assertThat(lookupRespForBucket.getErrorCode()).isEqualTo(expectedError.code());
        assertThat(lookupRespForBucket.getErrorMessage()).contains(expectErrMessage);
    }

    private static void verifyPrefixLookupBucketError(
            PbPrefixLookupRespForBucket prefixLookupRespForBucket,
            Errors expectedError,
            String expectErrMessage) {
        assertThat(prefixLookupRespForBucket.hasErrorCode()).isTrue();
        assertThat(prefixLookupRespForBucket.getErrorCode()).isEqualTo(expectedError.code());
        assertThat(prefixLookupRespForBucket.getErrorMessage()).contains(expectErrMessage);
    }

    private int getOneFollower(LeaderAndIsr leaderAndIsr) {
        int leader = leaderAndIsr.leader();
        for (int follower : leaderAndIsr.isr()) {
            if (follower != leader) {
                return follower;
            }
        }
        throw new FlussRuntimeException("No follower found");
    }

    private NotifyLeaderAndIsrRequest makeNotifyLeaderAndIsrRequest(
            PhysicalTablePath physicalTablePath,
            TableBucket tableBucket,
            LeaderAndIsr leaderAndIsr) {
        PbNotifyLeaderAndIsrReqForBucket reqForBucket =
                makeNotifyBucketLeaderAndIsr(
                        new NotifyLeaderAndIsrData(
                                physicalTablePath, tableBucket, leaderAndIsr.isr(), leaderAndIsr));
        return ServerRpcMessageUtils.makeNotifyLeaderAndIsrRequest(
                0, Collections.singletonList(reqForBucket));
    }
}

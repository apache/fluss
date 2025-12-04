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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.admin.FlussAdmin;
import org.apache.fluss.client.metadata.ClientSchemaGetter;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.metrics.TestingScannerMetricGroup;
import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbProduceLogRespForBucket;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA2;
import static org.apache.fluss.record.TestData.DATA2_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA2_SCHEMA;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LogFetcher}. */
public class LogFetcherTest extends ClientToServerITCaseBase {
    private LogFetcher logFetcher;
    private long tableId;
    private final int bucketId0 = 0;
    private final int bucketId1 = 1;
    private LogScannerStatus logScannerStatus;
    private FlussAdmin admin;
    private MetadataUpdater metadataUpdater;
    private ClientSchemaGetter clientSchemaGetter;

    // TODO covert this test to UT as kafka.

    @BeforeEach
    protected void setup() throws Exception {
        super.setup();

        // We create table data1NonPkTablePath previously.
        tableId = createTable(DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR, false);
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);

        RpcClient rpcClient = FLUSS_CLUSTER_EXTENSION.getRpcClient();
        metadataUpdater = new MetadataUpdater(clientConf, rpcClient);
        metadataUpdater.checkAndUpdateTableMetadata(Collections.singleton(DATA1_TABLE_PATH));

        Map<TableBucket, Long> scanBuckets = new HashMap<>();
        // add bucket 0 and bucket 1 to log scanner status.
        scanBuckets.put(new TableBucket(tableId, bucketId0), 0L);
        scanBuckets.put(new TableBucket(tableId, bucketId1), 0L);
        logScannerStatus = new LogScannerStatus();
        logScannerStatus.assignScanBuckets(scanBuckets);
        admin = new FlussAdmin(rpcClient, metadataUpdater);
        clientSchemaGetter =
                new ClientSchemaGetter(DATA1_TABLE_PATH, new SchemaInfo(DATA1_SCHEMA, 1), admin);
        logFetcher =
                new LogFetcher(
                        DATA1_TABLE_INFO,
                        null,
                        logScannerStatus,
                        clientConf,
                        metadataUpdater,
                        TestingScannerMetricGroup.newInstance(),
                        new RemoteFileDownloader(1),
                        clientSchemaGetter);
    }

    @Test
    void testFetchWithSchemaChange() throws Exception {
        // add one batch records to tb0.
        TableBucket tb0 = new TableBucket(tableId, bucketId0);
        addRecordsToBucket(tb0, genMemoryLogRecordsByObject(DATA1), 0L);

        // add new column(which equals to DATA2_ROW_TYPE)
        admin.alterTable(
                        DATA1_TABLE_PATH,
                        Collections.singletonList(
                                TableChange.addColumn(
                                        "c",
                                        DataTypes.STRING(),
                                        null,
                                        TableChange.ColumnPosition.last())),
                        false)
                .get();
        // add one batch records with new schema to tb0.
        addRecordsToBucket(
                tb0,
                genMemoryLogRecordsByObject(DATA2_ROW_TYPE, 2, CURRENT_LOG_MAGIC_VALUE, DATA2),
                10L);

        // Read data with old schema, thus DATA2 will be truncated as DATA1
        List<GenericRow> expectedRows =
                DATA1.stream().map(DataTestUtils::row).collect(Collectors.toList());
        expectedRows.addAll(DATA1.stream().map(DataTestUtils::row).collect(Collectors.toList()));
        logFetcher.sendFetches();
        // The fetcher is async to fetch data, so we need to wait the result write to the
        // logFetchBuffer.
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(logFetcher.hasAvailableFetches()).isTrue();
                    assertThat(logFetcher.getCompletedFetchesSize()).isEqualTo(2);
                });
        Map<TableBucket, List<ScanRecord>> records = logFetcher.collectFetch();
        assertThat(records.size()).isEqualTo(1);
        List<ScanRecord> scanRecords = records.get(tb0);
        assertThat(scanRecords.stream().map(ScanRecord::getRow).collect(Collectors.toList()))
                .isEqualTo(expectedRows);

        // read data with new schema, thus DATA2 will be appended with null value.
        expectedRows =
                DATA1.stream()
                        .map(row -> DataTestUtils.row(row[0], row[1], null))
                        .collect(Collectors.toList());
        expectedRows.addAll(DATA2.stream().map(DataTestUtils::row).collect(Collectors.toList()));
        logScannerStatus.assignScanBuckets(Collections.singletonMap(tb0, 0L));
        LogFetcher newSchemaLogFetcher =
                new LogFetcher(
                        new TableInfo(
                                DATA1_TABLE_INFO.getTablePath(),
                                tableId,
                                2,
                                DATA2_SCHEMA,
                                DATA1_TABLE_INFO.getBucketKeys(),
                                DATA1_TABLE_INFO.getPartitionKeys(),
                                DATA1_TABLE_INFO.getNumBuckets(),
                                DATA1_TABLE_INFO.getProperties(),
                                DATA1_TABLE_INFO.getCustomProperties(),
                                DATA1_TABLE_INFO.getComment().orElse(null),
                                DATA1_TABLE_INFO.getCreatedTime(),
                                DATA1_TABLE_INFO.getModifiedTime()),
                        null,
                        logScannerStatus,
                        clientConf,
                        metadataUpdater,
                        TestingScannerMetricGroup.newInstance(),
                        new RemoteFileDownloader(1),
                        clientSchemaGetter);
        newSchemaLogFetcher.sendFetches();
        // The fetcher is async to fetch data, so we need to wait the result write to the
        // logFetchBuffer.
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(newSchemaLogFetcher.hasAvailableFetches()).isTrue();
                    assertThat(newSchemaLogFetcher.getCompletedFetchesSize()).isEqualTo(2);
                });
        records = newSchemaLogFetcher.collectFetch();
        assertThat(records.size()).isEqualTo(1);
        assertThat(records.get(tb0)).hasSize(20);
        scanRecords = records.get(tb0);
        assertThat(scanRecords.stream().map(ScanRecord::getRow).collect(Collectors.toList()))
                .isEqualTo(expectedRows);
        newSchemaLogFetcher.close();
    }

    @Test
    void testFetch() throws Exception {
        // add one batch records to tb0.
        TableBucket tb0 = new TableBucket(tableId, bucketId0);
        addRecordsToBucket(tb0, genMemoryLogRecordsByObject(DATA1), 0L);

        // add one batch records to tb1.
        TableBucket tb1 = new TableBucket(tableId, bucketId1);
        addRecordsToBucket(tb1, genMemoryLogRecordsByObject(DATA1), 0L);

        assertThat(logFetcher.hasAvailableFetches()).isFalse();
        // collect fetch will be empty while no available fetch.
        assertThat(logFetcher.collectFetch()).isEmpty();

        // send fetcher to fetch data.
        logFetcher.sendFetches();
        // The fetcher is async to fetch data, so we need to wait the result write to the
        // logFetchBuffer.
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(logFetcher.hasAvailableFetches()).isTrue();
                    assertThat(logFetcher.getCompletedFetchesSize()).isEqualTo(2);
                });

        Map<TableBucket, List<ScanRecord>> records = logFetcher.collectFetch();
        assertThat(records.size()).isEqualTo(2);
        assertThat(records.get(tb0).size()).isEqualTo(10);
        assertThat(records.get(tb1).size()).isEqualTo(10);

        // after collect fetch, the fetcher is empty.
        assertThat(logFetcher.hasAvailableFetches()).isFalse();
        assertThat(logFetcher.getCompletedFetchesSize()).isEqualTo(0);
    }

    @Test
    void testFetchWhenDestinationIsNullInMetadata() throws Exception {
        TableBucket tb0 = new TableBucket(tableId, bucketId0);
        addRecordsToBucket(tb0, genMemoryLogRecordsByObject(DATA1), 0L);

        RpcClient rpcClient = FLUSS_CLUSTER_EXTENSION.getRpcClient();
        MetadataUpdater metadataUpdater = new MetadataUpdater(clientConf, rpcClient);
        metadataUpdater.checkAndUpdateTableMetadata(Collections.singleton(DATA1_TABLE_PATH));

        int leaderNode = metadataUpdater.leaderFor(DATA1_TABLE_PATH, tb0);

        // now, remove leader nodd ,so that fetch destination
        // server node is null
        Cluster oldCluster = metadataUpdater.getCluster();
        Map<Integer, ServerNode> aliveTabletServersById =
                new HashMap<>(oldCluster.getAliveTabletServers());
        aliveTabletServersById.remove(leaderNode);
        Cluster newCluster =
                new Cluster(
                        aliveTabletServersById,
                        oldCluster.getCoordinatorServer(),
                        oldCluster.getBucketLocationsByPath(),
                        oldCluster.getTableIdByPath(),
                        oldCluster.getPartitionIdByPath(),
                        oldCluster.getTableInfoByPath());
        metadataUpdater = new MetadataUpdater(rpcClient, newCluster);

        LogScannerStatus logScannerStatus = new LogScannerStatus();
        logScannerStatus.assignScanBuckets(Collections.singletonMap(tb0, 0L));

        ClientSchemaGetter clientSchemaGetter =
                new ClientSchemaGetter(
                        DATA1_TABLE_PATH,
                        new SchemaInfo(DATA1_SCHEMA, 1),
                        new FlussAdmin(rpcClient, metadataUpdater));

        LogFetcher logFetcher =
                new LogFetcher(
                        DATA1_TABLE_INFO,
                        null,
                        logScannerStatus,
                        clientConf,
                        metadataUpdater,
                        TestingScannerMetricGroup.newInstance(),
                        new RemoteFileDownloader(1),
                        clientSchemaGetter);

        // send fetches to fetch data, should have no available fetch.
        logFetcher.sendFetches();
        assertThat(logFetcher.hasAvailableFetches()).isFalse();

        // then fetches again, should have available fetch.
        // first send fetch is for update metadata
        logFetcher.sendFetches();
        // second send fetch will do real fetch data
        logFetcher.sendFetches();
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(logFetcher.hasAvailableFetches()).isTrue();
                    assertThat(logFetcher.getCompletedFetchesSize()).isEqualTo(1);
                });
        Map<TableBucket, List<ScanRecord>> records = logFetcher.collectFetch();
        assertThat(records.size()).isEqualTo(1);
        assertThat(records.get(tb0).size()).isEqualTo(10);
    }

    @Test
    void testFetchWithInvalidTableOrPartitions() throws Exception {
        MetadataUpdater metadataUpdater1 =
                new MetadataUpdater(clientConf, FLUSS_CLUSTER_EXTENSION.getRpcClient());
        ClientSchemaGetter clientSchemaGetter =
                new ClientSchemaGetter(
                        DATA1_TABLE_PATH,
                        new SchemaInfo(DATA1_SCHEMA, 1),
                        new FlussAdmin(FLUSS_CLUSTER_EXTENSION.getRpcClient(), metadataUpdater1));
        logFetcher =
                new LogFetcher(
                        DATA1_TABLE_INFO,
                        null,
                        logScannerStatus,
                        clientConf,
                        metadataUpdater1,
                        TestingScannerMetricGroup.newInstance(),
                        new RemoteFileDownloader(1),
                        clientSchemaGetter);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> future =
                executor.submit(
                        () -> {
                            // If this test blocked, please checking whether it was blocked with
                            // the same reason as https://github.com/apache/fluss/pull/1666
                            for (int i = 0; i < 1000; i++) {
                                logFetcher.sendFetches();
                                logFetcher.invalidTableOrPartitions(
                                        new LogFetcher.TableOrPartitions(
                                                Collections.singleton(tableId), null));
                            }
                        });

        future.get(30, TimeUnit.SECONDS);
        assertThat(future.isDone()).isTrue();
        executor.shutdownNow();
    }

    private void addRecordsToBucket(
            TableBucket tableBucket, MemoryLogRecords logRecords, long expectedBaseOffset)
            throws Exception {
        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tableBucket);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        assertProduceLogResponse(
                leaderGateWay
                        .produceLog(
                                newProduceLogRequest(
                                        tableBucket.getTableId(),
                                        tableBucket.getBucket(),
                                        -1, // need ack, so we can make sure every batch is acked.
                                        logRecords))
                        .get(),
                tableBucket.getBucket(),
                expectedBaseOffset);
    }

    private static void assertProduceLogResponse(
            ProduceLogResponse produceLogResponse, int bucketId, Long baseOffset) {
        assertThat(produceLogResponse.getBucketsRespsCount()).isEqualTo(1);
        PbProduceLogRespForBucket produceLogRespForBucket =
                produceLogResponse.getBucketsRespsList().get(0);
        assertThat(produceLogRespForBucket.getBucketId()).isEqualTo(bucketId);
        assertThat(produceLogRespForBucket.getBaseOffset()).isEqualTo(baseOffset);
    }
}

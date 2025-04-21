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

package com.alibaba.fluss.client.table.scanner.log;

import com.alibaba.fluss.client.admin.ClientToServerITCaseBase;
import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.client.metrics.TestingScannerMetricGroup;
import com.alibaba.fluss.client.table.scanner.RemoteFileDownloader;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.cluster.Cluster;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.PbProduceLogRespForBucket;
import com.alibaba.fluss.rpc.messages.ProduceLogResponse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.record.TestData.DATA1;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_INFO;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static com.alibaba.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LogFetcher}. */
public class LogFetcherTest extends ClientToServerITCaseBase {
    private LogFetcher logFetcher;
    private long tableId;
    private final int bucketId0 = 0;
    private final int bucketId1 = 1;

    // TODO covert this test to UT as kafka.

    @BeforeEach
    protected void setup() throws Exception {
        super.setup();

        // We create table data1NonPkTablePath previously.
        tableId = createTable(DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR, false);
        FLUSS_CLUSTER_EXTENSION.waitUtilTableReady(tableId);

        RpcClient rpcClient = FLUSS_CLUSTER_EXTENSION.getRpcClient();
        MetadataUpdater metadataUpdater = new MetadataUpdater(clientConf, rpcClient);
        metadataUpdater.checkAndUpdateTableMetadata(Collections.singleton(DATA1_TABLE_PATH));

        Map<TableBucket, Long> scanBuckets = new HashMap<>();
        // add bucket 0 and bucket 1 to log scanner status.
        scanBuckets.put(new TableBucket(tableId, bucketId0), 0L);
        scanBuckets.put(new TableBucket(tableId, bucketId1), 0L);
        LogScannerStatus logScannerStatus = new LogScannerStatus();
        logScannerStatus.assignScanBuckets(scanBuckets);
        TestingScannerMetricGroup scannerMetricGroup = TestingScannerMetricGroup.newInstance();
        logFetcher =
                new LogFetcher(
                        DATA1_TABLE_INFO,
                        null,
                        rpcClient,
                        logScannerStatus,
                        clientConf,
                        metadataUpdater,
                        scannerMetricGroup,
                        new RemoteFileDownloader(1));
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

        int leaderNode = metadataUpdater.leaderFor(tb0);

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

        LogFetcher logFetcher =
                new LogFetcher(
                        DATA1_TABLE_INFO,
                        null,
                        rpcClient,
                        logScannerStatus,
                        clientConf,
                        metadataUpdater,
                        TestingScannerMetricGroup.newInstance(),
                        new RemoteFileDownloader(1));

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

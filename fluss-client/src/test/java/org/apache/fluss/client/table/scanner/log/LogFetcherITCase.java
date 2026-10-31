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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.admin.FlussAdmin;
import org.apache.fluss.client.metadata.ClientSchemaGetter;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.metrics.TestingScannerMetricGroup;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.ArrowBatchData;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbProduceLogRespForBucket;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.fluss.metadata.LogFormat.ARROW;
import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA2;
import static org.apache.fluss.record.TestData.DATA2_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA2_SCHEMA;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT test for {@link LogFetcher}. */
public class LogFetcherITCase extends ClientToServerITCaseBase {
    private LogFetcher logFetcher;
    private long tableId;
    private TableInfo tableInfo;
    private final int bucketId0 = 0;
    private final int bucketId1 = 1;
    private LogScannerStatus logScannerStatus;
    private FlussAdmin admin;
    private MetadataUpdater metadataUpdater;
    private ClientSchemaGetter clientSchemaGetter;

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
                        "default-scanner",
                        logScannerStatus,
                        clientConf,
                        metadataUpdater,
                        TestingScannerMetricGroup.newInstance(),
                        new RemoteFileDownloader(1),
                        LogRecordReadContext.SchemaResolution.TARGET);
        tableInfo =
                TableInfo.of(
                        DATA1_TABLE_PATH,
                        tableId,
                        1,
                        DATA1_TABLE_DESCRIPTOR,
                        DEFAULT_REMOTE_DATA_DIR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());
        logFetcher.registerTable(new TableScanSpec(tableInfo, null, null), clientSchemaGetter);
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
        ScanRecords records = logFetcher.collectFetch();
        // Both polled buckets are exposed; tb1 was polled but produced no records.
        TableBucket tb1 = new TableBucket(tableId, bucketId1);
        assertThat(records.buckets()).containsExactlyInAnyOrder(tb0, tb1);
        assertThat(records.records(tb1)).isEmpty();
        List<ScanRecord> scanRecords = records.records(tb0);
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
                        "default-scanner",
                        logScannerStatus,
                        clientConf,
                        metadataUpdater,
                        TestingScannerMetricGroup.newInstance(),
                        new RemoteFileDownloader(1),
                        LogRecordReadContext.SchemaResolution.TARGET);
        newSchemaLogFetcher.registerTable(
                new TableScanSpec(
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
                                DATA1_TABLE_INFO.getRemoteDataDir(),
                                DATA1_TABLE_INFO.getComment().orElse(null),
                                DATA1_TABLE_INFO.getCreatedTime(),
                                DATA1_TABLE_INFO.getModifiedTime()),
                        null,
                        null),
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
        assertThat(records.buckets()).containsExactlyInAnyOrder(tb0, tb1);
        assertThat(records.records(tb1)).isEmpty();
        assertThat(records.records(tb0)).hasSize(20);
        scanRecords = records.records(tb0);
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

        ScanRecords records = logFetcher.collectFetch();
        assertThat(records.buckets().size()).isEqualTo(2);
        assertThat(records.records(tb0).size()).isEqualTo(10);
        assertThat(records.records(tb1).size()).isEqualTo(10);

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
                        Collections.emptyMap());
        metadataUpdater = new MetadataUpdater(rpcClient, clientConf, newCluster);

        LogScannerStatus logScannerStatus = new LogScannerStatus();
        logScannerStatus.assignScanBuckets(Collections.singletonMap(tb0, 0L));

        ClientSchemaGetter clientSchemaGetter =
                new ClientSchemaGetter(
                        DATA1_TABLE_PATH,
                        new SchemaInfo(DATA1_SCHEMA, 1),
                        new FlussAdmin(rpcClient, metadataUpdater));

        LogFetcher logFetcher =
                new LogFetcher(
                        DATA1_TABLE_PATH.toString(),
                        logScannerStatus,
                        clientConf,
                        metadataUpdater,
                        TestingScannerMetricGroup.newInstance(),
                        new RemoteFileDownloader(1),
                        LogRecordReadContext.SchemaResolution.TARGET);

        logFetcher.registerTable(new TableScanSpec(tableInfo, null, null), clientSchemaGetter);
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
        ScanRecords records = logFetcher.collectFetch();
        assertThat(records.buckets().size()).isEqualTo(1);
        assertThat(records.records(tb0).size()).isEqualTo(10);
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
                        DATA1_TABLE_PATH.toString(),
                        logScannerStatus,
                        clientConf,
                        metadataUpdater1,
                        TestingScannerMetricGroup.newInstance(),
                        new RemoteFileDownloader(1),
                        LogRecordReadContext.SchemaResolution.TARGET);
        logFetcher.registerTable(new TableScanSpec(tableInfo, null, null), clientSchemaGetter);

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

    @Test
    void testBoundedRowScanStopsInsideBatch() throws Exception {
        TableBucket bucket = new TableBucket(tableId, 0);
        addRecordsToBucket(bucket, genMemoryLogRecordsByObject(DATA1), 0L);
        try (Connection connection = ConnectionFactory.createConnection(clientConf);
                Table table = connection.getTable(DATA1_TABLE_PATH);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeBounded(0, 2L, 6L);
            List<Long> offsets = new ArrayList<>();
            int completionCount = 0;

            for (int i = 0; i < 10 && completionCount == 0; i++) {
                ScanRecords polled = scanner.poll(Duration.ofSeconds(2));
                for (ScanRecord record : polled.records(bucket)) {
                    offsets.add(record.logOffset());
                }
                if (polled.finishedBuckets().contains(bucket)) {
                    completionCount++;
                    assertThat(polled.consumedUpToOffset(bucket)).isEqualTo(6L);
                }
            }

            assertThat(offsets).containsExactly(2L, 3L, 4L, 5L);
            assertThat(completionCount).isEqualTo(1);

            ScanRecords afterCompletion = scanner.poll(Duration.ZERO);
            assertThat(afterCompletion.records(bucket)).isEmpty();
            assertThat(afterCompletion.finishedBuckets()).isEmpty();
        }
    }

    @Test
    void testBoundedArrowScanStopsInsideBatch() throws Exception {
        TableBucket bucket = new TableBucket(tableId, 0);
        MemoryLogRecords records =
                DataTestUtils.createRecordsWithoutBaseLogOffset(
                        DATA1_ROW_TYPE,
                        DEFAULT_SCHEMA_ID,
                        0,
                        0,
                        CURRENT_LOG_MAGIC_VALUE,
                        DATA1,
                        ARROW,
                        true);
        addRecordsToBucket(bucket, records, 0L);
        try (Connection connection = ConnectionFactory.createConnection(clientConf);
                Table table = connection.getTable(DATA1_TABLE_PATH);
                LogScannerImpl scanner = (LogScannerImpl) table.newScan().createLogScanner()) {
            scanner.subscribeBounded(0, 2L, 6L);
            List<Long> offsets = new ArrayList<>();

            int completionCount = 0;
            for (int i = 0; i < 10 && completionCount == 0; i++) {
                try (ArrowScanRecords polled = scanner.pollRecordBatch(Duration.ofSeconds(2))) {
                    for (ArrowBatchData batch : polled.records(bucket)) {
                        long baseOffset = batch.getBaseLogOffset();
                        for (int row = 0; row < batch.getRecordCount(); row++) {
                            offsets.add(baseOffset + row);
                        }
                    }
                    if (polled.finishedBuckets().contains(bucket)) {
                        completionCount++;
                        assertThat(polled.consumedUpToOffset(bucket)).isEqualTo(6L);
                    }
                }
            }

            assertThat(offsets).containsExactly(2L, 3L, 4L, 5L);
            assertThat(completionCount).isEqualTo(1);

            try (ArrowScanRecords afterCompletion = scanner.pollRecordBatch(Duration.ZERO)) {
                assertThat(afterCompletion.count()).isZero();
                assertThat(afterCompletion.finishedBuckets()).isEmpty();
            }
        }
    }

    @Test
    void testBoundedResubscribeAfterCompletion() throws Exception {
        TableBucket bucket = new TableBucket(tableId, 0);
        addRecordsToBucket(bucket, genMemoryLogRecordsByObject(DATA1), 0L);
        try (Connection connection = ConnectionFactory.createConnection(clientConf);
                Table table = connection.getTable(DATA1_TABLE_PATH);
                LogScanner scanner = table.newScan().createLogScanner()) {
            // An empty bounded range completes immediately.
            scanner.subscribeBounded(0, 10L, 10L);
            ScanRecords first = scanner.poll(Duration.ofSeconds(1));
            assertThat(first.count()).isZero();
            assertThat(first.records(bucket)).isEmpty();
            assertThat(first.finishedBuckets()).containsExactly(bucket);
            assertThat(first.hasProgress()).isTrue();
            // Completion must be reported only once.
            ScanRecords afterCompletion = scanner.poll(Duration.ZERO);
            assertThat(afterCompletion.count()).isZero();
            assertThat(afterCompletion.finishedBuckets()).isEmpty();
            assertThat(afterCompletion.hasProgress()).isFalse();
            // Replace the completed subscription with a new bounded range.
            scanner.subscribeBounded(0, 0L, 3L);
            List<Long> offsets = new ArrayList<>();
            int completionCount = 0;
            for (int i = 0; i < 5 && completionCount == 0; i++) {
                ScanRecords polled = scanner.poll(Duration.ofSeconds(1));
                for (ScanRecord record : polled.records(bucket)) {
                    offsets.add(record.logOffset());
                }
                if (polled.finishedBuckets().contains(bucket)) {
                    completionCount++;
                    assertThat(polled.consumedUpToOffset(bucket)).isEqualTo(3L);
                }
            }
            assertThat(offsets).containsExactly(0L, 1L, 2L);
            assertThat(completionCount).isEqualTo(1);
            // The new subscription must also report completion only once.
            ScanRecords afterResubscription = scanner.poll(Duration.ZERO);
            assertThat(afterResubscription.count()).isZero();
            assertThat(afterResubscription.records(bucket)).isEmpty();
            assertThat(afterResubscription.finishedBuckets()).isEmpty();
            assertThat(afterResubscription.hasProgress()).isFalse();
        }
    }

    @Test
    void testBoundedSubscribeRejectsInvalidArguments() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(clientConf);
                Table table = connection.getTable(DATA1_TABLE_PATH);
                LogScanner scanner = table.newScan().createLogScanner()) {
            // Negative starting offsets other than EARLIEST_OFFSET are invalid.
            assertThatThrownBy(() -> scanner.subscribeBounded(0, -1L, 10L))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> scanner.subscribeBounded(0, -3L, 10L))
                    .isInstanceOf(IllegalArgumentException.class);
            // Stopping offsets must be nonnegative.
            assertThatThrownBy(() -> scanner.subscribeBounded(0, 0L, -1L))
                    .isInstanceOf(IllegalArgumentException.class);
            // EARLIEST_OFFSET is a valid starting offset.
            scanner.subscribeBounded(0, LogScanner.EARLIEST_OFFSET, 0L);
        }
    }

    @Test
    void testBoundedSubscribeRejectsWrongPartitionOverload() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(clientConf);
                Table table = connection.getTable(DATA1_TABLE_PATH);
                LogScanner scanner = table.newScan().createLogScanner()) {

            assertThatThrownBy(() -> scanner.subscribeBounded(1L, 0, 0L, 10L))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("table partitioning");
        }
    }

    @Test
    void testBoundedEarliestCompatibilityWithZeroStartOffset() throws Exception {
        TableBucket bucket = new TableBucket(tableId, bucketId0);
        addRecordsToBucket(bucket, genMemoryLogRecordsByObject(DATA1), 0L);
        try (Connection connection = ConnectionFactory.createConnection(clientConf);
                Table table = connection.getTable(DATA1_TABLE_PATH);
                LogScanner scanner = table.newScan().createLogScanner()) {
            assertBoundedEarliestScan(scanner, bucket, 0L);
        }
    }

    @Test
    void testBoundedEarliestCompatibilityWithNonZeroStartOffset() throws Exception {
        TableBucket bucket = new TableBucket(tableId, bucketId0);
        long logStartOffset = 100L;
        resetLogStartOffset(bucket, logStartOffset);
        addRecordsToBucket(bucket, genMemoryLogRecordsByObject(DATA1), logStartOffset);
        try (Connection connection = ConnectionFactory.createConnection(clientConf);
                Table table = connection.getTable(DATA1_TABLE_PATH);
                LogScanner scanner = table.newScan().createLogScanner()) {
            assertBoundedEarliestScan(scanner, bucket, logStartOffset);
        }
    }

    @Test
    void testBoundedEarliestCompatibilityCompletesWhenStartExceedsStop() throws Exception {
        TableBucket bucket = new TableBucket(tableId, bucketId0);
        resetLogStartOffset(bucket, 100L);
        try (Connection connection = ConnectionFactory.createConnection(clientConf);
                Table table = connection.getTable(DATA1_TABLE_PATH);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeBounded(bucketId0, LogScanner.EARLIEST_OFFSET, 90L);
            ScanRecords first = scanner.poll(Duration.ofSeconds(2));
            assertThat(first.records(bucket)).isEmpty();
            assertThat(first.count()).isZero();
            assertThat(first.consumedUpToOffset(bucket)).isEqualTo(90L);
            assertThat(first.finishedBuckets()).containsExactly(bucket);
            ScanRecords second = scanner.poll(Duration.ZERO);
            assertThat(second.count()).isZero();
            assertThat(second.finishedBuckets()).isEmpty();
            assertThat(second.hasProgress()).isFalse();
        }
    }

    private void assertBoundedEarliestScan(
            LogScanner scanner, TableBucket bucket, long expectedStartOffset) {
        long stoppingOffset = expectedStartOffset + 3L;
        scanner.subscribeBounded(bucketId0, LogScanner.EARLIEST_OFFSET, stoppingOffset);
        List<Long> offsets = new ArrayList<>();
        int completionCount = 0;
        for (int i = 0; i < 10 && completionCount == 0; i++) {
            ScanRecords polled = scanner.poll(Duration.ofSeconds(2));
            for (ScanRecord record : polled.records(bucket)) {
                offsets.add(record.logOffset());
            }
            if (polled.finishedBuckets().contains(bucket)) {
                completionCount++;
                assertThat(polled.consumedUpToOffset(bucket)).isEqualTo(stoppingOffset);
            }
        }
        assertThat(offsets)
                .containsExactly(
                        expectedStartOffset, expectedStartOffset + 1L, expectedStartOffset + 2L);
        assertThat(completionCount).isEqualTo(1);
        // Completion must be delivered only once.
        ScanRecords afterCompletion = scanner.poll(Duration.ZERO);
        assertThat(afterCompletion.count()).isZero();
        assertThat(afterCompletion.records(bucket)).isEmpty();
        assertThat(afterCompletion.finishedBuckets()).isEmpty();
        assertThat(afterCompletion.hasProgress()).isFalse();
    }

    private void resetLogStartOffset(TableBucket bucket, long newStartOffset) {
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(bucket);
        for (org.apache.fluss.server.tablet.TabletServer tabletServer :
                FLUSS_CLUSTER_EXTENSION.getTabletServers()) {
            ReplicaManager replicaManager = tabletServer.getReplicaManager();
            ReplicaManager.HostedReplica hostedReplica = replicaManager.getReplica(bucket);
            if (hostedReplica instanceof ReplicaManager.OnlineReplica) {
                Replica replica = ((ReplicaManager.OnlineReplica) hostedReplica).getReplica();
                replica.truncateFullyAndStartAt(newStartOffset);

                assertThat(replica.getLogStartOffset()).isEqualTo(newStartOffset);
                assertThat(replica.getLocalLogEndOffset()).isEqualTo(newStartOffset);
            }
        }
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

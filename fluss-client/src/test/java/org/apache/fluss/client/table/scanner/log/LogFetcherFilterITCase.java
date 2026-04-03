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
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.metrics.TestingScannerMetricGroup;
import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.record.LogRecordBatchStatisticsTestUtils;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.record.TestData.TEST_SCHEMA_GETTER;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for {@link LogFetcher} with recordBatchFilter pushdown scenarios. */
public class LogFetcherFilterITCase extends ClientToServerITCaseBase {
    private LogFetcher logFetcher;
    private long tableId;
    private static final int BUCKET_ID_0 = 0;
    private static final int BUCKET_ID_1 = 1;

    // Use the same row type as DATA1 to avoid Arrow compatibility issues
    private static final RowType FILTER_TEST_ROW_TYPE = DATA1_ROW_TYPE;

    // Data that should match filter (a > 5) - using DATA1 compatible structure
    private static final List<Object[]> MATCHING_DATA =
            Arrays.asList(
                    new Object[] {6, "alice"},
                    new Object[] {7, "bob"},
                    new Object[] {8, "charlie"},
                    new Object[] {9, "david"},
                    new Object[] {10, "eve"});

    // Data that should NOT match filter (a <= 5)
    private static final List<Object[]> NON_MATCHING_DATA =
            Arrays.asList(
                    new Object[] {1, "anna"},
                    new Object[] {2, "brian"},
                    new Object[] {3, "cindy"},
                    new Object[] {4, "derek"},
                    new Object[] {5, "fiona"});

    @BeforeEach
    protected void setup() throws Exception {
        super.setup();

        // Create table for filter testing
        tableId = createTable(DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR, false);
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);

        RpcClient rpcClient = FLUSS_CLUSTER_EXTENSION.getRpcClient();
        MetadataUpdater metadataUpdater = new MetadataUpdater(clientConf, rpcClient);
        metadataUpdater.checkAndUpdateTableMetadata(Collections.singleton(DATA1_TABLE_PATH));

        Map<TableBucket, Long> scanBuckets = new HashMap<>();
        // Add bucket 0 and bucket 1 to log scanner status
        scanBuckets.put(new TableBucket(tableId, BUCKET_ID_0), 0L);
        scanBuckets.put(new TableBucket(tableId, BUCKET_ID_1), 0L);
        LogScannerStatus logScannerStatus = new LogScannerStatus();
        logScannerStatus.assignScanBuckets(scanBuckets);

        // Create predicate for filter testing: field 'a' > 5 (first field in DATA1 structure)
        PredicateBuilder builder = new PredicateBuilder(FILTER_TEST_ROW_TYPE);
        Predicate recordBatchFilter = builder.greaterThan(0, 5); // a > 5

        TestingScannerMetricGroup scannerMetricGroup = TestingScannerMetricGroup.newInstance();
        logFetcher =
                new LogFetcher(
                        DATA1_TABLE_INFO,
                        null, // projection
                        recordBatchFilter, // recordBatchFilter
                        logScannerStatus,
                        clientConf,
                        metadataUpdater,
                        scannerMetricGroup,
                        new RemoteFileDownloader(1),
                        TEST_SCHEMA_GETTER);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (logFetcher != null) {
            logFetcher.close();
        }
    }

    @Test
    void testFetchWithRecordBatchFilter() throws Exception {
        TableBucket tb0 = new TableBucket(tableId, BUCKET_ID_0);
        TableBucket tb1 = new TableBucket(tableId, BUCKET_ID_1);

        // Add matching data to bucket 0 - should be included in results
        MemoryLogRecords matchingRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        MATCHING_DATA, FILTER_TEST_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);
        addRecordsToBucket(tb0, matchingRecords);

        // Add non-matching data to bucket 1 - should be filtered out
        MemoryLogRecords nonMatchingRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        NON_MATCHING_DATA, FILTER_TEST_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);
        addRecordsToBucket(tb1, nonMatchingRecords);

        assertThat(logFetcher.hasAvailableFetches()).isFalse();

        // Send fetcher to fetch data
        logFetcher.sendFetches();

        // Wait for fetch results
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(logFetcher.hasAvailableFetches()).isTrue();
                });

        Map<TableBucket, List<ScanRecord>> records = logFetcher.collectFetch();

        // Verify that only matching data is returned
        // tb0 should have records (matching data), tb1 should have no records (filtered out)
        assertThat(records.containsKey(tb0)).isTrue();
        assertThat(records.get(tb0)).isNotEmpty();
        assertThat(records.get(tb0).size()).isEqualTo(MATCHING_DATA.size());

        // Verify the content of returned records - all should match filter (a > 5)
        List<ScanRecord> tb0Records = records.get(tb0);
        List<Integer> aValues =
                tb0Records.stream().map(r -> r.getRow().getInt(0)).collect(Collectors.toList());
        assertThat(aValues).containsExactlyInAnyOrder(6, 7, 8, 9, 10);

        // Verify tb1 (non-matching data) does not surface user-visible records.
        // The server may return a filteredEndOffset response for the bucket, but collectFetch()
        // only materializes buckets with actual records.
        assertThat(records.getOrDefault(tb1, Collections.emptyList())).isEmpty();

        // After collect fetch, the fetcher should be empty
        assertThat(logFetcher.hasAvailableFetches()).isFalse();
        assertThat(logFetcher.getCompletedFetchesSize()).isEqualTo(0);
    }

    @Test
    void testBatchLevelFilterIncludesEntireBatchWhenStatisticsOverlap() throws Exception {
        TableBucket tb0 = new TableBucket(tableId, BUCKET_ID_0);

        // Create mixed data: some matching, some not matching (using DATA1 structure)
        List<Object[]> mixedData =
                Arrays.asList(
                        new Object[] {1, "low1"}, // Should be filtered (a <= 5)
                        new Object[] {6, "high1"}, // Should pass (a > 5)
                        new Object[] {3, "low2"}, // Should be filtered (a <= 5)
                        new Object[] {8, "high2"}, // Should pass (a > 5)
                        new Object[] {2, "low3"} // Should be filtered (a <= 5)
                        );

        MemoryLogRecords mixedRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        mixedData, FILTER_TEST_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);
        addRecordsToBucket(tb0, mixedRecords);

        logFetcher.sendFetches();

        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(logFetcher.hasAvailableFetches()).isTrue();
                });

        Map<TableBucket, List<ScanRecord>> records = logFetcher.collectFetch();

        // With recordBatchFilter at batch level, the behavior depends on the batch statistics
        // Since mixedData contains values 1,6,3,8,2 (min=1, max=8), and our filter is a > 5,
        // the entire batch should be included because max value (8) > 5
        assertThat(records.containsKey(tb0)).isTrue();
        assertThat(records.get(tb0)).isNotEmpty();

        List<ScanRecord> tb0Records = records.get(tb0);
        // All records in the batch should be returned (including those that don't match
        // individually)
        // because recordBatchFilter works at batch level based on statistics
        assertThat(tb0Records.size()).isEqualTo(mixedData.size());

        // Verify concrete values: batch-level filter includes the entire batch
        List<Integer> aValues =
                tb0Records.stream().map(r -> r.getRow().getInt(0)).collect(Collectors.toList());
        assertThat(aValues).containsExactly(1, 6, 3, 8, 2);
    }

    @Test
    void testFilterCompletelyRejectsNonMatchingBatch() throws Exception {
        TableBucket tb0 = new TableBucket(tableId, BUCKET_ID_0);

        // Create a batch where ALL records don't match filter (all a <= 5)
        List<Object[]> allNonMatchingData =
                Arrays.asList(
                        new Object[] {1, "reject1"},
                        new Object[] {2, "reject2"},
                        new Object[] {3, "reject3"},
                        new Object[] {4, "reject4"},
                        new Object[] {5, "reject5"});

        MemoryLogRecords nonMatchingRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        allNonMatchingData, FILTER_TEST_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);
        addRecordsToBucket(tb0, nonMatchingRecords);

        logFetcher.sendFetches();

        retry(
                Duration.ofMinutes(1),
                () -> {
                    // The fetch may complete even if all batches are filtered out
                    // depending on implementation
                    assertThat(logFetcher.hasAvailableFetches()).isTrue();
                });

        Map<TableBucket, List<ScanRecord>> records = logFetcher.collectFetch();

        // For a batch where max value = 5 and filter is a > 5, the entire batch should be
        // filtered out at the server side. collectFetch() may omit the bucket entirely because
        // there are no user-visible records to return.
        assertThat(records.getOrDefault(tb0, Collections.emptyList())).isEmpty();
    }

    @Test
    void testConsecutiveFetchesWithFilteredOffsetAdvancement() throws Exception {
        // Verify that when all batches in a bucket are filtered out, the client's
        // fetch offset advances via filteredEndOffset and doesn't get stuck polling.
        //
        // Write only non-matching data to bucket 0, matching data to bucket 1.
        // Bucket 0 should be fully filtered — the client must advance its offset
        // past the filtered data and not re-fetch the same offset indefinitely.
        TableBucket tb0 = new TableBucket(tableId, BUCKET_ID_0);
        TableBucket tb1 = new TableBucket(tableId, BUCKET_ID_1);

        // Bucket 0: non-matching data only (a <= 5, filtered by a > 5)
        addRecordsToBucket(
                tb0,
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        NON_MATCHING_DATA, FILTER_TEST_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID));

        // Bucket 1: matching data (a > 5)
        addRecordsToBucket(
                tb1,
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        MATCHING_DATA, FILTER_TEST_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID));

        // Use retry loop to handle the case where bucket 0's HW hasn't advanced yet
        // on the first fetch cycle. The filtered-empty response (with filteredEndOffset)
        // is only returned when the server has data up to HW to scan.
        retry(
                Duration.ofMinutes(1),
                () -> {
                    logFetcher.sendFetches();
                    retry(
                            Duration.ofSeconds(30),
                            () -> assertThat(logFetcher.hasAvailableFetches()).isTrue());
                    logFetcher.collectFetch();

                    // Verify that bucket 0's fetch offset has advanced past 0.
                    Long tb0Offset = logFetcher.getLogScannerStatus().getBucketOffset(tb0);
                    assertThat(tb0Offset)
                            .as("Bucket 0 fetch offset should advance past filtered data")
                            .isGreaterThan(0L);
                });

        // After offset advancement, fetcher should be clean
        assertThat(logFetcher.getCompletedFetchesSize()).isEqualTo(0);
    }

    private void addRecordsToBucket(TableBucket tableBucket, MemoryLogRecords logRecords)
            throws Exception {
        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tableBucket);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        // Simplify to avoid the missing ProduceLogResponse import issue
        leaderGateWay
                .produceLog(
                        newProduceLogRequest(
                                tableBucket.getTableId(),
                                tableBucket.getBucket(),
                                -1, // need ack
                                logRecords))
                .get();
    }
}

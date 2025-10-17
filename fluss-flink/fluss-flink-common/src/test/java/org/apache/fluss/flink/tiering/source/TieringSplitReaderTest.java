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

package org.apache.fluss.flink.tiering.source;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.TableWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.client.write.HashBucketAssigner;
import org.apache.fluss.flink.tiering.TestingLakeTieringFactory;
import org.apache.fluss.flink.tiering.TestingWriteResult;
import org.apache.fluss.flink.tiering.source.split.TieringLogSplit;
import org.apache.fluss.flink.tiering.source.split.TieringSnapshotSplit;
import org.apache.fluss.flink.tiering.source.split.TieringSplit;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** UT for {@link TieringSplitReader}. */
class TieringSplitReaderTest extends FlinkTestBase {

    @Test
    void testTieringTable() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "fluss_test_tiering_one_table");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        try (Connection connection =
                        ConnectionFactory.createConnection(
                                FLUSS_CLUSTER_EXTENSION.getClientConfig());
                TieringSplitReader<TestingWriteResult> tieringSplitReader =
                        createTieringReader(connection)) {
            // test empty splits
            SplitsAddition<TieringSplit> splitsAddition =
                    new SplitsAddition<>(
                            Arrays.asList(
                                    createLogSplit(tablePath, tableId, 0, EARLIEST_OFFSET, 0),
                                    createLogSplit(tablePath, tableId, 1, EARLIEST_OFFSET, 0),
                                    createLogSplit(tablePath, tableId, 2, EARLIEST_OFFSET, 0)));
            tieringSplitReader.handleSplitsChanges(splitsAddition);

            // one fetch to mark the splits as empty split
            tieringSplitReader.fetch();

            // fetch again to get the fetch result of the splits
            RecordsWithSplitIds<TableBucketWriteResult<TestingWriteResult>> fetchResult =
                    tieringSplitReader.fetch();
            for (int i = 0; i < 3; i++) {
                fetchResult.nextSplit();
                // should has result, but the writeResult should be null
                TableBucketWriteResult<TestingWriteResult> nextRecord =
                        fetchResult.nextRecordFromSplit();
                assertThat(nextRecord).isNotNull();
                assertThat(nextRecord.writeResult()).isNull();
            }
            assertThat(fetchResult.nextSplit()).isNull();
            assertThat(fetchResult.finishedSplits())
                    .isEqualTo(
                            splitsAddition.splits().stream()
                                    .map(SourceSplit::splitId)
                                    .collect(Collectors.toSet()));

            // test snapshot splits
            // firstly, write some records into the table
            Map<TableBucket, List<InternalRow>> firstRows = putRows(tableId, tablePath, 10);

            // check the expected records
            waitUntilSnapshot(tableId, 0);

            splitsAddition =
                    new SplitsAddition<>(
                            Arrays.asList(
                                    createSnapshotSplit(tablePath, tableId, 0, 0),
                                    createSnapshotSplit(tablePath, tableId, 1, 0),
                                    createSnapshotSplit(tablePath, tableId, 2, 0)));
            tieringSplitReader.handleSplitsChanges(splitsAddition);

            // fetch firstly to make snapshot splits as pending splits
            tieringSplitReader.fetch();
            for (int i = 0; i < 3; i++) {
                // one fetch to make tiering the snapshot records
                tieringSplitReader.fetch();
                // one fetch to make this snapshot split as finished
                fetchResult = tieringSplitReader.fetch();
                fetchResult.nextSplit();
                TableBucketWriteResult<TestingWriteResult> tableBucketWriteResult =
                        fetchResult.nextRecordFromSplit();
                assertThat(tableBucketWriteResult).isNotNull();
                TestingWriteResult testingWriteResult = tableBucketWriteResult.writeResult();
                assertThat(testingWriteResult).isNotNull();
                int writeResult = testingWriteResult.getWriteResult();
                TableBucket tableBucket = tableBucketWriteResult.tableBucket();
                // check write result
                assertThat(writeResult).isEqualTo(firstRows.get(tableBucket).size());
            }

            // test log splits, should produce -U, +U for each record
            Map<TableBucket, List<InternalRow>> secondRows = putRows(tableId, tablePath, 10);
            Map<TableBucket, Integer> expectedRowCount = new HashMap<>();
            Set<String> expectFinishTieringSplits = new HashSet<>();
            List<TieringSplit> logSplits = new ArrayList<>();
            for (int bucket = 0; bucket < 3; bucket++) {
                TableBucket tableBucket = new TableBucket(tableId, bucket);
                long startingOffset = firstRows.get(tableBucket).size();
                // -U, +U
                long stoppingOffset = startingOffset + secondRows.get(tableBucket).size() * 2L;
                expectedRowCount.put(tableBucket, secondRows.get(tableBucket).size() * 2);
                TieringLogSplit tieringLogSplit =
                        createLogSplit(tablePath, tableId, bucket, startingOffset, stoppingOffset);
                logSplits.add(tieringLogSplit);
                expectFinishTieringSplits.add(tieringLogSplit.splitId());
            }
            tieringSplitReader.handleSplitsChanges(new SplitsAddition<>(logSplits));
            verifyTieringRows(
                    tieringSplitReader, tableId, expectedRowCount, expectFinishTieringSplits);
        }
    }

    @Test
    void testTieringMixTables() throws Exception {
        TablePath tablePath0 = TablePath.of("fluss", "tiering_table0");
        long tableId0 = createTable(tablePath0, DEFAULT_PK_TABLE_DESCRIPTOR);
        TablePath tablePath1 = TablePath.of("fluss", "tiering_table1");
        long tableId1 = createTable(tablePath1, DEFAULT_PK_TABLE_DESCRIPTOR);

        try (Connection connection =
                        ConnectionFactory.createConnection(
                                FLUSS_CLUSTER_EXTENSION.getClientConfig());
                TieringSplitReader<TestingWriteResult> tieringSplitReader =
                        createTieringReader(connection)) {
            Map<TableBucket, List<InternalRow>> table0Rows = putRows(tableId0, tablePath0, 10);
            Map<TableBucket, List<InternalRow>> table1Rows = putRows(tableId1, tablePath1, 10);
            waitUntilSnapshot(tableId0, 0);
            waitUntilSnapshot(tableId1, 0);

            // first add snapshot split of bucket 0, bucket 1 of table id 0
            SplitsAddition<TieringSplit> splitsAddition =
                    new SplitsAddition<>(
                            Arrays.asList(
                                    createSnapshotSplit(tablePath0, tableId0, 0, 0),
                                    createSnapshotSplit(tablePath0, tableId0, 1, 0)));
            Set<String> table0Splits =
                    splitsAddition.splits().stream()
                            .map(TieringSplit::splitId)
                            .collect(Collectors.toSet());
            tieringSplitReader.handleSplitsChanges(splitsAddition);

            // then add bucket0, bucket1, bucket2 of table id 1
            splitsAddition =
                    new SplitsAddition<>(
                            Arrays.asList(
                                    createLogSplit(
                                            tablePath1,
                                            tableId1,
                                            0,
                                            EARLIEST_OFFSET,
                                            table1Rows.get(new TableBucket(tableId1, 0)).size()),
                                    createSnapshotSplit(tablePath1, tableId1, 1, 0),
                                    createLogSplit(
                                            tablePath1,
                                            tableId1,
                                            2,
                                            EARLIEST_OFFSET,
                                            table1Rows.get(new TableBucket(tableId1, 2)).size())));
            tieringSplitReader.handleSplitsChanges(splitsAddition);
            Set<String> table1Splits =
                    splitsAddition.splits().stream()
                            .map(TieringSplit::splitId)
                            .collect(Collectors.toSet());

            // add bucket2 of table id 0
            splitsAddition =
                    new SplitsAddition<>(
                            Collections.singletonList(
                                    createLogSplit(
                                            tablePath0,
                                            tableId0,
                                            2,
                                            EARLIEST_OFFSET,
                                            table0Rows.get(new TableBucket(tableId0, 2)).size())));
            table0Splits.addAll(
                    splitsAddition.splits().stream()
                            .map(TieringSplit::splitId)
                            .collect(Collectors.toSet()));
            tieringSplitReader.handleSplitsChanges(splitsAddition);

            // verify should first finish table0, and then finish table1
            LinkedHashMap<Long, Map<TableBucket, Integer>> expectTierRows = new LinkedHashMap<>();
            Map<TableBucket, Integer> tableId0ExpectTierRows =
                    table0Rows.entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()));
            expectTierRows.put(tableId0, tableId0ExpectTierRows);
            Map<TableBucket, Integer> tableId1ExpectTierRows =
                    table1Rows.entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()));
            expectTierRows.put(tableId1, tableId1ExpectTierRows);

            LinkedHashMap<Long, Set<String>> expectedFinishSplits = new LinkedHashMap<>();
            expectedFinishSplits.put(tableId0, table0Splits);
            expectedFinishSplits.put(tableId1, table1Splits);

            verifyTieringRows(tieringSplitReader, expectTierRows, expectedFinishSplits);

            // test tiering another log table2
            TablePath tablePath2 = TablePath.of("fluss", "tiering_table2");
            long tableId2 = createTable(tablePath2, DEFAULT_LOG_TABLE_DESCRIPTOR);
            Map<TableBucket, List<InternalRow>> table2Rows = putRows(tableId2, tablePath2, 10);
            splitsAddition =
                    new SplitsAddition<>(
                            Arrays.asList(
                                    createLogSplit(
                                            tablePath2,
                                            tableId2,
                                            0,
                                            EARLIEST_OFFSET,
                                            table2Rows.get(new TableBucket(tableId2, 0)).size()),
                                    createLogSplit(
                                            tablePath2,
                                            tableId2,
                                            1,
                                            EARLIEST_OFFSET,
                                            table2Rows.get(new TableBucket(tableId2, 1)).size()),
                                    createLogSplit(
                                            tablePath2,
                                            tableId2,
                                            2,
                                            EARLIEST_OFFSET,
                                            table2Rows.get(new TableBucket(tableId2, 2)).size())));
            Set<String> table2Splits =
                    splitsAddition.splits().stream()
                            .map(TieringSplit::splitId)
                            .collect(Collectors.toSet());
            tieringSplitReader.handleSplitsChanges(splitsAddition);
            Map<TableBucket, Integer> expectedRowCount =
                    table2Rows.entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()));
            verifyTieringRows(tieringSplitReader, tableId2, expectedRowCount, table2Splits);
        }
    }

    private TieringSplitReader<TestingWriteResult> createTieringReader(Connection connection) {
        return new TieringSplitReader<>(connection, new TestingLakeTieringFactory());
    }

    private void verifyTieringRows(
            TieringSplitReader<TestingWriteResult> tieringSplitReader,
            long tableId,
            Map<TableBucket, Integer> expectTierRows,
            Set<String> expectedFinishSplits)
            throws IOException {
        LinkedHashMap<Long, Map<TableBucket, Integer>> expectTierRowsMap = new LinkedHashMap<>();
        expectTierRowsMap.put(tableId, expectTierRows);

        LinkedHashMap<Long, Set<String>> expectedFinishSplitsMap = new LinkedHashMap<>();
        expectedFinishSplitsMap.put(tableId, expectedFinishSplits);

        verifyTieringRows(tieringSplitReader, expectTierRowsMap, expectedFinishSplitsMap);
    }

    private void verifyTieringRows(
            TieringSplitReader<TestingWriteResult> tieringSplitReader,
            LinkedHashMap<Long, Map<TableBucket, Integer>> expectTierRows,
            LinkedHashMap<Long, Set<String>> expectedFinishSplits)
            throws IOException {
        RecordsWithSplitIds<TableBucketWriteResult<TestingWriteResult>> fetchResult;
        for (Map.Entry<Long, Map<TableBucket, Integer>> expectTieringRowEntry :
                expectTierRows.entrySet()) {
            long tableId = expectTieringRowEntry.getKey();
            Map<TableBucket, Integer> expectRows = expectTieringRowEntry.getValue();
            Map<TableBucket, Integer> actualRows = new HashMap<>();
            Set<String> actualFinishSplits = new HashSet<>();

            while (expectRows.size() != actualRows.size()) {
                fetchResult = tieringSplitReader.fetch();
                actualFinishSplits.addAll(fetchResult.finishedSplits());
                while (fetchResult.nextSplit() != null) {
                    TableBucketWriteResult<TestingWriteResult> tableBucketWriteResult =
                            fetchResult.nextRecordFromSplit();
                    assertThat(tableBucketWriteResult).isNotNull();
                    TableBucket tableBucket = tableBucketWriteResult.tableBucket();
                    assertThat(tableBucket.getTableId()).isEqualTo(tableId);
                    TestingWriteResult testingWriteResult = tableBucketWriteResult.writeResult();
                    assertThat(testingWriteResult).isNotNull();
                    actualRows.put(tableBucket, testingWriteResult.getWriteResult());
                }
            }
            assertThat(actualRows).isEqualTo(expectRows);
            assertThat(actualFinishSplits).isEqualTo(expectedFinishSplits.get(tableId));
        }
    }

    private TieringLogSplit createLogSplit(
            TablePath tablePath,
            long tableId,
            int bucket,
            long startingOffset,
            long stoppingOffset) {
        TableBucket tableBucket = new TableBucket(tableId, bucket);
        return new TieringLogSplit(tablePath, tableBucket, null, startingOffset, stoppingOffset, 3);
    }

    private TieringSnapshotSplit createSnapshotSplit(
            TablePath tablePath, long tableId, int bucket, long snapshotId) {
        TableBucket tableBucket = new TableBucket(tableId, bucket);
        return new TieringSnapshotSplit(tablePath, tableBucket, null, snapshotId, 10, 3);
    }

    private Map<TableBucket, List<InternalRow>> putRows(long tableId, TablePath tablePath, int rows)
            throws Exception {
        Map<TableBucket, List<InternalRow>> rowsByBuckets = new HashMap<>();
        try (Table table = conn.getTable(tablePath)) {
            boolean isPrimaryKey = table.getTableInfo().hasPrimaryKey();
            TableWriter tableWriter =
                    isPrimaryKey
                            ? table.newUpsert().createWriter()
                            : table.newAppend().createWriter();
            for (int i = 0; i < rows; i++) {
                InternalRow row = row(i, "v" + i);
                if (tableWriter instanceof UpsertWriter) {
                    ((UpsertWriter) tableWriter).upsert(row);
                } else {
                    ((AppendWriter) tableWriter).append(row);
                }
                TableBucket tableBucket = new TableBucket(tableId, getBucketId(row));
                rowsByBuckets.computeIfAbsent(tableBucket, k -> new ArrayList<>()).add(row);
            }
            tableWriter.flush();
        }
        return rowsByBuckets;
    }

    private static int getBucketId(InternalRow row) {
        CompactedKeyEncoder keyEncoder =
                new CompactedKeyEncoder(
                        DEFAULT_PK_TABLE_SCHEMA.getRowType(),
                        DEFAULT_PK_TABLE_SCHEMA.getPrimaryKeyIndexes());
        byte[] key = keyEncoder.encodeKey(row);
        HashBucketAssigner hashBucketAssigner = new HashBucketAssigner(DEFAULT_BUCKET_NUM);
        return hashBucketAssigner.assignBucket(key);
    }
}

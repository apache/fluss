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
import org.apache.fluss.client.tiering.TableBucketWriteResult;
import org.apache.fluss.client.tiering.TieringLogSplit;
import org.apache.fluss.client.tiering.TieringSnapshotSplit;
import org.apache.fluss.client.write.HashBucketAssigner;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.flink.tiering.TestingLakeTieringFactory;
import org.apache.fluss.flink.tiering.TestingWriteResult;
import org.apache.fluss.flink.tiering.source.metrics.TieringMetrics;
import org.apache.fluss.flink.tiering.source.split.FlinkTieringSplit;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.server.replica.Replica;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
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
            SplitsAddition<FlinkTieringSplit> splitsAddition =
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
            FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);

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
            List<FlinkTieringSplit> logSplits = new ArrayList<>();
            for (int bucket = 0; bucket < 3; bucket++) {
                TableBucket tableBucket = new TableBucket(tableId, bucket);
                long startingOffset = firstRows.get(tableBucket).size();
                // -U, +U
                long stoppingOffset = startingOffset + secondRows.get(tableBucket).size() * 2L;
                expectedRowCount.put(tableBucket, secondRows.get(tableBucket).size() * 2);
                FlinkTieringSplit tieringLogSplit =
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
            FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath0);
            FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath1);

            // first add snapshot split of bucket 0, bucket 1 of table id 0
            SplitsAddition<FlinkTieringSplit> splitsAddition =
                    new SplitsAddition<>(
                            Arrays.asList(
                                    createSnapshotSplit(tablePath0, tableId0, 0, 0),
                                    createSnapshotSplit(tablePath0, tableId0, 1, 0)));
            Set<String> table0Splits =
                    splitsAddition.splits().stream()
                            .map(FlinkTieringSplit::splitId)
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
                            .map(FlinkTieringSplit::splitId)
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
                            .map(FlinkTieringSplit::splitId)
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
                            .map(FlinkTieringSplit::splitId)
                            .collect(Collectors.toSet());
            tieringSplitReader.handleSplitsChanges(splitsAddition);
            Map<TableBucket, Integer> expectedRowCount =
                    table2Rows.entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()));
            verifyTieringRows(tieringSplitReader, tableId2, expectedRowCount, table2Splits);
        }
    }

    @Test
    void testLogSplitWithoutWritableRecordsCanCompleteLakeWriter() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "tiering_table_without_writable_records");
        TableDescriptor singleBucketPkTableDescriptor =
                TableDescriptor.builder()
                        .schema(DEFAULT_PK_TABLE_SCHEMA)
                        .distributedBy(1, "id")
                        .build();
        long tableId = createTable(tablePath, singleBucketPkTableDescriptor);
        try (Connection connection =
                        ConnectionFactory.createConnection(
                                FLUSS_CLUSTER_EXTENSION.getClientConfig());
                Table table = connection.getTable(tablePath);
                TieringSplitReader<TestingWriteResult> tieringSplitReader =
                        createTieringReader(
                                connection, new ThrowOnEmptyCompleteLakeTieringFactory())) {
            int key1 = 1;
            int key2 = 2;
            int bucket = 0;
            TableBucket tableBucket = new TableBucket(tableId, bucket);

            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            //  produce offset0
            upsertWriter.upsert(row(key1, "v1")).get();
            // Deleting a non-existent key still advances the log offset, but this range does not
            // produce any tierable record. produce offset1
            upsertWriter.delete(row(key2, (Object) null)).get();
            // produce offset2
            upsertWriter.upsert(row(key2, "v2")).get();

            long startingOffset = 1;
            long stoppingOffset = 2;
            TieringLogSplit tieringLogSplit =
                    new TieringLogSplit(
                            tablePath, tableBucket, null, startingOffset, stoppingOffset, 1);

            // The custom factory fails if complete() is called on a writer that never received any
            // record, which captures the regression this test covers.
            tieringSplitReader.handleSplitsChanges(
                    new SplitsAddition<>(
                            Collections.singletonList(new FlinkTieringSplit(tieringLogSplit))));

            RecordsWithSplitIds<TableBucketWriteResult<TestingWriteResult>> result =
                    tieringSplitReader.fetch();

            assertThat(result.nextSplit()).isEqualTo(tieringLogSplit.splitId());
            TableBucketWriteResult<TestingWriteResult> writeResult = result.nextRecordFromSplit();
            assertThat(writeResult).isNotNull();
            // expect null write result since no any records written
            assertThat(writeResult.writeResult()).isNull();
        }
    }

    /**
     * Verifies that the tiering service finishes under {@code first_row} merge engine even when
     * duplicate upserts produce empty WAL batches.
     */
    @Test
    void testTieringFirstRowMergeEngineFinishes() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "tiering_first_row_finish");
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(DEFAULT_PK_TABLE_SCHEMA)
                        .distributedBy(DEFAULT_BUCKET_NUM, "id")
                        .property(ConfigOptions.TABLE_MERGE_ENGINE, MergeEngineType.FIRST_ROW)
                        .build();
        long tableId = createTable(tablePath, descriptor);

        // Duplicate upserts under FIRST_ROW: only the first per id yields a CDC
        // record, the rest become empty WAL batches that still advance the offset.
        int distinctKeys = 5;
        int duplicatesPerKey = 10;
        try (Table table = conn.getTable(tablePath)) {
            for (int round = 0; round < duplicatesPerKey; round++) {
                UpsertWriter writer = table.newUpsert().createWriter();
                for (int id = 0; id < distinctKeys; id++) {
                    writer.upsert(row(id, "v" + round));
                }
                writer.flush();
            }
        }

        // Build log splits whose stoppingOffset equals the leader's current logEndOffset.
        List<FlinkTieringSplit> logSplits = new ArrayList<>();
        Set<String> splitIds = new HashSet<>();
        long totalLogEndOffset = 0L;
        for (int bucket = 0; bucket < DEFAULT_BUCKET_NUM; bucket++) {
            TableBucket tb = new TableBucket(tableId, bucket);
            Replica leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tb);
            long stoppingOffset = leader.getLogTablet().localLogEndOffset();
            totalLogEndOffset += stoppingOffset;
            if (stoppingOffset <= 0) {
                continue;
            }
            FlinkTieringSplit split =
                    createLogSplit(tablePath, tableId, bucket, EARLIEST_OFFSET, stoppingOffset);
            logSplits.add(split);
            splitIds.add(split.splitId());
        }
        assertThat(logSplits).isNotEmpty();
        // Pre-condition: total log offsets must exceed distinct-key count, otherwise
        // no empty batch was produced.
        assertThat(totalLogEndOffset)
                .as(
                        "Expected logEndOffset (%d) to exceed distinctKeys (%d) so that "
                                + "empty batches are produced under FIRST_ROW",
                        totalLogEndOffset, distinctKeys)
                .isGreaterThan(distinctKeys);

        try (Connection connection =
                        ConnectionFactory.createConnection(
                                FLUSS_CLUSTER_EXTENSION.getClientConfig());
                TieringSplitReader<TestingWriteResult> tieringSplitReader =
                        createTieringReader(connection)) {
            tieringSplitReader.handleSplitsChanges(new SplitsAddition<>(logSplits));

            // With the fix every split must finish within a few fetch rounds.
            Set<String> finished = new HashSet<>();
            int maxRounds = 10;
            for (int i = 0; i < maxRounds && !finished.containsAll(splitIds); i++) {
                RecordsWithSplitIds<TableBucketWriteResult<TestingWriteResult>> fetchResult =
                        tieringSplitReader.fetch();
                finished.addAll(fetchResult.finishedSplits());
                // drain the iterator so that the reader advances internal state
                while (fetchResult.nextSplit() != null) {
                    while (fetchResult.nextRecordFromSplit() != null) {
                        // consume
                    }
                }
            }

            assertThat(finished)
                    .as(
                            "All tiering splits must finish under FIRST_ROW merge engine "
                                    + "with duplicate keys. Finished: %s, expected: %s",
                            finished, splitIds)
                    .containsAll(splitIds);
        }
    }

    private TieringSplitReader<TestingWriteResult> createTieringReader(Connection connection) {
        final TieringMetrics tieringMetrics =
                new TieringMetrics(
                        InternalSourceReaderMetricGroup.mock(
                                new MetricListener().getMetricGroup()));
        return new TieringSplitReader<>(
                connection,
                new TestingLakeTieringFactory(),
                Thread.currentThread().getContextClassLoader(),
                tieringMetrics);
    }

    private TieringSplitReader<TestingWriteResult> createTieringReader(
            Connection connection, TestingLakeTieringFactory lakeTieringFactory) {
        final TieringMetrics tieringMetrics =
                new TieringMetrics(
                        InternalSourceReaderMetricGroup.mock(
                                new MetricListener().getMetricGroup()));
        return new TieringSplitReader<>(
                connection,
                lakeTieringFactory,
                Thread.currentThread().getContextClassLoader(),
                tieringMetrics);
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

    private FlinkTieringSplit createLogSplit(
            TablePath tablePath,
            long tableId,
            int bucket,
            long startingOffset,
            long stoppingOffset) {
        TableBucket tableBucket = new TableBucket(tableId, bucket);
        return new FlinkTieringSplit(
                new TieringLogSplit(
                        tablePath, tableBucket, null, startingOffset, stoppingOffset, 3));
    }

    private FlinkTieringSplit createSnapshotSplit(
            TablePath tablePath, long tableId, int bucket, long snapshotId) {
        TableBucket tableBucket = new TableBucket(tableId, bucket);
        return new FlinkTieringSplit(
                new TieringSnapshotSplit(tablePath, tableBucket, null, snapshotId, 10, 3));
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

    private static class ThrowOnEmptyCompleteLakeTieringFactory extends TestingLakeTieringFactory {

        @Override
        public LakeWriter<TestingWriteResult> createLakeWriter(WriterInitContext writerInitContext)
                throws IOException {
            return new ThrowOnEmptyCompleteLakeWriter();
        }
    }

    private static class ThrowOnEmptyCompleteLakeWriter implements LakeWriter<TestingWriteResult> {

        private int writtenRecords;

        @Override
        public void write(LogRecord record) throws IOException {
            writtenRecords++;
        }

        @Override
        public TestingWriteResult complete() throws IOException {
            if (writtenRecords == 0) {
                throw new IOException("complete called without any written records");
            }
            return new TestingWriteResult(writtenRecords);
        }

        @Override
        public void close() throws IOException {}
    }
}

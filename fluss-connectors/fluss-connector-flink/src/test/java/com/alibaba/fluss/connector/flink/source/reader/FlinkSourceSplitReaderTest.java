/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.connector.flink.source.reader;

import com.alibaba.fluss.client.metadata.KvSnapshots;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.client.write.HashBucketAssigner;
import com.alibaba.fluss.connector.flink.source.metrics.FlinkSourceReaderMetrics;
import com.alibaba.fluss.connector.flink.source.split.HybridSnapshotLogSplit;
import com.alibaba.fluss.connector.flink.source.split.LogSplit;
import com.alibaba.fluss.connector.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.connector.flink.source.testutils.FlinkTestBase;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.RowKind;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.encode.KeyEncoder;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.table.api.ValidationException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

import static com.alibaba.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static com.alibaba.fluss.connector.flink.source.testutils.RecordAndPosAssert.assertThatRecordAndPos;
import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.testutils.DataTestUtils.compactedRow;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link FlinkSourceSplitReader}. */
class FlinkSourceSplitReaderTest extends FlinkTestBase {

    @Test
    void testSanityCheck() throws Exception {
        TablePath tablePath1 = TablePath.of(DEFAULT_DB, "test1");
        Schema schema1 =
                Schema.newBuilder()
                        .primaryKey("id")
                        .column("id", DataTypes.BIGINT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .build();
        TableDescriptor descriptor1 = TableDescriptor.builder().schema(schema1).build();
        createTable(tablePath1, descriptor1);

        assertThatThrownBy(
                        () ->
                                new FlinkSourceSplitReader(
                                        clientConf,
                                        tablePath1,
                                        DataTypes.ROW(
                                                DataTypes.FIELD("id", DataTypes.BIGINT()),
                                                DataTypes.FIELD("name", DataTypes.STRING()),
                                                DataTypes.FIELD("age", DataTypes.INT())),
                                        null,
                                        createMockSourceReaderMetrics()))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "The Flink query schema is not matched to Fluss table schema. \n"
                                + "Flink query schema: ROW<`id` BIGINT, `name` STRING, `age` INT>\n"
                                + "Fluss table schema: ROW<`id` BIGINT NOT NULL, `name` STRING, `age` INT>");

        assertThatThrownBy(
                        () ->
                                new FlinkSourceSplitReader(
                                        clientConf,
                                        tablePath1,
                                        DataTypes.ROW(
                                                DataTypes.FIELD(
                                                        "id", DataTypes.BIGINT().copy(false)),
                                                DataTypes.FIELD("name", DataTypes.STRING())),
                                        new int[] {1, 0},
                                        createMockSourceReaderMetrics()))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "The Flink query schema is not matched to Fluss table schema. \n"
                                + "Flink query schema: ROW<`id` BIGINT NOT NULL, `name` STRING>\n"
                                + "Fluss table schema: ROW<`name` STRING, `id` BIGINT NOT NULL> (projection [1, 0])");
    }

    @Test
    void testHandleHybridSnapshotLogSplitChangesAndFetch() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test-only-snapshot-table");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        try (FlinkSourceSplitReader splitReader =
                createSplitReader(tablePath, DEFAULT_PK_TABLE_SCHEMA.toRowType())) {

            // no any records
            List<SourceSplitBase> hybridSnapshotLogSplits = new ArrayList<>();
            Map<String, List<RecordAndPos>> expectedRecords = new HashMap<>();
            assignSplitsAndFetchUntilRetrieveRecords(
                    splitReader,
                    hybridSnapshotLogSplits,
                    expectedRecords,
                    DEFAULT_PK_TABLE_SCHEMA.toRowType());

            // now, write some records into the table
            Map<TableBucket, List<InternalRow>> rows = putRows(tableId, tablePath, 10);

            // check the expected records
            waitUntilSnapshot(tableId, 0);

            hybridSnapshotLogSplits = getHybridSnapshotLogSplits(tablePath);

            expectedRecords = constructRecords(rows);

            assignSplitsAndFetchUntilRetrieveRecords(
                    splitReader,
                    hybridSnapshotLogSplits,
                    expectedRecords,
                    DEFAULT_PK_TABLE_SCHEMA.toRowType());
        }
    }

    private Map<String, List<RecordAndPos>> constructRecords(
            Map<TableBucket, List<InternalRow>> rows) {
        Map<String, List<RecordAndPos>> expectedRecords = new HashMap<>();
        for (Map.Entry<TableBucket, List<InternalRow>> bucketRowsEntry : rows.entrySet()) {
            TableBucket tb = bucketRowsEntry.getKey();
            String splitId = toHybridSnapshotLogSplitId(tb);
            List<RecordAndPos> records = new ArrayList<>(bucketRowsEntry.getValue().size());
            List<InternalRow> kvRows = bucketRowsEntry.getValue();
            int currentReadRecords = 1;
            for (InternalRow row : kvRows) {
                records.add(new RecordAndPos(new ScanRecord(row), currentReadRecords++));
            }
            expectedRecords.put(splitId, records);
        }
        return expectedRecords;
    }

    @Test
    void testHandleLogSplitChangesAndFetch() throws Exception {
        final Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();

        final TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1).build();
        TablePath tablePath1 = TablePath.of(DEFAULT_DB, "test-only-log-table");
        long tableId = createTable(tablePath1, tableDescriptor);

        try (FlinkSourceSplitReader splitReader =
                createSplitReader(tablePath1, schema.toRowType())) {

            // no any records
            List<SourceSplitBase> logSplits = new ArrayList<>();
            Map<String, List<RecordAndPos>> expectedRecords = new HashMap<>();
            assignSplitsAndFetchUntilRetrieveRecords(
                    splitReader, logSplits, expectedRecords, schema.toRowType());

            // now, write some records into the table
            List<InternalRow> internalRows = appendRows(tablePath1, 5);
            List<RecordAndPos> expected = new ArrayList<>(internalRows.size());
            for (int i = 0; i < internalRows.size(); i++) {
                expected.add(
                        new RecordAndPos(
                                new ScanRecord(i, i, RowKind.APPEND_ONLY, internalRows.get(i))));
            }

            TableBucket tableBucket = new TableBucket(tableId, 0);
            String splitId = toLogSplitId(tableBucket);
            expectedRecords.put(splitId, expected);

            logSplits.add(new LogSplit(tableBucket, null, 0L));

            assignSplitsAndFetchUntilRetrieveRecords(
                    splitReader, logSplits, expectedRecords, schema.toRowType());
        }
    }

    @Test
    void testHandleMixSnapshotLogSplitChangesAndFetch() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test-mix-snapshot-log-table");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);

        try (FlinkSourceSplitReader splitReader =
                createSplitReader(tablePath, DEFAULT_PK_TABLE_SCHEMA.toRowType())) {

            // now, write some records into the table
            Map<TableBucket, List<InternalRow>> rows = putRows(tableId, tablePath, 10);

            // check the expected records
            waitUntilSnapshot(tableId, 0);

            List<SourceSplitBase> totalSplits =
                    new ArrayList<>(getHybridSnapshotLogSplits(tablePath));
            // construct expected records for snapshot;
            Map<String, List<RecordAndPos>> expectedRecords = constructRecords(rows);

            // now, put data again;
            Map<TableBucket, Integer> bucketOffsets = new HashMap<>();
            for (int bucket = 0; bucket < DEFAULT_BUCKET_NUM; bucket++) {
                LogSplit logSplit =
                        new LogSplit(
                                new TableBucket(tableId, bucket),
                                null,
                                rows.get(new TableBucket(tableId, bucket)).size());
                // add log splits
                totalSplits.add(logSplit);
                bucketOffsets.put(
                        new TableBucket(tableId, bucket),
                        rows.get(new TableBucket(tableId, bucket)).size());
            }

            putRows(tableId, tablePath, 10);

            // add record to expected records for log,
            // should contain -U,+U
            for (Map.Entry<TableBucket, List<InternalRow>> bucketRowsEntry : rows.entrySet()) {
                TableBucket tb = bucketRowsEntry.getKey();
                String splitId = toLogSplitId(tb);
                List<RecordAndPos> records = new ArrayList<>(2 * bucketRowsEntry.getValue().size());
                List<InternalRow> kvRows = bucketRowsEntry.getValue();
                int offset = bucketOffsets.get(tb);
                for (InternalRow row : kvRows) {
                    records.add(
                            new RecordAndPos(
                                    new ScanRecord(offset++, -1, RowKind.UPDATE_BEFORE, row)));

                    records.add(
                            new RecordAndPos(
                                    new ScanRecord(offset++, -1, RowKind.UPDATE_AFTER, row)));
                }
                expectedRecords.put(splitId, records);
            }

            assignSplitsAndFetchUntilRetrieveRecords(
                    splitReader, totalSplits, expectedRecords, DEFAULT_PK_TABLE_SCHEMA.toRowType());
        }
    }

    @Test
    void testNoSubscribedBucket() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test-no-subscribe-bucket-table");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();
        final TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1).build();
        createTable(tablePath, tableDescriptor);

        try (FlinkSourceSplitReader splitReader =
                createSplitReader(tablePath, schema.toRowType())) {
            // fetch shouldn't throw exception
            RecordsWithSplitIds<RecordAndPos> records = splitReader.fetch();
            assertThat(records.nextSplit()).isNull();
        }
    }

    @Test
    void testSubscribeEmptySplits() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test-subscribe-empty-splits");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();
        long tableId =
                createTable(
                        tablePath,
                        TableDescriptor.builder().schema(schema).distributedBy(3).build());

        // create two empty splits with log start offset equal to end offset
        LogSplit split1 = new LogSplit(new TableBucket(tableId, 0), null, 0, 0);
        LogSplit split2 = new LogSplit(new TableBucket(tableId, 1), null, 0, 0);
        LogSplit split3 = new LogSplit(new TableBucket(tableId, 2), null, EARLIEST_OFFSET);
        List<SourceSplitBase> subscribeSplits = Arrays.asList(split1, split2, split3);

        try (FlinkSourceSplitReader splitReader =
                createSplitReader(tablePath, schema.toRowType())) {
            splitReader.handleSplitsChanges(new SplitsAddition<>(subscribeSplits));

            // fetch records
            RecordsWithSplitIds<RecordAndPos> records = splitReader.fetch();
            // finished splits should be split1,split2
            assertThat(records.finishedSplits())
                    .containsExactlyInAnyOrder(split1.splitId(), split2.splitId());
        }
    }

    // ------------------

    private void assignSplitsAndFetchUntilRetrieveRecords(
            FlinkSourceSplitReader reader,
            List<SourceSplitBase> splits,
            Map<String, List<RecordAndPos>> expectedRecords,
            RowType rowType)
            throws IOException {

        // assign the splits to the reader
        assignSplits(reader, splits);

        Map<String, List<RecordAndPos>> splitConsumedRecords = new HashMap<>();
        Set<String> finishedSplits = new HashSet<>();

        while (finishedSplits.size() < splits.size()) {
            RecordsWithSplitIds<RecordAndPos> recordsBySplitIds = reader.fetch();
            String splitId = recordsBySplitIds.nextSplit();
            while (splitId != null) {
                // Collect the records in this split.
                List<RecordAndPos> splitFetch = new ArrayList<>();
                RecordAndPos record;
                while ((record = recordsBySplitIds.nextRecordFromSplit()) != null) {
                    splitFetch.add(new RecordAndPos(record.record(), record.readRecordsCount()));
                }

                splitConsumedRecords
                        .computeIfAbsent(splitId, k -> new ArrayList<>())
                        .addAll(splitFetch);

                // if records retrieved from this split is greater or equal to expected records,
                // it means we should stop read
                if (splitConsumedRecords.getOrDefault(splitId, Collections.emptyList()).size()
                        >= expectedRecords.get(splitId).size()) {
                    finishedSplits.add(splitId);
                }
                splitId = recordsBySplitIds.nextSplit();
            }
            recordsBySplitIds.recycle();
        }

        // now, verify the records consumed from each split.
        verifyConsumedRecords(splitConsumedRecords, expectedRecords, rowType);
    }

    private void verifyConsumedRecords(
            Map<String, List<RecordAndPos>> actualRecords,
            Map<String, List<RecordAndPos>> expectedRecords,
            RowType rowType) {
        assertThat(actualRecords.size()).isEqualTo(expectedRecords.size());
        for (Map.Entry<String, List<RecordAndPos>> splitRecordsEntry : actualRecords.entrySet()) {
            List<RecordAndPos> actualSplitRecords = splitRecordsEntry.getValue();
            List<RecordAndPos> expectedRecordsForSplit =
                    expectedRecords.get(splitRecordsEntry.getKey());

            assertThat(actualSplitRecords.size()).isEqualTo(expectedRecordsForSplit.size());

            for (int i = 0; i < actualSplitRecords.size(); i++) {
                RecordAndPos actualRecord = actualSplitRecords.get(i);
                RecordAndPos expectedRecord = expectedRecordsForSplit.get(i);
                assertThatRecordAndPos(actualRecord).withSchema(rowType).isEqualTo(expectedRecord);
            }
        }
    }

    private void assignSplits(FlinkSourceSplitReader splitReader, List<SourceSplitBase> splits) {
        SplitsChange<SourceSplitBase> splitsChange = new SplitsAddition<>(splits);
        splitReader.handleSplitsChanges(splitsChange);
    }

    private FlinkSourceSplitReader createSplitReader(TablePath tablePath, RowType rowType) {
        return new FlinkSourceSplitReader(
                clientConf, tablePath, rowType, null, createMockSourceReaderMetrics());
    }

    private FlinkSourceReaderMetrics createMockSourceReaderMetrics() {
        MetricListener metricListener = new MetricListener();
        return new FlinkSourceReaderMetrics(
                InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup()));
    }

    private Map<TableBucket, List<InternalRow>> putRows(long tableId, TablePath tablePath, int rows)
            throws Exception {
        Map<TableBucket, List<InternalRow>> rowsByBuckets = new HashMap<>();
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            for (int i = 0; i < rows; i++) {
                InternalRow compactedRow = compactedRow(DATA1_ROW_TYPE, new Object[] {i, "v" + i});
                upsertWriter.upsert(compactedRow);
                TableBucket tableBucket = new TableBucket(tableId, getBucketId(compactedRow));
                rowsByBuckets
                        .computeIfAbsent(tableBucket, k -> new ArrayList<>())
                        .add(compactedRow);
            }
            upsertWriter.flush();
        }
        return rowsByBuckets;
    }

    private List<InternalRow> appendRows(TablePath tablePath, int rows) throws Exception {
        List<InternalRow> internalRows = new ArrayList<>(rows);
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            for (int i = 0; i < rows; i++) {
                InternalRow row = row(DATA1_ROW_TYPE, new Object[] {i, "v" + i});
                appendWriter.append(row);
                internalRows.add(row);
            }
            appendWriter.flush();
        }

        return internalRows;
    }

    private static String toLogSplitId(TableBucket tableBucket) {
        return new LogSplit(tableBucket, null, 0L).splitId();
    }

    private static String toHybridSnapshotLogSplitId(TableBucket tableBucket) {
        // snapshotId and logOffset doesn't affect splitId, use mocked 0 value.
        return new HybridSnapshotLogSplit(tableBucket, null, 0, 0).splitId();
    }

    private static int getBucketId(InternalRow row) {
        KeyEncoder keyEncoder =
                new KeyEncoder(
                        DEFAULT_PK_TABLE_SCHEMA.toRowType(),
                        DEFAULT_PK_TABLE_SCHEMA.getPrimaryKeyIndexes());
        byte[] key = keyEncoder.encode(row);
        HashBucketAssigner hashBucketAssigner = new HashBucketAssigner(DEFAULT_BUCKET_NUM);
        return hashBucketAssigner.assignBucket(key, null);
    }

    private List<SourceSplitBase> getHybridSnapshotLogSplits(TablePath tablePath) throws Exception {
        KvSnapshots snapshots = admin.getLatestKvSnapshots(tablePath).get();
        List<SourceSplitBase> hybridSnapshotLogSplits = new ArrayList<>();
        for (Integer bucketId : snapshots.getBucketIds()) {
            TableBucket tableBucket = new TableBucket(snapshots.getTableId(), bucketId);
            OptionalLong snapshotId = snapshots.getSnapshotId(bucketId);
            OptionalLong logOffset = snapshots.getLogOffset(bucketId);
            if (snapshotId.isPresent() && logOffset.isPresent()) {
                hybridSnapshotLogSplits.add(
                        new HybridSnapshotLogSplit(
                                tableBucket, null, snapshotId.getAsLong(), logOffset.getAsLong()));
            }
        }
        return hybridSnapshotLogSplits;
    }
}

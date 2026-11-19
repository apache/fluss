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

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Test for {@link KvSnapshotAndLogBatchScanner}. */
class KvSnapshotAndLogBatchScannerTest {

    private static final Duration TIMEOUT = Duration.ofMillis(10);

    private @TempDir Path tempDir;

    @Test
    void testSnapshotRecordIteratorPollsAfterEmptyBatch() throws Exception {
        StubBatchScanner scanner =
                new StubBatchScanner(
                        Arrays.asList(
                                Collections.emptyList(),
                                Collections.singletonList(GenericRow.of(1)),
                                Collections.emptyList(),
                                Collections.singletonList(GenericRow.of(2))));

        KvSnapshotAndLogBatchScanner.SnapshotRecordIterator iterator =
                new KvSnapshotAndLogBatchScanner.SnapshotRecordIterator(scanner, TIMEOUT);

        List<Integer> values = new ArrayList<>();
        while (iterator.hasNext()) {
            LogRecord record = iterator.next();
            values.add(record.getRow().getInt(0));
        }

        assertThat(values).containsExactly(1, 2);
        assertThat(scanner.pollCount).isEqualTo(5);
    }

    @Test
    void testMergeSnapshotAndLogRows() throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID_PK, 0);
        long snapshotId = 1L;

        // snapshot data
        StubBatchScanner snapshotScanner =
                new StubBatchScanner(
                        Arrays.asList(
                                Arrays.asList(
                                        row(DATA1_ROW_TYPE, 0, "snapshot-0"),
                                        row(DATA1_ROW_TYPE, 1, "old-1")),
                                Arrays.asList(
                                        row(DATA1_ROW_TYPE, 2, "snapshot-2"),
                                        row(DATA1_ROW_TYPE, 3, "delete-me"),
                                        row(DATA1_ROW_TYPE, 5, "snapshot-5"))));

        // cdc log
        List<LogRecord> logRecords =
                Arrays.asList(
                        logRecord(0, ChangeType.INSERT, 0, "insert-0"),
                        logRecord(1, ChangeType.UPDATE_BEFORE, 1, "old-1"),
                        logRecord(2, ChangeType.UPDATE_AFTER, 1, "new-1"),
                        logRecord(4, ChangeType.DELETE, 2, "delete-2"),
                        logRecord(4, ChangeType.DELETE, 3, "delete-me"),
                        logRecord(3, ChangeType.INSERT, 2, "insert-2"),
                        logRecord(5, ChangeType.INSERT, 4, "insert-4"));

        TestingLogScanner logScanner =
                new TestingLogScanner(scanRecords(tableBucket, logRecords, logRecords.size()));

        Table table = mock(Table.class);
        Scan scan = mock(Scan.class);
        when(table.getTableInfo()).thenReturn(DATA1_TABLE_INFO_PK);
        when(table.newScan()).thenReturn(scan);
        when(scan.project(any(int[].class))).thenReturn(scan);
        when(scan.createBatchScanner(tableBucket, snapshotId)).thenReturn(snapshotScanner);
        when(scan.createLogScanner()).thenReturn(logScanner);

        try (KvSnapshotAndLogBatchScanner scanner =
                new KvSnapshotAndLogBatchScanner(
                        table,
                        tableBucket,
                        snapshotId,
                        0L,
                        logRecords.size(),
                        null,
                        tempDir.toString(),
                        3)) {
            CloseableIterator<InternalRow> loadingRows = scanner.pollBatch(TIMEOUT);
            assertThat(loadingRows).isNotNull();
            assertThat(loadingRows.hasNext()).isFalse();
            loadingRows.close();

            assertThat(scanner.isLogRowsSpilled()).isTrue();
            assertThat(logScanner.closed).isTrue();

            CloseableIterator<InternalRow> mergedRows = scanner.pollBatch(TIMEOUT);
            assertThat(mergedRows).isNotNull();

            List<InternalRow> actualRows = collectGenericRows(mergedRows);
            assertThat(actualRows)
                    .containsExactly(
                            row(DATA1_ROW_TYPE, 0, "insert-0"),
                            row(DATA1_ROW_TYPE, 1, "new-1"),
                            row(DATA1_ROW_TYPE, 2, "insert-2"),
                            row(DATA1_ROW_TYPE, 4, "insert-4"),
                            row(DATA1_ROW_TYPE, 5, "snapshot-5"));
            assertThat(scanner.pollBatch(TIMEOUT)).isNull();
        }
    }

    private static LogRecord logRecord(long offset, ChangeType changeType, int key, String value) {
        return new ScanRecord(offset, 0L, changeType, row(DATA1_ROW_TYPE, key, value));
    }

    private static ScanRecords scanRecords(
            TableBucket tableBucket, List<LogRecord> records, long consumedUpToOffset) {
        Map<TableBucket, List<ScanRecord>> recordsByBucket = new HashMap<>();
        List<ScanRecord> scanRecords = new ArrayList<>();
        for (LogRecord record : records) {
            scanRecords.add((ScanRecord) record);
        }
        recordsByBucket.put(tableBucket, scanRecords);

        Map<TableBucket, Long> consumedUpToOffsets = new HashMap<>();
        consumedUpToOffsets.put(tableBucket, consumedUpToOffset);
        return new ScanRecords(recordsByBucket, consumedUpToOffsets);
    }

    private static List<InternalRow> collectGenericRows(CloseableIterator<InternalRow> iterator) {
        List<InternalRow> rows = new ArrayList<>();
        try {
            while (iterator.hasNext()) {
                InternalRow internalRow = iterator.next();
                rows.add(
                        row(
                                DATA1_ROW_TYPE,
                                internalRow.getInt(0),
                                internalRow.getString(1).toString()));
            }
        } finally {
            iterator.close();
        }
        return rows;
    }

    private static class StubBatchScanner implements BatchScanner {

        private final Queue<List<InternalRow>> batches;
        private int pollCount;

        StubBatchScanner(List<List<InternalRow>> batches) {
            this.batches = new LinkedBlockingQueue<>(batches);
        }

        @Nullable
        @Override
        public CloseableIterator<InternalRow> pollBatch(Duration timeout) {
            pollCount++;
            if (batches.isEmpty()) {
                return null;
            }
            return CloseableIterator.wrap(batches.poll().iterator());
        }

        @Override
        public void close() {
            // do nothing
        }
    }

    private static class TestingLogScanner implements LogScanner {

        private final Queue<ScanRecords> records = new ArrayDeque<>();
        private boolean closed;

        private TestingLogScanner(ScanRecords... records) {
            this.records.addAll(Arrays.asList(records));
        }

        @Override
        public ScanRecords poll(Duration timeout) {
            return records.isEmpty() ? ScanRecords.EMPTY : records.poll();
        }

        @Override
        public void subscribe(int bucket, long offset) {
            // do nothing
        }

        @Override
        public void subscribe(long partitionId, int bucket, long offset) {
            // do nothing
        }

        @Override
        public void unsubscribe(long partitionId, int bucket) {
            // do nothing
        }

        @Override
        public void unsubscribe(int bucket) {
            // do nothing
        }

        @Override
        public void wakeup() {
            // do nothing
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}

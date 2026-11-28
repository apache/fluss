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

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.KeyValueRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SortedLogRows}. */
class SortedLogRowsTest {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                    new String[] {"id", "name"});
    private static final int[] KEY_INDEXES = new int[] {0};
    private static final TableBucket TABLE_BUCKET = new TableBucket(1L, 0);
    private static final Duration TIMEOUT = Duration.ofMillis(1);
    private static final int TEST_SPILL_THRESHOLD = 4;
    private static final KeyEncoder ASCENDING_KEY_ENCODER = row -> encodeSortableInt(row.getInt(0));

    private @TempDir Path tempDir;

    @Test
    void testDoesNotSpillBeforeThreshold() throws Exception {
        List<ScanRecord> records = new ArrayList<>();
        for (int i = 0; i < TEST_SPILL_THRESHOLD; i++) {
            records.add(record(i, row(ROW_TYPE, i, "v" + i)));
        }

        try (SortedLogRows logRows = createLogRows(records, TEST_SPILL_THRESHOLD)) {
            load(logRows);

            assertThat(logRows.isSpilled()).isFalse();
            List<InternalRow> expectedRows = expectedRows(0, TEST_SPILL_THRESHOLD);
            assertThat(collectValueRows(logRows)).containsExactlyElementsOf(expectedRows);
        }
    }

    @Test
    void testSpillsWhenDistinctRowsExceedThreshold() throws Exception {
        List<ScanRecord> records = new ArrayList<>();
        for (int i = 0; i <= TEST_SPILL_THRESHOLD; i++) {
            records.add(record(i, row(ROW_TYPE, i, "v" + i)));
        }

        Path spillDirectory;
        try (SortedLogRows logRows = createLogRows(records, TEST_SPILL_THRESHOLD + 1)) {
            load(logRows);

            spillDirectory = logRows.spillDirectory();
            assertThat(logRows.isSpilled()).isTrue();
            assertThat(spillDirectory).isNotNull();
            assertThat(spillDirectory).startsWith(tempDir);
            assertThat(Files.exists(spillDirectory)).isTrue();

            List<KeyValueRow> rows = collectRows(logRows);
            List<InternalRow> expectedRows = expectedRows(0, TEST_SPILL_THRESHOLD + 1);
            assertThat(toValueRows(rows)).containsExactlyElementsOf(expectedRows);
            assertThat(rows.stream().map(KeyValueRow::isDelete).collect(Collectors.toList()))
                    .containsOnly(false);
        }

        assertThat(Files.exists(spillDirectory)).isFalse();
    }

    @Test
    void testDoesNotSpillWhenOnlyRawRecordsExceedThreshold() throws Exception {
        List<ScanRecord> records = new ArrayList<>();
        for (int i = 0; i <= TEST_SPILL_THRESHOLD; i++) {
            records.add(record(i, row(ROW_TYPE, 1, "v" + i)));
        }

        try (SortedLogRows logRows = createLogRows(records, TEST_SPILL_THRESHOLD + 1)) {
            load(logRows);

            assertThat(logRows.isSpilled()).isFalse();
            List<InternalRow> expectedRows =
                    Collections.singletonList(row(ROW_TYPE, 1, "v" + TEST_SPILL_THRESHOLD));
            assertThat(collectValueRows(logRows)).containsExactlyElementsOf(expectedRows);
        }
    }

    @Test
    void testDeduplicatesAndKeepsLastRowInMemory() throws Exception {
        List<ScanRecord> records =
                Arrays.asList(
                        record(0, row(ROW_TYPE, 2, "old")),
                        record(1, row(ROW_TYPE, 1, "one")),
                        record(2, row(ROW_TYPE, 2, "new")));

        try (SortedLogRows logRows = createLogRows(records, 3)) {
            load(logRows);

            List<InternalRow> expectedRows =
                    Arrays.asList(row(ROW_TYPE, 1, "one"), row(ROW_TYPE, 2, "new"));
            assertThat(collectValueRows(logRows)).containsExactlyElementsOf(expectedRows);
        }
    }

    @Test
    void testDeduplicatesAndKeepsLastRowAfterSpill() throws Exception {
        List<ScanRecord> records = new ArrayList<>();
        for (int i = 0; i <= TEST_SPILL_THRESHOLD; i++) {
            records.add(record(i, row(ROW_TYPE, i, "v" + i)));
        }
        records.add(record(TEST_SPILL_THRESHOLD + 1, row(ROW_TYPE, 1, "new-one")));
        records.add(
                record(TEST_SPILL_THRESHOLD + 2, row(ROW_TYPE, TEST_SPILL_THRESHOLD, "new-last")));

        try (SortedLogRows logRows = createLogRows(records, TEST_SPILL_THRESHOLD + 3)) {
            load(logRows);

            assertThat(logRows.isSpilled()).isTrue();
            List<InternalRow> expectedRows =
                    Arrays.asList(
                            row(ROW_TYPE, 0, "v0"),
                            row(ROW_TYPE, 1, "new-one"),
                            row(ROW_TYPE, 2, "v2"),
                            row(ROW_TYPE, 3, "v3"),
                            row(ROW_TYPE, 4, "new-last"));
            assertThat(collectValueRows(logRows)).containsExactlyElementsOf(expectedRows);
        }
    }

    @Test
    void testSpilledIteratorUsesEncodedKeyOrder() throws Exception {
        KeyEncoder descendingKeyEncoder = row -> encodeSortableInt(-row.getInt(0));
        List<ScanRecord> records = new ArrayList<>();
        for (int i = 0; i <= TEST_SPILL_THRESHOLD; i++) {
            records.add(record(i, row(ROW_TYPE, i, "v" + i)));
        }

        try (SortedLogRows logRows =
                createLogRows(records, TEST_SPILL_THRESHOLD + 1, descendingKeyEncoder)) {
            load(logRows);

            assertThat(logRows.isSpilled()).isTrue();
            List<InternalRow> expectedReversedRows = expectedRows(0, TEST_SPILL_THRESHOLD + 1);
            Collections.reverse(expectedReversedRows);
            assertThat(collectValueRows(logRows)).containsExactlyElementsOf(expectedReversedRows);
        }
    }

    @Test
    void testDeleteTombstoneIsPreservedInMemory() throws Exception {
        List<ScanRecord> records =
                Arrays.asList(
                        record(0, row(ROW_TYPE, 1, "old")),
                        record(1, ChangeType.DELETE, row(ROW_TYPE, 1, "deleted")));

        try (SortedLogRows logRows = createLogRows(records, 2)) {
            load(logRows);

            List<KeyValueRow> rows = collectRows(logRows);
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).isDelete()).isTrue();
            assertThat(toGenericRow(rows.get(0).valueRow())).isEqualTo(row(ROW_TYPE, 1, "deleted"));
        }
    }

    @Test
    void testLoadCanBeCalledAcrossPolls() throws Exception {
        TestingLogScanner logScanner =
                new TestingLogScanner(
                        scanRecords(
                                Collections.singletonList(record(0, row(ROW_TYPE, 1, "one"))), 1),
                        scanRecords(
                                Collections.singletonList(record(1, row(ROW_TYPE, 2, "two"))), 2));

        try (SortedLogRows logRows =
                new SortedLogRows(
                        ROW_TYPE,
                        KEY_INDEXES,
                        ASCENDING_KEY_ENCODER,
                        logScanner,
                        TABLE_BUCKET,
                        2,
                        tempDir.toString(),
                        TEST_SPILL_THRESHOLD)) {
            assertThat(logRows.load(TIMEOUT)).isFalse();
            assertThat(logRows.load(TIMEOUT)).isTrue();
            List<InternalRow> expectedRows =
                    Arrays.asList(row(ROW_TYPE, 1, "one"), row(ROW_TYPE, 2, "two"));
            assertThat(collectValueRows(logRows)).containsExactlyElementsOf(expectedRows);
        }
    }

    private SortedLogRows createLogRows(List<ScanRecord> records, long stoppingOffset) {
        return createLogRows(records, stoppingOffset, ASCENDING_KEY_ENCODER);
    }

    private SortedLogRows createLogRows(
            List<ScanRecord> records, long stoppingOffset, KeyEncoder primaryKeyEncoder) {
        return new SortedLogRows(
                ROW_TYPE,
                KEY_INDEXES,
                primaryKeyEncoder,
                new TestingLogScanner(scanRecords(records, stoppingOffset)),
                TABLE_BUCKET,
                stoppingOffset,
                tempDir.toString(),
                TEST_SPILL_THRESHOLD);
    }

    private static byte[] encodeSortableInt(int value) {
        int normalized = value ^ Integer.MIN_VALUE;
        return new byte[] {
            (byte) (normalized >>> 24),
            (byte) (normalized >>> 16),
            (byte) (normalized >>> 8),
            (byte) normalized
        };
    }

    private static ScanRecord record(long offset, InternalRow row) {
        return record(offset, ChangeType.INSERT, row);
    }

    private static ScanRecord record(long offset, ChangeType changeType, InternalRow row) {
        return new ScanRecord(offset, 0L, changeType, row);
    }

    private static ScanRecords scanRecords(List<ScanRecord> records, long consumedUpToOffset) {
        Map<TableBucket, List<ScanRecord>> recordsByBucket = new HashMap<>();
        recordsByBucket.put(TABLE_BUCKET, records);
        Map<TableBucket, Long> consumedUpToOffsets = new HashMap<>();
        consumedUpToOffsets.put(TABLE_BUCKET, consumedUpToOffset);
        return new ScanRecords(recordsByBucket, consumedUpToOffsets);
    }

    private static void load(SortedLogRows logRows) throws Exception {
        while (!logRows.load(TIMEOUT)) {
            // keep polling until the bounded log range has been fully materialized
        }
    }

    private static List<InternalRow> collectValueRows(SortedLogRows logRows) throws Exception {
        return toValueRows(collectRows(logRows));
    }

    private static List<InternalRow> toValueRows(List<KeyValueRow> keyValueRows) {
        return keyValueRows.stream()
                .map(keyValueRow -> toGenericRow(keyValueRow.valueRow()))
                .collect(Collectors.toList());
    }

    private static List<InternalRow> expectedRows(int startInclusive, int endExclusive) {
        return IntStream.range(startInclusive, endExclusive)
                .mapToObj(i -> row(ROW_TYPE, i, "v" + i))
                .collect(Collectors.toList());
    }

    private static GenericRow toGenericRow(InternalRow internalRow) {
        return row(ROW_TYPE, internalRow.getInt(0), internalRow.getString(1).toString());
    }

    private static List<KeyValueRow> collectRows(SortedLogRows logRows) throws Exception {
        List<KeyValueRow> rows = new ArrayList<>();
        try (CloseableIterator<KeyValueRow> iterator = logRows.newIterator()) {
            while (iterator.hasNext()) {
                rows.add(iterator.next());
            }
        }
        return rows;
    }

    private static class TestingLogScanner implements LogScanner {

        private final Queue<ScanRecords> records = new ArrayDeque<>();

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
            // do nothing
        }
    }
}

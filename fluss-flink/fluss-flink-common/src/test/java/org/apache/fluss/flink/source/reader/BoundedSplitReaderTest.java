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

package org.apache.fluss.flink.source.reader;

import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.testutils.DataTestUtils.indexedRow;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link BoundedSplitReader}. */
class BoundedSplitReaderTest {

    @Test
    void testReadWithOutSkip() throws IOException {
        List<InternalRow> rows = mockRows(10);
        TestingBatchScanner scanner = new TestingBatchScanner(rows);
        BoundedSplitReader reader = new BoundedSplitReader(scanner, 0);

        List<RecordAndPos> records = collectRecords(reader);
        assertRecords(records, rows, 1);
    }

    @Test
    void testReadWithSkip() throws IOException {
        List<InternalRow> rows = mockRows(20);
        TestingBatchScanner scanner = new TestingBatchScanner(rows);
        BoundedSplitReader reader = new BoundedSplitReader(scanner, 10);

        List<RecordAndPos> records = collectRecords(reader);
        assertRecords(records, rows.subList(10, rows.size()), 11);

        // skip all
        scanner = new TestingBatchScanner(rows);
        reader = new BoundedSplitReader(scanner, 20);
        records = collectRecords(reader);
        assertThat(records).isEmpty();
    }

    @Test
    void testReadWithSkipOverTotalRecordsNum() {
        List<InternalRow> rows = mockRows(10);
        TestingBatchScanner scanner = new TestingBatchScanner(rows);
        BoundedSplitReader snapshotReader = new BoundedSplitReader(scanner, 11);

        assertThatThrownBy(() -> collectRecords(snapshotReader))
                .isInstanceOf(RuntimeException.class)
                .hasMessage(
                        "Skip more than the number of total records, has skipped 10 record(s), but remain 1 record(s) to skip.");
    }

    @Test
    void testSizeInBytesWithIndexedRow() throws IOException {
        // Use IndexedRow which implements MemoryAwareGetters
        List<InternalRow> rows = mockIndexedRows(DATA1_ROW_TYPE, 5);
        TestingBatchScanner scanner = new TestingBatchScanner(rows);
        BoundedSplitReader reader = new BoundedSplitReader(scanner, 0);

        List<RecordAndPos> records = collectRecords(reader);
        assertThat(records).hasSize(5);

        for (int i = 0; i < records.size(); i++) {
            RecordAndPos recordAndPos = records.get(i);
            IndexedRow expectedRow = (IndexedRow) rows.get(i);
            // sizeInBytes should be extracted from IndexedRow
            assertThat(recordAndPos.record().getSizeInBytes())
                    .isEqualTo(expectedRow.getSizeInBytes());
        }
    }

    @Test
    void testSizeInBytesWithProjectedIndexedRow() throws IOException {
        // Use ProjectedRow wrapping IndexedRow
        List<IndexedRow> indexedRows = new ArrayList<>();
        List<InternalRow> projectedRows = new ArrayList<>();
        int[] projection = new int[] {1, 0}; // project columns in reverse order
        ProjectedRow projectedRow = ProjectedRow.from(projection);

        for (int i = 0; i < 5; i++) {
            IndexedRow row = indexedRow(DATA1_ROW_TYPE, new Object[] {i, "test" + i});
            indexedRows.add(row);
            projectedRows.add(projectedRow.replaceRow(row));
        }

        TestingBatchScanner scanner = new TestingBatchScanner(projectedRows);
        BoundedSplitReader reader = new BoundedSplitReader(scanner, 0);

        List<RecordAndPos> records = collectRecords(reader);
        assertThat(records).hasSize(5);

        for (int i = 0; i < records.size(); i++) {
            RecordAndPos recordAndPos = records.get(i);
            // sizeInBytes should be extracted from the underlying IndexedRow
            assertThat(recordAndPos.record().getSizeInBytes())
                    .isEqualTo(indexedRows.get(i).getSizeInBytes());
        }
    }

    @Test
    void testSizeInBytesWithGenericRow() throws IOException {
        // GenericRow does not implement MemoryAwareGetters
        List<InternalRow> rows = mockRows(5);
        TestingBatchScanner scanner = new TestingBatchScanner(rows);
        BoundedSplitReader reader = new BoundedSplitReader(scanner, 0);

        List<RecordAndPos> records = collectRecords(reader);
        assertThat(records).hasSize(5);

        for (RecordAndPos recordAndPos : records) {
            // sizeInBytes should fall back to -1 for GenericRow
            assertThat(recordAndPos.record().getSizeInBytes()).isEqualTo(-1);
        }
    }

    /** A testing {@link BatchScanner} with static returned records. */
    private static class TestingBatchScanner implements BatchScanner {

        private final CloseableIterator<InternalRow> rowIterator;

        public TestingBatchScanner(List<InternalRow> rows) {
            this.rowIterator = CloseableIterator.wrap(rows.iterator());
        }

        @Override
        @Nullable
        public CloseableIterator<InternalRow> pollBatch(Duration timeout) {
            return rowIterator.hasNext() ? rowIterator : null;
        }

        @Override
        public void close() throws IOException {
            // do nothing
        }
    }

    private List<InternalRow> mockRows(int numRows) {
        List<InternalRow> rows = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; i++) {
            rows.add(row(i, "test" + i));
        }
        return rows;
    }

    private List<InternalRow> mockIndexedRows(RowType rowType, int numRows) {
        List<InternalRow> rows = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; i++) {
            rows.add(indexedRow(rowType, new Object[] {i, "test" + i}));
        }
        return rows;
    }

    private void assertRecords(
            List<RecordAndPos> readRecords, List<InternalRow> expectedRows, int startRecordCount) {
        // check records num
        assertThat(readRecords.size()).isEqualTo(expectedRows.size());

        for (int i = 0; i < readRecords.size(); i++) {
            RecordAndPos recordAndPos = readRecords.get(i);

            // check record
            assertThat(recordAndPos.record().getRow()).isEqualTo(expectedRows.get(i));

            // check pos
            assertThat(recordAndPos.readRecordsCount()).isEqualTo(startRecordCount + i);
        }
    }

    private List<RecordAndPos> collectRecords(BoundedSplitReader reader) throws IOException {
        List<RecordAndPos> records = new ArrayList<>();
        while (true) {
            CloseableIterator<RecordAndPos> recordIter = reader.readBatch();
            if (recordIter == null) {
                break;
            }
            while (recordIter.hasNext()) {
                RecordAndPos recordAndPos = recordIter.next();
                records.add(
                        new RecordAndPos(recordAndPos.scanRecord, recordAndPos.readRecordsCount));
            }
            recordIter.close();
        }
        return records;
    }
}

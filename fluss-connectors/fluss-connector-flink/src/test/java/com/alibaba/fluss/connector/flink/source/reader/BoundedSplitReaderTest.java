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

import com.alibaba.fluss.client.table.scanner.batch.BatchScanner;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.fluss.testutils.DataTestUtils.row;
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

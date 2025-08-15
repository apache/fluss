/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.lake.reader;

import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.ProjectedRow;
import com.alibaba.fluss.types.IntType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.StringType;
import com.alibaba.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SortMergeReader}. */
public class SortMergeReaderTest {

    private static class FlussRowComparator implements Comparator<InternalRow> {

        private final int keyIndex;

        public FlussRowComparator(int keyIndex) {
            this.keyIndex = keyIndex;
        }

        @Override
        public int compare(InternalRow o1, InternalRow o2) {
            int compare = o1.getInt(keyIndex) - o2.getInt(keyIndex);
            return compare;
        }
    }

    @Test
    void testReadBatch() {
        int keyIndex = 0;
        int[] pkIndexes = new int[] {keyIndex};
        List<LogRecord> logRecords1 = createRecords(0, 10, false);
        List<LogRecord> logRecords2 = createRecords(10, 10, false);
        List<KeyValueRow> logRecords3 =
                createRecords(5, 10, true).stream()
                        .map(logRecord -> new KeyValueRow(pkIndexes, logRecord.getRow(), false))
                        .collect(Collectors.toList());

        SortMergeReader sortMergeReader =
                new SortMergeReader(
                        null,
                        new int[] {keyIndex},
                        Arrays.asList(
                                CloseableIterator.wrap(logRecords2.iterator()),
                                CloseableIterator.wrap(logRecords1.iterator())),
                        new FlussRowComparator(keyIndex),
                        CloseableIterator.wrap(logRecords3.iterator()));

        List<InternalRow> actualRows = new ArrayList<>();
        InternalRow.FieldGetter[] fieldGetters =
                InternalRow.createFieldGetters(
                        RowType.of(new IntType(), new StringType(), new StringType()));
        try (CloseableIterator<InternalRow> iterator = sortMergeReader.readBatch()) {
            actualRows.addAll(materializeRows(iterator, fieldGetters));
        }
        assertThat(actualRows).hasSize(20);
        List<LogRecord> excepted = createRecords(0, 5, false);
        excepted.addAll(createRecords(5, 10, true));
        excepted.addAll(createRecords(15, 5, false));
        assertThat(actualRows)
                .isEqualTo(
                        materializeRows(
                                CloseableIterator.wrap(
                                        excepted.stream().map(LogRecord::getRow).iterator()),
                                fieldGetters));
    }

    @Test
    void testReadBatchWithProjectedFields() {
        int keyIndex = 0;
        int[] projectedFields = new int[] {keyIndex, 1};
        int[] pkIndexes = new int[] {keyIndex};
        List<LogRecord> logRecords1 = createRecords(0, 10, false);
        List<LogRecord> logRecords2 = createRecords(10, 10, false);
        List<KeyValueRow> logRecords3 =
                createRecords(5, 10, true).stream()
                        .map(logRecord -> new KeyValueRow(pkIndexes, logRecord.getRow(), false))
                        .collect(Collectors.toList());

        SortMergeReader sortMergeReader =
                new SortMergeReader(
                        projectedFields,
                        new int[] {keyIndex},
                        Arrays.asList(
                                CloseableIterator.wrap(logRecords2.iterator()),
                                CloseableIterator.wrap(logRecords1.iterator())),
                        new FlussRowComparator(keyIndex),
                        CloseableIterator.wrap(logRecords3.iterator()));

        List<InternalRow> actualRows = new ArrayList<>();
        InternalRow.FieldGetter[] fieldGetters =
                InternalRow.createFieldGetters(RowType.of(new IntType(), new StringType()));
        try (CloseableIterator<InternalRow> iterator = sortMergeReader.readBatch()) {
            actualRows.addAll(materializeRows(iterator, fieldGetters));
        }
        assertThat(actualRows).hasSize(20);
        List<LogRecord> excepted = createRecords(0, 5, false);
        excepted.addAll(createRecords(5, 10, true));
        excepted.addAll(createRecords(15, 5, false));
        ProjectedRow projectedRow = ProjectedRow.from(projectedFields);
        assertThat(actualRows)
                .isEqualTo(
                        materializeRows(
                                projected(
                                        CloseableIterator.wrap(excepted.iterator()), projectedRow),
                                fieldGetters));
    }

    private CloseableIterator<InternalRow> projected(
            CloseableIterator<LogRecord> originElementIterator, final ProjectedRow projectedRow) {
        return new CloseableIterator<InternalRow>() {
            private final CloseableIterator<LogRecord> inner = originElementIterator;

            @Override
            public void close() {
                inner.close();
            }

            @Override
            public boolean hasNext() {
                return inner.hasNext();
            }

            @Override
            public InternalRow next() {
                LogRecord element = inner.next();
                return projectedRow.replaceRow(element.getRow());
            }
        };
    }

    private List<InternalRow> materializeRows(
            CloseableIterator<InternalRow> iterator, InternalRow.FieldGetter[] fieldGetters) {
        List<InternalRow> actualRows = new ArrayList<>();
        while (iterator != null && iterator.hasNext()) {
            InternalRow row = iterator.next();
            GenericRow genericRow = new GenericRow(fieldGetters.length);
            for (int i = 0; i < fieldGetters.length; i++) {
                genericRow.setField(i, fieldGetters[i].getFieldOrNull(row));
            }
            actualRows.add(genericRow);
        }
        return actualRows;
    }

    private List<LogRecord> createRecords(int startId, int count, boolean isLog) {
        List<LogRecord> logRecords = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            GenericRow row =
                    row(
                            startId + i,
                            BinaryString.fromString(isLog ? "a" + "_updated" : "a"),
                            BinaryString.fromString(isLog ? "A" + "_updated" : "A"));
            logRecords.add(new ScanRecord(i, System.currentTimeMillis(), ChangeType.INSERT, row));
        }
        return logRecords;
    }
}

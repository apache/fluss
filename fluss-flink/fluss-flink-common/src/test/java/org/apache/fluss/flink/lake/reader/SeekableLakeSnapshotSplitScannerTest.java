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

package org.apache.fluss.flink.lake.reader;

import org.apache.fluss.flink.source.reader.BoundedSplitReader;
import org.apache.fluss.flink.source.reader.RecordAndPos;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.TestingLakeSource;
import org.apache.fluss.lake.source.TestingLakeSplit;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests snapshot scanning past empty lake splits and from restored split positions. */
class SeekableLakeSnapshotSplitScannerTest {

    static Stream<int[]> splitSizes() {
        return Stream.of(
                new int[] {},
                new int[] {0, 0, 0},
                new int[] {0, 0, 2},
                new int[] {2, 0, 0, 3},
                new int[] {2, 0, 0},
                new int[] {1, 2, 3});
    }

    @ParameterizedTest
    @MethodSource("splitSizes")
    void testScanAllSplitsAndRestoredPositions(int[] sizes) throws Exception {
        for (int start = 0; start <= sizes.length; start++) {
            RowsPerSplitLakeSource source = new RowsPerSplitLakeSource(sizes);
            List<Integer> actual = new ArrayList<>();
            try (SeekableLakeSnapshotSplitScanner scanner =
                    new SeekableLakeSnapshotSplitScanner(source, source.splits(), start)) {
                CloseableIterator<InternalRow> batch;
                while ((batch = scanner.pollBatch(Duration.ZERO)) != null) {
                    int index = ((IndexedLakeSplitRecordIterator) batch).getCurrentLakeSplitIndex();
                    // the lake splits before the current one, empty or not, are closed
                    assertThat(source.openSplits()).containsExactly(index);
                    while (batch.hasNext()) {
                        assertThat(batch.next().getInt(0)).isEqualTo(index);
                        actual.add(index);
                    }
                }
                assertThat(scanner.pollBatch(Duration.ZERO)).isNull();
            }
            List<Integer> expected = new ArrayList<>();
            List<Integer> expectedOpened = new ArrayList<>();
            for (int index = start; index < sizes.length; index++) {
                expected.addAll(Collections.nCopies(sizes[index], index));
                expectedOpened.add(index);
            }
            assertThat(actual)
                    .as("sizes=%s, start=%s", Arrays.toString(sizes), start)
                    .containsExactlyElementsOf(expected);
            assertThat(source.closeCounts.keySet()).containsExactlyElementsOf(expectedOpened);
            assertThat(source.closeCounts.values()).allMatch(count -> count == 1);
        }
    }

    @Test
    void testRestoreClosesSkippedLakeSplits() throws Exception {
        // a position from a checkpoint taken before positions were split-local: index 0 and the
        // records read across all lake splits, so splits 0 and 1 are skipped on restore
        RowsPerSplitLakeSource source = new RowsPerSplitLakeSource(2, 0, 2, 3);
        List<Integer> actual = new ArrayList<>();
        try (BoundedSplitReader reader =
                new BoundedSplitReader(
                        new SeekableLakeSnapshotSplitScanner(source, source.splits(), 0), 3)) {
            CloseableIterator<RecordAndPos> batch;
            while ((batch = reader.readBatch()) != null) {
                while (batch.hasNext()) {
                    RecordAndPos recordAndPos = batch.next();
                    // the lake splits skipped on restore are closed
                    assertThat(source.openSplits())
                            .containsExactly(recordAndPos.getCurrentSplitIndex());
                    actual.add(recordAndPos.getCurrentSplitIndex());
                }
                batch.close();
            }
        }
        assertThat(actual).containsExactly(2, 3, 3, 3);
        assertThat(source.closeCounts).containsOnlyKeys(0, 1, 2, 3);
        assertThat(source.closeCounts.values()).allMatch(count -> count == 1);
    }

    /**
     * A testing lake source whose lake split {@code i} holds {@code sizes[i]} rows, tracking how
     * often the record iterator of each lake split is closed.
     */
    private static class RowsPerSplitLakeSource extends TestingLakeSource {
        private final int[] sizes;
        private final Map<Integer, Integer> closeCounts = new LinkedHashMap<>();

        private RowsPerSplitLakeSource(int... sizes) {
            this.sizes = sizes;
        }

        private List<LakeSplit> splits() {
            List<LakeSplit> splits = new ArrayList<>();
            for (int index = 0; index < sizes.length; index++) {
                // the bucket identifies the lake split
                splits.add(new TestingLakeSplit(index, Collections.emptyList()));
            }
            return splits;
        }

        private List<Integer> openSplits() {
            List<Integer> openSplits = new ArrayList<>();
            closeCounts.forEach(
                    (index, count) -> {
                        if (count == 0) {
                            openSplits.add(index);
                        }
                    });
            return openSplits;
        }

        @Override
        public RecordReader createRecordReader(ReaderContext<LakeSplit> context) {
            int index = context.lakeSplit().bucket();
            List<LogRecord> rows = new ArrayList<>();
            for (int row = 0; row < sizes[index]; row++) {
                rows.add(new GenericRecord(-1, -1, ChangeType.INSERT, GenericRow.of(index)));
            }
            closeCounts.put(index, 0);
            CloseableIterator<LogRecord> iterator = CloseableIterator.wrap(rows.iterator());
            return () ->
                    new CloseableIterator<LogRecord>() {
                        @Override
                        public boolean hasNext() {
                            return iterator.hasNext();
                        }

                        @Override
                        public LogRecord next() {
                            return iterator.next();
                        }

                        @Override
                        public void close() {
                            closeCounts.merge(index, 1, Integer::sum);
                        }
                    };
        }
    }
}

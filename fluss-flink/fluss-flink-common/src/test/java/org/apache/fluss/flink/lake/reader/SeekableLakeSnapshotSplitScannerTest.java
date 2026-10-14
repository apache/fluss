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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
            List<LakeSplit> splits = new ArrayList<>();
            for (int index = 0; index < sizes.length; index++) {
                splits.add(new TestingLakeSplit(index, Collections.emptyList()));
            }
            TestingLakeSource source =
                    new TestingLakeSource() {
                        @Override
                        public RecordReader createRecordReader(ReaderContext<LakeSplit> context) {
                            int index = context.lakeSplit().bucket();
                            List<LogRecord> rows = new ArrayList<>();
                            for (int row = 0; row < sizes[index]; row++) {
                                rows.add(
                                        new GenericRecord(
                                                -1, -1, ChangeType.INSERT, GenericRow.of(index)));
                            }
                            return () -> CloseableIterator.wrap(rows.iterator());
                        }
                    };
            List<Integer> actual = new ArrayList<>();
            try (SeekableLakeSnapshotSplitScanner scanner =
                    new SeekableLakeSnapshotSplitScanner(source, splits, start)) {
                CloseableIterator<InternalRow> batch;
                while ((batch = scanner.pollBatch(Duration.ZERO)) != null) {
                    int index = ((IndexedLakeSplitRecordIterator) batch).getCurrentLakeSplitIndex();
                    while (batch.hasNext()) {
                        assertThat(batch.next().getInt(0)).isEqualTo(index);
                        actual.add(index);
                    }
                }
                assertThat(scanner.pollBatch(Duration.ZERO)).isNull();
            }
            List<Integer> expected = new ArrayList<>();
            for (int index = start; index < sizes.length; index++) {
                expected.addAll(Collections.nCopies(sizes[index], index));
            }
            assertThat(actual)
                    .as("sizes=%s, start=%s", Arrays.toString(sizes), start)
                    .containsExactlyElementsOf(expected);
        }
    }
}

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

package org.apache.fluss.flink.lake.reader;

import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.TestingLakeSource;
import org.apache.fluss.lake.source.TestingLakeSplit;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for reading all lake splits, including splits made empty by filtering. */
class SeekableLakeSnapshotSplitScannerTest {

    @Test
    void testEmptySplitsDoNotFinishSnapshotEarly() throws Exception {
        TestingLakeSource source =
                new TestingLakeSource() {
                    @Override
                    public RecordReader createRecordReader(ReaderContext<LakeSplit> context) {
                        String partition = context.lakeSplit().partition().get(0);
                        if (partition.equals("empty")) {
                            return CloseableIterator::emptyIterator;
                        }
                        LogRecord record = mock(LogRecord.class);
                        when(record.getRow()).thenReturn(row(partition));
                        return () ->
                                CloseableIterator.wrap(Collections.singleton(record).iterator());
                    }
                };
        try (SeekableLakeSnapshotSplitScanner scanner =
                new SeekableLakeSnapshotSplitScanner(
                        source,
                        Arrays.asList(
                                split("empty"),
                                split("empty"),
                                split("a"),
                                split("empty"),
                                split("b"),
                                split("empty")),
                        0)) {
            try (CloseableIterator<InternalRow> first = scanner.pollBatch(Duration.ZERO)) {
                assertThat(first).isNotNull();
                assertThat(first.next().getString(0).toString()).isEqualTo("a");
                assertThat(first.hasNext()).isFalse();
            }
            try (CloseableIterator<InternalRow> second = scanner.pollBatch(Duration.ZERO)) {
                assertThat(second).isNotNull();
                assertThat(second.next().getString(0).toString()).isEqualTo("b");
                assertThat(second.hasNext()).isFalse();
            }
            assertThat(scanner.pollBatch(Duration.ZERO)).isNull();
        }
    }

    private static LakeSplit split(String partition) {
        return new TestingLakeSplit(0, Collections.singletonList(partition));
    }
}

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
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests cleanup of returned, skipped, and empty lake snapshot iterators. */
class LakeSnapshotIteratorCleanupTest {
    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 4, 5})
    void testCleanupThroughBoundedReader(int toSkip) throws Exception {
        List<TrackingIterator> opened = new ArrayList<>();
        int[] sizes = {0, 2, 3, 0};
        List<LakeSplit> splits = new ArrayList<>();
        for (int i = 0; i < sizes.length; i++) {
            splits.add(new TestingLakeSplit(i, Collections.emptyList()));
        }
        TestingLakeSource source =
                new TestingLakeSource() {
                    @Override
                    public RecordReader createRecordReader(ReaderContext<LakeSplit> context) {
                        TrackingIterator iterator =
                                new TrackingIterator(sizes[context.lakeSplit().bucket()]);
                        opened.add(iterator);
                        return () -> iterator;
                    }
                };
        int count = 0;
        try (BoundedSplitReader reader =
                new BoundedSplitReader(
                        new SeekableLakeSnapshotSplitScanner(source, splits, 0), toSkip)) {
            CloseableIterator<RecordAndPos> batch;
            while ((batch = reader.readBatch()) != null) {
                try (CloseableIterator<RecordAndPos> records = batch) {
                    while (records.hasNext()) {
                        records.next();
                        count++;
                    }
                }
            }
            assertThat(reader.readBatch()).isNull();
        }
        assertThat(count).isEqualTo(5 - toSkip);
        assertThat(opened).hasSize(sizes.length);
        opened.forEach(iterator -> assertThat(iterator.closeCalls).isEqualTo(1));
    }

    @Test
    void testCloseFailureIsNotRetried() {
        TrackingIterator underlying = new TrackingIterator(1);
        underlying.failClose = true;
        IndexedLakeSplitRecordIterator iterator = new IndexedLakeSplitRecordIterator(underlying, 0);
        assertThatThrownBy(iterator::close)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("close failed");
        assertThat(iterator.hasNext()).isFalse();
        iterator.close();
        assertThat(underlying.closeCalls).isEqualTo(1);
    }

    @Test
    void testScannerClosesPartiallyConsumedIteratorOnce() throws Exception {
        TrackingIterator underlying = new TrackingIterator(2);
        TestingLakeSource source =
                new TestingLakeSource() {
                    @Override
                    public RecordReader createRecordReader(ReaderContext<LakeSplit> context) {
                        return () -> underlying;
                    }
                };
        SeekableLakeSnapshotSplitScanner scanner =
                new SeekableLakeSnapshotSplitScanner(
                        source,
                        Collections.singletonList(new TestingLakeSplit(0, Collections.emptyList())),
                        0);
        CloseableIterator<?> batch = scanner.pollBatch(Duration.ZERO);
        batch.next();
        scanner.close();
        batch.close();
        scanner.close();
        assertThat(batch.hasNext()).isFalse();
        assertThat(underlying.closeCalls).isEqualTo(1);
    }

    private static class TrackingIterator implements CloseableIterator<LogRecord> {
        private int remaining;
        private int closeCalls;
        private boolean failClose;

        private TrackingIterator(int remaining) {
            this.remaining = remaining;
        }

        @Override
        public void close() {
            closeCalls++;
            assertThat(closeCalls).as("underlying close attempts").isEqualTo(1);
            if (failClose) {
                throw new IllegalStateException("close failed");
            }
        }

        @Override
        public boolean hasNext() {
            assertThat(closeCalls).as("hasNext must not access a closed iterator").isZero();
            return remaining > 0;
        }

        @Override
        public LogRecord next() {
            remaining--;
            return new GenericRecord(-1, -1, ChangeType.INSERT, GenericRow.of(remaining));
        }
    }
}

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

import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;

/** A scanner for reading lake split {@link LakeSnapshotSplit}. */
public class LakeSnapshotScanner implements BatchScanner {

    private final LakeSource<LakeSplit> lakeSource;
    private final LakeSnapshotSplit lakeSnapshotSplit;

    private CloseableIterator<InternalRow> rowsIterator;

    public LakeSnapshotScanner(
            LakeSource<LakeSplit> lakeSource, LakeSnapshotSplit lakeSnapshotSplit) {
        this.lakeSource = lakeSource;
        this.lakeSnapshotSplit = lakeSnapshotSplit;
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (rowsIterator == null) {
            rowsIterator =
                    InternalRowIterator.wrap(
                            lakeSource
                                    .createRecordReader(
                                            (LakeSource.ReaderContext<LakeSplit>)
                                                    lakeSnapshotSplit::getLakeSplit)
                                    .read());
        }
        return rowsIterator.hasNext() ? rowsIterator : null;
    }

    @Override
    public void close() throws IOException {
        if (rowsIterator != null) {
            rowsIterator.close();
        }
    }

    private static class InternalRowIterator implements CloseableIterator<InternalRow> {

        private final CloseableIterator<LogRecord> recordCloseableIterator;

        private static InternalRowIterator wrap(
                CloseableIterator<LogRecord> recordCloseableIterator) {
            return new InternalRowIterator(recordCloseableIterator);
        }

        private InternalRowIterator(CloseableIterator<LogRecord> recordCloseableIterator) {
            this.recordCloseableIterator = recordCloseableIterator;
        }

        @Override
        public void close() {
            recordCloseableIterator.close();
        }

        @Override
        public boolean hasNext() {
            return recordCloseableIterator.hasNext();
        }

        @Override
        public InternalRow next() {
            return recordCloseableIterator.next().getRow();
        }
    }
}

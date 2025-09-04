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

import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

/**
 * A scanner that supports seeking to a specific LakeSplit and reading from that point.
 *
 * <p>This scanner enables direct positioning to any LakeSplit in the list using its index, and then
 * sequentially reads data starting from that specific split. It provides fine-grained control over
 * where to begin the scanning process.
 *
 * <p>Key capabilities:
 *
 * <ul>
 *   <li>Direct seeking to any LakeSplit by index
 *   <li>Sequential reading starting from the sought split
 *   <li>Precise positioning within the split collection
 *   <li>Resumable scanning from arbitrary positions
 * </ul>
 */
public class SeekableLakeSnapshotSplitScanner implements BatchScanner {

    private final List<LakeSplit> lakeSplits;
    private int nextLakeSplitIndex;
    private final LakeSource<LakeSplit> lakeSource;
    private CloseableIterator<InternalRow> currentLakeRecordIterator;

    public SeekableLakeSnapshotSplitScanner(
            LakeSource<LakeSplit> lakeSource,
            List<LakeSplit> lakeSplits,
            int currentLakeSplitIndex) {
        // add lake splits
        this.lakeSplits = lakeSplits;
        this.nextLakeSplitIndex = currentLakeSplitIndex;
        this.lakeSource = lakeSource;
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (currentLakeRecordIterator == null) {
            updateCurrentIterator();
        }

        // has no next record in currentIterator, update currentIterator
        if (currentLakeRecordIterator != null && !currentLakeRecordIterator.hasNext()) {
            updateCurrentIterator();
        }

        return currentLakeRecordIterator != null && currentLakeRecordIterator.hasNext()
                ? currentLakeRecordIterator
                : null;
    }

    private void updateCurrentIterator() throws IOException {
        if (lakeSplits.size() <= nextLakeSplitIndex) {
            currentLakeRecordIterator = null;
        } else {
            int currentLakeSplitIndex = nextLakeSplitIndex;
            LakeSplit split = lakeSplits.get(currentLakeSplitIndex);
            CloseableIterator<LogRecord> lakeRecords =
                    lakeSource
                            .createRecordReader((LakeSource.ReaderContext<LakeSplit>) () -> split)
                            .read();
            currentLakeRecordIterator =
                    new IndexedLakeSplitRecordIterator(lakeRecords, currentLakeSplitIndex);
            nextLakeSplitIndex += 1;
        }
    }

    @Override
    public void close() throws IOException {
        if (currentLakeRecordIterator != null) {
            currentLakeRecordIterator.close();
            currentLakeRecordIterator = null;
        }
    }
}

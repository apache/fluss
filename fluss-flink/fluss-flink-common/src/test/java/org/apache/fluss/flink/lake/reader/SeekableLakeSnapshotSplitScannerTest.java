/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.lake.reader;

import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link SeekableLakeSnapshotSplitScanner}. */
class SeekableLakeSnapshotSplitScannerTest {

    @Test
    void testSkipEmptyLakeSplits() throws Exception {
        LakeSplit firstSplit = mock(LakeSplit.class);
        LakeSplit emptySplit = mock(LakeSplit.class);
        LakeSplit lastSplit = mock(LakeSplit.class);
        InternalRow firstRow = row(1, "first");
        InternalRow lastRow = row(2, "last");

        Map<LakeSplit, List<InternalRow>> rowsBySplit = new HashMap<>();
        rowsBySplit.put(firstSplit, Arrays.asList(firstRow));
        rowsBySplit.put(emptySplit, new ArrayList<>());
        rowsBySplit.put(lastSplit, Arrays.asList(lastRow));

        LakeSource<LakeSplit> lakeSource = createLakeSource(rowsBySplit);
        SeekableLakeSnapshotSplitScanner scanner =
                new SeekableLakeSnapshotSplitScanner(
                        lakeSource, Arrays.asList(firstSplit, emptySplit, lastSplit), 0);

        List<InternalRow> actualRows = new ArrayList<>();
        CloseableIterator<InternalRow> batch;
        while ((batch = scanner.pollBatch(Duration.ZERO)) != null) {
            while (batch.hasNext()) {
                actualRows.add(batch.next());
            }
            batch.close();
        }
        scanner.close();

        assertThat(actualRows).containsExactly(firstRow, lastRow);
    }

    @SuppressWarnings("unchecked")
    private LakeSource<LakeSplit> createLakeSource(Map<LakeSplit, List<InternalRow>> rowsBySplit)
            throws Exception {
        LakeSource<LakeSplit> lakeSource = mock(LakeSource.class);
        when(lakeSource.createRecordReader(any()))
                .thenAnswer(
                        invocation -> {
                            LakeSource.ReaderContext<LakeSplit> context = invocation.getArgument(0);
                            List<LogRecord> records = new ArrayList<>();
                            for (InternalRow row : rowsBySplit.get(context.lakeSplit())) {
                                records.add(new GenericRecord(0, 0, ChangeType.INSERT, row));
                            }
                            RecordReader reader = () -> CloseableIterator.wrap(records.iterator());
                            return reader;
                        });
        return lakeSource;
    }
}

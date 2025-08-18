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

import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.batch.BatchScanner;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import com.alibaba.fluss.lake.source.LakeSource;
import com.alibaba.fluss.lake.source.LakeSplit;
import com.alibaba.fluss.lake.source.RecordReader;
import com.alibaba.fluss.lake.source.SortedRecordReader;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** A scanner to merge the lakehouse's snapshot and change log. */
public class LakeSnapshotAndLogSplitScanner implements BatchScanner {

    private final LakeSnapshotAndFlussLogSplit lakeSnapshotSplitAndFlussLogSplit;
    private Comparator<InternalRow> rowComparator;
    private List<CloseableIterator<LogRecord>> lakeRecordIterators = new ArrayList<>();
    private final LakeSource<LakeSplit> lakeSource;

    private final int[] pkIndexes;

    // the indexes of primary key in emitted row by paimon and fluss
    private int[] keyIndexesInRow;
    @Nullable private int[] adjustProjectedFields;
    private final int[] newProjectedFields;

    // the sorted logs in memory, mapping from key -> value
    private Map<InternalRow, KeyValueRow> logRows;

    private final LogScanner logScanner;
    private final long stoppingOffset;
    private boolean logScanFinished;

    private SortMergeReader currentSortMergeReader;

    public LakeSnapshotAndLogSplitScanner(
            Table table,
            LakeSource<LakeSplit> lakeSource,
            LakeSnapshotAndFlussLogSplit lakeSnapshotAndFlussLogSplit,
            @Nullable int[] projectedFields) {
        this.pkIndexes = table.getTableInfo().getSchema().getPrimaryKeyIndexes();
        this.lakeSnapshotSplitAndFlussLogSplit = lakeSnapshotAndFlussLogSplit;
        this.lakeSource = lakeSource;
        this.newProjectedFields = getNeedProjectFields(table, projectedFields);

        this.logScanner = table.newScan().project(newProjectedFields).createLogScanner();
        this.lakeSource.withProject(
                Arrays.stream(newProjectedFields)
                        .mapToObj(field -> new int[] {field})
                        .toArray(int[][]::new));

        TableBucket tableBucket = lakeSnapshotAndFlussLogSplit.getTableBucket();
        if (tableBucket.getPartitionId() != null) {
            this.logScanner.subscribe(
                    tableBucket.getPartitionId(),
                    tableBucket.getBucket(),
                    lakeSnapshotAndFlussLogSplit.getStartingOffset());
        } else {
            this.logScanner.subscribe(
                    tableBucket.getBucket(), lakeSnapshotAndFlussLogSplit.getStartingOffset());
        }

        this.stoppingOffset =
                lakeSnapshotAndFlussLogSplit
                        .getStoppingOffset()
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "StoppingOffset is null for split: "
                                                        + lakeSnapshotAndFlussLogSplit));

        this.logScanFinished = lakeSnapshotAndFlussLogSplit.getStartingOffset() >= stoppingOffset;
    }

    private int[] getNeedProjectFields(Table flussTable, @Nullable int[] projectedFields) {
        if (projectedFields != null) {
            // we need to include the primary key in projected fields to sort merge by pk
            // if the provided don't include, we need to include it
            List<Integer> newProjectedFields =
                    Arrays.stream(projectedFields).boxed().collect(Collectors.toList());

            // the indexes of primary key with new projected fields
            keyIndexesInRow = new int[pkIndexes.length];
            for (int i = 0; i < pkIndexes.length; i++) {
                int primaryKeyIndex = pkIndexes[i];
                // search the pk in projected fields
                int indexInProjectedFields = findIndex(projectedFields, primaryKeyIndex);
                if (indexInProjectedFields >= 0) {
                    keyIndexesInRow[i] = indexInProjectedFields;
                } else {
                    // no pk in projected fields, we must include it to do
                    // merge sort
                    newProjectedFields.add(primaryKeyIndex);
                    keyIndexesInRow[i] = newProjectedFields.size() - 1;
                }
            }
            int[] newProjection = newProjectedFields.stream().mapToInt(Integer::intValue).toArray();
            // the underlying scan will use the new projection to scan data,
            // but will still need to map from the new projection to the origin projected fields
            int[] adjustProjectedFields = new int[projectedFields.length];
            for (int i = 0; i < projectedFields.length; i++) {
                adjustProjectedFields[i] = findIndex(newProjection, projectedFields[i]);
            }
            this.adjustProjectedFields = adjustProjectedFields;
            return newProjection;
        } else {
            // no projectedFields, use all fields
            keyIndexesInRow = pkIndexes;
            return IntStream.range(0, flussTable.getTableInfo().getRowType().getFieldCount())
                    .toArray();
        }
    }

    private int findIndex(int[] array, int target) {
        int index = -1;
        for (int i = 0; i < array.length; i++) {
            if (array[i] == target) {
                index = i;
                break;
            }
        }
        return index;
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (logScanFinished) {
            if (lakeRecordIterators.isEmpty()) {
                if (lakeSnapshotSplitAndFlussLogSplit.getLakeSplits() == null
                        || lakeSnapshotSplitAndFlussLogSplit.getLakeSplits().isEmpty()) {
                    lakeRecordIterators = Collections.emptyList();
                } else {
                    for (LakeSplit lakeSplit : lakeSnapshotSplitAndFlussLogSplit.getLakeSplits()) {
                        lakeRecordIterators.add(
                                lakeSource.createRecordReader(() -> lakeSplit).read());
                    }
                }
            }
            if (currentSortMergeReader == null) {
                currentSortMergeReader =
                        new SortMergeReader(
                                adjustProjectedFields,
                                keyIndexesInRow,
                                lakeRecordIterators,
                                rowComparator,
                                CloseableIterator.wrap(
                                        logRows == null
                                                ? Collections.emptyIterator()
                                                : logRows.values().iterator()));
            }
            return currentSortMergeReader.readBatch();
        } else {
            if (lakeRecordIterators.isEmpty()) {
                if (lakeSnapshotSplitAndFlussLogSplit.getLakeSplits() == null
                        || lakeSnapshotSplitAndFlussLogSplit.getLakeSplits().isEmpty()) {
                    lakeRecordIterators = Collections.emptyList();
                    logRows = new LinkedHashMap<>();
                } else {
                    for (LakeSplit lakeSplit : lakeSnapshotSplitAndFlussLogSplit.getLakeSplits()) {
                        RecordReader reader = lakeSource.createRecordReader(() -> lakeSplit);
                        if (reader instanceof SortedRecordReader) {
                            rowComparator = ((SortedRecordReader) reader).order();
                        } else {
                            throw new UnsupportedOperationException(
                                    "lake records must instance of sorted view.");
                        }
                        lakeRecordIterators.add(reader.read());
                    }
                    logRows = new TreeMap<>(rowComparator);
                }
            }
            pollLogRecords(timeout);
            return CloseableIterator.wrap(Collections.emptyIterator());
        }
    }

    private void pollLogRecords(Duration timeout) {
        ScanRecords scanRecords = logScanner.poll(timeout);
        for (ScanRecord scanRecord : scanRecords) {
            boolean isDelete =
                    scanRecord.getChangeType() == ChangeType.DELETE
                            || scanRecord.getChangeType() == ChangeType.UPDATE_BEFORE;
            KeyValueRow keyValueRow =
                    new KeyValueRow(keyIndexesInRow, scanRecord.getRow(), isDelete);
            InternalRow keyRow = keyValueRow.keyRow();
            // upsert the key value row
            logRows.put(keyRow, keyValueRow);
            if (scanRecord.logOffset() >= stoppingOffset - 1) {
                // has reached to the end
                logScanFinished = true;
                break;
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (logScanner != null) {
                logScanner.close();
            }
            if (lakeRecordIterators != null) {
                for (CloseableIterator<LogRecord> iterator : lakeRecordIterators) {
                    iterator.close();
                }
            }
        } catch (Exception e) {
            throw new IOException("Failed to close resources", e);
        }
    }
}

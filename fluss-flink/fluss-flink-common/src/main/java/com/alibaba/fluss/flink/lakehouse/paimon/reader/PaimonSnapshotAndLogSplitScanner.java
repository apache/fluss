/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.flink.lakehouse.paimon.reader;

import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.batch.BatchScanner;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.flink.lakehouse.paimon.split.PaimonSnapshotAndFlussLogSplit;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.utils.CloseableIterator;

import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.reader.EmptyRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.TableRead;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** A scanner to merge the paimon's snapshot and change log. */
public class PaimonSnapshotAndLogSplitScanner implements BatchScanner {

    private final TableRead tableRead;
    private final PaimonSnapshotAndFlussLogSplit snapshotAndFlussLogSplit;
    // the origin indexes of primary key in origin table
    private final int[] pkIndexes;

    // the indexes of primary key in emitted row by paimon and fluss
    private int[] keyIndexesInRow;
    private final Comparator<InternalRow> keyComparator;

    // the sorted logs in memory, mapping from key -> value
    private final SortedMap<InternalRow, KeyValueRow> logRows;

    private final LogScanner logScanner;
    private final long stoppingOffset;

    private boolean logScanFinished;
    private SortMergeReader currentSortMergeReader;
    private RecordReader<InternalRow> snapshotRecordReader;
    @Nullable private int[] adjustProjectedFields;

    public PaimonSnapshotAndLogSplitScanner(
            Table flussTable,
            FileStoreTable fileStoreTable,
            PaimonSnapshotAndFlussLogSplit snapshotAndFlussLogSplit,
            @Nullable int[] projectedFields) {
        this.pkIndexes = flussTable.getTableInfo().getSchema().getPrimaryKeyIndexes();
        int[] newProjectedFields = getNeedProjectFields(flussTable, projectedFields);
        this.tableRead =
                fileStoreTable.newReadBuilder().withProjection(newProjectedFields).newRead();
        this.snapshotAndFlussLogSplit = snapshotAndFlussLogSplit;
        this.keyComparator = ((KeyValueFileStore) fileStoreTable.store()).newKeyComparator();
        this.logRows = new TreeMap<>(keyComparator);
        this.logScanner = flussTable.newScan().project(newProjectedFields).createLogScanner();

        TableBucket tableBucket = snapshotAndFlussLogSplit.getTableBucket();
        if (tableBucket.getPartitionId() != null) {
            this.logScanner.subscribe(
                    tableBucket.getPartitionId(),
                    tableBucket.getBucket(),
                    snapshotAndFlussLogSplit.getStartingOffset());
        } else {
            this.logScanner.subscribe(
                    tableBucket.getBucket(), snapshotAndFlussLogSplit.getStartingOffset());
        }

        this.stoppingOffset =
                snapshotAndFlussLogSplit
                        .getStoppingOffset()
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "StoppingOffset is null for split: "
                                                        + snapshotAndFlussLogSplit));

        // starting offset is greater than or equal to stoppingOffset, no any log need to scan
        this.logScanFinished = snapshotAndFlussLogSplit.getStartingOffset() >= stoppingOffset;
    }

    @Override
    @Nullable
    public CloseableIterator<com.alibaba.fluss.row.InternalRow> pollBatch(Duration poolTimeOut)
            throws IOException {
        if (logScanFinished) {
            if (currentSortMergeReader == null) {
                currentSortMergeReader = createSortMergeReader();
            }
            return currentSortMergeReader.readBatch();
        } else {
            pollLogRecords(poolTimeOut);
            return CloseableIterator.wrap(Collections.emptyIterator());
        }
    }

    private int[] getNeedProjectFields(Table flussTable, @Nullable int[] originProjectedFields) {
        if (originProjectedFields != null) {
            // we need to include the primary key in projected fields to sort merge by pk
            // if the provided don't include, we need to include it
            List<Integer> newProjectedFields =
                    Arrays.stream(originProjectedFields).boxed().collect(Collectors.toList());

            // the indexes of primary key with new projected fields
            keyIndexesInRow = new int[pkIndexes.length];
            for (int i = 0; i < pkIndexes.length; i++) {
                int primaryKeyIndex = pkIndexes[i];
                // search the pk in projected fields
                int indexInProjectedFields = findIndex(originProjectedFields, primaryKeyIndex);
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
            int[] adjustProjectedFields = new int[originProjectedFields.length];
            for (int i = 0; i < originProjectedFields.length; i++) {
                adjustProjectedFields[i] = findIndex(newProjection, originProjectedFields[i]);
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

    private SortMergeReader createSortMergeReader() throws IOException {
        FileStoreSourceSplit fileStoreSourceSplit = snapshotAndFlussLogSplit.getSnapshotSplit();
        snapshotRecordReader =
                fileStoreSourceSplit == null
                        ? new EmptyRecordReader<>()
                        : tableRead.createReader(fileStoreSourceSplit.split());
        return new SortMergeReader(
                adjustProjectedFields,
                keyIndexesInRow,
                snapshotRecordReader,
                CloseableIterator.wrap(logRows.values().iterator()),
                keyComparator);
    }

    private void pollLogRecords(Duration timeout) {
        ScanRecords scanRecords = logScanner.poll(timeout);
        for (ScanRecord scanRecord : scanRecords) {
            InternalRow paimonRow = new ScanRecordWrapper(scanRecord);
            boolean isDelete =
                    scanRecord.getChangeType() == ChangeType.DELETE
                            || scanRecord.getChangeType() == ChangeType.UPDATE_BEFORE;
            KeyValueRow keyValueRow = new KeyValueRow(keyIndexesInRow, paimonRow, isDelete);
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
            if (snapshotRecordReader != null) {
                snapshotRecordReader.close();
                snapshotRecordReader = null;
            }
        } catch (Exception e) {
            throw new IOException("Failed to close resources", e);
        }
    }
}

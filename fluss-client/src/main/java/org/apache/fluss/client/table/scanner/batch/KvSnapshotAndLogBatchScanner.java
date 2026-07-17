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

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.SortMergeReader;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.KeyValueRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * A bounded scanner that merges an immutable KV snapshot with a fixed primary-key changelog range.
 */
@Internal
public class KvSnapshotAndLogBatchScanner implements BatchScanner {

    private final TableBucket tableBucket;
    private final long logStoppingOffset;
    private final int[] keyIndexesInScanRow;
    @Nullable private final int[] adjustProjectedFields;
    private final Comparator<InternalRow> primaryKeyComparator;
    private final Map<InternalRow, KeyValueRow> logRows;

    @Nullable private final BatchScanner snapshotScanner;
    @Nullable private final LogScanner logScanner;

    private boolean logScanFinished;
    private boolean snapshotIteratorInitialized;
    private boolean finished;
    private boolean closed;

    @Nullable private CloseableIterator<LogRecord> snapshotRecordIterator;
    @Nullable private SortMergeReader sortMergeReader;
    @Nullable private CloseableIterator<InternalRow> sortMergeIterator;

    public KvSnapshotAndLogBatchScanner(
            Table table,
            TableBucket tableBucket,
            long snapshotId,
            long logStartingOffset,
            long logStoppingOffset,
            @Nullable int[] projectedFields) {
        checkArgument(
                table.getTableInfo().hasPrimaryKey(),
                "KvSnapshotAndLogBatchScanner only supports primary-key tables.");
        this.tableBucket = tableBucket;
        this.logStoppingOffset = logStoppingOffset;

        ProjectionPlan projectionPlan = createProjectionPlan(table.getTableInfo(), projectedFields);
        this.keyIndexesInScanRow = projectionPlan.keyIndexesInScanRow;
        this.adjustProjectedFields = projectionPlan.adjustProjectedFields;
        this.primaryKeyComparator = createPrimaryKeyComparator(table.getTableInfo());
        this.logRows = new TreeMap<>(primaryKeyComparator);

        this.snapshotScanner =
                snapshotId >= 0
                        ? table.newScan()
                                .project(projectionPlan.scanProjectedFields)
                                .createBatchScanner(tableBucket, snapshotId)
                        : null;

        boolean emptyLogRange = logStartingOffset >= logStoppingOffset || logStoppingOffset <= 0;
        this.logScanFinished = emptyLogRange;
        if (emptyLogRange) {
            this.logScanner = null;
        } else {
            this.logScanner =
                    table.newScan().project(projectionPlan.scanProjectedFields).createLogScanner();
            Long partitionId = tableBucket.getPartitionId();
            if (partitionId == null) {
                this.logScanner.subscribe(tableBucket.getBucket(), logStartingOffset);
            } else {
                this.logScanner.subscribe(partitionId, tableBucket.getBucket(), logStartingOffset);
            }
        }
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (closed || finished) {
            return null;
        }

        if (!logScanFinished) {
            pollLogRecords(timeout);
            return CloseableIterator.emptyIterator();
        }

        if (sortMergeReader == null) {
            if (!initializeSnapshotIterator(timeout)) {
                return CloseableIterator.emptyIterator();
            }

            sortMergeReader =
                    new SortMergeReader(
                            adjustProjectedFields,
                            keyIndexesInScanRow,
                            snapshotRecordIterator,
                            primaryKeyComparator,
                            CloseableIterator.wrap(logRows.values().iterator()));
        }

        sortMergeIterator = sortMergeReader.readBatch();
        if (sortMergeIterator == null) {
            finished = true;
        }
        return sortMergeIterator;
    }

    private void pollLogRecords(Duration timeout) {
        ScanRecords scanRecords = logScanner.poll(timeout);
        for (ScanRecord scanRecord : scanRecords.records(tableBucket)) {
            long logOffset = scanRecord.logOffset();
            if (logOffset >= logStoppingOffset) {
                logScanFinished = true;
                break;
            }

            reduceLogRecord(scanRecord);
            if (logOffset >= logStoppingOffset - 1) {
                logScanFinished = true;
                break;
            }
        }

        Long consumedUpToOffset = scanRecords.consumedUpToOffset(tableBucket);
        if (consumedUpToOffset != null && consumedUpToOffset >= logStoppingOffset) {
            logScanFinished = true;
        }
    }

    private void reduceLogRecord(ScanRecord scanRecord) {
        ChangeType changeType = scanRecord.getChangeType();
        boolean isDelete =
                changeType == ChangeType.DELETE || changeType == ChangeType.UPDATE_BEFORE;
        KeyValueRow keyValueRow =
                new KeyValueRow(keyIndexesInScanRow, scanRecord.getRow(), isDelete);
        logRows.put(keyValueRow.keyRow(), keyValueRow);
    }

    private boolean initializeSnapshotIterator(Duration timeout) throws IOException {
        if (snapshotIteratorInitialized) {
            return true;
        }

        if (snapshotScanner == null) {
            snapshotRecordIterator = CloseableIterator.emptyIterator();
            snapshotIteratorInitialized = true;
            return true;
        }

        CloseableIterator<InternalRow> snapshotRows = snapshotScanner.pollBatch(timeout);
        if (snapshotRows == null) {
            snapshotRecordIterator = CloseableIterator.emptyIterator();
            snapshotIteratorInitialized = true;
            return true;
        }

        if (!snapshotRows.hasNext()) {
            snapshotRows.close();
            return false;
        }

        snapshotRecordIterator = new SnapshotRecordIterator(snapshotRows);
        snapshotIteratorInitialized = true;
        return true;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOUtils.closeQuietly(sortMergeIterator);
        IOUtils.closeQuietly(snapshotRecordIterator);
        IOUtils.closeQuietly(snapshotScanner);
        IOUtils.closeQuietly(logScanner);
    }

    private static ProjectionPlan createProjectionPlan(
            TableInfo tableInfo, @Nullable int[] projectedFields) {
        int[] physicalPrimaryKeyIndexes = getPhysicalPrimaryKeyIndexes(tableInfo);
        int fieldCount = tableInfo.getRowType().getFieldCount();
        if (projectedFields == null) {
            return new ProjectionPlan(
                    IntStream.range(0, fieldCount).toArray(), physicalPrimaryKeyIndexes, null);
        }

        List<Integer> scanProjectedFields =
                Arrays.stream(projectedFields).boxed().collect(Collectors.toList());
        int[] keyIndexesInScanRow = new int[physicalPrimaryKeyIndexes.length];
        for (int i = 0; i < physicalPrimaryKeyIndexes.length; i++) {
            int primaryKeyIndex = physicalPrimaryKeyIndexes[i];
            int indexInProjectedFields = findIndex(projectedFields, primaryKeyIndex);
            if (indexInProjectedFields >= 0) {
                keyIndexesInScanRow[i] = indexInProjectedFields;
            } else {
                scanProjectedFields.add(primaryKeyIndex);
                keyIndexesInScanRow[i] = scanProjectedFields.size() - 1;
            }
        }

        int[] scanProjection = scanProjectedFields.stream().mapToInt(Integer::intValue).toArray();
        int[] adjustProjectedFields = new int[projectedFields.length];
        for (int i = 0; i < projectedFields.length; i++) {
            adjustProjectedFields[i] = findIndex(scanProjection, projectedFields[i]);
        }
        return new ProjectionPlan(scanProjection, keyIndexesInScanRow, adjustProjectedFields);
    }

    private static Comparator<InternalRow> createPrimaryKeyComparator(TableInfo tableInfo) {
        int[] physicalPrimaryKeyIndexes = getPhysicalPrimaryKeyIndexes(tableInfo);
        RowType primaryKeyRowType =
                Schema.getKeyRowType(tableInfo.getSchema(), physicalPrimaryKeyIndexes);
        KeyEncoder primaryKeyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        primaryKeyRowType,
                        tableInfo.getPhysicalPrimaryKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());
        return (row1, row2) -> {
            byte[] key1 = primaryKeyEncoder.encodeKey(row1);
            byte[] key2 = primaryKeyEncoder.encodeKey(row2);
            return MemorySegment.wrap(key1)
                    .compare(MemorySegment.wrap(key2), 0, 0, key1.length, key2.length);
        };
    }

    private static int[] getPhysicalPrimaryKeyIndexes(TableInfo tableInfo) {
        return tableInfo.getPhysicalPrimaryKeys().stream()
                .mapToInt(primaryKey -> tableInfo.getRowType().getFieldIndex(primaryKey))
                .toArray();
    }

    private static int findIndex(int[] array, int target) {
        for (int i = 0; i < array.length; i++) {
            if (array[i] == target) {
                return i;
            }
        }
        return -1;
    }

    private static class ProjectionPlan {
        private final int[] scanProjectedFields;
        private final int[] keyIndexesInScanRow;
        @Nullable private final int[] adjustProjectedFields;

        private ProjectionPlan(
                int[] scanProjectedFields,
                int[] keyIndexesInScanRow,
                @Nullable int[] adjustProjectedFields) {
            this.scanProjectedFields = scanProjectedFields;
            this.keyIndexesInScanRow = keyIndexesInScanRow;
            this.adjustProjectedFields = adjustProjectedFields;
        }
    }

    private static class SnapshotRecordIterator implements CloseableIterator<LogRecord> {
        private final CloseableIterator<InternalRow> rows;

        private SnapshotRecordIterator(CloseableIterator<InternalRow> rows) {
            this.rows = rows;
        }

        @Override
        public void close() {
            rows.close();
        }

        @Override
        public boolean hasNext() {
            return rows.hasNext();
        }

        @Override
        public LogRecord next() {
            return new ScanRecord(rows.next());
        }
    }
}

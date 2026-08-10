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
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.SortMergeReader;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Comparator;
import java.util.NoSuchElementException;

import static org.apache.fluss.client.table.scanner.batch.SortedLogRows.DEFAULT_SPILL_THRESHOLD;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * A bounded scanner that merges an immutable KV snapshot with a fixed primary-key changelog range.
 */
@Internal
public class KvSnapshotAndLogBatchScanner implements BatchScanner {

    private final int[] keyIndexesInScanRow;
    @Nullable private final int[] adjustProjectedFields;
    private final Comparator<InternalRow> primaryKeyComparator;
    @Nullable private final SortedLogRows logRows;

    @Nullable private final BatchScanner snapshotScanner;

    private boolean logRowsLoaded;
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
            @Nullable int[] projectedFields,
            String scannerTmpDir) {
        this(
                table,
                tableBucket,
                snapshotId,
                logStartingOffset,
                logStoppingOffset,
                projectedFields,
                scannerTmpDir,
                DEFAULT_SPILL_THRESHOLD);
    }

    @VisibleForTesting
    KvSnapshotAndLogBatchScanner(
            Table table,
            TableBucket tableBucket,
            long snapshotId,
            long logStartingOffset,
            long logStoppingOffset,
            @Nullable int[] projectedFields,
            String scannerTmpDir,
            int spillThreshold) {
        checkArgument(
                table.getTableInfo().hasPrimaryKey(),
                "KvSnapshotAndLogBatchScanner only supports primary-key tables.");

        ProjectionPlan projectionPlan = createProjectionPlan(table.getTableInfo(), projectedFields);
        this.keyIndexesInScanRow = projectionPlan.keyIndexesInScanRow;
        this.adjustProjectedFields = projectionPlan.adjustProjectedFields;
        this.primaryKeyComparator = createPrimaryKeyComparator(table.getTableInfo());

        this.snapshotScanner =
                snapshotId >= 0
                        ? table.newScan()
                                .project(projectionPlan.scanProjectedFields)
                                .createBatchScanner(tableBucket, snapshotId)
                        : null;

        boolean emptyLogRange = logStartingOffset >= logStoppingOffset || logStoppingOffset <= 0;
        this.logRowsLoaded = emptyLogRange;
        if (emptyLogRange) {
            this.logRows = null;
        } else {
            LogScanner logScanner =
                    table.newScan().project(projectionPlan.scanProjectedFields).createLogScanner();
            Long partitionId = tableBucket.getPartitionId();
            if (partitionId == null) {
                logScanner.subscribe(tableBucket.getBucket(), logStartingOffset);
            } else {
                logScanner.subscribe(partitionId, tableBucket.getBucket(), logStartingOffset);
            }
            this.logRows =
                    new SortedLogRows(
                            table.getTableInfo()
                                    .getRowType()
                                    .project(projectionPlan.scanProjectedFields),
                            keyIndexesInScanRow,
                            primaryKeyComparator,
                            logScanner,
                            tableBucket,
                            logStoppingOffset,
                            scannerTmpDir,
                            spillThreshold);
        }
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (closed || finished) {
            return null;
        }

        // 1. collect the bounded log range before merging.
        if (logRows != null && !logRowsLoaded) {
            logRowsLoaded = logRows.load(timeout);
            return CloseableIterator.emptyIterator();
        }

        if (sortMergeReader == null) {
            CloseableIterator<LogRecord> snapshotRecords = createSnapshotRecordIterator(timeout);

            // 3. merge the snapshot stream with the reduced changelog rows by primary key.
            sortMergeReader =
                    new SortMergeReader(
                            adjustProjectedFields,
                            keyIndexesInScanRow,
                            snapshotRecords,
                            primaryKeyComparator,
                            logRows == null
                                    ? CloseableIterator.emptyIterator()
                                    : logRows.newIterator());
        }

        try {
            sortMergeIterator = sortMergeReader.readBatch();
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }

        // 4. mark the scanner as finished once the merge reader has been drained.
        if (sortMergeIterator == null) {
            finished = true;
        }
        return sortMergeIterator;
    }

    private CloseableIterator<LogRecord> createSnapshotRecordIterator(Duration timeout) {
        if (snapshotScanner == null) {
            snapshotRecordIterator = CloseableIterator.emptyIterator();
        } else {
            snapshotRecordIterator = new SnapshotRecordIterator(snapshotScanner, timeout);
        }
        return snapshotRecordIterator;
    }

    @VisibleForTesting
    boolean isLogRowsSpilled() {
        return logRows != null && logRows.isSpilled();
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
        IOUtils.closeQuietly(logRows);
    }

    private static ProjectionPlan createProjectionPlan(
            TableInfo tableInfo, @Nullable int[] projectedFields) {
        return ProjectionPlan.create(
                tableInfo.getRowType().getFieldCount(),
                getPhysicalPrimaryKeyIndexes(tableInfo),
                projectedFields);
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

    @VisibleForTesting
    static class SnapshotRecordIterator implements CloseableIterator<LogRecord> {
        private final BatchScanner scanner;
        private final Duration timeout;
        @Nullable private CloseableIterator<InternalRow> rows;
        private boolean snapshotFinished;

        @VisibleForTesting
        SnapshotRecordIterator(BatchScanner scanner, Duration timeout) {
            this.scanner = scanner;
            this.timeout = timeout;
        }

        @Override
        public void close() {
            IOUtils.closeQuietly(rows);
            rows = null;
        }

        @Override
        public boolean hasNext() {
            while (true) {
                if (rows != null) {
                    if (rows.hasNext()) {
                        return true;
                    }
                    rows.close();
                    rows = null;
                }

                if (snapshotFinished) {
                    return false;
                }

                try {
                    rows = scanner.pollBatch(timeout);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                if (rows == null) {
                    snapshotFinished = true;
                    return false;
                }
            }
        }

        @Override
        public LogRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return new ScanRecord(rows.next());
        }
    }
}

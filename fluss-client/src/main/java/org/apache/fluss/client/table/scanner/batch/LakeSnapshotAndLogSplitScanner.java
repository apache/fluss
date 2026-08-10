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

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.SortMergeReader;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.SortedRecordReader;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.KeyValueRow;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkState;

/** A scanner to merge the lakehouse's snapshot and change log. */
public class LakeSnapshotAndLogSplitScanner implements BatchScanner {

    private final TableBucket tableBucket;
    private final @Nullable List<LakeSplit> lakeSplits;
    private Comparator<InternalRow> rowComparator;
    private List<CloseableIterator<LogRecord>> lakeRecordIterators = new ArrayList<>();
    private boolean lakeRecordIteratorsInitialized;
    private final LakeSource<LakeSplit> lakeSource;

    private final int[] pkIndexes;

    // the indexes of primary key in emitted row by lake and fluss
    private int[] keyIndexesInRow;
    @Nullable private int[] adjustProjectedFields;
    private final RowType scanRowType;
    private final String scannerTmpDir;

    private @Nullable SortedLogRows logRows;
    private @Nullable LogScanner logScanner;

    private final long stoppingOffset;
    private boolean logRowsLoaded;

    private SortMergeReader currentSortMergeReader;
    private CloseableIterator<InternalRow> currentSortMergeIterator;

    public LakeSnapshotAndLogSplitScanner(
            Table table,
            LakeSource<LakeSplit> lakeSource,
            @Nullable List<LakeSplit> lakeSplits,
            TableBucket tableBucket,
            long startingOffset,
            long stoppingOffset,
            @Nullable int[] projectedFields,
            String scannerTmpDir) {
        this.tableBucket = tableBucket;
        this.pkIndexes = table.getTableInfo().getSchema().getPrimaryKeyIndexes();
        this.lakeSplits = lakeSplits;
        this.lakeSource = lakeSource;
        this.stoppingOffset = stoppingOffset;
        ProjectionPlan projectionPlan =
                ProjectionPlan.create(
                        table.getTableInfo().getRowType().getFieldCount(),
                        pkIndexes,
                        projectedFields);
        this.keyIndexesInRow = projectionPlan.keyIndexesInScanRow;
        this.adjustProjectedFields = projectionPlan.adjustProjectedFields;
        int[] newProjectedFields = projectionPlan.scanProjectedFields;
        this.scanRowType = table.getTableInfo().getRowType().project(newProjectedFields);
        this.scannerTmpDir = scannerTmpDir;

        this.lakeSource.withProject(
                Arrays.stream(newProjectedFields)
                        .mapToObj(field -> new int[] {field})
                        .toArray(int[][]::new));

        this.logRowsLoaded = startingOffset >= stoppingOffset || stoppingOffset <= 0;
        if (logRowsLoaded) {
            this.logScanner = null;
        } else {
            this.logScanner = table.newScan().project(newProjectedFields).createLogScanner();
            if (tableBucket.getPartitionId() != null) {
                this.logScanner.subscribe(
                        tableBucket.getPartitionId(), tableBucket.getBucket(), startingOffset);
            } else {
                this.logScanner.subscribe(tableBucket.getBucket(), startingOffset);
            }
        }
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (!logRowsLoaded) {
            initializeLakeRecordIterators();
            initializeLogRows();
            logRowsLoaded = logRows.load(timeout);
            return CloseableIterator.emptyIterator();
        }

        initializeLakeRecordIterators();
        if (currentSortMergeReader == null) {
            CloseableIterator<KeyValueRow> logRowsIterator =
                    logRows == null ? CloseableIterator.emptyIterator() : logRows.newIterator();
            currentSortMergeReader =
                    new SortMergeReader(
                            adjustProjectedFields,
                            keyIndexesInRow,
                            lakeRecordIterators,
                            rowComparator,
                            logRowsIterator);
        }
        currentSortMergeIterator = currentSortMergeReader.readBatch();
        return currentSortMergeIterator;
    }

    private void initializeLakeRecordIterators() throws IOException {
        if (lakeRecordIteratorsInitialized) {
            return;
        }

        List<RecordReader> recordReaders = new ArrayList<>();
        if (lakeSplits == null || lakeSplits.isEmpty()) {
            // pass null split to get rowComparator
            recordReaders.add(lakeSource.createRecordReader(sortedReaderContext(null)));
        } else {
            for (LakeSplit lakeSplit : lakeSplits) {
                recordReaders.add(lakeSource.createRecordReader(sortedReaderContext(lakeSplit)));
            }
        }
        for (RecordReader reader : recordReaders) {
            if (reader instanceof SortedRecordReader) {
                rowComparator = ((SortedRecordReader) reader).order();
            } else {
                throw new UnsupportedOperationException(
                        "lake records must instance of sorted view.");
            }
            lakeRecordIterators.add(reader.read());
        }
        lakeRecordIteratorsInitialized = true;
    }

    private LakeSource.ReaderContext<LakeSplit> sortedReaderContext(@Nullable LakeSplit lakeSplit) {
        return new LakeSource.ReaderContext<LakeSplit>() {
            @Nullable
            @Override
            public LakeSplit lakeSplit() {
                return lakeSplit;
            }

            @Override
            public boolean requireSortedRecords() {
                return true;
            }
        };
    }

    private void initializeLogRows() {
        if (logRows != null) {
            return;
        }
        checkState(logScanner != null, "Log scanner must be initialized.");
        logRows =
                new SortedLogRows(
                        scanRowType,
                        keyIndexesInRow,
                        rowComparator,
                        logScanner,
                        tableBucket,
                        stoppingOffset,
                        scannerTmpDir);
        logScanner = null;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(currentSortMergeIterator);
        IOUtils.closeQuietly(logRows);
        IOUtils.closeQuietly(logScanner);
        if (lakeRecordIterators != null) {
            for (CloseableIterator<LogRecord> iterator : lakeRecordIterators) {
                IOUtils.closeQuietly(iterator);
            }
        }
    }
}

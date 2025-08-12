/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.ProjectedRow;
import com.alibaba.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/** . */
public class SortMergeReader {

    private final ProjectedRow snapshotProjectedPkRow;
    private final CloseableIterator<LogRecord> lakeRecordIterator;
    private final Comparator<InternalRow> userKeyComparator;
    private CloseableIterator<KeyValueRow> changeLogIterator;

    private final SnapshotMergedRowIteratorWrapper snapshotMergedRowIteratorWrapper;

    private final ChangeLogIteratorWrapper changeLogIteratorWrapper;

    public SortMergeReader(
            int[] pkIndexes,
            CloseableIterator<LogRecord> lakeRecordIterator,
            Comparator<InternalRow> userKeyComparator,
            CloseableIterator<KeyValueRow> changeLogIterator) {
        this.lakeRecordIterator = lakeRecordIterator;
        this.userKeyComparator = userKeyComparator;
        this.snapshotProjectedPkRow = ProjectedRow.from(pkIndexes);
        this.changeLogIterator = changeLogIterator;
        this.changeLogIteratorWrapper = new ChangeLogIteratorWrapper();
        this.snapshotMergedRowIteratorWrapper = new SnapshotMergedRowIteratorWrapper();
    }

    @Nullable
    public CloseableIterator<InternalRow> readBatch() {
        if (!lakeRecordIterator.hasNext()) {
            return changeLogIterator.hasNext()
                    ? changeLogIteratorWrapper.replace(changeLogIterator)
                    : null;
        } else {
            CloseableIterator<SortMergeRows> mergedRecordIterator =
                    transform(lakeRecordIterator, this::sortMergeWithChangeLog);

            return snapshotMergedRowIteratorWrapper.replace(mergedRecordIterator);
        }
    }

    private SortMergeRows sortMergeWithChangeLog(InternalRow lakeSnapshotRow) {
        // no log record, we return the snapshot record
        if (!changeLogIterator.hasNext()) {
            return new SortMergeRows(lakeSnapshotRow);
        }
        KeyValueRow logKeyValueRow = changeLogIterator.next();
        // now, let's compare with the snapshot row with log row
        int compareResult =
                userKeyComparator.compare(
                        snapshotProjectedPkRow.replaceRow(lakeSnapshotRow),
                        logKeyValueRow.keyRow());
        if (compareResult == 0) {
            // record of snapshot is equal to log, but the log record is delete,
            // we shouldn't emit record
            if (logKeyValueRow.isDelete()) {
                return SortMergeRows.EMPTY;
            } else {
                // return the log record
                return new SortMergeRows(logKeyValueRow.valueRow());
            }
        }

        // the snapshot record is less than the log record, emit the
        // snapshot record
        if (compareResult < 0) {
            // need to put back the log record to log iterator to make the log record
            // can be advanced again
            changeLogIterator = addElementToHead(logKeyValueRow, changeLogIterator);
            return new SortMergeRows(lakeSnapshotRow);
        } else {
            // snapshot record > log record
            // we should emit the log record firsts; and still need to iterator changelog to find
            // the first change log greater than the snapshot record
            List<InternalRow> emitRows = new ArrayList<>();
            emitRows.add(logKeyValueRow.valueRow());
            boolean shouldEmitSnapshotRecord = true;
            while (changeLogIterator.hasNext()) {
                // get the next log record
                logKeyValueRow = changeLogIterator.next();
                // compare with the snapshot row,
                compareResult =
                        userKeyComparator.compare(
                                snapshotProjectedPkRow.replaceRow(lakeSnapshotRow),
                                logKeyValueRow.keyRow());
                // if snapshot record < the log record
                if (compareResult < 0) {
                    // we can break the loop
                    changeLogIterator = addElementToHead(logKeyValueRow, changeLogIterator);
                    break;
                } else if (compareResult > 0) {
                    // snapshot record > the log record
                    // the log record should be emitted
                    emitRows.add(logKeyValueRow.valueRow());
                } else {
                    // log record == snapshot record
                    // the log record should be emitted if is not delete, but the snapshot record
                    // shouldn't be emitted
                    if (!logKeyValueRow.isDelete()) {
                        emitRows.add(logKeyValueRow.valueRow());
                    }
                    shouldEmitSnapshotRecord = false;
                }
            }

            if (shouldEmitSnapshotRecord) {
                emitRows.add(lakeSnapshotRow);
            }
            return new SortMergeRows(emitRows);
        }
    }

    private static class ChangeLogIteratorWrapper implements CloseableIterator<InternalRow> {
        private CloseableIterator<KeyValueRow> changeLogRecordIterator;

        public ChangeLogIteratorWrapper() {}

        public ChangeLogIteratorWrapper replace(
                CloseableIterator<KeyValueRow> changeLogRecordIterator) {
            this.changeLogRecordIterator = changeLogRecordIterator;
            return this;
        }

        @Override
        public void close() {
            if (changeLogRecordIterator != null) {
                changeLogRecordIterator.close();
            }
        }

        @Override
        public boolean hasNext() {
            return changeLogRecordIterator != null && changeLogRecordIterator.hasNext();
        }

        @Override
        public InternalRow next() {
            return changeLogRecordIterator.next().valueRow();
        }
    }

    private static class SnapshotMergedRowIteratorWrapper
            implements CloseableIterator<InternalRow> {
        private CloseableIterator<SortMergeRows> currentLakeSnapshotRecords;

        private @Nullable Iterator<InternalRow> currentMergedRows;

        // the row to be returned
        private @Nullable InternalRow returnedRow;

        public SnapshotMergedRowIteratorWrapper replace(
                CloseableIterator<SortMergeRows> currentLakeSnapshotRecords) {
            this.currentLakeSnapshotRecords = currentLakeSnapshotRecords;
            this.returnedRow = null;
            this.currentMergedRows = null;
            return this;
        }

        @Override
        public void close() {
            currentLakeSnapshotRecords.close();
        }

        @Override
        public boolean hasNext() {
            if (returnedRow != null) {
                return true;
            }
            try {
                // if currentMergedRows is null, we need to get the next mergedRows
                if (currentMergedRows == null) {
                    SortMergeRows sortMergeRows =
                            currentLakeSnapshotRecords.hasNext()
                                    ? currentLakeSnapshotRecords.next()
                                    : null;
                    //  next mergedRows is not null and is not empty, set the currentMergedRows
                    if (sortMergeRows != null && !sortMergeRows.mergedRows.isEmpty()) {
                        currentMergedRows = sortMergeRows.mergedRows.iterator();
                    }
                }
                // check whether has next row, if does, set the internalRow to returned in method
                // next;
                if (currentMergedRows != null && currentMergedRows.hasNext()) {
                    returnedRow = currentMergedRows.next();
                }
                return returnedRow != null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public InternalRow next() {
            InternalRow returnedRow = this.returnedRow;
            // now, we can set the internalRow to null,
            // if no any row remain in current merged row, set the currentMergedRows to null
            // to enable fetch next merged rows
            this.returnedRow = null;
            if (currentMergedRows != null && !currentMergedRows.hasNext()) {
                currentMergedRows = null;
            }
            return returnedRow;
        }
    }

    private static class SortMergeRows {
        private static final SortMergeRows EMPTY = new SortMergeRows(Collections.emptyList());

        // the rows merge with change log, one snapshot row may advance multiple change log
        private final List<InternalRow> mergedRows;

        public SortMergeRows(List<InternalRow> mergedRows) {
            this.mergedRows = mergedRows;
        }

        public SortMergeRows(InternalRow internalRow) {
            this.mergedRows = Collections.singletonList(internalRow);
        }
    }

    private <T> CloseableIterator<T> addElementToHead(
            T firstElement, CloseableIterator<T> originElementIterator) {
        if (originElementIterator instanceof SingleElementHeadIterator) {
            SingleElementHeadIterator<T> singleElementHeadIterator =
                    (SingleElementHeadIterator<T>) originElementIterator;
            singleElementHeadIterator.set(firstElement, singleElementHeadIterator.inner);
            return singleElementHeadIterator;
        } else {
            return new SingleElementHeadIterator<>(firstElement, originElementIterator);
        }
    }

    private static class SingleElementHeadIterator<T> implements CloseableIterator<T> {
        private T singleElement;
        private CloseableIterator<T> inner;
        private boolean singleElementReturned;

        public SingleElementHeadIterator(T element, CloseableIterator<T> inner) {
            this.singleElement = element;
            this.inner = inner;
            this.singleElementReturned = false;
        }

        public void set(T element, CloseableIterator<T> inner) {
            this.singleElement = element;
            this.inner = inner;
            this.singleElementReturned = false;
        }

        @Override
        public boolean hasNext() {
            return !singleElementReturned || inner.hasNext();
        }

        @Override
        public T next() {
            if (singleElementReturned) {
                return inner.next();
            }
            singleElementReturned = true;
            return singleElement;
        }

        @Override
        public void close() {
            inner.close();
        }
    }

    private <R> CloseableIterator<R> transform(
            CloseableIterator<LogRecord> originElementIterator,
            final Function<InternalRow, R> function) {
        return new CloseableIterator<R>() {
            private final CloseableIterator<LogRecord> inner = originElementIterator;

            @Override
            public void close() {
                inner.close();
            }

            @Override
            public boolean hasNext() {
                return inner.hasNext();
            }

            @Override
            public R next() {
                LogRecord element = inner.next();
                return function.apply(element.getRow());
            }
        };
    }
}

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

<<<<<<<< HEAD:fluss-flink/fluss-flink-common/src/main/java/com/alibaba/fluss/flink/lake/reader/SortMergeReader.java
package com.alibaba.fluss.flink.lake.reader;

import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.ProjectedRow;
import com.alibaba.fluss.utils.CloseableIterator;
========
package org.apache.fluss.flink.lakehouse.paimon.reader;

import org.apache.fluss.utils.CloseableIterator;
>>>>>>>> c4d07399 ([INFRA] The project package name updated to org.apache.fluss.):fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/lakehouse/paimon/reader/SortMergeReader.java

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.function.Function;

/** A sort merge reader to merge lakehouse snapshot record and fluss change log. */
class SortMergeReader {

    private final ProjectedRow snapshotProjectedPkRow;
    private final CloseableIterator<LogRecord> lakeRecordIterator;
    private final Comparator<InternalRow> userKeyComparator;
    private CloseableIterator<KeyValueRow> changeLogIterator;

    private final SnapshotMergedRowIteratorWrapper snapshotMergedRowIteratorWrapper;

    private final ChangeLogIteratorWrapper changeLogIteratorWrapper;
    private @Nullable final ProjectedRow projectedRow;

    public SortMergeReader(
            @Nullable int[] projectedFields,
            int[] pkIndexes,
            List<CloseableIterator<LogRecord>> lakeRecordIterators,
            Comparator<InternalRow> userKeyComparator,
            CloseableIterator<KeyValueRow> changeLogIterator) {
        this.userKeyComparator = userKeyComparator;
        this.snapshotProjectedPkRow = ProjectedRow.from(pkIndexes);
        this.lakeRecordIterator =
                ConcatRecordIterator.wrap(lakeRecordIterators, userKeyComparator, pkIndexes);
        this.changeLogIterator = changeLogIterator;
        this.changeLogIteratorWrapper = new ChangeLogIteratorWrapper();
        this.snapshotMergedRowIteratorWrapper = new SnapshotMergedRowIteratorWrapper();
        // to project to fields provided by user
        this.projectedRow = projectedFields == null ? null : ProjectedRow.from(projectedFields);
    }

    @Nullable
<<<<<<<< HEAD:fluss-flink/fluss-flink-common/src/main/java/com/alibaba/fluss/flink/lake/reader/SortMergeReader.java
    public CloseableIterator<InternalRow> readBatch() {
        if (!lakeRecordIterator.hasNext()) {
========
    public org.apache.fluss.utils.CloseableIterator<org.apache.fluss.row.InternalRow> readBatch()
            throws IOException {
        RecordReader.RecordIterator<InternalRow> nextBatch = paimonReader.readBatch();
        // no any snapshot record, now, read log
        if (nextBatch == null) {
>>>>>>>> c4d07399 ([INFRA] The project package name updated to org.apache.fluss.):fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/lakehouse/paimon/reader/SortMergeReader.java
            return changeLogIterator.hasNext()
                    ? changeLogIteratorWrapper.replace(changeLogIterator)
                    : null;
        } else {
            CloseableIterator<SortMergeRows> mergedRecordIterator =
                    transform(lakeRecordIterator, this::sortMergeWithChangeLog);

            return snapshotMergedRowIteratorWrapper.replace(mergedRecordIterator);
        }
    }

<<<<<<<< HEAD:fluss-flink/fluss-flink-common/src/main/java/com/alibaba/fluss/flink/lake/reader/SortMergeReader.java
    /** A concat record iterator to concat multiple record iterator. */
    private static class ConcatRecordIterator implements CloseableIterator<LogRecord> {
        private final PriorityQueue<SingleElementHeadIterator<LogRecord>> priorityQueue;
        private final ProjectedRow snapshotProjectedPkRow1;
        private final ProjectedRow snapshotProjectedPkRow2;
========
    /**
     * The IteratorWrapper to wrap Paimon's RecordReader.RecordIterator which emit the merged rows
     * with paimon snapshot and fluss change log.
     */
    private class SnapshotMergedRowIteratorWrapper
            implements CloseableIterator<org.apache.fluss.row.InternalRow> {
        private RecordReader.RecordIterator<SortMergeRows> currentBatch;
>>>>>>>> c4d07399 ([INFRA] The project package name updated to org.apache.fluss.):fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/lakehouse/paimon/reader/SortMergeReader.java

        public ConcatRecordIterator(
                List<CloseableIterator<LogRecord>> iteratorList,
                int[] pkIndexes,
                Comparator<InternalRow> comparator) {
            this.snapshotProjectedPkRow1 = ProjectedRow.from(pkIndexes);
            this.snapshotProjectedPkRow2 = ProjectedRow.from(pkIndexes);
            this.priorityQueue =
                    new PriorityQueue<>(
                            Math.max(1, iteratorList.size()),
                            (s1, s2) ->
                                    comparator.compare(
                                            getComparableRow(s1, snapshotProjectedPkRow1),
                                            getComparableRow(s2, snapshotProjectedPkRow2)));
            iteratorList.stream()
                    .filter(Iterator::hasNext)
                    .map(
                            iterator ->
                                    SingleElementHeadIterator.addElementToHead(
                                            iterator.next(), iterator))
                    .forEach(priorityQueue::add);
        }

        public static CloseableIterator<LogRecord> wrap(
                List<CloseableIterator<LogRecord>> iteratorList,
                Comparator<InternalRow> comparator,
                int[] pkIndexes) {
            if (iteratorList.isEmpty()) {
                return CloseableIterator.wrap(Collections.emptyIterator());
            }
            return new ConcatRecordIterator(iteratorList, pkIndexes, comparator);
        }

        private InternalRow getComparableRow(
                SingleElementHeadIterator<LogRecord> iterator, ProjectedRow projectedRow) {
            return projectedRow.replaceRow(iterator.peek().getRow());
        }

        @Override
        public void close() {
            while (!priorityQueue.isEmpty()) {
                priorityQueue.poll().close();
            }
        }

        @Override
        public boolean hasNext() {
            while (!priorityQueue.isEmpty()) {
                CloseableIterator<LogRecord> iterator = priorityQueue.peek();
                if (iterator.hasNext()) {
                    return true;
                }
                priorityQueue.poll().close();
            }
            return false;
        }

        @Override
<<<<<<<< HEAD:fluss-flink/fluss-flink-common/src/main/java/com/alibaba/fluss/flink/lake/reader/SortMergeReader.java
        public LogRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
========
        public org.apache.fluss.row.InternalRow next() {
            InternalRow returnedRow =
                    projectedRow == null
                            ? this.returnedRow
                            : projectedRow.replaceRow(this.returnedRow);
            // now, we can set the internalRow to null,
            // if no any row remain in current merged row, set the currentMergedRows to null
            // to enable fetch next merged rows
            this.returnedRow = null;
            if (currentMergedRows != null && !currentMergedRows.hasNext()) {
                currentMergedRows = null;
>>>>>>>> c4d07399 ([INFRA] The project package name updated to org.apache.fluss.):fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/lakehouse/paimon/reader/SortMergeReader.java
            }
            return priorityQueue.peek().next();
        }
    }

<<<<<<<< HEAD:fluss-flink/fluss-flink-common/src/main/java/com/alibaba/fluss/flink/lake/reader/SortMergeReader.java
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
            changeLogIterator =
                    SingleElementHeadIterator.addElementToHead(logKeyValueRow, changeLogIterator);
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
                    changeLogIterator =
                            SingleElementHeadIterator.addElementToHead(
                                    logKeyValueRow, changeLogIterator);
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

    private static class SingleElementHeadIterator<T> implements CloseableIterator<T> {
        private T singleElement;
        private CloseableIterator<T> inner;
        private boolean singleElementReturned;

        public SingleElementHeadIterator(T element, CloseableIterator<T> inner) {
            this.singleElement = element;
            this.inner = inner;
            this.singleElementReturned = false;
        }

        public static <T> SingleElementHeadIterator<T> addElementToHead(
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

        public T peek() {
            if (singleElementReturned) {
                this.singleElement = inner.next();
                this.singleElementReturned = false;
                return this.singleElement;
            }
            return singleElement;
        }

        @Override
        public void close() {
            inner.close();
        }
    }

    private static class ChangeLogIteratorWrapper implements CloseableIterator<InternalRow> {
========
    private class ChangeLogIteratorWrapper
            implements CloseableIterator<org.apache.fluss.row.InternalRow> {
>>>>>>>> c4d07399 ([INFRA] The project package name updated to org.apache.fluss.):fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/lakehouse/paimon/reader/SortMergeReader.java
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
<<<<<<<< HEAD:fluss-flink/fluss-flink-common/src/main/java/com/alibaba/fluss/flink/lake/reader/SortMergeReader.java
        public InternalRow next() {
            return changeLogRecordIterator.next().valueRow();
========
        public org.apache.fluss.row.InternalRow next() {
            InternalRow returnedRow = changeLogRecordIterator.next().valueRow();
            if (projectedRow != null) {
                returnedRow = projectedRow.replaceRow(returnedRow);
            }
            return new PaimonRowWrapper(returnedRow);
>>>>>>>> c4d07399 ([INFRA] The project package name updated to org.apache.fluss.):fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/lakehouse/paimon/reader/SortMergeReader.java
        }
    }

    private class SnapshotMergedRowIteratorWrapper implements CloseableIterator<InternalRow> {
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
                // check if has next row, whether does, set the internalRow to returned in method
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
            InternalRow returnedRow =
                    projectedRow == null
                            ? this.returnedRow
                            : projectedRow.replaceRow(this.returnedRow);
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

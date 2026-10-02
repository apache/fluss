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

package org.apache.fluss.client.table.scanner;

import org.apache.fluss.client.utils.SingleElementHeadIterator;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.KeyValueRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * A sort merge reader that merges snapshot records and change log records into a single stream
 * sorted by primary key. Both inputs must share the same primary key encoding.
 *
 * <p>The merge is driven by a single {@link MergeIterator} (a two-way merge). For the same key the
 * change log wins over the snapshot: an update overrides the value, a delete removes the row. Using
 * one iterator keeps the result identical regardless of how many times {@link #readBatch()} is
 * called, so trailing change log records (keys greater than the max snapshot key) are never
 * dropped.
 */
public class SortMergeReader {

    private final ProjectedRow snapshotProjectedPkRow;
    private final CloseableIterator<LogRecord> snapshotRecordIterator;
    private final Comparator<InternalRow> userKeyComparator;
    private final CloseableIterator<KeyValueRow> changeLogIterator;
    private @Nullable final ProjectedRow projectedRow;

    private @Nullable MergeIterator mergeIterator;

    public SortMergeReader(
            @Nullable int[] projectedFields,
            int[] pkIndexes,
            @Nullable CloseableIterator<LogRecord> snapshotRecordIterator,
            Comparator<InternalRow> userKeyComparator,
            CloseableIterator<KeyValueRow> changeLogIterator) {
        this(
                projectedFields,
                pkIndexes,
                snapshotRecordIterator == null
                        ? Collections.emptyList()
                        : Collections.singletonList(snapshotRecordIterator),
                userKeyComparator,
                changeLogIterator);
    }

    public SortMergeReader(
            @Nullable int[] projectedFields,
            int[] pkIndexes,
            List<CloseableIterator<LogRecord>> snapshotRecordIterators,
            Comparator<InternalRow> userKeyComparator,
            CloseableIterator<KeyValueRow> changeLogIterator) {
        this.userKeyComparator = userKeyComparator;
        this.snapshotProjectedPkRow = ProjectedRow.from(pkIndexes);
        this.snapshotRecordIterator =
                ConcatRecordIterator.wrap(snapshotRecordIterators, userKeyComparator, pkIndexes);
        this.changeLogIterator = changeLogIterator;
        this.projectedRow = projectedFields == null ? null : ProjectedRow.from(projectedFields);
    }

    /**
     * Returns the merged-row iterator, or {@code null} when nothing is left. The same instance is
     * returned on every call, so repeated invocations keep draining the snapshot and change log.
     */
    @Nullable
    public CloseableIterator<InternalRow> readBatch() {
        if (mergeIterator == null) {
            mergeIterator = new MergeIterator();
        }
        return mergeIterator.hasNext() ? mergeIterator : null;
    }

    /** Two-way merge iterator over the snapshot and change log streams. */
    private class MergeIterator implements CloseableIterator<InternalRow> {

        // peeked head of each stream; null means not peeked yet or drained
        private @Nullable LogRecord pendingSnapshot;
        private @Nullable KeyValueRow pendingLog;

        // next row to return (before projection) and whether it has been computed
        private @Nullable InternalRow nextRow;
        private boolean nextComputed;

        private @Nullable LogRecord peekSnapshot() {
            if (pendingSnapshot == null && snapshotRecordIterator.hasNext()) {
                pendingSnapshot = snapshotRecordIterator.next();
            }
            return pendingSnapshot;
        }

        private @Nullable KeyValueRow peekLog() {
            if (pendingLog == null && changeLogIterator.hasNext()) {
                pendingLog = changeLogIterator.next();
            }
            return pendingLog;
        }

        @Override
        public boolean hasNext() {
            if (!nextComputed) {
                nextRow = advance();
                nextComputed = true;
            }
            return nextRow != null;
        }

        @Override
        public InternalRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            InternalRow row = nextRow;
            nextRow = null;
            nextComputed = false;
            return projectedRow == null ? row : projectedRow.replaceRow(row);
        }

        /** Advances to the next merged row, or {@code null} when both streams are drained. */
        private @Nullable InternalRow advance() {
            while (true) {
                LogRecord snapshot = peekSnapshot();
                KeyValueRow log = peekLog();

                if (snapshot == null && log == null) {
                    return null;
                }

                // only the change log remains
                if (snapshot == null) {
                    InternalRow row = takeLog(log);
                    if (row != null) {
                        return row;
                    }
                    continue;
                }

                InternalRow snapshotRow = snapshot.getRow();

                // only the snapshot remains
                if (log == null) {
                    pendingSnapshot = null;
                    return snapshotRow;
                }

                int compareResult =
                        userKeyComparator.compare(
                                snapshotProjectedPkRow.replaceRow(snapshotRow), log.keyRow());
                if (compareResult < 0) {
                    pendingSnapshot = null;
                    return snapshotRow;
                } else if (compareResult > 0) {
                    InternalRow row = takeLog(log);
                    if (row != null) {
                        return row;
                    }
                    continue;
                } else {
                    // same key: change log overrides snapshot, delete drops both
                    pendingSnapshot = null;
                    InternalRow row = takeLog(log);
                    if (row != null) {
                        return row;
                    }
                    continue;
                }
            }
        }

        /**
         * Consumes the change log head; returns its value row, or {@code null} if it is a delete.
         */
        private @Nullable InternalRow takeLog(KeyValueRow log) {
            pendingLog = null;
            return log.isDelete() ? null : log.valueRow();
        }

        @Override
        public void close() {
            snapshotRecordIterator.close();
            changeLogIterator.close();
        }
    }

    /** A concat record iterator to concat multiple record iterator. */
    private static class ConcatRecordIterator implements CloseableIterator<LogRecord> {
        private final PriorityQueue<SingleElementHeadIterator<LogRecord>> priorityQueue;
        private final ProjectedRow snapshotProjectedPkRow1;
        private final ProjectedRow snapshotProjectedPkRow2;

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
        public LogRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return priorityQueue.peek().next();
        }
    }
}

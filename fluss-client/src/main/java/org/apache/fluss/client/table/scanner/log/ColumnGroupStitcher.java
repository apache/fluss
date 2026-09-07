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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.entity.ColumnGroupFetchResult;
import org.apache.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Stitches column-group rows onto base rows by offset for one completed fetch (FIP-45).
 *
 * <p>The base batches and every group's batches are ordered by base offset, so the stitch is a
 * merge-join: for each base record the cursor of every touched group is advanced to the same
 * offset. Batch boundaries need not line up between the base log and the group logs.
 */
@Internal
final class ColumnGroupStitcher implements Closeable {

    private final ColumnGroupReadPlan plan;
    private final GroupCursor[] cursors;
    private final InternalRow.FieldGetter[][] groupFieldGetters;

    ColumnGroupStitcher(
            ColumnGroupReadPlan plan,
            Map<String, LogRecordReadContext> groupReadContexts,
            Map<String, ColumnGroupFetchResult> groupResults,
            boolean checkCrcs) {
        this.plan = plan;
        List<String> groups = plan.touchedGroups();
        this.cursors = new GroupCursor[groups.size()];
        this.groupFieldGetters = new InternalRow.FieldGetter[groups.size()][];
        for (int i = 0; i < groups.size(); i++) {
            String group = groups.get(i);
            ColumnGroupFetchResult result = groupResults.get(group);
            LogRecords records = result == null ? MemoryLogRecords.EMPTY : result.getRecords();
            cursors[i] = new GroupCursor(records, groupReadContexts.get(group), checkCrcs);
            groupFieldGetters[i] = plan.groupFieldGetters(group);
        }
    }

    /**
     * Positions every group cursor at {@code offset}. Returns false when some group has no row for
     * it, which means the fetch ran out of group records: the caller must stop consuming this fetch
     * at {@code offset} and fetch again from there.
     */
    boolean prepare(long offset) {
        for (GroupCursor cursor : cursors) {
            if (!cursor.advanceTo(offset)) {
                return false;
            }
        }
        return true;
    }

    /** Builds the output row of {@code baseRecord} after a successful {@link #prepare}. */
    InternalRow stitch(LogRecord baseRecord, InternalRow.FieldGetter[] baseFieldGetters) {
        GenericRow row = new GenericRow(plan.outputCount());
        InternalRow baseRow = baseRecord.getRow();
        for (int i = 0; i < plan.outputCount(); i++) {
            int source = plan.outputSource(i);
            int field = plan.outputField(i);
            if (source < 0) {
                row.setField(i, baseFieldGetters[field].getFieldOrNull(baseRow));
            } else {
                InternalRow groupRow = cursors[source].currentRow();
                row.setField(i, groupFieldGetters[source][field].getFieldOrNull(groupRow));
            }
        }
        return row;
    }

    @Override
    public void close() {
        for (GroupCursor cursor : cursors) {
            cursor.close();
        }
    }

    /** A cursor over the records of one column group, ordered by base offset. */
    private static final class GroupCursor implements Closeable {
        private final Iterator<LogRecordBatch> batches;
        private final LogRecordReadContext readContext;
        private final boolean checkCrcs;
        @Nullable private CloseableIterator<LogRecord> records;
        @Nullable private LogRecord current;

        GroupCursor(LogRecords logRecords, LogRecordReadContext readContext, boolean checkCrcs) {
            this.batches = logRecords.batches().iterator();
            this.readContext = readContext;
            this.checkCrcs = checkCrcs;
        }

        boolean advanceTo(long offset) {
            while (current == null || current.logOffset() < offset) {
                if (records != null && records.hasNext()) {
                    current = records.next();
                    continue;
                }
                if (records != null) {
                    records.close();
                    records = null;
                }
                if (!batches.hasNext()) {
                    current = null;
                    return false;
                }
                LogRecordBatch batch = batches.next();
                if (checkCrcs) {
                    batch.ensureValid();
                }
                records = batch.records(readContext);
            }
            return current.logOffset() == offset;
        }

        InternalRow currentRow() {
            if (current == null) {
                throw new IllegalStateException("No column group row prepared.");
            }
            return current.getRow();
        }

        @Override
        public void close() {
            if (records != null) {
                records.close();
                records = null;
            }
            current = null;
        }
    }
}

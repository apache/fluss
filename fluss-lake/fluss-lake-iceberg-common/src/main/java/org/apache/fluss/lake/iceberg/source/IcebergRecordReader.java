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

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.lake.iceberg.utils.IcebergUtils;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.data.IcebergGenericReader;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.fluss.lake.iceberg.IcebergSchemaUtils.LEGACY_SYSTEM_COLUMNS;

/**
 * Iceberg record reader. The filter is applied during the plan phase of IcebergSplitPlanner, so the
 * RecordReader does not need to apply the filter again.
 *
 * <p>Refer to {@link org.apache.iceberg.data.GenericReader#open(FileScanTask)} and {@link
 * org.apache.iceberg.Scan#ignoreResiduals()} for details.
 */
public class IcebergRecordReader implements RecordReader {

    /** Sentinel value emitted when the lake table has no per-record offset/timestamp. */
    private static final long NO_SYSTEM_COLUMN_VALUE = -1L;

    protected IcebergRecordAsFlussRecordIterator iterator;
    protected @Nullable int[][] project;
    protected Types.StructType struct;

    public IcebergRecordReader(FileScanTask fileScanTask, Table table, @Nullable int[][] project) {
        TableScan tableScan = table.newScan();
        if (project != null) {
            tableScan = applyProject(tableScan, project);
        }
        IcebergGenericReader reader = new IcebergGenericReader(tableScan, true);
        struct = tableScan.schema().asStruct();
        this.iterator = new IcebergRecordAsFlussRecordIterator(reader.open(fileScanTask), struct);
    }

    @Override
    public CloseableIterator<LogRecord> read() throws IOException {
        return iterator;
    }

    private TableScan applyProject(TableScan tableScan, int[][] projects) {
        Types.StructType structType = tableScan.schema().asStruct();
        List<Types.NestedField> cols = new ArrayList<>(projects.length);
        // The projected column ids reference the user (business) columns; the log offset /
        // timestamp
        // are not read from the lake table (a clean table has none, and for a legacy table we no
        // longer read them), so no system column needs to be projected.
        for (int[] project : projects) {
            cols.add(structType.fields().get(project[0]));
        }
        return tableScan.project(new Schema(cols));
    }

    /** Iterator for iceberg record as fluss record. */
    public static class IcebergRecordAsFlussRecordIterator implements CloseableIterator<LogRecord> {

        private final org.apache.iceberg.io.CloseableIterator<Record> icebergRecordIterator;

        private final ProjectedRow projectedRow;
        private final IcebergRecordAsFlussRow icebergRecordAsFlussRow;

        public IcebergRecordAsFlussRecordIterator(
                CloseableIterable<Record> icebergRecordIterator, Types.StructType struct) {
            this.icebergRecordIterator = icebergRecordIterator.iterator();

            // A legacy table read without projection still exposes its three trailing system
            // columns; trim them so only the business columns are emitted. A clean table (or any
            // projected read) has no system columns to trim.
            int businessFieldCount =
                    IcebergUtils.isLegacyTable(new Schema(struct.fields()))
                            ? struct.fields().size() - LEGACY_SYSTEM_COLUMNS.size()
                            : struct.fields().size();
            projectedRow = ProjectedRow.from(IntStream.range(0, businessFieldCount).toArray());
            icebergRecordAsFlussRow = new IcebergRecordAsFlussRow();
        }

        @Override
        public void close() {
            try {
                icebergRecordIterator.close();
            } catch (Exception e) {
                throw new RuntimeException("Fail to close iterator.", e);
            }
        }

        @Override
        public boolean hasNext() {
            return icebergRecordIterator.hasNext();
        }

        @Override
        public LogRecord next() {
            Record icebergRecord = icebergRecordIterator.next();
            // The lake table does not carry a per-record log offset / timestamp (a clean table has
            // no system columns, and for a legacy table we no longer read them), so a sentinel -1
            // is
            // emitted, consistent with the Paimon reader and the LakeRecordRecordEmitter contract.
            return new GenericRecord(
                    NO_SYSTEM_COLUMN_VALUE,
                    NO_SYSTEM_COLUMN_VALUE,
                    ChangeType.INSERT,
                    projectedRow.replaceRow(
                            icebergRecordAsFlussRow.replaceIcebergRecord(icebergRecord)));
        }
    }
}

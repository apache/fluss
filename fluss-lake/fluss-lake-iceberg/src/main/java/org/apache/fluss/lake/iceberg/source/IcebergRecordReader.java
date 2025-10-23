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
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;

/**
 * Iceberg record reader. The filter is applied during the plan phase of IcebergSplitPlanner, so the
 * RecordReader does not need to apply the filter again.
 *
 * <p>Refer to {@link org.apache.iceberg.data.GenericReader#open(FileScanTask)} and {@link
 * org.apache.iceberg.Scan#ignoreResiduals()} for details.
 */
public class IcebergRecordReader implements RecordReader {
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
        List<Types.NestedField> cols = new ArrayList<>(projects.length + 2);

        for (int[] project : projects) {
            cols.add(structType.fields().get(project[0]));
        }

        cols.add(structType.field(OFFSET_COLUMN_NAME));
        cols.add(structType.field(TIMESTAMP_COLUMN_NAME));
        return tableScan.project(new Schema(cols));
    }

    /** Iterator for iceberg record as fluss record. */
    public static class IcebergRecordAsFlussRecordIterator implements CloseableIterator<LogRecord> {

        private final org.apache.iceberg.io.CloseableIterator<Record> icebergRecordIterator;

        private final ProjectedRow projectedRow;
        private final IcebergRecordAsFlussRow icebergRecordAsFlussRow;

        private final int logOffsetColIndex;
        private final int timestampColIndex;

        public IcebergRecordAsFlussRecordIterator(
                CloseableIterable<Record> icebergRecordIterator, Types.StructType struct) {
            this.icebergRecordIterator = icebergRecordIterator.iterator();
            this.logOffsetColIndex = struct.fields().indexOf(struct.field(OFFSET_COLUMN_NAME));
            this.timestampColIndex = struct.fields().indexOf(struct.field(TIMESTAMP_COLUMN_NAME));

            int[] project = IntStream.range(0, struct.fields().size() - 2).toArray();
            projectedRow = ProjectedRow.from(project);
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
            long offset = icebergRecord.get(logOffsetColIndex, Long.class);
            long timestamp =
                    icebergRecord
                            .get(timestampColIndex, OffsetDateTime.class)
                            .toInstant()
                            .toEpochMilli();

            return new GenericRecord(
                    offset,
                    timestamp,
                    ChangeType.INSERT,
                    projectedRow.replaceRow(
                            icebergRecordAsFlussRow.replaceIcebergRecord(icebergRecord)));
        }
    }
}

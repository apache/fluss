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

package org.apache.fluss.lake.delta.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.delta.DeltaConfig;
import org.apache.fluss.lake.delta.utils.CloseableIterators;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.utils.CloseableIterator;

import io.delta.kernel.Scan;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.FileReadResult;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.utils.FileStatus;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * Reads the data file of a {@link DeltaSplit} with the Kernel Parquet handler and turns it into the
 * logical rows of the table with {@code Scan#transformPhysicalData}, which adds the partition
 * columns and drops the rows deleted by a deletion vector.
 */
public class DeltaRecordReader implements RecordReader {

    /** Delta tables have no per-record log offset or timestamp, same as the other lake readers. */
    private static final long NO_SYSTEM_COLUMN_VALUE = -1L;

    private final io.delta.kernel.utils.CloseableIterator<FilteredColumnarBatch> logicalData;

    public DeltaRecordReader(Configuration options, DeltaSplit split) throws IOException {
        Engine engine = DeltaConfig.createEngine(options);
        Row scanState =
                KernelRowJson.fromJson(
                        engine,
                        split.scanState(),
                        DataTypeJsonSerDe.deserializeStructType(split.scanStateSchema()));
        Row scanFile =
                KernelRowJson.fromJson(
                        engine,
                        split.scanFile(),
                        DataTypeJsonSerDe.deserializeStructType(split.scanFileSchema()));
        FileStatus file = InternalScanFileUtils.getAddFileStatus(scanFile);
        io.delta.kernel.utils.CloseableIterator<ColumnarBatch> physicalData =
                engine.getParquetHandler()
                        .readParquetFiles(
                                CloseableIterators.of(Collections.singletonList(file).iterator()),
                                ScanStateRow.getPhysicalDataReadSchema(scanState),
                                Optional.empty())
                        .map(FileReadResult::getData);
        this.logicalData = Scan.transformPhysicalData(engine, scanState, scanFile, physicalData);
    }

    @Override
    public CloseableIterator<LogRecord> read() {
        return new LogRecordIterator();
    }

    /** Iterates the rows of the logical batches that survive the selection vector. */
    private class LogRecordIterator implements CloseableIterator<LogRecord> {

        private DeltaColumnarRow row;
        private int size;
        private Optional<ColumnVector> selection = Optional.empty();
        private int nextRowId;

        @Override
        public boolean hasNext() {
            while (true) {
                while (nextRowId < size) {
                    if (isSelected(nextRowId)) {
                        return true;
                    }
                    nextRowId++;
                }
                if (!logicalData.hasNext()) {
                    return false;
                }
                FilteredColumnarBatch batch = logicalData.next();
                ColumnarBatch data = batch.getData();
                row = DeltaColumnarRow.of(data);
                size = data.getSize();
                selection = batch.getSelectionVector();
                nextRowId = 0;
            }
        }

        @Override
        public LogRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            row.setRowId(nextRowId++);
            return new GenericRecord(
                    NO_SYSTEM_COLUMN_VALUE, NO_SYSTEM_COLUMN_VALUE, ChangeType.INSERT, row);
        }

        private boolean isSelected(int rowId) {
            return !selection.isPresent()
                    || (!selection.get().isNullAt(rowId) && selection.get().getBoolean(rowId));
        }

        @Override
        public void close() {
            try {
                logicalData.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}

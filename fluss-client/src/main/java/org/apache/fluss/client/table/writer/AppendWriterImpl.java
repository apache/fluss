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

package org.apache.fluss.client.table.writer;

import org.apache.fluss.client.write.WriteRecord;
import org.apache.fluss.client.write.WriterClient;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.InternalRow.FieldGetter;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.encode.CompactedRowEncoder;
import org.apache.fluss.row.encode.IndexedRowEncoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/** The writer to write data to the log table. */
class AppendWriterImpl extends AbstractTableWriter implements AppendWriter {
    private static final AppendResult APPEND_SUCCESS = new AppendResult();

    private final @Nullable KeyEncoder bucketKeyEncoder;

    private final LogFormat logFormat;
    private final IndexedRowEncoder indexedRowEncoder;
    private final CompactedRowEncoder compactedRowEncoder;
    private final FieldGetter[] fieldGetters;
    private final TableInfo tableInfo;

    AppendWriterImpl(TablePath tablePath, TableInfo tableInfo, WriterClient writerClient) {
        super(tablePath, tableInfo, writerClient);
        List<String> bucketKeys = tableInfo.getBucketKeys();
        if (bucketKeys.isEmpty()) {
            this.bucketKeyEncoder = null;
        } else {
            RowType rowType = tableInfo.getSchema().getRowType();
            DataLakeFormat lakeFormat = tableInfo.getTableConfig().getDataLakeFormat().orElse(null);
            this.bucketKeyEncoder = KeyEncoder.of(rowType, bucketKeys, lakeFormat);
        }

        DataType[] fieldDataTypes =
                tableInfo.getSchema().getRowType().getChildren().toArray(new DataType[0]);

        this.logFormat = tableInfo.getTableConfig().getLogFormat();
        this.indexedRowEncoder = new IndexedRowEncoder(tableInfo.getRowType());
        this.compactedRowEncoder = new CompactedRowEncoder(fieldDataTypes);
        this.fieldGetters = InternalRow.createFieldGetters(tableInfo.getRowType());
        this.tableInfo = tableInfo;
    }

    /**
     * Append row into Fluss non-pk table.
     *
     * @param row the row to append.
     * @return A {@link CompletableFuture} that always returns null when complete normally.
     */
    public CompletableFuture<AppendResult> append(InternalRow row) {
        checkFieldCount(row);

        PhysicalTablePath physicalPath = getPhysicalPath(row);
        byte[] bucketKey = bucketKeyEncoder != null ? bucketKeyEncoder.encodeKey(row) : null;

        final WriteRecord record;
        if (logFormat == LogFormat.INDEXED) {
            IndexedRow indexedRow = encodeIndexedRow(row);
            record = WriteRecord.forIndexedAppend(tableInfo, physicalPath, indexedRow, bucketKey);
        } else if (logFormat == LogFormat.COMPACTED) {
            CompactedRow compactedRow = encodeCompactedRow(row);
            record =
                    WriteRecord.forCompactedAppend(
                            tableInfo, physicalPath, compactedRow, bucketKey);
        } else {
            // ARROW format supports general internal row
            record = WriteRecord.forArrowAppend(tableInfo, physicalPath, row, bucketKey);
        }
        return send(record).thenApply(ignored -> APPEND_SUCCESS);
    }

    private CompactedRow encodeCompactedRow(InternalRow row) {
        if (row instanceof CompactedRow) {
            return (CompactedRow) row;
        }

        compactedRowEncoder.startNewRow();
        for (int i = 0; i < fieldCount; i++) {
            compactedRowEncoder.encodeField(i, fieldGetters[i].getFieldOrNull(row));
        }
        return compactedRowEncoder.finishRow();
    }

    private IndexedRow encodeIndexedRow(InternalRow row) {
        if (row instanceof IndexedRow) {
            return (IndexedRow) row;
        }

        indexedRowEncoder.startNewRow();
        for (int i = 0; i < fieldCount; i++) {
            indexedRowEncoder.encodeField(i, fieldGetters[i].getFieldOrNull(row));
        }
        return indexedRowEncoder.finishRow();
    }
}

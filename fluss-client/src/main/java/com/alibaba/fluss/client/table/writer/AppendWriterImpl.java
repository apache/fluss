/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.table.writer;

import com.alibaba.fluss.client.write.WriteRecord;
import com.alibaba.fluss.client.write.WriterClient;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.InternalRow.FieldGetter;
import com.alibaba.fluss.row.encode.IndexedRowEncoder;
import com.alibaba.fluss.row.encode.KeyEncoder;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/** The writer to write data to the log table. */
class AppendWriterImpl extends AbstractTableWriter implements AppendWriter {
    private static final AppendResult APPEND_SUCCESS = new AppendResult();

    private final @Nullable KeyEncoder bucketKeyEncoder;

    private final LogFormat logFormat;
    private final IndexedRowEncoder indexedRowEncoder;
    private final FieldGetter[] fieldGetters;

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

        this.logFormat = tableInfo.getTableConfig().getLogFormat();
        this.indexedRowEncoder = new IndexedRowEncoder(tableInfo.getRowType());
        this.fieldGetters = InternalRow.createFieldGetters(tableInfo.getRowType());
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
            record = WriteRecord.forIndexedAppend(physicalPath, indexedRow, bucketKey);
        } else {
            // ARROW format supports general internal row
            record = WriteRecord.forArrowAppend(physicalPath, row, bucketKey);
        }
        return send(record).thenApply(ignored -> APPEND_SUCCESS);
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

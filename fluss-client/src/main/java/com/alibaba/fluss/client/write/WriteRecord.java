/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.client.write;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.record.DefaultKvRecord;
import com.alibaba.fluss.record.DefaultKvRecordBatch;
import com.alibaba.fluss.record.DefaultLogRecordBatch;
import com.alibaba.fluss.record.IndexedLogRecord;
import com.alibaba.fluss.row.BinaryRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.indexed.IndexedRow;

import javax.annotation.Nullable;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * A record to write to a table. It can represent an upsert operation, a delete operation, or an
 * append operation.
 */
@Internal
public final class WriteRecord {

    /** Create a write record for upsert operation and partial-upsert operation. */
    public static WriteRecord forUpsert(
            PhysicalTablePath tablePath,
            BinaryRow row,
            byte[] key,
            byte[] bucketKey,
            @Nullable int[] targetColumns) {
        checkNotNull(row, "row must not be null");
        checkNotNull(key, "key must not be null");
        checkNotNull(bucketKey, "key must not be null");
        int estimatedSizeInBytes =
                DefaultKvRecord.sizeOf(key, row) + DefaultKvRecordBatch.RECORD_BATCH_HEADER_SIZE;
        return new WriteRecord(
                tablePath,
                key,
                bucketKey,
                row,
                WriteFormat.KV,
                targetColumns,
                estimatedSizeInBytes);
    }

    /** Create a write record for delete operation and partial-delete update. */
    public static WriteRecord forDelete(
            PhysicalTablePath tablePath,
            byte[] key,
            byte[] bucketKey,
            @Nullable int[] targetColumns) {
        checkNotNull(key, "key must not be null");
        checkNotNull(bucketKey, "key must not be null");
        int estimatedSizeInBytes =
                DefaultKvRecord.sizeOf(key, null) + DefaultKvRecordBatch.RECORD_BATCH_HEADER_SIZE;
        return new WriteRecord(
                tablePath,
                key,
                bucketKey,
                null,
                WriteFormat.KV,
                targetColumns,
                estimatedSizeInBytes);
    }

    /** Create a write record for append operation for indexed format. */
    public static WriteRecord forIndexedAppend(
            PhysicalTablePath tablePath, IndexedRow row, @Nullable byte[] bucketKey) {
        checkNotNull(row);
        int estimatedSizeInBytes =
                IndexedLogRecord.sizeOf(row) + DefaultLogRecordBatch.RECORD_BATCH_HEADER_SIZE;
        return new WriteRecord(
                tablePath,
                null,
                bucketKey,
                row,
                WriteFormat.INDEXED_LOG,
                null,
                estimatedSizeInBytes);
    }

    /** Creates a write record for append operation for Arrow format. */
    public static WriteRecord forArrowAppend(
            PhysicalTablePath tablePath, InternalRow row, @Nullable byte[] bucketKey) {
        checkNotNull(row);
        // the write row maybe GenericRow, can't estimate the size.
        // it is not necessary to estimate size for Arrow format.
        int estimatedSizeInBytes = -1;
        return new WriteRecord(
                tablePath, null, bucketKey, row, WriteFormat.ARROW_LOG, null, estimatedSizeInBytes);
    }

    // ------------------------------------------------------------------------------------------

    private final PhysicalTablePath physicalTablePath;

    private final @Nullable byte[] key;
    private final @Nullable byte[] bucketKey;
    private final @Nullable InternalRow row;
    private final WriteFormat writeFormat;

    // will be null if it's not for partial update
    private final @Nullable int[] targetColumns;
    private final int estimatedSizeInBytes;

    private WriteRecord(
            PhysicalTablePath physicalTablePath,
            @Nullable byte[] key,
            @Nullable byte[] bucketKey,
            @Nullable InternalRow row,
            WriteFormat writeFormat,
            @Nullable int[] targetColumns,
            int estimatedSizeInBytes) {
        this.physicalTablePath = physicalTablePath;
        this.key = key;
        this.bucketKey = bucketKey;
        this.row = row;
        this.writeFormat = writeFormat;
        this.targetColumns = targetColumns;
        this.estimatedSizeInBytes = estimatedSizeInBytes;
    }

    public PhysicalTablePath getPhysicalTablePath() {
        return physicalTablePath;
    }

    public @Nullable byte[] getKey() {
        return key;
    }

    public @Nullable byte[] getBucketKey() {
        return bucketKey;
    }

    public @Nullable InternalRow getRow() {
        return row;
    }

    @Nullable
    public int[] getTargetColumns() {
        return targetColumns;
    }

    public WriteFormat getWriteFormat() {
        return writeFormat;
    }

    /**
     * Get the estimated size in bytes of the record with batch header.
     *
     * @return the estimated size in bytes of the record with batch header
     * @throws IllegalStateException if the estimated size in bytes is not supported for the write
     *     format
     */
    public int getEstimatedSizeInBytes() {
        if (estimatedSizeInBytes < 0) {
            throw new IllegalStateException(
                    String.format(
                            "The estimated size in bytes is not supported for %s write format.",
                            writeFormat));
        }
        return estimatedSizeInBytes;
    }
}

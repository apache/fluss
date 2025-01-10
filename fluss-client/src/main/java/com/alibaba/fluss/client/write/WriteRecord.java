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
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.record.DefaultKvRecord;
import com.alibaba.fluss.record.DefaultKvRecordBatch;
import com.alibaba.fluss.record.DefaultLogRecord;
import com.alibaba.fluss.record.MemoryLogRecordBatch;
import com.alibaba.fluss.row.InternalRow;

import javax.annotation.Nullable;

/**
 * Write record convert from {@link InternalRow}.
 *
 * <p>For table with primary key, if we use {@link UpsertWriter#upsert(InternalRow)} to send record,
 * it will convert to {@link WriteRecord} with key part, value row part and write kind {@link
 * WriteKind#PUT}. If we use {@link UpsertWriter#delete(InternalRow)} to send record, it will
 * convert to {@link WriteRecord} with key part , empty value row and write kind {@link
 * WriteKind#DELETE}.
 *
 * <p>For none-pk table, if we use {@link AppendWriter#append(InternalRow)} to send record, it will
 * convert to {@link WriteRecord} without key, value row part and write kind {@link
 * WriteKind#APPEND}.
 */
@Internal
public final class WriteRecord {
    private final PhysicalTablePath physicalTablePath;
    private final WriteKind writeKind;

    private final @Nullable byte[] key;
    private final @Nullable byte[] bucketKey;
    private final @Nullable InternalRow row;

    // will be null if it's not for partial update
    private final @Nullable int[] targetColumns;
    private final int estimatedSizeInBytes;

    public WriteRecord(
            PhysicalTablePath tablePath, WriteKind writeKind, InternalRow row, byte[] bucketKey) {
        this(tablePath, writeKind, null, bucketKey, row, null);
    }

    public WriteRecord(
            PhysicalTablePath physicalTablePath,
            WriteKind writeKind,
            @Nullable byte[] key,
            @Nullable byte[] bucketKey,
            @Nullable InternalRow row,
            @Nullable int[] targetColumns) {
        this.physicalTablePath = physicalTablePath;
        this.writeKind = writeKind;
        this.key = key;
        this.bucketKey = bucketKey;
        this.row = row;
        this.targetColumns = targetColumns;
        this.estimatedSizeInBytes =
                key != null
                        ? DefaultKvRecord.sizeOf(key, row)
                                + DefaultKvRecordBatch.RECORD_BATCH_HEADER_SIZE
                        // TODO: row maybe not IndexedRow, which can't be estimated size
                        //   and the size maybe not accurate when the format is arrow.
                        : DefaultLogRecord.sizeOf(row)
                                + MemoryLogRecordBatch.RECORD_BATCH_HEADER_SIZE;
    }

    public PhysicalTablePath getPhysicalTablePath() {
        return physicalTablePath;
    }

    public WriteKind getWriteKind() {
        return writeKind;
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

    /**
     * Get the estimated size in bytes of the record with batch header.
     *
     * @return the estimated size in bytes of the record with batch header
     */
    public int getEstimatedSizeInBytes() {
        return estimatedSizeInBytes;
    }
}

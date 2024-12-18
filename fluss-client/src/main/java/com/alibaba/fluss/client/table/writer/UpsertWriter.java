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

package com.alibaba.fluss.client.table.writer;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.client.write.WriteKind;
import com.alibaba.fluss.client.write.WriteRecord;
import com.alibaba.fluss.client.write.WriterClient;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.encode.KeyEncoder;
import com.alibaba.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.BitSet;
import java.util.concurrent.CompletableFuture;

/**
 * The writer to write data to the primary key table.
 *
 * @since 0.2
 */
@PublicEvolving
public class UpsertWriter extends TableWriter {

    private final KeyEncoder keyEncoder;
    private final KeyEncoder bucketKeyEncoder;
    private final @Nullable int[] targetColumns;

    public UpsertWriter(
            TablePath tablePath,
            TableDescriptor tableDescriptor,
            UpsertWrite upsertWrite,
            WriterClient writerClient,
            MetadataUpdater metadataUpdater) {
        super(tablePath, tableDescriptor, metadataUpdater, writerClient);
        Schema schema = tableDescriptor.getSchema();
        sanityCheck(schema, upsertWrite.getPartialUpdateColumns());

        RowType rowType = schema.toRowType();
        this.targetColumns = upsertWrite.getPartialUpdateColumns();

        this.keyEncoder =
                KeyEncoder.createKeyEncoder(
                        rowType,
                        schema.getPrimaryKey().get().getColumnNames(),
                        tableDescriptor.getPartitionKeys());

        int[] bucketKeyIndexes = tableDescriptor.getBucketKeyIndexes();
        if (bucketKeyIndexes.length != 0) {
            this.bucketKeyEncoder = new KeyEncoder(rowType, bucketKeyIndexes);
        } else {
            this.bucketKeyEncoder = keyEncoder;
        }
    }

    private static void sanityCheck(Schema schema, @Nullable int[] targetColumns) {
        // skip check when target columns is null
        if (targetColumns == null) {
            return;
        }
        BitSet targetColumnsSet = new BitSet();
        for (int targetColumnIndex : targetColumns) {
            targetColumnsSet.set(targetColumnIndex);
        }

        int[] pkIndexes = schema.getPrimaryKeyIndexes();
        BitSet pkColumnSet = new BitSet();
        // check the target columns contains the primary key
        for (int pkIndex : pkIndexes) {
            if (!targetColumnsSet.get(pkIndex)) {
                throw new IllegalArgumentException(
                        String.format(
                                "The target write columns %s must contain the primary key columns %s.",
                                schema.getColumnNames(targetColumns),
                                schema.getColumnNames(pkIndexes)));
            }
            pkColumnSet.set(pkIndex);
        }

        RowType rowType = schema.toRowType();
        // check the columns not in targetColumns should be nullable
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            // column not in primary key
            if (!pkColumnSet.get(i)) {
                // the column should be nullable
                if (!rowType.getTypeAt(i).isNullable()) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Partial Update requires all columns except primary key to be nullable, but column %s is NOT NULL.",
                                    schema.getColumnNames().get(i)));
                }
            }
        }
    }

    /**
     * Inserts row into Fluss table if they do not already exist, or updates them if they do exist.
     *
     * @param row the row to upsert.
     * @return A {@link CompletableFuture} that always returns null when complete normally.
     */
    public CompletableFuture<Void> upsert(InternalRow row) {
        byte[] key = keyEncoder.encode(row);
        byte[] bucketKey = bucketKeyEncoder.encode(row);
        return send(
                new WriteRecord(
                        getPhysicalPath(row), WriteKind.PUT, key, bucketKey, row, targetColumns));
    }

    /**
     * Delete certain row by the input row in Fluss table, the input row must contain the primary
     * key.
     *
     * @param row the row to delete.
     * @return A {@link CompletableFuture} that always returns null when complete normally.
     */
    public CompletableFuture<Void> delete(InternalRow row) {
        byte[] key = keyEncoder.encode(row);
        byte[] bucketKey = bucketKeyEncoder.encode(row);
        return send(
                new WriteRecord(
                        getPhysicalPath(row),
                        WriteKind.DELETE,
                        key,
                        bucketKey,
                        null,
                        targetColumns));
    }
}

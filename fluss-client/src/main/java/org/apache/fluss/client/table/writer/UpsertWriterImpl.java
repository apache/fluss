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

import org.apache.fluss.client.write.WriteFormat;
import org.apache.fluss.client.write.WriteRecord;
import org.apache.fluss.client.write.WriterClient;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.InternalRow.FieldGetter;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** The writer to write data to the primary key table. */
class UpsertWriterImpl extends AbstractTableWriter implements UpsertWriter {

    private final TableInfo tableInfo;
    private final KeyEncoder primaryKeyEncoder;
    private final @Nullable int[] targetColumns;

    // same to primaryKeyEncoder if the bucket key is the same to the primary key
    private final KeyEncoder bucketKeyEncoder;

    private final KvFormat kvFormat;
    private final WriteFormat writeFormat;
    private final RowEncoder rowEncoder;
    private final FieldGetter[] fieldGetters;

    /** The merge mode for this writer. This controls how the server handles data merging. */
    private final MergeMode mergeMode;

    /** Indexes of the NOT NULL target columns, empty when there is nothing to check. */
    private final int[] notNullTargetColumns;

    UpsertWriterImpl(
            TablePath tablePath,
            TableInfo tableInfo,
            @Nullable int[] partialUpdateColumns,
            WriterClient writerClient) {
        this(tablePath, tableInfo, partialUpdateColumns, writerClient, MergeMode.DEFAULT);
    }

    UpsertWriterImpl(
            TablePath tablePath,
            TableInfo tableInfo,
            @Nullable int[] partialUpdateColumns,
            WriterClient writerClient,
            MergeMode mergeMode) {
        super(tablePath, tableInfo, writerClient);
        RowType rowType = tableInfo.getRowType();
        sanityCheck(
                rowType,
                tableInfo.getPrimaryKeys(),
                tableInfo.getSchema().getAutoIncrementColumnNames(),
                partialUpdateColumns,
                tableInfo.getTableConfig().getMergeEngineType().orElse(null),
                mergeMode);

        this.targetColumns = partialUpdateColumns;
        // encode primary key using physical primary key
        this.primaryKeyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        tableInfo.getRowType(),
                        tableInfo.getPhysicalPrimaryKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());
        this.bucketKeyEncoder =
                KeyEncoder.ofBucketKeyEncoder(
                        tableInfo.getRowType(),
                        tableInfo.getBucketKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey(),
                        primaryKeyEncoder);

        this.kvFormat = tableInfo.getTableConfig().getKvFormat();
        this.writeFormat = WriteFormat.fromKvFormat(this.kvFormat);
        this.rowEncoder = RowEncoder.create(kvFormat, rowType);
        this.fieldGetters = InternalRow.createFieldGetters(rowType);

        this.tableInfo = tableInfo;
        this.mergeMode = mergeMode;
        this.notNullTargetColumns = findNotNullTargetColumns(rowType, partialUpdateColumns);
    }

    private static int[] findNotNullTargetColumns(RowType rowType, @Nullable int[] targetColumns) {
        if (targetColumns == null) {
            return new int[0];
        }
        int[] indexes = new int[targetColumns.length];
        int count = 0;
        for (int targetColumn : targetColumns) {
            if (!rowType.getTypeAt(targetColumn).isNullable()) {
                indexes[count++] = targetColumn;
            }
        }
        return Arrays.copyOf(indexes, count);
    }

    private static void sanityCheck(
            RowType rowType,
            List<String> primaryKeys,
            List<String> autoIncrementColumnNames,
            @Nullable int[] targetColumns,
            @Nullable MergeEngineType mergeEngineType,
            MergeMode mergeMode) {
        // skip check when target columns is null
        if (targetColumns == null) {
            if (!autoIncrementColumnNames.isEmpty()) {
                throw new IllegalArgumentException(
                        String.format(
                                "This table has auto increment column %s. "
                                        + "Explicitly specifying values for an auto increment column is not allowed. "
                                        + "Please specify non-auto-increment columns as target columns using partialUpdate first.",
                                autoIncrementColumnNames));
            }
            return;
        }
        BitSet targetColumnsSet = new BitSet();
        for (int targetColumnIndex : targetColumns) {
            targetColumnsSet.set(targetColumnIndex);
        }

        // check the target columns contains the primary key
        for (String key : primaryKeys) {
            int pkIndex = rowType.getFieldIndex(key);
            if (!targetColumnsSet.get(pkIndex)) {
                throw new IllegalArgumentException(
                        String.format(
                                "The target write columns %s must contain the primary key columns %s.",
                                rowType.project(targetColumns).getFieldNames(), primaryKeys));
            }
        }

        BitSet autoIncrementColumnSet = new BitSet();
        // explicitly specifying values for an auto increment column is not allowed
        for (String autoIncrementColumnName : autoIncrementColumnNames) {
            int autoIncrementColumnIndex = rowType.getFieldIndex(autoIncrementColumnName);
            if (targetColumnsSet.get(autoIncrementColumnIndex)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Explicitly specifying values for the auto increment column %s is not allowed.",
                                autoIncrementColumnName));
            }
            autoIncrementColumnSet.set(autoIncrementColumnIndex);
        }

        // the first_row and versioned mergers reject partial update outright on the server, at
        // the first write, so fail at writer creation instead. OVERWRITE is exempt, since the
        // server merges it with the default merger.
        if (mergeMode != MergeMode.OVERWRITE) {
            if (mergeEngineType == MergeEngineType.FIRST_ROW) {
                throw new IllegalArgumentException(
                        "Partial update is not supported for the first_row merge engine.");
            } else if (mergeEngineType == MergeEngineType.VERSIONED) {
                throw new IllegalArgumentException(
                        "Partial update is not supported for the versioned merge engine.");
            }
        }

        // the aggregation merge engine does not fill omitted columns with null on the first
        // write, so it keeps requiring every column except the primary key to be nullable, like
        // the server. OVERWRITE is exempt, since the server merges it with the default merger.
        if (mergeEngineType == MergeEngineType.AGGREGATION && mergeMode != MergeMode.OVERWRITE) {
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                if (!primaryKeys.contains(rowType.getFieldNames().get(i))
                        && !rowType.getTypeAt(i).isNullable()) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Partial aggregate requires all columns except primary key to be nullable, but column %s is NOT NULL.",
                                    rowType.getFieldNames().get(i)));
                }
            }
        }

        // an omitted column is written as null, so it must be nullable. auto increment columns
        // are always omitted and only get their value on the server.
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (!targetColumnsSet.get(i) && !rowType.getTypeAt(i).isNullable()) {
                String columnName = rowType.getFieldNames().get(i);
                if (autoIncrementColumnSet.get(i)) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Partial Update requires the auto increment column %s to be nullable, since it is always omitted from the target columns and assigned by the server.",
                                    columnName));
                }
                throw new IllegalArgumentException(
                        String.format(
                                "Partial Update requires all columns omitted from the target columns to be nullable, but omitted column %s is NOT NULL.",
                                columnName));
            }
        }
    }

    /**
     * Inserts row into Fluss table if they do not already exist, or updates them if they do exist.
     *
     * @param row the row to upsert.
     * @return A {@link CompletableFuture} that returns upsert result with bucket and offset info.
     */
    @Override
    public CompletableFuture<UpsertResult> upsert(InternalRow row) {
        checkFieldCount(row);
        checkNotNullTargetColumns(row);
        byte[] key = primaryKeyEncoder.encodeKey(row);
        byte[] bucketKey =
                bucketKeyEncoder == primaryKeyEncoder ? key : bucketKeyEncoder.encodeKey(row);
        WriteRecord record =
                WriteRecord.forUpsert(
                        tableInfo,
                        getPhysicalPath(row),
                        encodeRow(row),
                        key,
                        bucketKey,
                        writeFormat,
                        targetColumns,
                        mergeMode);
        return sendWithResult(record, UpsertResult::new);
    }

    /**
     * Delete certain row by the input row in Fluss table, the input row must contain the primary
     * key.
     *
     * @param row the row to delete.
     * @return A {@link CompletableFuture} that returns delete result with bucket and offset info.
     */
    @Override
    public CompletableFuture<DeleteResult> delete(InternalRow row) {
        checkFieldCount(row);
        byte[] key = primaryKeyEncoder.encodeKey(row);
        byte[] bucketKey =
                bucketKeyEncoder == primaryKeyEncoder ? key : bucketKeyEncoder.encodeKey(row);
        WriteRecord record =
                WriteRecord.forDelete(
                        tableInfo,
                        getPhysicalPath(row),
                        key,
                        bucketKey,
                        writeFormat,
                        targetColumns,
                        mergeMode);
        return sendWithResult(record, DeleteResult::new);
    }

    /**
     * Rejects a null in a NOT NULL target column. Runs before any field getter or encoding, which
     * would fail with a bare NullPointerException. When the target columns cover every schema
     * column the server skips the PartialUpdater and this check is the only guard, and the encoders
     * would reject the null anyway, so it never changes which rows are accepted.
     */
    private void checkNotNullTargetColumns(InternalRow row) {
        for (int index : notNullTargetColumns) {
            if (row.isNullAt(index)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Target column %s is NOT NULL but the written row has no value for it.",
                                tableInfo.getRowType().getFieldNames().get(index)));
            }
        }
    }

    private BinaryRow encodeRow(InternalRow row) {
        if (kvFormat == KvFormat.INDEXED && row instanceof IndexedRow) {
            return (IndexedRow) row;
        } else if (kvFormat == KvFormat.COMPACTED && row instanceof CompactedRow) {
            return (CompactedRow) row;
        }

        // encode the row to target format
        rowEncoder.startNewRow();
        for (int i = 0; i < fieldCount; i++) {
            rowEncoder.encodeField(i, fieldGetters[i].getFieldOrNull(row));
        }
        return rowEncoder.finishRow();
    }
}

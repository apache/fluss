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

import org.apache.fluss.client.write.WriterClient;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.Optional;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** API for configuring and creating {@link UpsertWriter}. */
public class TableUpsert implements Upsert {

    private final TablePath tablePath;
    private final TableInfo tableInfo;
    private final WriterClient writerClient;
    private final @Nullable int[] targetColumns;
    private final MergeMode mergeMode;

    public TableUpsert(TablePath tablePath, TableInfo tableInfo, WriterClient writerClient) {
        this(tablePath, tableInfo, writerClient, null, MergeMode.DEFAULT);
    }

    private TableUpsert(
            TablePath tablePath,
            TableInfo tableInfo,
            WriterClient writerClient,
            @Nullable int[] targetColumns,
            MergeMode mergeMode) {
        this.tablePath = tablePath;
        this.tableInfo = tableInfo;
        this.writerClient = writerClient;
        this.targetColumns = targetColumns;
        this.mergeMode = mergeMode;
    }

    @Override
    public Upsert partialUpdate(@Nullable int[] targetColumns) {
        // check if the target columns are valid and throw pretty exception messages
        if (targetColumns != null) {
            int numColumns = tableInfo.getRowType().getFieldCount();
            for (int targetColumn : targetColumns) {
                if (targetColumn < 0 || targetColumn >= numColumns) {
                    throw new IllegalArgumentException(
                            "Invalid target column index: "
                                    + targetColumn
                                    + " for table "
                                    + tablePath
                                    + ". The table only has "
                                    + numColumns
                                    + " columns.");
                }
            }
        }
        return new TableUpsert(tablePath, tableInfo, writerClient, targetColumns, this.mergeMode);
    }

    @Override
    public Upsert partialUpdate(String... targetColumnNames) {
        checkNotNull(targetColumnNames, "targetColumnNames");
        // check if the target columns are valid
        RowType rowType = tableInfo.getRowType();
        int[] targetColumns = new int[targetColumnNames.length];
        for (int i = 0; i < targetColumnNames.length; i++) {
            targetColumns[i] = rowType.getFieldIndex(targetColumnNames[i]);
            if (targetColumns[i] == -1) {
                throw new IllegalArgumentException(
                        "Can not find target column: "
                                + targetColumnNames[i]
                                + " for table "
                                + tablePath
                                + ".");
            }
        }
        return partialUpdate(targetColumns);
    }

    @Override
    public Upsert mergeMode(MergeMode mode) {
        checkNotNull(mode, "merge mode");
        return new TableUpsert(tablePath, tableInfo, writerClient, this.targetColumns, mode);
    }

    @Override
    public UpsertWriter createWriter() {
        // Check that OVERWRITE mode is only used with tables that have a merge engine
        if (mergeMode == MergeMode.OVERWRITE) {
            Optional<MergeEngineType> mergeEngineType =
                    tableInfo.getTableConfig().getMergeEngineType();
            if (!mergeEngineType.isPresent()) {
                throw new IllegalArgumentException(
                        String.format(
                                "MergeMode %s is only supported for tables with a merge engine, "
                                        + "but table %s does not have a merge engine configured.",
                                mergeMode, tablePath));
            }
        }
        return new UpsertWriterImpl(tablePath, tableInfo, targetColumns, writerClient, mergeMode);
    }

    @Override
    public <T> TypedUpsertWriter<T> createTypedWriter(Class<T> pojoClass) {
        // TypedUpsertWriterImpl doesn't support mergeMode yet
        if (mergeMode != MergeMode.DEFAULT) {
            throw new UnsupportedOperationException(
                    String.format(
                            "TypedUpsertWriter does not support MergeMode %s yet. "
                                    + "Please use createWriter() instead.",
                            mergeMode));
        }
        return new TypedUpsertWriterImpl<>(createWriter(), pojoClass, tableInfo, targetColumns);
    }
}

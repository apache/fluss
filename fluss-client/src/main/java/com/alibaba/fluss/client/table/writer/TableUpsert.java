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

package com.alibaba.fluss.client.table.writer;

import com.alibaba.fluss.client.write.WriterClient;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.types.RowType;

import javax.annotation.Nullable;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/** API for configuring and creating {@link UpsertWriter}. */
public class TableUpsert implements Upsert {

    private final TablePath tablePath;
    private final TableInfo tableInfo;
    private final WriterClient writerClient;

    private final @Nullable int[] targetColumns;

    public TableUpsert(TablePath tablePath, TableInfo tableInfo, WriterClient writerClient) {
        this(tablePath, tableInfo, writerClient, null);
    }

    private TableUpsert(
            TablePath tablePath,
            TableInfo tableInfo,
            WriterClient writerClient,
            @Nullable int[] targetColumns) {
        this.tablePath = tablePath;
        this.tableInfo = tableInfo;
        this.writerClient = writerClient;
        this.targetColumns = targetColumns;
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
        return new TableUpsert(tablePath, tableInfo, writerClient, targetColumns);
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
    public UpsertWriter createWriter() {
        return new UpsertWriterImpl(tablePath, tableInfo, targetColumns, writerClient);
    }
}

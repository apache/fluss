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

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.row.InternalRow;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Used to describe the operation to write data by {@link UpsertWriter} to a table.
 *
 * @since 0.2
 */
@PublicEvolving
public class UpsertWrite {

    private final @Nullable int[] targetColumns;

    public UpsertWrite() {
        this(null);
    }

    private UpsertWrite(@Nullable int[] targetColumns) {
        this.targetColumns = targetColumns;
    }

    /**
     * Returns a new instance of UpsertWrite description with partial update the specified columns.
     *
     * <p>For {@link UpsertWriter#upsert(InternalRow)} operation, only the specified columns will be
     * updated and other columns will remain unchanged if the row exists or set to null if the row
     * doesn't exist.
     *
     * <p>For {@link UpsertWriter#delete(InternalRow)} operation, the entire row will not be
     * removed, but only the specified columns except primary key will be set to null. The entire
     * row will be removed when all columns except primary key are null after a {@link
     * UpsertWriter#delete(InternalRow)} operation.
     *
     * <p>Note: The specified columns must be a contains all columns of primary key, and all columns
     * except primary key should be nullable.
     *
     * @param targetColumns the columns to partial update,
     */
    public UpsertWrite withPartialUpdate(int[] targetColumns) {
        checkNotNull(targetColumns, "targetColumns");
        return new UpsertWrite(targetColumns);
    }

    /**
     * Returns the columns to partial update. Returns null if update all columns.
     *
     * @return the columns to partial update.
     */
    public @Nullable int[] getPartialUpdateColumns() {
        return targetColumns;
    }
}

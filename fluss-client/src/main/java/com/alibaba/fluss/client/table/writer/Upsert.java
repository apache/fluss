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

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.row.InternalRow;

import javax.annotation.Nullable;

/**
 * Used to configure and create {@link UpsertWriter} to upsert and delete data to a Primary Key
 * Table.
 *
 * <p>{@link Upsert} objects are immutable and can be shared between threads. Refinement methods,
 * like {@link #partialUpdate}, create new Upsert instances.
 *
 * @since 0.6
 */
@PublicEvolving
public interface Upsert {

    /**
     * Apply partial update columns and returns a new Upsert instance.
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
     * @param targetColumns the column indexes to partial update
     */
    Upsert partialUpdate(@Nullable int[] targetColumns);

    /**
     * @see #partialUpdate(int[]) for more details.
     * @param targetColumnNames the column names to partial update
     */
    Upsert partialUpdate(String... targetColumnNames);

    /**
     * Create a new {@link UpsertWriter} with the optional {@link #partialUpdate(String...)}
     * information to upsert and delete data to a Primary Key Table.
     */
    UpsertWriter createWriter();
}

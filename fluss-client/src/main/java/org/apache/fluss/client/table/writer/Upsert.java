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
import org.apache.fluss.rpc.protocol.MergeMode;

import javax.annotation.Nullable;

/**
 * Used to configure and create {@link UpsertWriter} to upsert and delete data to a Primary Key
 * Table.
 *
 * <p>{@link Upsert} objects are immutable and can be shared between threads. Refinement methods,
 * like {@link #partialUpdate(int[])} and {@link #mergeMode(MergeMode)}, create new Upsert
 * instances.
 *
 * @since 0.6
 */
@PublicEvolving
public interface Upsert {

    /**
     * Apply partial update columns and returns a new Upsert instance.
     *
     * <p>For upsert operations, only the specified columns will be updated and other columns will
     * remain unchanged if the row exists or set to null if the row doesn't exist.
     *
     * <p>For delete operations, the entire row will not be removed immediately, but only the
     * specified columns except primary key will be set to null. The entire row will be removed when
     * all columns except primary key are null after a delete operation.
     *
     * <p>Note: The specified columns must contain all columns of primary key, and all columns
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
     * Specify merge mode for the UpsertWriter and returns a new Upsert instance.
     *
     * <p>This method controls how the created UpsertWriter handles data merging for tables with
     * merge engines:
     *
     * <ul>
     *   <li>{@link MergeMode#DEFAULT} (default): Data is merged through the server-side merge
     *       engine. The behavior depends on the configured merge engine type:
     *       <ul>
     *         <li>For aggregation merge engine: applies aggregation functions (e.g., SUM, MAX,
     *             MIN).
     *         <li>For first-row merge engine: retains the first observed row.
     *         <li>For versioned merge engine: keeps the row with the highest version.
     *       </ul>
     *   <li>{@link MergeMode#OVERWRITE}: Data directly overwrites values, bypassing the merge
     *       engine. This is useful for undo/recovery operations to restore exact historical values.
     * </ul>
     *
     * <p>Example usage:
     *
     * <pre>{@code
     * // Default merge mode
     * UpsertWriter normalWriter = table.newUpsert()
     *     .mergeMode(MergeMode.DEFAULT)
     *     .createWriter();
     *
     * // Overwrite mode for undo recovery
     * UpsertWriter undoWriter = table.newUpsert()
     *     .mergeMode(MergeMode.OVERWRITE)
     *     .createWriter();
     * }</pre>
     *
     * @param mode the merge mode
     * @return a new Upsert instance with the specified merge mode
     * @since 0.9
     */
    Upsert mergeMode(MergeMode mode);

    /**
     * Create a new {@link UpsertWriter} using {@code InternalRow} with the optional {@link
     * #partialUpdate(String...)} and {@link #mergeMode(MergeMode)} information to upsert and delete
     * data to a Primary Key Table.
     */
    UpsertWriter createWriter();

    /** Create a new typed {@link UpsertWriter} to write POJOs directly. */
    <T> TypedUpsertWriter<T> createTypedWriter(Class<T> pojoClass);
}

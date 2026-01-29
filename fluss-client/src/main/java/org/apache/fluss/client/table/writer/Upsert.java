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
import org.apache.fluss.rpc.protocol.AggMode;

import javax.annotation.Nullable;

/**
 * Used to configure and create {@link UpsertWriter} to upsert and delete data to a Primary Key
 * Table.
 *
 * <p>{@link Upsert} objects are immutable and can be shared between threads. Refinement methods,
 * like {@link #partialUpdate(int[])} and {@link #aggregationMode(AggMode)}, create new Upsert
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
     * Specify aggregation mode for the UpsertWriter and returns a new Upsert instance.
     *
     * <p>This method controls how the created UpsertWriter handles data aggregation:
     *
     * <ul>
     *   <li>{@link AggMode#AGGREGATE} (default): Data is aggregated through server-side merge
     *       engine
     *   <li>{@link AggMode#OVERWRITE}: Data directly overwrites values, bypassing merge engine (for
     *       undo recovery)
     *   <li>{@link AggMode#LOCAL_AGGREGATE}: Client-side pre-aggregation before server-side
     *       aggregation (reserved for future implementation, not yet supported)
     * </ul>
     *
     * <p>Example usage:
     *
     * <pre>{@code
     * // Normal aggregation mode (default)
     * UpsertWriter normalWriter = table.newUpsert()
     *     .aggregationMode(AggMode.AGGREGATE)
     *     .createWriter();
     *
     * // Overwrite mode for undo recovery
     * UpsertWriter undoWriter = table.newUpsert()
     *     .aggregationMode(AggMode.OVERWRITE)
     *     .createWriter();
     * }</pre>
     *
     * @param mode the aggregation mode
     * @return a new Upsert instance with the specified aggregation mode
     * @since 0.9
     */
    Upsert aggregationMode(AggMode mode);

    /**
     * Create a new {@link UpsertWriter} using {@code InternalRow} with the optional {@link
     * #partialUpdate(String...)} and {@link #aggregationMode(AggMode)} information to upsert and
     * delete data to a Primary Key Table.
     */
    UpsertWriter createWriter();

    /** Create a new typed {@link UpsertWriter} to write POJOs directly. */
    <T> TypedUpsertWriter<T> createTypedWriter(Class<T> pojoClass);
}

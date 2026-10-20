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
import org.apache.fluss.client.table.scanner.MultiTableRecord;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A write-side record for {@link MultiTableWriter}. Carries only the fields a writer needs:
 *
 * <ul>
 *   <li>{@link #getTablePath()} &mdash; target table
 *   <li>{@link #getOperation()} &mdash; APPEND / UPSERT / DELETE
 *   <li>{@link #getRow()} &mdash; the row payload encoded against the table's schema
 *   <li>{@link #getSchemaId()} &mdash; the schema id the row was encoded against; must be a valid
 *       (positive) schema id obtained from table metadata
 *   <li>{@link #getBucketId()} &mdash; optional target bucket for APPEND within the target table or
 *       partition
 * </ul>
 *
 * <p>Server-assigned identity ({@code tableId}) and read-only metadata ({@code schema} object,
 * {@code logOffset}, {@code timestamp}) are intentionally absent here &mdash; they belong to the
 * read-side {@link MultiTableRecord}.
 *
 * <p>Explicit bucket ids are zero-based and scoped to the target table or partition. Negative ids
 * are rejected when an append record is created; out-of-range ids and ids that disagree with a
 * keyed log table's routing are rejected when the record is written. Upsert and delete records are
 * routed by the primary-key table's bucket key.
 *
 * <p>Instances are immutable.
 *
 * @since 0.7
 */
@PublicEvolving
public final class MultiTableWriteRecord {

    /**
     * The write operation understood by {@link MultiTableWriter}.
     *
     * <p>Log tables accept {@link #APPEND}; primary-key tables accept {@link #UPSERT} and {@link
     * #DELETE}.
     */
    @PublicEvolving
    public enum Operation {
        APPEND,
        UPSERT,
        DELETE
    }

    private final TablePath tablePath;
    private final Operation operation;
    private final InternalRow row;
    private final int schemaId;
    @Nullable private final Integer bucketId;

    private MultiTableWriteRecord(
            TablePath tablePath,
            Operation operation,
            InternalRow row,
            int schemaId,
            @Nullable Integer bucketId) {
        this.tablePath = checkNotNull(tablePath, "tablePath");
        this.operation = checkNotNull(operation, "operation");
        this.row = checkNotNull(row, "row");
        this.schemaId = schemaId;
        checkArgument(bucketId == null || bucketId >= 0, "bucketId must be non-negative");
        this.bucketId = bucketId;
    }

    /**
     * Build an append record for a log table.
     *
     * @param tablePath target log table
     * @param row the row payload
     * @param schemaId the schema id the row was encoded against; must be a valid positive id
     */
    public static MultiTableWriteRecord forAppend(
            TablePath tablePath, InternalRow row, int schemaId) {
        return new MultiTableWriteRecord(tablePath, Operation.APPEND, row, schemaId, null);
    }

    /**
     * Build an append record for a log table with a target bucket. If the table has bucket keys,
     * the bucket must match its bucket-key routing.
     *
     * @param tablePath target log table
     * @param row the row payload
     * @param schemaId the schema id the row was encoded against
     * @param bucketId zero-based bucket id, less than the target table or partition's bucket count
     */
    public static MultiTableWriteRecord forAppend(
            TablePath tablePath, InternalRow row, int schemaId, int bucketId) {
        return new MultiTableWriteRecord(tablePath, Operation.APPEND, row, schemaId, bucketId);
    }

    /**
     * Build an upsert record for a primary-key table.
     *
     * @param tablePath target primary-key table
     * @param row the row payload
     * @param schemaId the schema id the row was encoded against
     */
    public static MultiTableWriteRecord forUpsert(
            TablePath tablePath, InternalRow row, int schemaId) {
        return new MultiTableWriteRecord(tablePath, Operation.UPSERT, row, schemaId, null);
    }

    /**
     * Build a delete record for a primary-key table.
     *
     * @param tablePath target primary-key table
     * @param row the row payload
     * @param schemaId the schema id the row was encoded against
     */
    public static MultiTableWriteRecord forDelete(
            TablePath tablePath, InternalRow row, int schemaId) {
        return new MultiTableWriteRecord(tablePath, Operation.DELETE, row, schemaId, null);
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public Operation getOperation() {
        return operation;
    }

    public InternalRow getRow() {
        return row;
    }

    /** Returns the schema id the row was encoded against. */
    public int getSchemaId() {
        return schemaId;
    }

    /** Returns the requested bucket, or null when the writer should route the row. */
    @Nullable
    public Integer getBucketId() {
        return bucketId;
    }

    @Override
    public String toString() {
        return "MultiTableWriteRecord{"
                + "tablePath="
                + tablePath
                + ", operation="
                + operation
                + ", schemaId="
                + schemaId
                + ", bucketId="
                + bucketId
                + '}';
    }
}

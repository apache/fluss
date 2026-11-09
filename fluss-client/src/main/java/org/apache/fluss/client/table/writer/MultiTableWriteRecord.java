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
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;

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
 * </ul>
 *
 * <p>Server-assigned identity ({@code tableId}) and read-only metadata ({@code schema} object,
 * {@code logOffset}, {@code timestamp}) are intentionally absent here &mdash; they belong to the
 * read-side {@link org.apache.fluss.client.table.scanner.MultiTableRecord}.
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

    private MultiTableWriteRecord(
            TablePath tablePath, Operation operation, InternalRow row, int schemaId) {
        this.tablePath = checkNotNull(tablePath, "tablePath");
        this.operation = checkNotNull(operation, "operation");
        this.row = checkNotNull(row, "row");
        this.schemaId = schemaId;
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
        return new MultiTableWriteRecord(tablePath, Operation.APPEND, row, schemaId);
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
        return new MultiTableWriteRecord(tablePath, Operation.UPSERT, row, schemaId);
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
        return new MultiTableWriteRecord(tablePath, Operation.DELETE, row, schemaId);
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

    @Override
    public String toString() {
        return "MultiTableWriteRecord{"
                + "tablePath="
                + tablePath
                + ", operation="
                + operation
                + ", schemaId="
                + schemaId
                + '}';
    }
}

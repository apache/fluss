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
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.InternalRow;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A write-side record for {@link MultiTableWriter}. Carries only the fields a writer needs:
 *
 * <ul>
 *   <li>{@link #getTablePath()} &mdash; target table
 *   <li>{@link #getChangeType()} &mdash; INSERT / UPDATE_AFTER / UPDATE_BEFORE / DELETE /
 *       APPEND_ONLY
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

    private final TablePath tablePath;
    private final ChangeType changeType;
    private final InternalRow row;
    private final int schemaId;

    private MultiTableWriteRecord(
            TablePath tablePath, ChangeType changeType, InternalRow row, int schemaId) {
        this.tablePath = checkNotNull(tablePath, "tablePath");
        this.changeType = checkNotNull(changeType, "changeType");
        this.row = checkNotNull(row, "row");
        this.schemaId = schemaId;
    }

    /**
     * Build a write record asserting the row was encoded against the given {@code schemaId}. If the
     * writer's cached schema id differs, it will refresh metadata and rebuild encoders. If after
     * the refresh the {@code schemaId} is still ahead of the table's current schema id, the write
     * fails fast.
     *
     * @param tablePath target table
     * @param changeType the change type for this record
     * @param row the row payload
     * @param schemaId the schema id the row was encoded against; must be a valid positive id
     */
    public static MultiTableWriteRecord of(
            TablePath tablePath, ChangeType changeType, InternalRow row, int schemaId) {
        return new MultiTableWriteRecord(tablePath, changeType, row, schemaId);
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public ChangeType getChangeType() {
        return changeType;
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
                + ", changeType="
                + changeType
                + ", schemaId="
                + schemaId
                + '}';
    }
}

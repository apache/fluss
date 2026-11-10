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

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.PublicEvolving;

import java.util.Objects;
import java.util.UUID;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Immutable table identity of a BulkLoad transaction. */
@PublicEvolving
public final class BulkLoadHandle {

    private final TablePath tablePath;
    private final long tableId;
    private final String bulkLoadId;

    /** Creates a table-level BulkLoad handle with a canonical lowercase UUID. */
    public BulkLoadHandle(TablePath tablePath, long tableId, String bulkLoadId) {
        this.tablePath = checkNotNull(tablePath, "BulkLoad table path must not be null.");
        checkArgument(tablePath.isValid(), "BulkLoad table path must be valid.");
        checkArgument(tableId >= 0, "BulkLoad table ID must be non-negative.");
        this.bulkLoadId = validateBulkLoadId(bulkLoadId);
        this.tableId = tableId;
    }

    /** Returns the original table path. */
    public TablePath getTablePath() {
        return tablePath;
    }

    /** Returns the physical table ID. */
    public long getTableId() {
        return tableId;
    }

    /** Returns the canonical lowercase BulkLoad UUID. */
    public String getBulkLoadId() {
        return bulkLoadId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BulkLoadHandle that = (BulkLoadHandle) o;
        return tableId == that.tableId
                && Objects.equals(tablePath, that.tablePath)
                && Objects.equals(bulkLoadId, that.bulkLoadId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tablePath, tableId, bulkLoadId);
    }

    @Override
    public String toString() {
        return "BulkLoadHandle{"
                + "tablePath="
                + tablePath
                + ", tableId="
                + tableId
                + ", bulkLoadId='"
                + bulkLoadId
                + '\''
                + '}';
    }

    private static String validateBulkLoadId(String bulkLoadId) {
        checkNotNull(bulkLoadId, "BulkLoad ID must not be null.");
        try {
            checkArgument(
                    UUID.fromString(bulkLoadId).toString().equals(bulkLoadId),
                    "BulkLoad ID must be a canonical lowercase UUID.");
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "BulkLoad ID must be a canonical lowercase UUID: " + bulkLoadId, e);
        }
        return bulkLoadId;
    }
}

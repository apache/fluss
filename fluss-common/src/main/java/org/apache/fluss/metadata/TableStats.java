/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Statistics of a table.
 *
 * <p>The data size is the current local physical size reported by the tablet leaders. It may be
 * unavailable when the server does not support this field.
 *
 * @since 0.9
 */
@PublicEvolving
public class TableStats {

    private final long rowCount;
    private final @Nullable Long dataSizeBytes;
    private final long collectedAtMs;

    public TableStats(long rowCount) {
        this(rowCount, null, -1L);
    }

    public TableStats(long rowCount, @Nullable Long dataSizeBytes, long collectedAtMs) {
        this.rowCount = rowCount;
        this.dataSizeBytes = dataSizeBytes;
        this.collectedAtMs = collectedAtMs;
    }

    /** Returns the current total row count of the table. */
    public long getRowCount() {
        return rowCount;
    }

    /** Returns the current local data size of the table, or null if it is unavailable. */
    public @Nullable Long getDataSizeBytes() {
        return dataSizeBytes;
    }

    /** Returns the time at which the table statistics response was collected. */
    public long getCollectedAtMs() {
        return collectedAtMs;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableStats that = (TableStats) o;
        return rowCount == that.rowCount
                && collectedAtMs == that.collectedAtMs
                && Objects.equals(dataSizeBytes, that.dataSizeBytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowCount, dataSizeBytes, collectedAtMs);
    }

    @Override
    public String toString() {
        return "TableStats{"
                + "rowCount="
                + rowCount
                + ", dataSizeBytes="
                + dataSizeBytes
                + ", collectedAtMs="
                + collectedAtMs
                + '}';
    }
}

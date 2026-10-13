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

import java.util.Objects;

/**
 * Statistics of a table.
 *
 * @since 0.9
 */
@PublicEvolving
public class TableStats {

    private final long rowCount;

    public TableStats(long rowCount) {
        this.rowCount = rowCount;
    }

    /** Returns the current total row count of the table. */
    public long getRowCount() {
        return rowCount;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableStats that = (TableStats) o;
        return rowCount == that.rowCount;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(rowCount);
    }

    @Override
    public String toString() {
        return "TableStats{" + "rowCount=" + rowCount + '}';
    }
}

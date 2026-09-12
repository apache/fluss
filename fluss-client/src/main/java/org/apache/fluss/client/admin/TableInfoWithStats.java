/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.admin;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TableStats;

import java.util.Objects;

/**
 * A table's static metadata together with its current statistics.
 *
 * @since 0.9
 */
@PublicEvolving
public final class TableInfoWithStats {

    private final TableInfo tableInfo;
    private final TableStats tableStats;

    /** Creates a table information and statistics result. */
    public TableInfoWithStats(TableInfo tableInfo, TableStats tableStats) {
        this.tableInfo = tableInfo;
        this.tableStats = tableStats;
    }

    /** Returns the static table metadata. */
    public TableInfo getTableInfo() {
        return tableInfo;
    }

    /** Returns the current table statistics. */
    public TableStats getTableStats() {
        return tableStats;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        TableInfoWithStats that = (TableInfoWithStats) other;
        return Objects.equals(tableInfo, that.tableInfo)
                && Objects.equals(tableStats, that.tableStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableInfo, tableStats);
    }

    @Override
    public String toString() {
        return "TableInfoWithStats{"
                + "tableInfo="
                + tableInfo
                + ", tableStats="
                + tableStats
                + '}';
    }
}

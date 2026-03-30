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

/**
 * Aggregated summary information of a database for listing purposes.
 *
 * <p>This class contains aggregated metadata about a database, including creation time, table
 * count, and other summary statistics. It is distinct from {@link DatabaseInfo} which contains
 * complete database metadata including the {@link DatabaseDescriptor}.
 *
 * @since 0.6
 */
@PublicEvolving
public class DatabaseSummary {
    private final String databaseName;
    private final long createdTime;
    private final int tableCount;

    public DatabaseSummary(String databaseName, long createdTime, int tableCount) {
        this.databaseName = databaseName;
        this.createdTime = createdTime;
        this.tableCount = tableCount;
    }

    /**
     * Returns the name of the database.
     *
     * @return the database name
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Returns the creation time of the database in milliseconds since epoch.
     *
     * @return the creation timestamp
     */
    public long getCreatedTime() {
        return createdTime;
    }

    /**
     * Returns the number of tables in this database.
     *
     * @return the table count
     */
    public int getTableCount() {
        return tableCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DatabaseSummary that = (DatabaseSummary) o;
        return Objects.equals(createdTime, that.createdTime)
                && Objects.equals(tableCount, that.tableCount)
                && Objects.equals(databaseName, that.databaseName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, createdTime, tableCount);
    }

    @Override
    public String toString() {
        return "DatabaseSummary{"
                + "databaseName='"
                + databaseName
                + '\''
                + ", createdTime="
                + createdTime
                + ", tableCount="
                + tableCount
                + '\''
                + '}';
    }
}

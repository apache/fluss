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

package com.alibaba.fluss.server.coordinator.event;

import java.util.Objects;

/** An event for delete table. */
public class DropTableEvent implements CoordinatorEvent {

    private final long tableId;

    // true if the table is with auto partition enabled
    private final boolean isAutoPartitionTable;
    private final boolean isDataLakeEnabled;

    public DropTableEvent(long tableId, boolean isAutoPartitionTable, boolean isDataLakeEnabled) {
        this.tableId = tableId;
        this.isAutoPartitionTable = isAutoPartitionTable;
        this.isDataLakeEnabled = isDataLakeEnabled;
    }

    public long getTableId() {
        return tableId;
    }

    public boolean isAutoPartitionTable() {
        return isAutoPartitionTable;
    }

    public boolean isDataLakeEnabled() {
        return isDataLakeEnabled;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof DropTableEvent)) {
            return false;
        }
        DropTableEvent that = (DropTableEvent) object;
        return tableId == that.tableId
                && isAutoPartitionTable == that.isAutoPartitionTable
                && isDataLakeEnabled == that.isDataLakeEnabled;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, isAutoPartitionTable, isDataLakeEnabled);
    }

    @Override
    public String toString() {
        return "DropTableEvent{"
                + "tableId="
                + tableId
                + ", isAutoPartitionTable="
                + isAutoPartitionTable
                + ", isDataLakeEnabled="
                + isDataLakeEnabled
                + '}';
    }
}

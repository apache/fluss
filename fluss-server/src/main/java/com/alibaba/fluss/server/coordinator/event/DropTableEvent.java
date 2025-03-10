/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

    public DropTableEvent(long tableId, boolean isAutoPartitionTable) {
        this.tableId = tableId;
        this.isAutoPartitionTable = isAutoPartitionTable;
    }

    public long getTableId() {
        return tableId;
    }

    public boolean isAutoPartitionTable() {
        return isAutoPartitionTable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DropTableEvent that = (DropTableEvent) o;
        return tableId == that.tableId && isAutoPartitionTable == that.isAutoPartitionTable;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, isAutoPartitionTable);
    }

    @Override
    public String toString() {
        return "DropTableEvent{"
                + "tableId="
                + tableId
                + ", isAutoPartitionTable="
                + isAutoPartitionTable
                + '}';
    }
}

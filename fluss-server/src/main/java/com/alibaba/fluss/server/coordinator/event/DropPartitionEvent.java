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

/** An event for drop a partition of a table. */
public class DropPartitionEvent implements CoordinatorEvent {

    private final long tableId;

    private final long partitionId;

    private final String partitionName;

    public DropPartitionEvent(long tableId, long partitionId, String partitionName) {
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.partitionName = partitionName;
    }

    public long getTableId() {
        return tableId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public String getPartitionName() {
        return partitionName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DropPartitionEvent that = (DropPartitionEvent) o;
        return tableId == that.tableId
                && Objects.equals(partitionId, that.partitionId)
                && Objects.equals(partitionName, that.partitionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, partitionId, partitionName);
    }

    @Override
    public String toString() {
        return "DropPartitionEvent{"
                + "tableId="
                + tableId
                + ", partitionId="
                + partitionId
                + ", partitionName='"
                + partitionName
                + '}';
    }
}

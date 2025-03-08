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

import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.zk.data.PartitionAssignment;

import java.util.Objects;

/** An event for create a partition for a table. */
public class CreatePartitionEvent implements CoordinatorEvent {

    private final TablePath tablePath;
    private final long tableId;

    private final String partitionName;
    private final long partitionId;
    private final PartitionAssignment partitionAssignment;

    public CreatePartitionEvent(
            TablePath tablePath,
            long tableId,
            long partitionId,
            String partitionName,
            PartitionAssignment partitionAssignment) {
        this.tablePath = tablePath;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.partitionName = partitionName;
        this.partitionAssignment = partitionAssignment;
    }

    public TablePath getTablePath() {
        return tablePath;
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

    public PartitionAssignment getPartitionAssignment() {
        return partitionAssignment;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CreatePartitionEvent that = (CreatePartitionEvent) o;
        return tableId == that.tableId
                && partitionId == that.partitionId
                && Objects.equals(tablePath, that.tablePath)
                && Objects.equals(partitionName, that.partitionName)
                && Objects.equals(partitionAssignment, that.partitionAssignment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tablePath, tableId, partitionName, partitionId, partitionAssignment);
    }

    @Override
    public String toString() {
        return "CreatePartitionEvent{"
                + "tablePath="
                + tablePath
                + ", tableId="
                + tableId
                + ", partitionName='"
                + partitionName
                + '\''
                + ", partitionId="
                + partitionId
                + ", partitionAssignment="
                + partitionAssignment
                + '}';
    }
}

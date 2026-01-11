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

package org.apache.fluss.server.coordinator.event;

import org.apache.fluss.server.zk.data.TableAssignment;

import javax.annotation.Nullable;

import java.util.Objects;

/** An event for alter table or partition bucket number. */
public class AlterTableOrPartitionBucketEvent implements CoordinatorEvent {

    private final long tableId;
    private @Nullable Long partitionId;
    private final TableAssignment tableAssignment;

    public AlterTableOrPartitionBucketEvent(long tableId, TableAssignment tableAssignment) {
        this.tableId = tableId;
        this.tableAssignment = tableAssignment;
    }

    public AlterTableOrPartitionBucketEvent(
            long tableId, @Nullable Long partitionId, TableAssignment tableAssignment) {
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.tableAssignment = tableAssignment;
    }

    public long getTableId() {
        return tableId;
    }

    @Nullable
    public Long getPartitionId() {
        return partitionId;
    }

    public TableAssignment getTableOrPartitionAssignment() {
        return tableAssignment;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AlterTableOrPartitionBucketEvent that = (AlterTableOrPartitionBucketEvent) o;
        return tableId == that.tableId
                && Objects.equals(partitionId, that.partitionId)
                && Objects.equals(tableAssignment, that.tableAssignment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, partitionId, tableAssignment);
    }

    @Override
    public String toString() {
        return "AlterTableOrPartitionBucketEvent{"
                + "tableId="
                + tableId
                + ", partitionId="
                + partitionId
                + ", tableAssignment="
                + tableAssignment
                + '}';
    }
}

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

package com.alibaba.fluss.metadata;

import com.alibaba.fluss.annotation.PublicEvolving;

import java.util.Objects;

/**
 * Information of a partition metadata, includes the partition's name and the partition id that
 * represents the unique identifier of the partition.
 *
 * @since 0.2
 */
@PublicEvolving
public class PartitionInfo {
    private final long partitionId;
    private final ResolvedPartitionSpec partitionSpec;

    public PartitionInfo(long partitionId, ResolvedPartitionSpec partitionSpec) {
        this.partitionId = partitionId;
        this.partitionSpec = partitionSpec;
    }

    /** Get the partition id. The id is globally unique in the Fluss cluster. */
    public long getPartitionId() {
        return partitionId;
    }

    /**
     * Get the partition name. The partition name is like table name to reference the partition. The
     * format of partition name follows {@link ResolvedPartitionSpec#getPartitionName()}.
     */
    public String getPartitionName() {
        return partitionSpec.getPartitionName();
    }

    public ResolvedPartitionSpec getResolvedPartitionSpec() {
        return partitionSpec;
    }

    public PartitionSpec getPartitionSpec() {
        return partitionSpec.toPartitionSpec();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionInfo that = (PartitionInfo) o;
        return partitionId == that.partitionId && Objects.equals(partitionSpec, that.partitionSpec);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionId, partitionSpec);
    }

    @Override
    public String toString() {
        return "Partition{name='" + getPartitionName() + '\'' + ", id=" + partitionId + '}';
    }
}

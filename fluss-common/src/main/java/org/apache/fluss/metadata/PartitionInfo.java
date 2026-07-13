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

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Information of a partition metadata, includes partition id (unique identifier of the partition),
 * partition name, remote data dir for partitioned data storage, etc.
 *
 * @since 0.2
 */
@PublicEvolving
public class PartitionInfo {
    private final long partitionId;
    private final ResolvedPartitionSpec partitionSpec;
    private final @Nullable String remoteDataDir;

    /**
     * The bucket count of this partition (i.e. bucket.num.actual). Always resolved: for partitions
     * created by older versions that did not persist a per-partition bucket count, the table-level
     * bucket count is filled in at construction time.
     */
    private final int bucketCount;

    public PartitionInfo(
            long partitionId,
            ResolvedPartitionSpec partitionSpec,
            @Nullable String remoteDataDir,
            int bucketCount) {
        this.partitionId = partitionId;
        this.partitionSpec = partitionSpec;
        this.remoteDataDir = remoteDataDir;
        this.bucketCount = bucketCount;
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

    @Nullable
    public String getRemoteDataDir() {
        return remoteDataDir;
    }

    /**
     * Get the bucket count of this partition (bucket.num.actual). For partitions created by older
     * versions without a persisted per-partition bucket count, this is the table-level bucket
     * count.
     */
    public int getBucketCount() {
        return bucketCount;
    }

    /**
     * Resolves the effective bucket count for a (possibly absent) partition: returns the
     * partition's bucket count when {@code partitionInfo} is non-null, otherwise the table-level
     * bucket count. The null case represents a non-partitioned table or a partition whose
     * PartitionInfo is not available.
     */
    public static int bucketCountOrDefault(
            @Nullable PartitionInfo partitionInfo, int tableBucketCount) {
        return partitionInfo != null ? partitionInfo.getBucketCount() : tableBucketCount;
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
        return partitionId == that.partitionId
                && Objects.equals(partitionSpec, that.partitionSpec)
                && Objects.equals(remoteDataDir, that.remoteDataDir)
                && bucketCount == that.bucketCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionId, partitionSpec, remoteDataDir, bucketCount);
    }

    @Override
    public String toString() {
        return "Partition{name='"
                + getPartitionName()
                + '\''
                + ", id="
                + partitionId
                + ", remoteDataDir="
                + remoteDataDir
                + ", bucketCount="
                + bucketCount
                + '}';
    }
}

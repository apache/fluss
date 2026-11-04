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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.TablePartition;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * The registration information of partition in {@link ZkData.PartitionZNode}. It is used to store
 * the partition information in zookeeper.
 *
 * @see PartitionRegistrationJsonSerde for json serialization and deserialization.
 */
public class PartitionRegistration {

    private final long tableId;
    private final long partitionId;

    /**
     * The remote data directory of the partition. It is null if and only if it is deserialized by
     * {@link PartitionRegistrationJsonSerde} from an existing node produced by an older version
     * that does not support multiple remote paths. But immediately after that, we will set it as
     * the default remote file path configured by {@link ConfigOptions#REMOTE_DATA_DIR} (see {@link
     * org.apache.fluss.server.zk.ZooKeeperClient#getPartition}). This unifies subsequent usage and
     * eliminates the need to account for differences between versions.
     */
    private final @Nullable String remoteDataDir;

    /**
     * The bucket count of this partition. It is null when deserialized from an older version that
     * does not persist per-partition bucket count. In that case, callers should fall back to the
     * table-level bucket count.
     */
    private final @Nullable Integer bucketCount;

    public PartitionRegistration(
            long tableId,
            long partitionId,
            @Nullable String remoteDataDir,
            @Nullable Integer bucketCount) {
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.remoteDataDir = remoteDataDir;
        this.bucketCount = bucketCount;
    }

    public long getTableId() {
        return tableId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    @Nullable
    public String getRemoteDataDir() {
        return remoteDataDir;
    }

    /** Returns the bucket count of this partition, or null if not persisted (old data). */
    @Nullable
    public Integer getBucketCount() {
        return bucketCount;
    }

    /**
     * Returns the bucket count of this partition, falling back to the given table-level bucket
     * count when this partition was persisted by an older version that does not store the
     * per-partition count.
     *
     * <p>The fallback is only valid at {@code bucketCountEpoch == 0} (legacy table or old server).
     * At {@code bucketCountEpoch > 0}, the first ALTER bucket.num atomically backfills the
     * per-partition count of every existing partition in the same ZooKeeper transaction that bumps
     * the epoch (see {@code ZooKeeperClient#updateTableWithPartitionBucketCountBackfill}), so a
     * partition observed at {@code bucketCountEpoch > 0} always carries a count. Reaching the throw
     * below therefore means that invariant was broken (an internal bug, e.g. a partial backfill);
     * it is theoretically unreachable and is surfaced as an {@link IllegalStateException} rather
     * than a retriable error, since a client metadata refresh cannot repair server-side state.
     */
    public int getBucketCountOrDefault(int tableBucketCount, long bucketCountEpoch) {
        if (bucketCount != null) {
            return bucketCount;
        }
        if (bucketCountEpoch == 0) {
            return tableBucketCount;
        }
        throw new IllegalStateException(
                "Partition "
                        + partitionId
                        + " is missing a per-partition bucket count at bucketCountEpoch "
                        + bucketCountEpoch
                        + "; the ALTER bucket.num backfill invariant was broken.");
    }

    public TablePartition toTablePartition() {
        return new TablePartition(tableId, partitionId);
    }

    /**
     * Returns a new registration with the given remote data directory. Should only be called by
     * {@link org.apache.fluss.server.zk.ZooKeeperClient#getPartition} when deserialize an old
     * PartitionRegistration node without remote data dir configured.
     *
     * @param remoteDataDir the remote data directory
     * @return a new registration with the given remote data directory
     */
    public PartitionRegistration newRemoteDataDir(String remoteDataDir) {
        return new PartitionRegistration(tableId, partitionId, remoteDataDir, bucketCount);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionRegistration that = (PartitionRegistration) o;
        return tableId == that.tableId
                && partitionId == that.partitionId
                && Objects.equals(remoteDataDir, that.remoteDataDir)
                && Objects.equals(bucketCount, that.bucketCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, partitionId, remoteDataDir, bucketCount);
    }

    @Override
    public String toString() {
        return "PartitionRegistration{"
                + "tableId="
                + tableId
                + ", partitionId="
                + partitionId
                + ", remoteDataDir='"
                + remoteDataDir
                + '\''
                + ", bucketCount="
                + bucketCount
                + '}';
    }
}

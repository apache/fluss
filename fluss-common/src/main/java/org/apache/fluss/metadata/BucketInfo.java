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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Information about a physical table bucket, including its replicas and leader/ISR state.
 *
 * @since 1.0
 */
@PublicEvolving
public final class BucketInfo {
    private final TablePath tablePath;
    private final long tableId;
    private final @Nullable Long partitionId;
    private final @Nullable String partitionName;
    private final int bucketId;
    private final @Nullable Integer leaderId;
    private final @Nullable Integer leaderEpoch;
    private final @Nullable Integer bucketEpoch;
    private final List<Integer> replicas;
    private final List<Integer> isr;

    /** Creates bucket information. */
    public BucketInfo(
            TablePath tablePath,
            long tableId,
            @Nullable Long partitionId,
            @Nullable String partitionName,
            int bucketId,
            @Nullable Integer leaderId,
            @Nullable Integer leaderEpoch,
            @Nullable Integer bucketEpoch,
            List<Integer> replicas,
            List<Integer> isr) {
        this.tablePath = checkNotNull(tablePath, "tablePath should not be null.");
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.partitionName = partitionName;
        this.bucketId = bucketId;
        this.leaderId = leaderId;
        this.leaderEpoch = leaderEpoch;
        this.bucketEpoch = bucketEpoch;
        this.replicas =
                Collections.unmodifiableList(
                        new ArrayList<>(checkNotNull(replicas, "replicas should not be null.")));
        this.isr =
                Collections.unmodifiableList(
                        new ArrayList<>(checkNotNull(isr, "isr should not be null.")));
    }

    /** Returns the table path. */
    public TablePath getTablePath() {
        return tablePath;
    }

    /** Returns the table ID. */
    public long getTableId() {
        return tableId;
    }

    /** Returns the partition ID, or an empty optional for a non-partitioned table. */
    public OptionalLong getPartitionId() {
        return partitionId == null ? OptionalLong.empty() : OptionalLong.of(partitionId);
    }

    /** Returns the partition name, or {@code null} for a non-partitioned table. */
    @Nullable
    public String getPartitionName() {
        return partitionName;
    }

    /** Returns the bucket ID. */
    public int getBucketId() {
        return bucketId;
    }

    /** Returns the leader ID, or an empty optional if no leader has been elected. */
    public OptionalInt getLeaderId() {
        return leaderId == null ? OptionalInt.empty() : OptionalInt.of(leaderId);
    }

    /** Returns the leader epoch, or an empty optional if no leader has been elected. */
    public OptionalInt getLeaderEpoch() {
        return leaderEpoch == null ? OptionalInt.empty() : OptionalInt.of(leaderEpoch);
    }

    /**
     * Returns the generation of the leader/ISR state, or an empty optional for legacy metadata. The
     * value {@code -1} indicates that no leader/ISR state exists.
     */
    public OptionalInt getBucketEpoch() {
        return bucketEpoch == null ? OptionalInt.empty() : OptionalInt.of(bucketEpoch);
    }

    /** Returns the replica IDs. */
    public List<Integer> getReplicas() {
        return replicas;
    }

    /** Returns the in-sync replica IDs. */
    public List<Integer> getIsr() {
        return isr;
    }

    @Override
    public String toString() {
        return "BucketInfo{"
                + "tablePath="
                + tablePath
                + ", tableId="
                + tableId
                + ", partitionId="
                + partitionId
                + ", partitionName='"
                + partitionName
                + '\''
                + ", bucketId="
                + bucketId
                + ", leaderId="
                + leaderId
                + ", leaderEpoch="
                + leaderEpoch
                + ", bucketEpoch="
                + bucketEpoch
                + ", replicas="
                + replicas
                + ", isr="
                + isr
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BucketInfo)) {
            return false;
        }
        BucketInfo that = (BucketInfo) o;
        return tableId == that.tableId
                && bucketId == that.bucketId
                && Objects.equals(tablePath, that.tablePath)
                && Objects.equals(partitionId, that.partitionId)
                && Objects.equals(partitionName, that.partitionName)
                && Objects.equals(leaderId, that.leaderId)
                && Objects.equals(leaderEpoch, that.leaderEpoch)
                && Objects.equals(bucketEpoch, that.bucketEpoch)
                && replicas.equals(that.replicas)
                && isr.equals(that.isr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                tablePath,
                tableId,
                partitionId,
                partitionName,
                bucketId,
                leaderId,
                leaderEpoch,
                bucketEpoch,
                replicas,
                isr);
    }
}

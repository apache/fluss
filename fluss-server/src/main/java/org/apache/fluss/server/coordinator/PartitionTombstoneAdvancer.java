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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.PartitionTombstone;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Pure helper for evolving a {@link PartitionTombstone} as partitions are dropped.
 *
 * <p>On each drop the helper either appends the dropped partition id to {@code explicitSet} (when
 * it sits above the current floor) or treats the call as a no-op for that partition when it is
 * already covered by the floor. When the caller provides the alive partition ids after the drop,
 * the helper safely advances {@code floor} to the highest id below every alive partition. The
 * version is always bumped by one so observers can detect that <em>something</em> happened, even
 * when the dropped partition was already tombstoned.
 */
@Internal
public final class PartitionTombstoneAdvancer {

    private PartitionTombstoneAdvancer() {}

    /**
     * Returns a new {@link PartitionTombstone} reflecting the drop of {@code partitionId}.
     *
     * @param before the current tombstone snapshot
     * @param partitionId the id of the partition being dropped
     * @return a new tombstone with {@code partitionId} absorbed (either into the floor or into the
     *     explicit set) and {@code version = before.getVersion() + 1}
     */
    /**
     * Returns a new {@link PartitionTombstone} reflecting the drop of {@code partitionId}.
     *
     * <p>When {@code alivePartitionIdsAfterDrop} is present, {@code floor} is advanced only to a
     * value below every alive partition id. When the set is empty, every known dropped id is safe
     * to fold into the floor. When the snapshot is unavailable, the update falls back to a
     * conservative explicit-set append.
     */
    public static PartitionTombstone dropPartition(
            PartitionTombstone before,
            long partitionId,
            @Nullable Collection<Long> alivePartitionIdsAfterDrop) {
        Set<Long> explicit = new HashSet<>(before.getExplicitSet());
        long floor = before.getFloor();
        if (partitionId > floor) {
            explicit.add(partitionId);
        }

        long newFloor = floor;
        if (alivePartitionIdsAfterDrop != null) {
            if (alivePartitionIdsAfterDrop.isEmpty()) {
                newFloor = Math.max(newFloor, partitionId);
                for (long explicitPartitionId : explicit) {
                    newFloor = Math.max(newFloor, explicitPartitionId);
                }
            } else {
                long minAlivePartitionId = Long.MAX_VALUE;
                for (long alivePartitionId : alivePartitionIdsAfterDrop) {
                    checkArgument(
                            alivePartitionId > floor,
                            "Alive partition id %s must be greater than tombstone floor %s.",
                            alivePartitionId,
                            floor);
                    checkArgument(
                            alivePartitionId != partitionId,
                            "Dropped partition id %s must not remain in alive partition ids.",
                            partitionId);
                    minAlivePartitionId = Math.min(minAlivePartitionId, alivePartitionId);
                }
                newFloor = Math.max(newFloor, minAlivePartitionId - 1);
            }
        }

        long floorToKeep = newFloor;
        explicit.removeIf(explicitPartitionId -> explicitPartitionId <= floorToKeep);
        return new PartitionTombstone(newFloor, explicit, before.getVersion() + 1);
    }

    /**
     * Validates that a newly allocated source partition id is not covered by the tombstone floor.
     */
    public static void validateNewPartitionId(PartitionTombstone tombstone, long partitionId) {
        checkArgument(
                partitionId > tombstone.getFloor(),
                "New partition id %s must be greater than tombstone floor %s.",
                partitionId,
                tombstone.getFloor());
    }
}

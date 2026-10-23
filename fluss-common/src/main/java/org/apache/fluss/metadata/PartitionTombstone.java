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

import org.apache.fluss.annotation.Internal;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Blacklist of dropped partitions for a partitioned source table. Index Bucket apply path and
 * compaction filter use this to drop entries whose source partition no longer exists.
 *
 * <p>Encoded as: {@code partitionId} is tombstoned iff {@code partitionId <= floor ||
 * explicitSet.contains(partitionId)}. The floor advances monotonically as an alive-partition
 * watermark: no alive source partition may have {@code partitionId <= floor}.
 *
 * <p>Stale views (TabletServer-side cache lag) are subsets of the authoritative state, so the
 * cleanup is fail-open: at worst we keep more rows than strictly necessary, never erase rows that
 * should still exist.
 */
@Internal
public final class PartitionTombstone {

    public static final PartitionTombstone EMPTY =
            new PartitionTombstone(-1L, Collections.emptySet(), 0L);

    private final long floor;
    private final Set<Long> explicitSet;
    private final long version;

    public PartitionTombstone(long floor, Set<Long> explicitSet, long version) {
        checkNotNull(explicitSet, "explicitSet");
        checkArgument(version >= 0L, "version must be non-negative, got %s", version);
        for (Long partitionId : explicitSet) {
            checkNotNull(partitionId, "explicitSet must not contain null");
            checkArgument(
                    partitionId > floor,
                    "explicit partition id %s must be greater than floor %s",
                    partitionId,
                    floor);
        }
        this.floor = floor;
        this.explicitSet = Collections.unmodifiableSet(new HashSet<>(explicitSet));
        this.version = version;
    }

    public long getFloor() {
        return floor;
    }

    public Set<Long> getExplicitSet() {
        return explicitSet;
    }

    public long getVersion() {
        return version;
    }

    public boolean isEmpty() {
        return floor < 0 && explicitSet.isEmpty();
    }

    public boolean isTombstoned(long partitionId) {
        return partitionId <= floor || explicitSet.contains(partitionId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PartitionTombstone)) {
            return false;
        }
        PartitionTombstone that = (PartitionTombstone) o;
        return floor == that.floor
                && version == that.version
                && explicitSet.equals(that.explicitSet);
    }

    @Override
    public int hashCode() {
        return Objects.hash(floor, explicitSet, version);
    }

    @Override
    public String toString() {
        return "PartitionTombstone{floor="
                + floor
                + ", explicitSet="
                + explicitSet
                + ", version="
                + version
                + "}";
    }
}

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

package org.apache.fluss.flink.source.state;

import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** A checkpoint of the current state of the containing the buckets that is already assigned. */
public class SourceEnumeratorState {

    /** buckets that have been assigned to readers. */
    private final Set<TableBucket> assignedBuckets;

    // partitions that have been assigned to readers.
    // mapping from partition id to partition name
    private final Map<Long, String> assignedPartitions;

    // the unassigned lake splits of a lake snapshot and the fluss splits base on the
    // lake snapshot
    @Nullable private final List<SourceSplitBase> remainingHybridLakeFlussSplits;

    // lease context for restore.
    private final String leaseId;

    /** Whether the initial partition discovery has been completed. */
    private final boolean initialDiscoveryFinished;

    /** Splits that have been initialized (offsets resolved) but not yet assigned to readers. */
    private final Collection<SourceSplitBase> unassignedSplits;

    public SourceEnumeratorState(
            Set<TableBucket> assignedBuckets,
            Map<Long, String> assignedPartitions,
            @Nullable List<SourceSplitBase> remainingHybridLakeFlussSplits,
            String leaseId) {
        this(
                assignedBuckets,
                assignedPartitions,
                remainingHybridLakeFlussSplits,
                leaseId,
                true,
                Collections.emptyList());
    }

    public SourceEnumeratorState(
            Set<TableBucket> assignedBuckets,
            Map<Long, String> assignedPartitions,
            @Nullable List<SourceSplitBase> remainingHybridLakeFlussSplits,
            String leaseId,
            boolean initialDiscoveryFinished) {
        this(
                assignedBuckets,
                assignedPartitions,
                remainingHybridLakeFlussSplits,
                leaseId,
                initialDiscoveryFinished,
                Collections.emptyList());
    }

    public SourceEnumeratorState(
            Set<TableBucket> assignedBuckets,
            Map<Long, String> assignedPartitions,
            @Nullable List<SourceSplitBase> remainingHybridLakeFlussSplits,
            String leaseId,
            boolean initialDiscoveryFinished,
            Collection<SourceSplitBase> unassignedSplits) {
        this.assignedBuckets = assignedBuckets;
        this.assignedPartitions = assignedPartitions;
        this.remainingHybridLakeFlussSplits = remainingHybridLakeFlussSplits;
        this.leaseId = leaseId;
        this.initialDiscoveryFinished = initialDiscoveryFinished;
        this.unassignedSplits = unassignedSplits;
    }

    public Set<TableBucket> getAssignedBuckets() {
        return assignedBuckets;
    }

    public Map<Long, String> getAssignedPartitions() {
        return assignedPartitions;
    }

    @Nullable
    public List<SourceSplitBase> getRemainingHybridLakeFlussSplits() {
        return remainingHybridLakeFlussSplits;
    }

    public String getLeaseId() {
        return leaseId;
    }

    public boolean isInitialDiscoveryFinished() {
        return initialDiscoveryFinished;
    }

    public Collection<SourceSplitBase> getUnassignedSplits() {
        return unassignedSplits;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SourceEnumeratorState that = (SourceEnumeratorState) o;
        return Objects.equals(assignedBuckets, that.assignedBuckets)
                && Objects.equals(assignedPartitions, that.assignedPartitions)
                && Objects.equals(
                        remainingHybridLakeFlussSplits, that.remainingHybridLakeFlussSplits)
                && Objects.equals(leaseId, that.leaseId)
                && initialDiscoveryFinished == that.initialDiscoveryFinished
                && Objects.equals(unassignedSplits, that.unassignedSplits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                assignedBuckets,
                assignedPartitions,
                remainingHybridLakeFlussSplits,
                initialDiscoveryFinished,
                unassignedSplits);
    }

    @Override
    public String toString() {
        return "SourceEnumeratorState{"
                + "assignedBuckets="
                + assignedBuckets
                + ", assignedPartitions="
                + assignedPartitions
                + ", remainingHybridLakeFlussSplits="
                + remainingHybridLakeFlussSplits
                + ", leaseId="
                + leaseId
                + ", initialDiscoveryFinished="
                + initialDiscoveryFinished
                + ", unassignedSplits="
                + unassignedSplits
                + '}';
    }
}

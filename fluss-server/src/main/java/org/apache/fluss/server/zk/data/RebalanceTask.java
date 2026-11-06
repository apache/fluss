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

import org.apache.fluss.cluster.rebalance.RebalancePlan;
import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePartition;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The generated rebalance task for this cluster.
 *
 * <p>The latest execution {@link RebalancePlan} and related execution {@link RebalanceStatus} will
 * be stored in {@link ZkData.RebalanceZNode}.
 *
 * @see RebalanceTaskJsonSerde for json serialization and deserialization.
 */
public class RebalanceTask {

    /** The rebalance id to trace rebalace task. */
    private final String rebalanceId;

    /** The rebalance status for the overall rebalance. */
    private final RebalanceStatus rebalanceStatus;

    /** A mapping from tableBucket to RebalancePlanForBuckets of none-partitioned table. */
    private final Map<Long, List<RebalancePlanForBucket>> planForBuckets;

    /** A mapping from tableBucket to RebalancePlanForBuckets of partitioned table. */
    private final Map<TablePartition, List<RebalancePlanForBucket>>
            planForBucketsOfPartitionedTable;

    /** Per-bucket final status. A bucket without an entry has the {@link #rebalanceStatus}. */
    private final Map<TableBucket, RebalanceStatus> bucketStatuses;

    /** The time when this rebalance task was started, or {@code -1} if unset. */
    private final long startedAtMs;

    /** The time when this rebalance task reached a final status, or {@code -1} if unset. */
    private final long completedAtMs;

    /** Creates a task whose buckets all have the task status. */
    public RebalanceTask(
            String rebalanceId,
            RebalanceStatus rebalanceStatus,
            Map<TableBucket, RebalancePlanForBucket> bucketPlan,
            long startedAtMs,
            long completedAtMs) {
        this(
                rebalanceId,
                rebalanceStatus,
                bucketPlan,
                Collections.emptyMap(),
                startedAtMs,
                completedAtMs);
    }

    /**
     * Creates a task with per-bucket final statuses. A bucket missing from {@code bucketStatuses}
     * has the task status.
     */
    public RebalanceTask(
            String rebalanceId,
            RebalanceStatus rebalanceStatus,
            Map<TableBucket, RebalancePlanForBucket> bucketPlan,
            Map<TableBucket, RebalanceStatus> bucketStatuses,
            long startedAtMs,
            long completedAtMs) {
        this.rebalanceId = rebalanceId;
        this.rebalanceStatus = rebalanceStatus;
        // Only keep a bucket's own status when it differs from the task status: a bucket without
        // one already resolves to the task status, so this keeps equals() and the JSON form the
        // same for tasks with identical effective statuses.
        this.bucketStatuses = new HashMap<>();
        for (Map.Entry<TableBucket, RebalanceStatus> entry : bucketStatuses.entrySet()) {
            if (entry.getValue() != rebalanceStatus) {
                this.bucketStatuses.put(entry.getKey(), entry.getValue());
            }
        }
        this.planForBuckets = new HashMap<>();
        this.planForBucketsOfPartitionedTable = new HashMap<>();
        this.startedAtMs = startedAtMs;
        this.completedAtMs = completedAtMs;

        for (Map.Entry<TableBucket, RebalancePlanForBucket> entry : bucketPlan.entrySet()) {
            TableBucket tableBucket = entry.getKey();
            RebalancePlanForBucket rebalancePlanForBucket = entry.getValue();
            if (tableBucket.getPartitionId() == null) {
                planForBuckets
                        .computeIfAbsent(tableBucket.getTableId(), k -> new ArrayList<>())
                        .add(rebalancePlanForBucket);
            } else {
                TablePartition tp =
                        new TablePartition(tableBucket.getTableId(), tableBucket.getPartitionId());
                planForBucketsOfPartitionedTable
                        .computeIfAbsent(tp, k -> new ArrayList<>())
                        .add(rebalancePlanForBucket);
            }
        }
    }

    public String getRebalanceId() {
        return rebalanceId;
    }

    public RebalanceStatus getRebalanceStatus() {
        return rebalanceStatus;
    }

    public Map<Long, List<RebalancePlanForBucket>> getPlanForBuckets() {
        return planForBuckets;
    }

    public Map<TablePartition, List<RebalancePlanForBucket>> getPlanForBucketsOfPartitionedTable() {
        return planForBucketsOfPartitionedTable;
    }

    /** Returns the final status of the bucket, or the task status if the bucket has none. */
    public RebalanceStatus getBucketStatus(TableBucket tableBucket) {
        return bucketStatuses.getOrDefault(tableBucket, rebalanceStatus);
    }

    /** The time when this rebalance task was started, in epoch milliseconds. -1 if unset. */
    public long getStartedAtMs() {
        return startedAtMs;
    }

    /**
     * The time when this rebalance task reached a final status, in epoch milliseconds. -1 if unset.
     */
    public long getCompletedAtMs() {
        return completedAtMs;
    }

    public Map<TableBucket, RebalancePlanForBucket> getExecutePlan() {
        Map<TableBucket, RebalancePlanForBucket> executePlan = new HashMap<>();
        planForBuckets.forEach(
                (tableId, rebalancePlanForBuckets) ->
                        rebalancePlanForBuckets.forEach(
                                rebalancePlanForBucket ->
                                        executePlan.put(
                                                rebalancePlanForBucket.getTableBucket(),
                                                rebalancePlanForBucket)));

        planForBucketsOfPartitionedTable.forEach(
                (tablePartition, rebalancePlanForBuckets) ->
                        rebalancePlanForBuckets.forEach(
                                rebalancePlanForBucket ->
                                        executePlan.put(
                                                rebalancePlanForBucket.getTableBucket(),
                                                rebalancePlanForBucket)));
        return executePlan;
    }

    @Override
    public String toString() {
        return "RebalancePlan{"
                + "rebalanceId='"
                + rebalanceId
                + ", rebalanceStatus="
                + rebalanceStatus
                + ", planForBuckets="
                + planForBuckets
                + ", planForBucketsOfPartitionedTable="
                + planForBucketsOfPartitionedTable
                + ", bucketStatuses="
                + bucketStatuses
                + ", startedAtMs="
                + startedAtMs
                + ", completedAtMs="
                + completedAtMs
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RebalanceTask that = (RebalanceTask) o;
        return rebalanceStatus == that.rebalanceStatus
                && startedAtMs == that.startedAtMs
                && completedAtMs == that.completedAtMs
                && Objects.equals(rebalanceId, that.rebalanceId)
                && Objects.equals(planForBuckets, that.planForBuckets)
                && Objects.equals(
                        planForBucketsOfPartitionedTable, that.planForBucketsOfPartitionedTable)
                && Objects.equals(bucketStatuses, that.bucketStatuses);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                rebalanceId,
                rebalanceStatus,
                planForBuckets,
                planForBucketsOfPartitionedTable,
                bucketStatuses,
                startedAtMs,
                completedAtMs);
    }

    /** Returns the status recorded for the bucket itself, or null if it has none. */
    @Nullable
    RebalanceStatus getOwnBucketStatus(TableBucket tableBucket) {
        return bucketStatuses.get(tableBucket);
    }
}

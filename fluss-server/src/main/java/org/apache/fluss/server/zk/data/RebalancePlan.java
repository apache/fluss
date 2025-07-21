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

import org.apache.fluss.cluster.maintencance.RebalancePlanForBucket;
import org.apache.fluss.cluster.maintencance.RebalanceStatus;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The generated rebalance plan for this cluster.
 *
 * <p>The latest execution rebalance plan will be stored in {@link ZkData.RebalanceZNode}.
 *
 * @see RebalancePlanJsonSerde for json serialization and deserialization.
 */
public class RebalancePlan {

    /** The rebalance status of this rebalance plan. */
    private final RebalanceStatus rebalanceStatus;

    /** A mapping from tableBucket to RebalancePlanForBuckets of none-partitioned table. */
    private final Map<Long, List<RebalancePlanForBucket>> planForBuckets;

    /** A mapping from tableBucket to RebalancePlanForBuckets of partitioned table. */
    private final Map<TablePartition, List<RebalancePlanForBucket>>
            planForBucketsOfPartitionedTable;

    public RebalancePlan(
            RebalanceStatus rebalanceStatus, Map<TableBucket, RebalancePlanForBucket> bucketPlan) {
        this.planForBuckets = new HashMap<>();
        this.planForBucketsOfPartitionedTable = new HashMap<>();

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

        this.rebalanceStatus = rebalanceStatus;
    }

    public Map<Long, List<RebalancePlanForBucket>> getPlanForBuckets() {
        return planForBuckets;
    }

    public Map<TablePartition, List<RebalancePlanForBucket>> getPlanForBucketsOfPartitionedTable() {
        return planForBucketsOfPartitionedTable;
    }

    public RebalanceStatus getRebalanceStatus() {
        return rebalanceStatus;
    }

    @Override
    public String toString() {
        return "RebalancePlan{"
                + "planForBuckets="
                + planForBuckets
                + ", planForBucketsOfPartitionedTable="
                + planForBucketsOfPartitionedTable
                + ", rebalanceStatus="
                + rebalanceStatus
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

        RebalancePlan that = (RebalancePlan) o;

        if (!Objects.equals(planForBuckets, that.planForBuckets)) {
            return false;
        }
        return Objects.equals(
                        planForBucketsOfPartitionedTable, that.planForBucketsOfPartitionedTable)
                && rebalanceStatus == that.rebalanceStatus;
    }

    @Override
    public int hashCode() {
        return Objects.hash(planForBuckets, planForBucketsOfPartitionedTable, rebalanceStatus);
    }
}

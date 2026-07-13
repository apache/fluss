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

import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceResultForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.metadata.TableBucket;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.fluss.cluster.rebalance.RebalanceStatus.FINAL_STATUSES;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.NOT_STARTED;

/** Persistent task state for one round of a rebalance execution. */
public final class RebalanceRound {

    private final int roundIndex;
    private final Map<TableBucket, RebalanceResultForBucket> progressForBucketMap;

    public RebalanceRound(
            int roundIndex, Map<TableBucket, RebalanceResultForBucket> progressForBucketMap) {
        this.roundIndex = roundIndex;
        this.progressForBucketMap =
                Collections.unmodifiableMap(new LinkedHashMap<>(progressForBucketMap));
    }

    public static RebalanceRound ofPlan(
            int roundIndex, Map<TableBucket, RebalancePlanForBucket> executePlan) {
        Map<TableBucket, RebalanceResultForBucket> progressForBucketMap = new LinkedHashMap<>();
        for (Map.Entry<TableBucket, RebalancePlanForBucket> entry : executePlan.entrySet()) {
            progressForBucketMap.put(
                    entry.getKey(), RebalanceResultForBucket.of(entry.getValue(), NOT_STARTED));
        }
        return new RebalanceRound(roundIndex, progressForBucketMap);
    }

    public int getRoundIndex() {
        return roundIndex;
    }

    public Map<TableBucket, RebalanceResultForBucket> getProgressForBucketMap() {
        return progressForBucketMap;
    }

    public Map<TableBucket, RebalancePlanForBucket> getExecutePlan() {
        Map<TableBucket, RebalancePlanForBucket> executePlan = new LinkedHashMap<>();
        for (Map.Entry<TableBucket, RebalanceResultForBucket> entry :
                progressForBucketMap.entrySet()) {
            executePlan.put(entry.getKey(), entry.getValue().plan());
        }
        return executePlan;
    }

    public boolean hasUnfinishedTasks() {
        for (RebalanceResultForBucket resultForBucket : progressForBucketMap.values()) {
            if (!FINAL_STATUSES.contains(resultForBucket.status())) {
                return true;
            }
        }
        return false;
    }

    public RebalanceRound withBucketStatus(
            TableBucket tableBucket,
            RebalancePlanForBucket planForBucket,
            RebalanceStatus rebalanceStatus) {
        Map<TableBucket, RebalanceResultForBucket> newProgressForBucketMap =
                new LinkedHashMap<>(progressForBucketMap);
        newProgressForBucketMap.put(
                tableBucket, RebalanceResultForBucket.of(planForBucket, rebalanceStatus));
        return new RebalanceRound(roundIndex, newProgressForBucketMap);
    }

    public RebalanceRound withUnfinishedStatus(RebalanceStatus rebalanceStatus) {
        Map<TableBucket, RebalanceResultForBucket> newProgressForBucketMap = new LinkedHashMap<>();
        for (Map.Entry<TableBucket, RebalanceResultForBucket> entry :
                progressForBucketMap.entrySet()) {
            RebalanceResultForBucket resultForBucket = entry.getValue();
            if (FINAL_STATUSES.contains(resultForBucket.status())) {
                newProgressForBucketMap.put(entry.getKey(), resultForBucket);
            } else {
                newProgressForBucketMap.put(
                        entry.getKey(),
                        RebalanceResultForBucket.of(resultForBucket.plan(), rebalanceStatus));
            }
        }
        return new RebalanceRound(roundIndex, newProgressForBucketMap);
    }

    @Override
    public String toString() {
        return "RebalanceRound{"
                + "roundIndex="
                + roundIndex
                + ", progressForBucketMap="
                + progressForBucketMap
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
        RebalanceRound that = (RebalanceRound) o;
        if (roundIndex != that.roundIndex
                || progressForBucketMap.size() != that.progressForBucketMap.size()) {
            return false;
        }
        for (Map.Entry<TableBucket, RebalanceResultForBucket> entry :
                progressForBucketMap.entrySet()) {
            RebalanceResultForBucket thatResult = that.progressForBucketMap.get(entry.getKey());
            if (thatResult == null || !equalsRebalanceResult(entry.getValue(), thatResult)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int entriesHash = 0;
        for (Map.Entry<TableBucket, RebalanceResultForBucket> entry :
                progressForBucketMap.entrySet()) {
            RebalanceResultForBucket resultForBucket = entry.getValue();
            entriesHash +=
                    Objects.hash(entry.getKey(), resultForBucket.plan(), resultForBucket.status());
        }
        return 31 * Objects.hash(roundIndex) + entriesHash;
    }

    private static boolean equalsRebalanceResult(
            RebalanceResultForBucket left, RebalanceResultForBucket right) {
        return Objects.equals(left.plan(), right.plan()) && left.status() == right.status();
    }
}

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

import org.apache.fluss.cluster.rebalance.RebalanceStatus;

import java.util.Objects;

/** Persistent metadata for a round-based rebalance execution. */
public final class RebalanceExecution {

    private final String rebalanceId;
    private final RebalanceStatus rebalanceStatus;
    private final int maxBucketsPerRound;
    private final int currentRound;
    private final int totalRounds;

    public RebalanceExecution(
            String rebalanceId,
            RebalanceStatus rebalanceStatus,
            int maxBucketsPerRound,
            int currentRound,
            int totalRounds) {
        this.rebalanceId = rebalanceId;
        this.rebalanceStatus = rebalanceStatus;
        this.maxBucketsPerRound = maxBucketsPerRound;
        this.currentRound = currentRound;
        this.totalRounds = totalRounds;
    }

    public String getRebalanceId() {
        return rebalanceId;
    }

    public RebalanceStatus getRebalanceStatus() {
        return rebalanceStatus;
    }

    public int getMaxBucketsPerRound() {
        return maxBucketsPerRound;
    }

    public int getCurrentRound() {
        return currentRound;
    }

    public int getTotalRounds() {
        return totalRounds;
    }

    public RebalanceExecution withStatus(RebalanceStatus newStatus) {
        return new RebalanceExecution(
                rebalanceId, newStatus, maxBucketsPerRound, currentRound, totalRounds);
    }

    public RebalanceExecution withCurrentRound(int newCurrentRound) {
        return new RebalanceExecution(
                rebalanceId, rebalanceStatus, maxBucketsPerRound, newCurrentRound, totalRounds);
    }

    public RebalanceExecution withStatusAndCurrentRound(
            RebalanceStatus newStatus, int newCurrentRound) {
        return new RebalanceExecution(
                rebalanceId, newStatus, maxBucketsPerRound, newCurrentRound, totalRounds);
    }

    @Override
    public String toString() {
        return "RebalanceExecution{"
                + "rebalanceId='"
                + rebalanceId
                + '\''
                + ", rebalanceStatus="
                + rebalanceStatus
                + ", maxBucketsPerRound="
                + maxBucketsPerRound
                + ", currentRound="
                + currentRound
                + ", totalRounds="
                + totalRounds
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
        RebalanceExecution that = (RebalanceExecution) o;
        return maxBucketsPerRound == that.maxBucketsPerRound
                && currentRound == that.currentRound
                && totalRounds == that.totalRounds
                && Objects.equals(rebalanceId, that.rebalanceId)
                && rebalanceStatus == that.rebalanceStatus;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                rebalanceId, rebalanceStatus, maxBucketsPerRound, currentRound, totalRounds);
    }
}

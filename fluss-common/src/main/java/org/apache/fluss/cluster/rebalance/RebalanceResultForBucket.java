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

package org.apache.fluss.cluster.rebalance;

import org.apache.fluss.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

import static org.apache.fluss.cluster.rebalance.RebalanceStatusForBucket.FAILED;

/**
 * Result of rebalance process for a tabletBucket.
 *
 * @since 0.8
 */
@PublicEvolving
public class RebalanceResultForBucket {

    private final @Nullable Integer originalLeader;
    private final @Nullable Integer targetLeader;
    private final List<Integer> originReplicas;
    private final List<Integer> targetReplicas;
    private RebalanceStatusForBucket rebalanceStatusForBucket;

    public RebalanceResultForBucket(
            @Nullable Integer originalLeader,
            @Nullable Integer targetLeader,
            RebalanceStatusForBucket rebalanceStatusForBucket) {
        this(
                originalLeader,
                targetLeader,
                Collections.emptyList(),
                Collections.emptyList(),
                rebalanceStatusForBucket);
    }

    public RebalanceResultForBucket(
            List<Integer> originReplicas,
            List<Integer> targetReplicas,
            RebalanceStatusForBucket rebalanceStatusForBucket) {
        this(null, null, originReplicas, targetReplicas, rebalanceStatusForBucket);
    }

    public RebalanceResultForBucket(
            @Nullable Integer originalLeader,
            @Nullable Integer targetLeader,
            List<Integer> originReplicas,
            List<Integer> targetReplicas,
            RebalanceStatusForBucket rebalanceStatusForBucket) {
        this.originalLeader = originalLeader;
        this.targetLeader = targetLeader;
        this.originReplicas = originReplicas;
        this.targetReplicas = targetReplicas;
        this.rebalanceStatusForBucket = rebalanceStatusForBucket;
    }

    public @Nullable Integer originalLeader() {
        return originalLeader;
    }

    public @Nullable Integer targetLeader() {
        return targetLeader;
    }

    public List<Integer> replicas() {
        return originReplicas;
    }

    public List<Integer> targetReplicas() {
        return targetReplicas;
    }

    public RebalanceResultForBucket markFailed() {
        this.rebalanceStatusForBucket = FAILED;
        return this;
    }

    public RebalanceResultForBucket markCompleted() {
        this.rebalanceStatusForBucket = RebalanceStatusForBucket.COMPLETED;
        return this;
    }

    public boolean isLeaderAction() {
        return originalLeader != null && targetLeader != null;
    }

    public RebalanceStatusForBucket rebalanceStatus() {
        return rebalanceStatusForBucket;
    }

    public static RebalanceResultForBucket of(
            RebalancePlanForBucket planForBucket, RebalanceStatusForBucket status) {
        return new RebalanceResultForBucket(
                planForBucket.getOriginalLeader(),
                planForBucket.getNewLeader(),
                planForBucket.getOriginReplicas(),
                planForBucket.getNewReplicas(),
                status);
    }

    @Override
    public String toString() {
        return "RebalanceResultForBucket{"
                + "originalLeader="
                + originalLeader
                + ", targetLeader="
                + targetLeader
                + ", originReplicas="
                + originReplicas
                + ", targetReplicas="
                + targetReplicas
                + ", rebalanceStatusForBucket="
                + rebalanceStatusForBucket
                + '}';
    }
}

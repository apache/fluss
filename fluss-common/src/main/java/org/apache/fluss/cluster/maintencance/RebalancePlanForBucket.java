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

package org.apache.fluss.cluster.maintencance;

import org.apache.fluss.annotation.PublicEvolving;

import java.util.List;
import java.util.Objects;

/**
 * a Generated rebalance plan for a tableBucket.
 *
 * @since 0.8
 */
@PublicEvolving
public class RebalancePlanForBucket {
    private final int bucketId;
    private final int originalLeader;
    private final int newLeader;
    private final List<Integer> originReplicas;
    private final List<Integer> newReplicas;

    public RebalancePlanForBucket(
            int bucketId,
            int originalLeader,
            int newLeader,
            List<Integer> originReplicas,
            List<Integer> newReplicas) {
        this.bucketId = bucketId;
        this.originalLeader = originalLeader;
        this.newLeader = newLeader;
        this.originReplicas = originReplicas;
        this.newReplicas = newReplicas;
    }

    public int getBucketId() {
        return bucketId;
    }

    public Integer getOriginalLeader() {
        return originalLeader;
    }

    public Integer getNewLeader() {
        return newLeader;
    }

    public List<Integer> getOriginReplicas() {
        return originReplicas;
    }

    public List<Integer> getNewReplicas() {
        return newReplicas;
    }

    @Override
    public String toString() {
        return "RebalancePlanForBucket{"
                + "bucketId="
                + bucketId
                + ", originalLeader="
                + originalLeader
                + ", newLeader="
                + newLeader
                + ", originReplicas="
                + originReplicas
                + ", newReplicas="
                + newReplicas
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
        RebalancePlanForBucket that = (RebalancePlanForBucket) o;
        return bucketId == that.bucketId
                && originalLeader == that.originalLeader
                && newLeader == that.newLeader
                && Objects.equals(originReplicas, that.originReplicas)
                && Objects.equals(newReplicas, that.newReplicas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucketId, originalLeader, newLeader, originReplicas, newReplicas);
    }
}

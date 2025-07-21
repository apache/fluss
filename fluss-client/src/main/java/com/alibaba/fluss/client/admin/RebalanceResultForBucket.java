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

package com.alibaba.fluss.client.admin;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.cluster.maintencance.RebalanceStatusForBucket;

import java.util.List;

/**
 * Result of rebalance process for a tabletBucket.
 *
 * @since 0.8
 */
@PublicEvolving
public class RebalanceResultForBucket {

    private final List<Integer> originReplicas;
    private final List<Integer> newReplicas;
    private final RebalanceStatusForBucket rebalanceStatusForBucket;

    public RebalanceResultForBucket(
            List<Integer> originReplicas,
            List<Integer> newReplicas,
            RebalanceStatusForBucket rebalanceStatusForBucket) {
        this.originReplicas = originReplicas;
        this.newReplicas = newReplicas;
        this.rebalanceStatusForBucket = rebalanceStatusForBucket;
    }

    public List<Integer> replicas() {
        return originReplicas;
    }

    public List<Integer> newReplicas() {
        return newReplicas;
    }

    public RebalanceStatusForBucket rebalanceStatus() {
        return rebalanceStatusForBucket;
    }

    @Override
    public String toString() {
        return "BucketReassignment{"
                + "replicas="
                + originReplicas
                + ", newReplicas="
                + newReplicas
                + ", rebalanceStatus="
                + rebalanceStatusForBucket
                + '}';
    }
}

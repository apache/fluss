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
import org.apache.fluss.metadata.TableBucket;

import java.util.List;

/**
 * Status of rebalance process for a tabletBucket.
 *
 * @since 0.8
 */
@PublicEvolving
public class RebalanceResultForBucket {
    private final RebalancePlanForBucket rebalancePlanForBucket;
    private RebalanceStatusForBucket rebalanceStatusForBucket;

    private RebalanceResultForBucket(
            RebalancePlanForBucket rebalancePlanForBucket,
            RebalanceStatusForBucket rebalanceStatusForBucket) {
        this.rebalancePlanForBucket = rebalancePlanForBucket;
        this.rebalanceStatusForBucket = rebalanceStatusForBucket;
    }

    public TableBucket tableBucket() {
        return rebalancePlanForBucket.getTableBucket();
    }

    public RebalancePlanForBucket planForBucket() {
        return rebalancePlanForBucket;
    }

    public List<Integer> newReplicas() {
        return rebalancePlanForBucket.getNewReplicas();
    }

    public List<Integer> originReplicas() {
        return rebalancePlanForBucket.getOriginReplicas();
    }

    public RebalanceResultForBucket setNewStatus(RebalanceStatusForBucket status) {
        this.rebalanceStatusForBucket = status;
        return this;
    }

    public RebalanceStatusForBucket status() {
        return rebalanceStatusForBucket;
    }

    public static RebalanceResultForBucket of(
            RebalancePlanForBucket planForBucket, RebalanceStatusForBucket status) {
        return new RebalanceResultForBucket(planForBucket, status);
    }

    @Override
    public String toString() {
        return "RebalanceResultForBucket{"
                + "rebalancePlanForBucket="
                + rebalancePlanForBucket
                + ", rebalanceStatusForBucket="
                + rebalanceStatusForBucket
                + '}';
    }
}

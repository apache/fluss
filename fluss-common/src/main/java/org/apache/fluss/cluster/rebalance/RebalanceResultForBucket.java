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

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Status of rebalance process for a tabletBucket.
 *
 * @since 0.9
 */
@PublicEvolving
public class RebalanceResultForBucket {
    private final RebalancePlanForBucket rebalancePlanForBucket;
    private final RebalanceStatus rebalanceStatus;

    public RebalanceResultForBucket(
            RebalancePlanForBucket rebalancePlanForBucket, RebalanceStatus rebalanceStatus) {
        this.rebalancePlanForBucket = checkNotNull(rebalancePlanForBucket);
        this.rebalanceStatus = checkNotNull(rebalanceStatus);
    }

    public TableBucket tableBucket() {
        return rebalancePlanForBucket.getTableBucket();
    }

    public RebalancePlanForBucket plan() {
        return rebalancePlanForBucket;
    }

    public List<Integer> newReplicas() {
        return rebalancePlanForBucket.getNewReplicas();
    }

    public RebalanceStatus status() {
        return rebalanceStatus;
    }

    public static RebalanceResultForBucket of(
            RebalancePlanForBucket planForBucket, RebalanceStatus status) {
        return new RebalanceResultForBucket(planForBucket, status);
    }

    @Override
    public String toString() {
        return "RebalanceResultForBucket{"
                + "rebalancePlanForBucket="
                + rebalancePlanForBucket
                + ", rebalanceStatus="
                + rebalanceStatus
                + '}';
    }
}

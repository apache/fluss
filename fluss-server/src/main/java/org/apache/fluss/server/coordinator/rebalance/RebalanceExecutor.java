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

package org.apache.fluss.server.coordinator.rebalance;

import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.server.coordinator.CoordinatorContext;

/** Coordinator operations needed to execute and reconcile rebalance bucket plans. */
public interface RebalanceExecutor {

    /** Returns the coordinator state used to build a cluster model. */
    CoordinatorContext getCoordinatorContext();

    /** Starts or resumes one bucket plan. */
    void tryToExecuteRebalanceTask(RebalancePlanForBucket planForBucket);

    /** Returns whether a non-final persisted plan can be treated as complete during recovery. */
    boolean isRebalanceTaskComplete(RebalancePlanForBucket planForBucket);

    /** Returns whether the plan remains at its clean origin state. */
    boolean isRebalanceTaskAtOrigin(RebalancePlanForBucket planForBucket);
}

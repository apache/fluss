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

package org.apache.fluss.server.coordinator.event;

import org.apache.fluss.rpc.messages.RebalanceResponse;
import org.apache.fluss.server.coordinator.rebalance.goal.Goal;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/** The event for rebalance. */
public class RebalanceEvent implements CoordinatorEvent {

    private final List<Goal> goalsByPriority;
    private final CompletableFuture<RebalanceResponse> respCallback;

    public RebalanceEvent(
            List<Goal> goalsByPriority, CompletableFuture<RebalanceResponse> respCallback) {
        this.goalsByPriority = goalsByPriority;
        this.respCallback = respCallback;
    }

    public List<Goal> getGoalsByPriority() {
        return goalsByPriority;
    }

    public CompletableFuture<RebalanceResponse> getRespCallback() {
        return respCallback;
    }
}

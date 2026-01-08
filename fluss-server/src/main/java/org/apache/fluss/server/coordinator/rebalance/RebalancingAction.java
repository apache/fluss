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

import org.apache.fluss.metadata.TableBucket;

/** Represents the load rebalancing operation over a replica for Fluss Load GoalOptimizer. */
public class RebalancingAction {
    private final TableBucket tableBucket;
    private final Integer sourceServerId;
    private final Integer destinationServerId;
    private final ActionType actionType;

    public RebalancingAction(
            TableBucket tableBucket,
            Integer sourceServerId,
            Integer destinationServerId,
            ActionType actionType) {
        this.tableBucket = tableBucket;
        this.sourceServerId = sourceServerId;
        this.destinationServerId = destinationServerId;
        this.actionType = actionType;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public Integer getSourceServerId() {
        return sourceServerId;
    }

    public Integer getDestinationServerId() {
        return destinationServerId;
    }

    public ActionType getActionType() {
        return actionType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RebalancingAction that = (RebalancingAction) o;

        if (!tableBucket.equals(that.tableBucket)) {
            return false;
        }
        if (!sourceServerId.equals(that.sourceServerId)) {
            return false;
        }
        if (!destinationServerId.equals(that.destinationServerId)) {
            return false;
        }
        return actionType == that.actionType;
    }

    @Override
    public int hashCode() {
        int result = tableBucket.hashCode();
        result = 31 * result + sourceServerId.hashCode();
        result = 31 * result + destinationServerId.hashCode();
        result = 31 * result + actionType.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ReBalancingAction{"
                + "tableBucket="
                + tableBucket
                + ", sourceServerId="
                + sourceServerId
                + ", destinationServerId="
                + destinationServerId
                + ", actionType="
                + actionType
                + '}';
    }
}

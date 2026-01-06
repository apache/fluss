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

package org.apache.fluss.flink.procedure;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.cluster.rebalance.GoalType;
import org.apache.fluss.cluster.rebalance.RebalancePlan;
import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Procedure to trigger rebalance.
 *
 * <p>This procedure allows triggering rebalance with different goals. See {@link
 * Admin#rebalance(List, boolean)} for more details.
 *
 * <p>Usage examples:
 *
 * <pre>
 * -- Trigger rebalance with REPLICA_DISTRIBUTION goal
 * CALL sys.rebalance('REPLICA_DISTRIBUTION');
 * -- Trigger rebalance with REPLICA_DISTRIBUTION and LEADER_DISTRIBUTION goals
 * CALL sys.rebalance('REPLICA_DISTRIBUTION;LEADER_DISTRIBUTION');
 *
 * -- Trigger rebalance without dry run
 * CALL sys.rebalance('REPLICA_DISTRIBUTION', false);
 *
 * -- Trigger rebalance with dry run
 * CALL sys.rebalance('REPLICA_DISTRIBUTION', true);
 * </pre>
 */
public class RebalanceProcedure extends ProcedureBase {

    /**
     * As flink call don't support input a nested type like 'ARRAY'. So priorityGoals is defined as
     * a String type, and different goals are split by ';'.
     */
    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "priorityGoals", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "dryRun", type = @DataTypeHint("BOOLEAN"), isOptional = true)
            })
    public String[] call(ProcedureContext context, String priorityGoals, @Nullable Boolean dryRun)
            throws Exception {
        List<GoalType> goalTypes = validateAndGetPriorityGoals(priorityGoals);
        RebalancePlan rebalancePlan = admin.rebalance(goalTypes, dryRun != null && dryRun).get();
        return planToString(rebalancePlan);
    }

    private static String[] planToString(RebalancePlan plan) {
        List<String> result = new ArrayList<>();
        result.add("Rebalance id: " + plan.getRebalanceId());
        result.add("Detail rebalance plan:");
        plan.getPlanForBucketMap().values().stream()
                .map(RebalancePlanForBucket::toString)
                .forEach(result::add);
        return result.toArray(new String[0]);
    }

    private static List<GoalType> validateAndGetPriorityGoals(String priorityGoals) {
        if (priorityGoals == null || priorityGoals.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "priority goals cannot be null or empty. You can specify one goal as 'REPLICA_DISTRIBUTION' or "
                            + "specify multi goals as 'REPLICA_DISTRIBUTION;LEADER_DISTRIBUTION' (split by ';')");
        }

        priorityGoals = priorityGoals.trim();
        String[] splitGoals = priorityGoals.split(";");
        if (splitGoals.length == 0) {
            throw new IllegalArgumentException(
                    "priority goals cannot be empty. You can specify one goal as 'REPLICA_DISTRIBUTION' "
                            + "or specify multi goals as 'REPLICA_DISTRIBUTION;LEADER_DISTRIBUTION' (split by ';')");
        }
        List<GoalType> goalTypes = new ArrayList<>();
        for (String goal : splitGoals) {
            goalTypes.add(GoalType.valueOf(goal.toUpperCase()));
        }
        return goalTypes;
    }
}

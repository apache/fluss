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

package org.apache.fluss.server.coordinator.statemachine;

import java.util.List;
import java.util.Optional;

/** The class to elect the bucket leader for reassignment. */
public class ReassignmentLeaderElection extends AbstractLeaderElection {
    private final List<Integer> newReplicas;

    public ReassignmentLeaderElection(List<Integer> newReplicas) {
        this.newReplicas = newReplicas;
    }

    public Optional<Integer> leaderElection(List<Integer> liveReplicas, List<Integer> isr) {
        // currently, we always use the first replica in targetReplicas, which also in liveReplicas
        // and isr as the leader replica. For bucket reassignment, the first replica is the target
        // leader replica.
        for (int assignment : newReplicas) {
            if (liveReplicas.contains(assignment) && isr.contains(assignment)) {
                return Optional.of(assignment);
            }
        }

        return Optional.empty();
    }
}

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Replica reassignment. */
public class ReplicaReassignment {
    private final List<Integer> replicas;
    private final List<Integer> addingReplicas;
    private final List<Integer> removingReplicas;

    private ReplicaReassignment(
            List<Integer> replicas, List<Integer> addingReplicas, List<Integer> removingReplicas) {
        this.replicas = Collections.unmodifiableList(replicas);
        this.addingReplicas = Collections.unmodifiableList(addingReplicas);
        this.removingReplicas = Collections.unmodifiableList(removingReplicas);
    }

    private static ReplicaReassignment build(
            List<Integer> originReplicas, List<Integer> targetReplicas) {
        // targetReplicas behind originReplicas in full set.
        List<Integer> fullReplicaSet = new ArrayList<>(targetReplicas);
        fullReplicaSet.addAll(originReplicas);
        fullReplicaSet = fullReplicaSet.stream().distinct().collect(Collectors.toList());

        List<Integer> newAddingReplicas = new ArrayList<>(fullReplicaSet);
        newAddingReplicas.removeAll(originReplicas);

        List<Integer> newRemovingReplicas = new ArrayList<>(originReplicas);
        newRemovingReplicas.removeAll(targetReplicas);

        return new ReplicaReassignment(fullReplicaSet, newAddingReplicas, newRemovingReplicas);
    }

    private List<Integer> getTargetReplicas() {
        List<Integer> computed = new ArrayList<>(replicas);
        computed.removeAll(removingReplicas);
        return Collections.unmodifiableList(computed);
    }

    private List<Integer> getOriginReplicas() {
        List<Integer> computed = new ArrayList<>(replicas);
        computed.removeAll(addingReplicas);
        return Collections.unmodifiableList(computed);
    }

    private boolean isBeingReassigned() {
        return !addingReplicas.isEmpty() || !removingReplicas.isEmpty();
    }

    @Override
    public String toString() {
        return String.format(
                "ReplicaAssignment(replicas=%s, addingReplicas=%s, removingReplicas=%s)",
                replicas, addingReplicas, removingReplicas);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ReplicaReassignment that = (ReplicaReassignment) o;
        return Objects.equals(replicas, that.replicas)
                && Objects.equals(addingReplicas, that.addingReplicas)
                && Objects.equals(removingReplicas, that.removingReplicas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(replicas, addingReplicas, removingReplicas);
    }
}

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

package org.apache.fluss.client.admin;

import org.apache.fluss.annotation.PublicEvolving;

import java.util.Objects;

/**
 * Cluster health information returned by {@link Admin#getClusterHealth()}.
 *
 * <p>A standby coordinator answers this request instead of rejecting it (so Kubernetes readiness
 * probes can gate on it). A response served by a standby carries {@link #isServedByLeader()} {@code
 * false}, status {@link ClusterHealthStatus#UNKNOWN}, and zeroed replica counts — callers that
 * monitor cluster health should check {@link #isServedByLeader()} before interpreting the counts.
 *
 * @since 1.0
 */
@PublicEvolving
public final class ClusterHealth {

    private final int numReplicas;
    private final int inSyncReplicas;
    private final int numLeaderReplicas;
    private final int activeLeaderReplicas;
    private final ClusterHealthStatus status;
    private final boolean servedByLeader;
    private final boolean leaderElected;

    public ClusterHealth(
            int numReplicas,
            int inSyncReplicas,
            int numLeaderReplicas,
            int activeLeaderReplicas,
            ClusterHealthStatus status,
            boolean servedByLeader,
            boolean leaderElected) {
        this.numReplicas = numReplicas;
        this.inSyncReplicas = inSyncReplicas;
        this.numLeaderReplicas = numLeaderReplicas;
        this.activeLeaderReplicas = activeLeaderReplicas;
        this.status = Objects.requireNonNull(status, "status");
        this.servedByLeader = servedByLeader;
        this.leaderElected = leaderElected;
    }

    public int getNumReplicas() {
        return numReplicas;
    }

    public int getInSyncReplicas() {
        return inSyncReplicas;
    }

    public int getNumLeaderReplicas() {
        return numLeaderReplicas;
    }

    public int getActiveLeaderReplicas() {
        return activeLeaderReplicas;
    }

    public ClusterHealthStatus getStatus() {
        return status;
    }

    /**
     * Whether the coordinator that answered is the current leader. {@code false} means a standby
     * answered (e.g. the client's coordinator address was stale during a failover): the status is
     * {@link ClusterHealthStatus#UNKNOWN} and the replica counts are zero, not cluster facts.
     * Responses from servers that predate this field report {@code true} — a standby of those
     * versions rejects the request instead of answering.
     */
    public boolean isServedByLeader() {
        return servedByLeader;
    }

    /**
     * Whether the coordinator group currently has an elected leader — the answering server or any
     * other participant. Only meaningful when {@link #isServedByLeader()} is {@code false}; a
     * leader-served response always reports {@code true}.
     */
    public boolean isLeaderElected() {
        return leaderElected;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ClusterHealth)) {
            return false;
        }
        ClusterHealth that = (ClusterHealth) o;
        return numReplicas == that.numReplicas
                && inSyncReplicas == that.inSyncReplicas
                && numLeaderReplicas == that.numLeaderReplicas
                && activeLeaderReplicas == that.activeLeaderReplicas
                && status == that.status
                && servedByLeader == that.servedByLeader
                && leaderElected == that.leaderElected;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                numReplicas,
                inSyncReplicas,
                numLeaderReplicas,
                activeLeaderReplicas,
                status,
                servedByLeader,
                leaderElected);
    }

    @Override
    public String toString() {
        return "ClusterHealth{"
                + "numReplicas="
                + numReplicas
                + ", inSyncReplicas="
                + inSyncReplicas
                + ", numLeaderReplicas="
                + numLeaderReplicas
                + ", activeLeaderReplicas="
                + activeLeaderReplicas
                + ", status="
                + status
                + ", servedByLeader="
                + servedByLeader
                + ", leaderElected="
                + leaderElected
                + '}';
    }
}

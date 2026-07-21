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
 * Per tablet server replica load returned by {@link Admin#describeTabletServers()}.
 *
 * <p>It reports the replica counters of a single tablet server: how many replicas it hosts, how
 * many of those are in sync, how many buckets it leads, and how many of those leaders are active.
 * The {@link ClusterHealth} aggregates the same counters across all tablet servers.
 *
 * <p>Operational tooling can derive from it:
 *
 * <ul>
 *   <li>a scale-in safety gate: the server is empty and safe to remove iff {@code numReplicas ==
 *       0};
 *   <li>a per-server health (green) predicate for rolling upgrades: {@code inSyncReplicas ==
 *       numReplicas && activeLeaderReplicas == numLeaderReplicas}.
 * </ul>
 *
 * <p>Note: a bucket without an elected leader is attributed to no server's {@code
 * numLeaderReplicas}, so the per-server values sum to the cluster-wide {@link
 * ClusterHealth#getNumLeaderReplicas()} only when every bucket has an elected leader. Such buckets
 * still surface through {@code inSyncReplicas < numReplicas} on the assigned servers.
 *
 * @since 1.0
 */
@PublicEvolving
public final class TabletServerDescription {

    private final int serverId;
    private final int numReplicas;
    private final int inSyncReplicas;
    private final int numLeaderReplicas;
    private final int activeLeaderReplicas;

    public TabletServerDescription(
            int serverId,
            int numReplicas,
            int inSyncReplicas,
            int numLeaderReplicas,
            int activeLeaderReplicas) {
        this.serverId = serverId;
        this.numReplicas = numReplicas;
        this.inSyncReplicas = inSyncReplicas;
        this.numLeaderReplicas = numLeaderReplicas;
        this.activeLeaderReplicas = activeLeaderReplicas;
    }

    public int getServerId() {
        return serverId;
    }

    /** Number of replicas assigned to this tablet server. */
    public int getNumReplicas() {
        return numReplicas;
    }

    /** Number of assigned replicas that are in the ISR of their bucket. */
    public int getInSyncReplicas() {
        return inSyncReplicas;
    }

    /** Number of hosted replicas that are the elected leader of their bucket. */
    public int getNumLeaderReplicas() {
        return numLeaderReplicas;
    }

    /** Number of leader replicas on this server that are currently active. */
    public int getActiveLeaderReplicas() {
        return activeLeaderReplicas;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TabletServerDescription)) {
            return false;
        }
        TabletServerDescription that = (TabletServerDescription) o;
        return serverId == that.serverId
                && numReplicas == that.numReplicas
                && inSyncReplicas == that.inSyncReplicas
                && numLeaderReplicas == that.numLeaderReplicas
                && activeLeaderReplicas == that.activeLeaderReplicas;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                serverId, numReplicas, inSyncReplicas, numLeaderReplicas, activeLeaderReplicas);
    }

    @Override
    public String toString() {
        return "TabletServerDescription{"
                + "serverId="
                + serverId
                + ", numReplicas="
                + numReplicas
                + ", inSyncReplicas="
                + inSyncReplicas
                + ", numLeaderReplicas="
                + numLeaderReplicas
                + ", activeLeaderReplicas="
                + activeLeaderReplicas
                + '}';
    }
}

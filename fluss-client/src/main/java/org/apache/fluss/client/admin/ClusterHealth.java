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
 * @since 1.0
 */
@PublicEvolving
public final class ClusterHealth {

    private final int numReplicas;
    private final int inSyncReplicas;
    private final int numLeaderReplicas;
    private final int activeLeaderReplicas;
    private final ClusterHealthStatus status;

    public ClusterHealth(
            int numReplicas,
            int inSyncReplicas,
            int numLeaderReplicas,
            int activeLeaderReplicas,
            ClusterHealthStatus status) {
        this.numReplicas = numReplicas;
        this.inSyncReplicas = inSyncReplicas;
        this.numLeaderReplicas = numLeaderReplicas;
        this.activeLeaderReplicas = activeLeaderReplicas;
        this.status = Objects.requireNonNull(status, "status");
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
                && status == that.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                numReplicas, inSyncReplicas, numLeaderReplicas, activeLeaderReplicas, status);
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
                + '}';
    }
}

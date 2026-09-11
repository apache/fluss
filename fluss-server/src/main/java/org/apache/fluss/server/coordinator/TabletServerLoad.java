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

package org.apache.fluss.server.coordinator;

import java.util.Objects;

/**
 * An immutable, point-in-time replica/leader load for a single tablet server, derived from {@link
 * CoordinatorContext}'s bucket assignment and leader/ISR state.
 *
 * @see ClusterHealthSnapshot
 */
public final class TabletServerLoad {

    private final int serverId;
    private final int numReplicas;
    private final int inSyncReplicas;
    private final int numLeaderReplicas;
    private final int activeLeaderReplicas;

    TabletServerLoad(
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

    public int serverId() {
        return serverId;
    }

    public int numReplicas() {
        return numReplicas;
    }

    public int inSyncReplicas() {
        return inSyncReplicas;
    }

    public int numLeaderReplicas() {
        return numLeaderReplicas;
    }

    public int activeLeaderReplicas() {
        return activeLeaderReplicas;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TabletServerLoad)) {
            return false;
        }
        TabletServerLoad that = (TabletServerLoad) o;
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
        return "TabletServerLoad{"
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

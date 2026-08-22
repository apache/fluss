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

import java.util.Collections;
import java.util.Map;

/**
 * An immutable, point-in-time view of cluster-wide and per-tablet-server replica/leader health,
 * derived from {@link CoordinatorContext}.
 *
 * <p>{@code numReplicas}/{@code inSyncReplicas}/{@code numLeaderReplicas}/{@code
 * activeLeaderReplicas} are the same cluster-wide aggregates {@code
 * CoordinatorService#computeClusterHealth} reports; {@link #tabletServerLoads()} additionally
 * attributes replicas/ISR membership/leadership to the specific server holding them. Both are
 * computed together from a single pass over {@link CoordinatorContext#getAllBuckets()}.
 *
 * <p>Instances are published by {@link CoordinatorHealthCache} and are safe to read from any thread
 * without synchronization: once constructed, an instance is never mutated.
 */
public final class ClusterHealthSnapshot {

    public static final ClusterHealthSnapshot EMPTY =
            new ClusterHealthSnapshot(0, 0, 0, 0, Collections.emptyMap());

    private final int numReplicas;
    private final int inSyncReplicas;
    private final int numLeaderReplicas;
    private final int activeLeaderReplicas;
    private final Map<Integer, TabletServerLoad> tabletServerLoads;

    ClusterHealthSnapshot(
            int numReplicas,
            int inSyncReplicas,
            int numLeaderReplicas,
            int activeLeaderReplicas,
            Map<Integer, TabletServerLoad> tabletServerLoads) {
        this.numReplicas = numReplicas;
        this.inSyncReplicas = inSyncReplicas;
        this.numLeaderReplicas = numLeaderReplicas;
        this.activeLeaderReplicas = activeLeaderReplicas;
        this.tabletServerLoads = Collections.unmodifiableMap(tabletServerLoads);
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

    /** Per-tablet-server load, keyed by server id. Includes live and shutting-down servers. */
    public Map<Integer, TabletServerLoad> tabletServerLoads() {
        return tabletServerLoads;
    }
}

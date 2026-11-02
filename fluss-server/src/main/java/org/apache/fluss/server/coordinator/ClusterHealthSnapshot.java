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
 * <p>{@code numLeaderReplicas} counts buckets, not leaders -- it's incremented once per bucket
 * regardless of whether that bucket actually has a leader, matching {@code computeClusterHealth}'s
 * existing semantics exactly. The per-server breakdown only credits a server when it's actually the
 * leader, so the two can diverge: {@link #bucketsWithoutLeader()} makes that gap an explicit,
 * queryable fact instead of a silent discrepancy a consumer has to rediscover on its own.
 *
 * <p>{@link #EMPTY} is not a genuinely healthy all-zero cluster -- it's the placeholder before the
 * first real snapshot has ever been computed. Any consumer that derives a health status (GREEN /
 * YELLOW / RED) from this snapshot must check {@link #isInitialized()} first and report an
 * unknown/pending status when it's {@code false}, rather than applying the normal status rule to
 * all-zero counters and reporting a healthy cluster that hasn't actually been evaluated yet.
 *
 * <p>Instances are published by {@link CoordinatorHealthCache} and are safe to read from any thread
 * without synchronization: once constructed, an instance is never mutated.
 */
public final class ClusterHealthSnapshot {

    public static final ClusterHealthSnapshot EMPTY =
            new ClusterHealthSnapshot(0, 0, 0, 0, 0, Collections.emptyMap(), false);

    private final int numReplicas;
    private final int inSyncReplicas;
    private final int numLeaderReplicas;
    private final int activeLeaderReplicas;
    private final int bucketsWithoutLeader;
    private final Map<Integer, TabletServerLoad> tabletServerLoads;
    private final boolean initialized;

    ClusterHealthSnapshot(
            int numReplicas,
            int inSyncReplicas,
            int numLeaderReplicas,
            int activeLeaderReplicas,
            int bucketsWithoutLeader,
            Map<Integer, TabletServerLoad> tabletServerLoads) {
        this(
                numReplicas,
                inSyncReplicas,
                numLeaderReplicas,
                activeLeaderReplicas,
                bucketsWithoutLeader,
                tabletServerLoads,
                true);
    }

    private ClusterHealthSnapshot(
            int numReplicas,
            int inSyncReplicas,
            int numLeaderReplicas,
            int activeLeaderReplicas,
            int bucketsWithoutLeader,
            Map<Integer, TabletServerLoad> tabletServerLoads,
            boolean initialized) {
        this.numReplicas = numReplicas;
        this.inSyncReplicas = inSyncReplicas;
        this.numLeaderReplicas = numLeaderReplicas;
        this.activeLeaderReplicas = activeLeaderReplicas;
        this.bucketsWithoutLeader = bucketsWithoutLeader;
        this.tabletServerLoads = Collections.unmodifiableMap(tabletServerLoads);
        this.initialized = initialized;
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

    /**
     * Buckets counted in {@link #numLeaderReplicas()} that have no actual leader (either no {@code
     * LeaderAndIsr} at all, or an explicit {@code NO_LEADER}) -- the reconciling fact between the
     * aggregate bucket count and the per-server leader count in {@link #tabletServerLoads()}.
     */
    public int bucketsWithoutLeader() {
        return bucketsWithoutLeader;
    }

    /** Per-tablet-server load, keyed by server id. Includes live and shutting-down servers. */
    public Map<Integer, TabletServerLoad> tabletServerLoads() {
        return tabletServerLoads;
    }

    /**
     * Whether this snapshot was actually computed from a real {@code CoordinatorContext}, as
     * opposed to being {@link #EMPTY}. See the class javadoc -- an uninitialized snapshot must not
     * be treated as a genuinely healthy cluster.
     */
    public boolean isInitialized() {
        return initialized;
    }
}

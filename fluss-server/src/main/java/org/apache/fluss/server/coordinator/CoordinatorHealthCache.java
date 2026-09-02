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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.utils.CoalescingRefreshCache;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * A copy-on-write cache of cluster/per-tablet-server replica and leader health, derived from {@link
 * CoordinatorContext}.
 *
 * <p>This mirrors the pattern {@code CoordinatorMetadataCache} already uses for server topology: a
 * single {@code volatile} immutable {@link ClusterHealthSnapshot}, recomputed and swapped by the
 * coordinator event thread, and read lock-free by any thread via {@link #getSnapshot()} without
 * going through {@code AccessContextEvent}. The copy-on-write and coalescing mechanics themselves
 * live in {@link CoalescingRefreshCache}, shared with (or reusable by) any other coordinator-local
 * derived view that wants the same "callers report facts, this decides when to act" shape.
 *
 * <p>Callers report state transitions through the {@code onXxx} methods below; this class alone
 * decides whether a transition is urgent (should be reflected almost immediately) or can be batched
 * (reflected the next time the coordinator event queue drains). Callers never see or decide urgency
 * themselves — they only report what changed. Recomputing the snapshot is still an {@code
 * O(buckets)} scan, same as {@code CoordinatorService#computeClusterHealth} and {@code
 * #computeTabletServerLoads} today; what changes is how often that scan runs and who waits for it,
 * not its cost.
 *
 * <p>{@link #refresh(CoordinatorContext)} must only be called from the coordinator event thread,
 * since {@code CoordinatorContext} is not thread-safe -- see {@link CoalescingRefreshCache}'s
 * javadoc for the single-writer-thread assumption this relies on, and for why rate (these three
 * bounds) and coverage (the safety net) are deliberately independent mechanisms.
 */
public final class CoordinatorHealthCache implements CoordinatorContextListener {

    /**
     * Upper bound on how long an urgent (degrading) change may sit unreflected. Bounds worst-case
     * staleness for a safety-relevant signal without forcing a full rescan after every single event
     * in a burst.
     */
    @VisibleForTesting static final long URGENT_MAX_DELAY_MS = 200;

    /** Upper bound on how long a known, non-urgent change may sit unreflected. */
    @VisibleForTesting static final long NORMAL_MAX_DELAY_MS = 3_000;

    /**
     * Absolute ceiling on staleness, independent of whether anything was ever reported via an
     * {@code onXxx} call -- the coverage backstop. See {@link CoalescingRefreshCache}'s javadoc.
     */
    @VisibleForTesting static final long SAFETY_NET_MAX_DELAY_MS = 10_000;

    private final CoalescingRefreshCache<ClusterHealthSnapshot> cache =
            new CoalescingRefreshCache<>(
                    ClusterHealthSnapshot.EMPTY,
                    URGENT_MAX_DELAY_MS,
                    NORMAL_MAX_DELAY_MS,
                    SAFETY_NET_MAX_DELAY_MS);

    // --------------------------------------------------------------------------------------------
    // Reporting: callers state facts, this class decides what they mean.
    // --------------------------------------------------------------------------------------------

    /**
     * Reports the current (post-mutation) leader/ISR state for a bucket. Under-replication or a
     * missing leader is treated as urgent; anything else is batched.
     */
    @Override
    public void onBucketLeaderAndIsrChanged(
            TableBucket tableBucket, List<Integer> assignment, Optional<LeaderAndIsr> current) {
        boolean underReplicated =
                current.map(lai -> lai.isr().size() < assignment.size()).orElse(true);
        boolean leaderless =
                current.map(lai -> lai.leader() == LeaderAndIsr.NO_LEADER).orElse(true);
        cache.markDirty(underReplicated || leaderless);
    }

    /** Reports that a bucket's leader became active or inactive. Inactive is urgent. */
    @Override
    public void onLeaderActivityChanged(boolean isActive) {
        cache.markDirty(!isActive);
    }

    /** A tablet server died. Always urgent -- it can only make things worse. */
    @Override
    public void onTabletServerDied() {
        cache.markDirty(true);
    }

    /** A tablet server registered (startup or rejoin). Never urgent. */
    @Override
    public void onTabletServerRegistered() {
        cache.markDirty(false);
    }

    /**
     * Catch-all for topology changes that don't represent degradation: table/partition
     * create-delete, replica reassignment, server tag add/remove.
     */
    @Override
    public void onTopologyChanged() {
        cache.markDirty(false);
    }

    // --------------------------------------------------------------------------------------------
    // Refresh policy: the decision of when to act lives in CoalescingRefreshCache; the decision of
    // what "urgent" means for this data (above) and how to compute a snapshot (below) live here.
    // --------------------------------------------------------------------------------------------

    /**
     * Recomputes and republishes the snapshot if it's due: a known change (urgent or not) is
     * reflected within {@link #URGENT_MAX_DELAY_MS}/{@link #NORMAL_MAX_DELAY_MS}; an unconditional
     * {@link #SAFETY_NET_MAX_DELAY_MS} ceiling guarantees a bound on staleness even for a mutation
     * path nobody remembered to report through an {@code onXxx} call. Call this from the
     * coordinator event loop on every tick -- it's cheap (one elapsed-time check) when nothing is
     * due.
     *
     * <p>Must only be called from a thread that safely owns {@code ctx} (i.e. the coordinator event
     * thread) — {@code ctx} itself is not thread-safe.
     */
    public void refresh(CoordinatorContext ctx) {
        cache.refresh(() -> computeSnapshot(ctx));
    }

    /** Returns the most recently published snapshot. Safe to call from any thread. */
    public ClusterHealthSnapshot getSnapshot() {
        return cache.get();
    }

    @VisibleForTesting
    boolean isDirty() {
        return cache.isDirty();
    }

    @VisibleForTesting
    boolean isUrgentlyDirty() {
        return cache.isUrgentlyDirty();
    }

    /**
     * Computes both the cluster-wide aggregates ({@code CoordinatorService#computeClusterHealth}
     * semantics) and the per-server breakdown ({@code CoordinatorService#computeTabletServerLoads}
     * semantics) from a single pass over {@code ctx.getAllBuckets()}, instead of one pass per view.
     */
    private static ClusterHealthSnapshot computeSnapshot(CoordinatorContext ctx) {
        Map<Integer, MutableLoad> loads = new TreeMap<>();
        // report live and shutting-down servers even if they host no replicas, so that
        // an evacuated server explicitly shows zero replicas
        for (int serverId : ctx.liveOrShuttingDownTabletServers()) {
            getOrCreate(loads, serverId);
        }

        int numReplicas = 0;
        int inSyncReplicas = 0;
        int numLeaderReplicas = 0;
        int activeLeaderReplicas = 0;
        int bucketsWithoutLeader = 0;

        for (TableBucket tb : ctx.getAllBuckets()) {
            List<Integer> assignment = ctx.getAssignment(tb);
            numReplicas += assignment.size();
            // matches CoordinatorService#computeClusterHealth: counts buckets, not leaders
            numLeaderReplicas++;
            for (int serverId : assignment) {
                getOrCreate(loads, serverId).numReplicas++;
            }

            boolean leaderActive = ctx.isLeaderActive(tb);
            if (leaderActive) {
                activeLeaderReplicas++;
            }

            Optional<LeaderAndIsr> laiOpt = ctx.getBucketLeaderAndIsr(tb);
            boolean hasLeader = false;
            if (laiOpt.isPresent()) {
                LeaderAndIsr lai = laiOpt.get();
                inSyncReplicas += lai.isr().size();
                for (int serverId : lai.isr()) {
                    getOrCreate(loads, serverId).inSyncReplicas++;
                }
                if (lai.leader() != LeaderAndIsr.NO_LEADER) {
                    hasLeader = true;
                    MutableLoad leaderLoad = getOrCreate(loads, lai.leader());
                    leaderLoad.numLeaderReplicas++;
                    if (leaderActive) {
                        leaderLoad.activeLeaderReplicas++;
                    }
                }
            }
            // reconciles the aggregate numLeaderReplicas (a bucket count) against the per-server
            // breakdown (an actual-leader count): every bucket counted above but not here is one
            // where computeClusterHealth's aggregate and the per-server sum will diverge.
            if (!hasLeader) {
                bucketsWithoutLeader++;
            }
        }

        Map<Integer, TabletServerLoad> tabletServerLoads = new HashMap<>();
        for (Map.Entry<Integer, MutableLoad> entry : loads.entrySet()) {
            tabletServerLoads.put(entry.getKey(), entry.getValue().toImmutable());
        }

        return new ClusterHealthSnapshot(
                numReplicas,
                inSyncReplicas,
                numLeaderReplicas,
                activeLeaderReplicas,
                bucketsWithoutLeader,
                tabletServerLoads);
    }

    private static MutableLoad getOrCreate(Map<Integer, MutableLoad> loads, int serverId) {
        return loads.computeIfAbsent(serverId, MutableLoad::new);
    }

    /** Short-lived mutable accumulator used only while a snapshot is being computed. */
    private static final class MutableLoad {
        private final int serverId;
        private int numReplicas;
        private int inSyncReplicas;
        private int numLeaderReplicas;
        private int activeLeaderReplicas;

        private MutableLoad(int serverId) {
            this.serverId = serverId;
        }

        private TabletServerLoad toImmutable() {
            return new TabletServerLoad(
                    serverId, numReplicas, inSyncReplicas, numLeaderReplicas, activeLeaderReplicas);
        }
    }
}

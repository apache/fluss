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
 * <p>{@link #refresh(CoordinatorContext, boolean)} must only be called from the coordinator event
 * thread, since {@code CoordinatorContext} is not thread-safe -- see {@link
 * CoalescingRefreshCache}'s javadoc for the single-writer-thread assumption this relies on.
 */
public final class CoordinatorHealthCache {

    /**
     * Upper bound on how long an urgent (degrading) change may sit unreflected while the event
     * queue keeps draining new work. Bounds worst-case staleness for a safety-relevant signal
     * without forcing a full rescan after every single event in a burst.
     */
    @VisibleForTesting static final long URGENT_MAX_DELAY_MS = 200;

    private final CoalescingRefreshCache<ClusterHealthSnapshot> cache =
            new CoalescingRefreshCache<>(ClusterHealthSnapshot.EMPTY, URGENT_MAX_DELAY_MS);

    // --------------------------------------------------------------------------------------------
    // Reporting: callers state facts, this class decides what they mean.
    // --------------------------------------------------------------------------------------------

    /**
     * Reports the current (post-mutation) leader/ISR state for a bucket. Under-replication or a
     * missing leader is treated as urgent; anything else is batched.
     */
    public void onBucketLeaderAndIsrChanged(
            TableBucket tableBucket, List<Integer> assignment, Optional<LeaderAndIsr> current) {
        boolean underReplicated =
                current.map(lai -> lai.isr().size() < assignment.size()).orElse(true);
        boolean leaderless =
                current.map(lai -> lai.leader() == LeaderAndIsr.NO_LEADER).orElse(true);
        cache.markDirty(underReplicated || leaderless);
    }

    /** Reports that a bucket's leader became active or inactive. Inactive is urgent. */
    public void onLeaderActivityChanged(boolean isActive) {
        cache.markDirty(!isActive);
    }

    /** A tablet server died. Always urgent -- it can only make things worse. */
    public void onTabletServerDied() {
        cache.markDirty(true);
    }

    /** A tablet server registered (startup or rejoin). Never urgent. */
    public void onTabletServerRegistered() {
        cache.markDirty(false);
    }

    /**
     * Catch-all for topology changes that don't represent degradation: table/partition
     * create-delete, replica reassignment, server tag add/remove.
     */
    public void onTopologyChanged() {
        cache.markDirty(false);
    }

    // --------------------------------------------------------------------------------------------
    // Refresh policy: the decision of when to act lives in CoalescingRefreshCache; the decision of
    // what "urgent" means for this data (above) and how to compute a snapshot (below) live here.
    // --------------------------------------------------------------------------------------------

    /**
     * Recomputes and republishes the snapshot if, and only if, something has actually changed since
     * the last refresh and now is the right time to act on it: a non-urgent change needs {@code
     * force}; an urgent change is bounded by {@link #URGENT_MAX_DELAY_MS} regardless of {@code
     * force}. If nothing changed, this is a no-op regardless of {@code force} -- see {@link
     * CoalescingRefreshCache}'s javadoc for why that check always comes first.
     *
     * <p>Must only be called from a thread that safely owns {@code ctx} (i.e. the coordinator event
     * thread) — {@code ctx} itself is not thread-safe.
     *
     * @param force overrides the timing question directly. The coordinator event loop passes
     *     whether its own event queue is currently empty; an explicit warm-up (e.g. right after the
     *     coordinator finishes loading its initial state) or a test that wants to bypass the timing
     *     policy passes {@code true} unconditionally -- which still only takes effect because a
     *     freshly constructed cache starts dirty.
     */
    public void refresh(CoordinatorContext ctx, boolean force) {
        cache.refresh(() -> computeSnapshot(ctx), force);
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
            if (laiOpt.isPresent()) {
                LeaderAndIsr lai = laiOpt.get();
                inSyncReplicas += lai.isr().size();
                for (int serverId : lai.isr()) {
                    getOrCreate(loads, serverId).inSyncReplicas++;
                }
                if (lai.leader() != LeaderAndIsr.NO_LEADER) {
                    MutableLoad leaderLoad = getOrCreate(loads, lai.leader());
                    leaderLoad.numLeaderReplicas++;
                    if (leaderActive) {
                        leaderLoad.activeLeaderReplicas++;
                    }
                }
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

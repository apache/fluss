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

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.GetClusterHealthResponse;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CoordinatorHealthCache}. */
class CoordinatorHealthCacheTest {

    private CoordinatorContext ctx;
    private CoordinatorHealthCache cache;

    @BeforeEach
    void setUp() {
        ctx = new CoordinatorContext(ZkEpoch.INITIAL_EPOCH);
        ctx.setLiveTabletServers(
                Arrays.asList(makeServerInfo(0), makeServerInfo(1), makeServerInfo(2)));
        cache = new CoordinatorHealthCache();
    }

    @Test
    void testInitialSnapshotIsEmpty() {
        assertThat(cache.getSnapshot()).isSameAs(ClusterHealthSnapshot.EMPTY);
    }

    @Test
    void testEmptyClusterReportsZeroLoadPerLiveServer() {
        cache.refresh(ctx, true);

        ClusterHealthSnapshot snapshot = cache.getSnapshot();
        assertThat(snapshot.numReplicas()).isZero();
        assertThat(snapshot.inSyncReplicas()).isZero();
        assertThat(snapshot.activeLeaderReplicas()).isZero();
        assertThat(snapshot.tabletServerLoads()).hasSize(3);
        snapshot.tabletServerLoads()
                .values()
                .forEach(
                        load -> {
                            assertThat(load.numReplicas()).isZero();
                            assertThat(load.inSyncReplicas()).isZero();
                            assertThat(load.numLeaderReplicas()).isZero();
                            assertThat(load.activeLeaderReplicas()).isZero();
                        });
    }

    @Test
    void testAggregatesMatchComputeClusterHealth() {
        TableBucket tb1 = new TableBucket(1L, 0);
        TableBucket tb2 = new TableBucket(1L, 1);
        TableBucket tb3 = new TableBucket(2L, 0);

        ctx.updateBucketReplicaAssignment(tb1, Arrays.asList(0, 1));
        ctx.updateBucketReplicaAssignment(tb2, Arrays.asList(1, 2));
        ctx.updateBucketReplicaAssignment(tb3, Arrays.asList(0, 2));

        ctx.putBucketLeaderAndIsr(
                tb1, new LeaderAndIsr(0, 1, Arrays.asList(0, 1), Collections.emptyList(), 0, 1));
        ctx.putBucketLeaderAndIsr(
                tb2,
                new LeaderAndIsr(
                        1, 1, Collections.singletonList(1), Collections.emptyList(), 0, 1));
        ctx.putBucketLeaderAndIsr(
                tb3, new LeaderAndIsr(0, 1, Arrays.asList(0, 2), Collections.emptyList(), 0, 1));

        GetClusterHealthResponse expected = CoordinatorService.computeClusterHealth(ctx);

        cache.refresh(ctx, true);
        ClusterHealthSnapshot snapshot = cache.getSnapshot();

        // the cache must reproduce the exact same cluster-wide aggregates as the
        // AccessContextEvent-bound computation it is meant to replace.
        assertThat(snapshot.numReplicas()).isEqualTo(expected.getNumReplicas());
        assertThat(snapshot.inSyncReplicas()).isEqualTo(expected.getInSyncReplicas());
        assertThat(snapshot.numLeaderReplicas()).isEqualTo(expected.getNumLeaderReplicas());
        assertThat(snapshot.activeLeaderReplicas()).isEqualTo(expected.getActiveLeaderReplicas());
    }

    @Test
    void testPerServerBreakdownAttributesReplicasIsrAndLeader() {
        TableBucket tb = new TableBucket(1L, 0);
        ctx.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1, 2));
        ctx.putBucketLeaderAndIsr(
                tb, new LeaderAndIsr(0, 1, Arrays.asList(0, 1), Collections.emptyList(), 0, 1));

        cache.refresh(ctx, true);
        ClusterHealthSnapshot snapshot = cache.getSnapshot();

        TabletServerLoad server0 = snapshot.tabletServerLoads().get(0);
        TabletServerLoad server1 = snapshot.tabletServerLoads().get(1);
        TabletServerLoad server2 = snapshot.tabletServerLoads().get(2);

        // server 0: replica + in ISR + is the leader (and active, since it is in the ISR)
        assertThat(server0.numReplicas()).isEqualTo(1);
        assertThat(server0.inSyncReplicas()).isEqualTo(1);
        assertThat(server0.numLeaderReplicas()).isEqualTo(1);
        assertThat(server0.activeLeaderReplicas()).isEqualTo(1);

        // server 1: replica + in ISR, not the leader
        assertThat(server1.numReplicas()).isEqualTo(1);
        assertThat(server1.inSyncReplicas()).isEqualTo(1);
        assertThat(server1.numLeaderReplicas()).isZero();

        // server 2: replica, but out of ISR (not in the isr list above)
        assertThat(server2.numReplicas()).isEqualTo(1);
        assertThat(server2.inSyncReplicas()).isZero();
        assertThat(server2.numLeaderReplicas()).isZero();

        // sum of per-server replicas must reconcile with the cluster-wide aggregate
        int sumOfServerReplicas =
                snapshot.tabletServerLoads().values().stream()
                        .mapToInt(TabletServerLoad::numReplicas)
                        .sum();
        assertThat(sumOfServerReplicas).isEqualTo(snapshot.numReplicas());
    }

    @Test
    void testUnattributedLeaderDoesNotReconcileWithBucketCountAggregate() {
        // deliberately exercise the pre-existing semantic gap between
        // CoordinatorService#computeClusterHealth's numLeaderReplicas (a bucket count, always
        // incremented) and the per-server numLeaderReplicas (only incremented when a leader is
        // actually assigned): with NO_LEADER, the aggregate still counts the bucket, but no
        // server gets credited.
        TableBucket tb = new TableBucket(1L, 0);
        ctx.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1));
        ctx.putBucketLeaderAndIsr(
                tb,
                new LeaderAndIsr(
                        LeaderAndIsr.NO_LEADER,
                        1,
                        Arrays.asList(0, 1),
                        Collections.emptyList(),
                        0,
                        1));

        cache.refresh(ctx, true);
        ClusterHealthSnapshot snapshot = cache.getSnapshot();

        assertThat(snapshot.numLeaderReplicas()).isEqualTo(1); // one bucket, counted regardless
        int sumOfPerServerLeaderReplicas =
                snapshot.tabletServerLoads().values().stream()
                        .mapToInt(TabletServerLoad::numLeaderReplicas)
                        .sum();
        assertThat(sumOfPerServerLeaderReplicas).isZero(); // nobody is actually the leader
    }

    @Test
    void testEvacuatedLiveServerStillReportedWithZeroLoad() {
        TableBucket tb = new TableBucket(1L, 0);
        ctx.updateBucketReplicaAssignment(tb, Collections.singletonList(0));
        ctx.putBucketLeaderAndIsr(
                tb,
                new LeaderAndIsr(
                        0, 1, Collections.singletonList(0), Collections.emptyList(), 0, 1));

        cache.refresh(ctx, true);

        // server 1 and 2 host nothing, but are live, so they must still be present with 0 load
        // (mirrors the "evacuated server explicitly shows zero replicas" contract).
        TabletServerLoad server1 = cache.getSnapshot().tabletServerLoads().get(1);
        assertThat(server1).isNotNull();
        assertThat(server1.numReplicas()).isZero();
    }

    @Test
    void testPublishedSnapshotIsImmutableAcrossLaterUpdates() {
        TableBucket tb = new TableBucket(1L, 0);
        ctx.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1));
        cache.refresh(ctx, true);

        ClusterHealthSnapshot firstSnapshot = cache.getSnapshot();
        assertThat(firstSnapshot.numReplicas()).isEqualTo(2);

        // a later mutation + refresh must not retroactively change a snapshot a caller already
        // holds a reference to -- that is the entire point of copy-on-write.
        TableBucket tb2 = new TableBucket(2L, 0);
        ctx.updateBucketReplicaAssignment(tb2, Arrays.asList(0, 1, 2));
        cache.onTopologyChanged(); // real callers always report through onXxx before refreshing
        cache.refresh(ctx, true);

        assertThat(firstSnapshot.numReplicas()).isEqualTo(2);
        assertThat(cache.getSnapshot().numReplicas()).isEqualTo(5);
        assertThat(cache.getSnapshot()).isNotSameAs(firstSnapshot);
    }

    @Test
    void testConcurrentReadsNeverObserveATornSnapshot() throws InterruptedException {
        TableBucket tb = new TableBucket(1L, 0);
        ctx.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1, 2));
        ctx.putBucketLeaderAndIsr(
                tb, new LeaderAndIsr(0, 1, Arrays.asList(0, 1, 2), Collections.emptyList(), 0, 1));
        cache.refresh(ctx, true);

        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicReference<AssertionError> failure = new AtomicReference<>();

        Thread reader =
                new Thread(
                        () -> {
                            while (!stop.get()) {
                                ClusterHealthSnapshot snapshot = cache.getSnapshot();
                                int sumOfServerReplicas =
                                        snapshot.tabletServerLoads().values().stream()
                                                .mapToInt(TabletServerLoad::numReplicas)
                                                .sum();
                                // must ALWAYS reconcile: a torn read (fields from two different
                                // snapshot instances) would break this invariant.
                                if (sumOfServerReplicas != snapshot.numReplicas()) {
                                    failure.set(
                                            new AssertionError(
                                                    "torn snapshot: sumOfServerReplicas="
                                                            + sumOfServerReplicas
                                                            + " numReplicas="
                                                            + snapshot.numReplicas()));
                                    return;
                                }
                            }
                        });

        reader.start();
        // hammer refresh() from this thread while the reader spins, simulating the event thread
        // republishing the snapshot concurrently with RPC-thread reads. onTopologyChanged() keeps
        // marking it dirty so each iteration actually recomputes and swaps, not just the first.
        for (int i = 0; i < 2000 && failure.get() == null; i++) {
            cache.onTopologyChanged();
            cache.refresh(ctx, true);
        }
        stop.set(true);
        reader.join();

        assertThat(failure.get()).isNull();
    }

    @Test
    void testFullIsrAndActiveLeaderIsNotUrgent() {
        TableBucket tb = new TableBucket(1L, 0);
        cache.onBucketLeaderAndIsrChanged(
                tb,
                Arrays.asList(0, 1, 2),
                Optional.of(
                        new LeaderAndIsr(
                                0, 1, Arrays.asList(0, 1, 2), Collections.emptyList(), 0, 1)));

        assertThat(cache.isDirty()).isTrue();
        assertThat(cache.isUrgentlyDirty()).isFalse();
    }

    @Test
    void testUnderReplicatedIsrIsUrgent() {
        TableBucket tb = new TableBucket(1L, 0);
        cache.onBucketLeaderAndIsrChanged(
                tb,
                Arrays.asList(0, 1, 2),
                Optional.of(
                        new LeaderAndIsr(
                                0, 1, Arrays.asList(0, 1), Collections.emptyList(), 0, 1)));

        assertThat(cache.isUrgentlyDirty()).isTrue();
    }

    @Test
    void testMissingLeaderIsUrgent() {
        TableBucket tb = new TableBucket(1L, 0);
        cache.onBucketLeaderAndIsrChanged(tb, Arrays.asList(0, 1), Optional.empty());

        assertThat(cache.isUrgentlyDirty()).isTrue();
    }

    @Test
    void testTabletServerDiedIsUrgentButRegisteredAndTopologyChangeAreNot() {
        cache.onTabletServerDied();
        assertThat(cache.isUrgentlyDirty()).isTrue();

        cache = new CoordinatorHealthCache();
        cache.onTabletServerRegistered();
        assertThat(cache.isDirty()).isTrue();
        assertThat(cache.isUrgentlyDirty()).isFalse();

        cache = new CoordinatorHealthCache();
        cache.onTopologyChanged();
        assertThat(cache.isDirty()).isTrue();
        assertThat(cache.isUrgentlyDirty()).isFalse();

        cache = new CoordinatorHealthCache();
        cache.onLeaderActivityChanged(false);
        assertThat(cache.isUrgentlyDirty()).isTrue();
    }

    @Test
    void testRefreshIsNoOpWhenNotDirty() {
        cache.refresh(ctx, true);
        ClusterHealthSnapshot warm = cache.getSnapshot();

        // nothing reported dirty since the warm-up -- must not recompute, queue state aside.
        cache.refresh(ctx, true);
        assertThat(cache.getSnapshot()).isSameAs(warm);
    }

    @Test
    void testNonUrgentChangeWaitsForQueueToDrain() {
        cache.refresh(ctx, true);
        TableBucket tb = new TableBucket(1L, 0);
        ctx.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1));
        cache.onTopologyChanged();

        ClusterHealthSnapshot beforeDrain = cache.getSnapshot();
        cache.refresh(ctx, false); // queue still has work -- must not recompute yet
        assertThat(cache.getSnapshot()).isSameAs(beforeDrain);

        cache.refresh(ctx, true); // queue drained -- now it should
        assertThat(cache.getSnapshot()).isNotSameAs(beforeDrain);
        assertThat(cache.getSnapshot().numReplicas()).isEqualTo(2);
    }

    @Test
    void testUrgentChangeIsBoundedByMaxDelayEvenIfQueueNeverDrains() throws InterruptedException {
        cache.refresh(ctx, true); // establishes a real lastRefreshTimeMs baseline
        TableBucket tb = new TableBucket(1L, 0);
        ctx.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1));
        cache.onTabletServerDied(); // urgent

        ClusterHealthSnapshot beforeDelay = cache.getSnapshot();
        cache.refresh(ctx, false); // queue busy, delay not yet elapsed -- must wait
        assertThat(cache.getSnapshot()).isSameAs(beforeDelay);

        Thread.sleep(CoordinatorHealthCache.URGENT_MAX_DELAY_MS + 50);

        cache.refresh(ctx, false); // queue STILL busy, but the urgent bound is up
        assertThat(cache.getSnapshot()).isNotSameAs(beforeDelay);
        assertThat(cache.getSnapshot().numReplicas()).isEqualTo(2);
    }

    private static ServerInfo makeServerInfo(int id) {
        return new ServerInfo(
                id,
                "RACK" + id,
                Endpoint.fromListenersString("CLIENT://host" + id + ":9124"),
                ServerType.TABLET_SERVER);
    }
}

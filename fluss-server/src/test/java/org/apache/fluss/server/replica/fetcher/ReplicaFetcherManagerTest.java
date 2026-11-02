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

package org.apache.fluss.server.replica.fetcher;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.replica.ReplicaTestBase;
import org.apache.fluss.server.replica.fetcher.ReplicaFetcherManager.ServerIdAndFetcherId;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.server.tablet.bulkload.BulkLoadTargetMetadata;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadDataState;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ReplicaFetcherManager}. */
class ReplicaFetcherManagerTest extends ReplicaTestBase {

    private ServerNode leader;
    private ReplicaFetcherThread fetcherThread;

    @BeforeEach
    public void setup() throws Exception {
        super.setup();
        // with local test leader end point.
        leader = new ServerNode(1, "localhost", 9099, ServerType.COORDINATOR);
        fetcherThread =
                new ReplicaFetcherThread(
                        "test-fetcher-thread",
                        replicaManager,
                        new RemoteLeaderEndpoint(
                                conf,
                                TABLET_SERVER_ID,
                                leader.id(),
                                new TestTabletServerGateway(false, Collections.emptySet())),
                        (int)
                                conf.get(ConfigOptions.LOG_REPLICA_FETCH_BACKOFF_INTERVAL)
                                        .toMillis());
    }

    @Test
    void testAddAndRemoveBucket() {
        ReplicaFetcherManager fetcherManager =
                new TestingReplicaFetcherManager(TABLET_SERVER_ID, replicaManager, fetcherThread);

        long fetchOffset = 0L;
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);

        // make this table bucket as follower to make sure the fetcher thread can deal with the
        // fetched data from leader.
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                PhysicalTablePath.of(DATA1_TABLE_PATH),
                                tb,
                                Arrays.asList(leader.id(), TABLET_SERVER_ID),
                                new LeaderAndIsr(
                                        leader.id(),
                                        LeaderAndIsr.INITIAL_LEADER_EPOCH,
                                        Arrays.asList(leader.id(), TABLET_SERVER_ID),
                                        Collections.emptyList(),
                                        INITIAL_COORDINATOR_EPOCH,
                                        LeaderAndIsr.INITIAL_BUCKET_EPOCH),
                                3,
                                0L)),
                result -> {});

        InitialFetchStatus initialFetchStatus =
                new InitialFetchStatus(DATA1_TABLE_ID, DATA1_TABLE_PATH, leader.id(), fetchOffset);

        Map<TableBucket, InitialFetchStatus> initialFetchStateMap = new HashMap<>();
        initialFetchStateMap.put(tb, initialFetchStatus);
        fetcherManager.addFetcherForBuckets(initialFetchStateMap);
        Map<ReplicaFetcherManager.ServerIdAndFetcherId, ReplicaFetcherThread> fetcherThreadMap =
                fetcherManager.getFetcherThreadMap();
        assertThat(fetcherThreadMap.size()).isEqualTo(1);
        ReplicaFetcherThread thread =
                fetcherThreadMap.get(new ServerIdAndFetcherId(1, fetcherManager.getFetcherId(tb)));
        assertThat(thread).isEqualTo(fetcherThread);
        assertThat(fetcherThread.fetchStatus(tb).isPresent()).isTrue();

        fetcherManager.removeFetcherForBuckets(Collections.singleton(tb));
        // the fetcher thread will not be moved out from the map.
        assertThat(fetcherThreadMap.size()).isEqualTo(1);
        thread = fetcherThreadMap.get(new ServerIdAndFetcherId(1, fetcherManager.getFetcherId(tb)));

        assertThat(thread).isEqualTo(fetcherThread);
        assertThat(fetcherThread.fetchStatus(tb).isPresent()).isFalse();

        fetcherManager.shutdownIdleFetcherThreads();
        assertThat(fetcherThreadMap.size()).isEqualTo(0);
    }

    @Test
    void testFetcherIdWithinConfiguredRange() {
        int numFetchers = 2;
        conf.set(ConfigOptions.LOG_REPLICA_FETCHER_NUMBER, numFetchers);
        ReplicaFetcherManager fetcherManager =
                new ReplicaFetcherManager(
                        conf, null, TABLET_SERVER_ID, replicaManager, id -> Optional.of(leader));

        TableBucket positiveHashBucket = new TableBucket(0L, 0);
        TableBucket negativeHashBucket = new TableBucket(Integer.MAX_VALUE, 1);
        TableBucket evenHashBucket = new TableBucket(0L, 1);
        assertThat(positiveHashBucket.hashCode()).isPositive();
        assertThat(negativeHashBucket.hashCode()).isNegative();

        assertThat(fetcherManager.getFetcherId(positiveHashBucket)).isEqualTo(1);
        assertThat(fetcherManager.getFetcherId(negativeHashBucket)).isEqualTo(1);
        assertThat(fetcherManager.getFetcherId(evenHashBucket)).isZero();
    }

    @Test
    void testDoesNotAddFetcherWhenFollowerHasNoLeader() {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        AtomicReference<List<NotifyLeaderAndIsrResultForBucket>> result = new AtomicReference<>();

        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                PhysicalTablePath.of(DATA1_TABLE_PATH),
                                tb,
                                Collections.singletonList(TABLET_SERVER_ID),
                                new LeaderAndIsr(
                                        LeaderAndIsr.NO_LEADER,
                                        LeaderAndIsr.INITIAL_LEADER_EPOCH,
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        INITIAL_COORDINATOR_EPOCH,
                                        LeaderAndIsr.INITIAL_BUCKET_EPOCH),
                                3,
                                0L)),
                result::set);

        assertThat(result.get()).hasSize(1);
        assertThat(result.get().get(0).succeeded()).isTrue();
        assertThat(replicaManager.getReplicaFetcherManager().getFetcherThreadMap()).isEmpty();
    }

    @Test
    void testOrdinaryFollowerReplayRestoresFetcherAtUnchangedLeaderEpoch() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        List<Integer> replicas = Arrays.asList(TABLET_SERVER_ID, TABLET_SERVER_ID + 1);
        LeaderAndIsr remoteLeader =
                new LeaderAndIsr(
                        TABLET_SERVER_ID + 1,
                        1,
                        replicas,
                        Collections.emptyList(),
                        INITIAL_COORDINATOR_EPOCH,
                        LeaderAndIsr.INITIAL_BUCKET_EPOCH);
        CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> roleResult =
                new CompletableFuture<>();
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                PhysicalTablePath.of(DATA1_TABLE_PATH),
                                tb,
                                replicas,
                                remoteLeader)),
                roleResult::complete);
        assertThat(roleResult.get()).containsOnly(new NotifyLeaderAndIsrResultForBucket(tb));
        assertThat(replicaManager.getReplicaFetcherManager().hasFetcherForBucket(tb)).isTrue();
        replicaManager
                .getReplicaFetcherManager()
                .removeFetcherForBuckets(Collections.singleton(tb));
        assertThat(replicaManager.getReplicaFetcherManager().hasFetcherForBucket(tb)).isFalse();

        roleResult = new CompletableFuture<>();
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                PhysicalTablePath.of(DATA1_TABLE_PATH),
                                tb,
                                replicas,
                                remoteLeader)),
                roleResult::complete);

        assertThat(roleResult.get()).containsOnly(new NotifyLeaderAndIsrResultForBucket(tb));
        Replica follower = replicaManager.getReplicaOrException(tb);
        assertThat(follower.getBulkLoadTargetMetadata()).isNull();
        assertThat(follower.isLeader()).isFalse();
        assertThat(follower.getLeaderId()).isEqualTo(TABLET_SERVER_ID + 1);
        assertThat(replicaManager.getReplicaFetcherManager().hasFetcherForBucket(tb))
                .as("an exact ordinary follower replay must restore missing fetch ownership")
                .isTrue();
    }

    @Test
    void testLoadingFollowerRecoveryDoesNotStartOrdinaryFetcher() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 2);
        List<Integer> replicas = Arrays.asList(TABLET_SERVER_ID, TABLET_SERVER_ID + 1);
        LeaderAndIsr followerRole =
                new LeaderAndIsr(
                        TABLET_SERVER_ID + 1,
                        1,
                        replicas,
                        Collections.emptyList(),
                        INITIAL_COORDINATOR_EPOCH,
                        LeaderAndIsr.INITIAL_BUCKET_EPOCH);
        BulkLoadTargetMetadata loadingTarget =
                new BulkLoadTargetMetadata(tb, 1, BulkLoadDataState.LOADING);
        CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> roleResult =
                new CompletableFuture<>();
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                PhysicalTablePath.of(DATA1_TABLE_PATH),
                                tb,
                                replicas,
                                followerRole)),
                roleResult::complete);
        assertThat(roleResult.get()).containsOnly(new NotifyLeaderAndIsrResultForBucket(tb));
        assertThat(replicaManager.getReplicaFetcherManager().hasFetcherForBucket(tb)).isTrue();

        roleResult = new CompletableFuture<>();
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                PhysicalTablePath.of(DATA1_TABLE_PATH),
                                tb,
                                replicas,
                                followerRole,
                                loadingTarget)),
                roleResult::complete);

        assertThat(roleResult.get()).containsOnly(new NotifyLeaderAndIsrResultForBucket(tb));
        assertThat(replicaManager.getReplicaOrException(tb).getBulkLoadTargetMetadata())
                .isEqualTo(loadingTarget);
        assertThat(replicaManager.getReplicaFetcherManager().hasFetcherForBucket(tb))
                .as("LOADING recovery must not start ordinary follower replication")
                .isFalse();

        BulkLoadTargetMetadata activeTarget =
                new BulkLoadTargetMetadata(tb, 2, BulkLoadDataState.ACTIVE);
        roleResult = new CompletableFuture<>();
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                PhysicalTablePath.of(DATA1_TABLE_PATH),
                                tb,
                                replicas,
                                followerRole,
                                activeTarget)),
                roleResult::complete);
        assertThat(roleResult.get()).containsOnly(new NotifyLeaderAndIsrResultForBucket(tb));

        assertThat(replicaManager.getReplicaOrException(tb).getBulkLoadTargetMetadata())
                .isEqualTo(activeTarget);
        assertThat(replicaManager.getReplicaFetcherManager().hasFetcherForBucket(tb))
                .as("successful ACTIVE publication must restore ordinary follower replication")
                .isTrue();
    }

    @Test
    void testEqualEpochFollowerReplayMovesFetcherToChangedLeader() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 3);
        List<Integer> replicas =
                Arrays.asList(TABLET_SERVER_ID, TABLET_SERVER_ID + 1, TABLET_SERVER_ID + 2);
        int initialLeader = TABLET_SERVER_ID + 1;
        int changedLeader = TABLET_SERVER_ID + 2;
        int fetcherId = tb.hashCode() % conf.getInt(ConfigOptions.LOG_REPLICA_FETCHER_NUMBER);
        CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> roleResult =
                new CompletableFuture<>();
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                PhysicalTablePath.of(DATA1_TABLE_PATH),
                                tb,
                                replicas,
                                new LeaderAndIsr(
                                        initialLeader,
                                        1,
                                        replicas,
                                        Collections.emptyList(),
                                        INITIAL_COORDINATOR_EPOCH,
                                        LeaderAndIsr.INITIAL_BUCKET_EPOCH))),
                roleResult::complete);
        assertThat(roleResult.get()).containsOnly(new NotifyLeaderAndIsrResultForBucket(tb));
        assertThat(replicaManager.getReplicaFetcherManager().getFetcherThreadMap())
                .containsOnlyKeys(new ServerIdAndFetcherId(initialLeader, fetcherId));

        roleResult = new CompletableFuture<>();
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                PhysicalTablePath.of(DATA1_TABLE_PATH),
                                tb,
                                replicas,
                                new LeaderAndIsr(
                                        changedLeader,
                                        1,
                                        replicas,
                                        Collections.emptyList(),
                                        INITIAL_COORDINATOR_EPOCH,
                                        LeaderAndIsr.INITIAL_BUCKET_EPOCH))),
                roleResult::complete);

        assertThat(roleResult.get()).containsOnly(new NotifyLeaderAndIsrResultForBucket(tb));
        assertThat(replicaManager.getReplicaOrException(tb).getLeaderId()).isEqualTo(changedLeader);
        assertThat(replicaManager.getReplicaFetcherManager().getFetcherThreadMap())
                .as("an accepted leader change must leave exactly one fetch owner")
                .containsOnlyKeys(new ServerIdAndFetcherId(changedLeader, fetcherId));
    }

    private class TestingReplicaFetcherManager extends ReplicaFetcherManager {

        private final ReplicaFetcherThread fetcherThread;

        public TestingReplicaFetcherManager(
                int serverId, ReplicaManager replicaManager, ReplicaFetcherThread fetcherThread) {
            super(new Configuration(), null, serverId, replicaManager, (id) -> Optional.of(leader));
            this.fetcherThread = fetcherThread;
        }

        @Override
        public ReplicaFetcherThread createFetcherThread(int fetcherId, int leaderId) {
            return fetcherThread;
        }
    }
}

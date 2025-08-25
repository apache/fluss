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
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.replica.ReplicaTestBase;
import org.apache.fluss.server.replica.fetcher.ReplicaFetcherManager.ServerIdAndFetcherId;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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
                                new TestTabletServerGateway(false)),
                        (int)
                                conf.get(ConfigOptions.LOG_REPLICA_FETCH_BACKOFF_INTERVAL)
                                        .toMillis());
    }

    @Test
    void testAddAndRemoveBucket() {
        int numFetchers = 2;
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
                                        INITIAL_COORDINATOR_EPOCH,
                                        LeaderAndIsr.INITIAL_BUCKET_EPOCH))),
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
                fetcherThreadMap.get(new ServerIdAndFetcherId(1, tb.hashCode() % numFetchers));
        assertThat(thread).isEqualTo(fetcherThread);
        assertThat(fetcherThread.fetchStatus(tb).isPresent()).isTrue();

        fetcherManager.removeFetcherForBuckets(Collections.singleton(tb));
        // the fetcher thread will not be moved out from the map.
        assertThat(fetcherThreadMap.size()).isEqualTo(1);
        thread = fetcherThreadMap.get(new ServerIdAndFetcherId(1, tb.hashCode() % numFetchers));

        assertThat(thread).isEqualTo(fetcherThread);
        assertThat(fetcherThread.fetchStatus(tb).isPresent()).isFalse();

        fetcherManager.shutdownIdleFetcherThreads();
        assertThat(fetcherThreadMap.size()).isEqualTo(0);
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

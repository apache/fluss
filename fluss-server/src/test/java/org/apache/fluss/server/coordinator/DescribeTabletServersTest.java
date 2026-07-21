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
import org.apache.fluss.rpc.messages.DescribeTabletServersResponse;
import org.apache.fluss.rpc.messages.GetClusterHealthResponse;
import org.apache.fluss.rpc.messages.PbTabletServerLoad;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CoordinatorService#describeTabletServers(CoordinatorContext)}. */
class DescribeTabletServersTest {

    private CoordinatorContext ctx;

    @BeforeEach
    void setUp() {
        ctx = new CoordinatorContext(ZkEpoch.INITIAL_EPOCH);
        ctx.setLiveTabletServers(
                Arrays.asList(makeServerInfo(0), makeServerInfo(1), makeServerInfo(2)));
    }

    @Test
    void testEmptyClusterReportsLiveServersWithZeroCounters() {
        DescribeTabletServersResponse resp = CoordinatorService.describeTabletServers(ctx);

        assertThat(resp.getTabletServersList())
                .extracting(PbTabletServerLoad::getServerId)
                .containsExactly(0, 1, 2);
        assertLoad(resp, 0, 0, 0, 0, 0);
        assertLoad(resp, 1, 0, 0, 0, 0);
        assertLoad(resp, 2, 0, 0, 0, 0);
    }

    @Test
    void testAllInSyncAllLeadersActive() {
        TableBucket tb = new TableBucket(1L, 0);
        ctx.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1, 2));
        ctx.putBucketLeaderAndIsr(
                tb, new LeaderAndIsr(0, 1, Arrays.asList(0, 1, 2), Collections.emptyList(), 0, 1));

        DescribeTabletServersResponse resp = CoordinatorService.describeTabletServers(ctx);

        assertLoad(resp, 0, 1, 1, 1, 1);
        assertLoad(resp, 1, 1, 1, 0, 0);
        assertLoad(resp, 2, 1, 1, 0, 0);
    }

    @Test
    void testReplicaOutOfIsrOnlyAffectsThatServer() {
        TableBucket tb = new TableBucket(1L, 0);
        ctx.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1, 2));
        ctx.putBucketLeaderAndIsr(
                tb, new LeaderAndIsr(0, 1, Arrays.asList(0, 1), Collections.emptyList(), 0, 1));

        DescribeTabletServersResponse resp = CoordinatorService.describeTabletServers(ctx);

        assertLoad(resp, 0, 1, 1, 1, 1);
        assertLoad(resp, 1, 1, 1, 0, 0);
        assertLoad(resp, 2, 1, 0, 0, 0);
    }

    @Test
    void testInactiveLeader() {
        TableBucket tb = new TableBucket(1L, 0);
        ctx.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1));
        ctx.putBucketLeaderAndIsr(
                tb, new LeaderAndIsr(0, 1, Arrays.asList(0, 1), Collections.emptyList(), 0, 1));
        ctx.addPendingLeaderActivation(tb);

        assertLoad(CoordinatorService.describeTabletServers(ctx), 0, 1, 1, 1, 0);

        ctx.clearPendingLeaderActivation(tb);

        assertLoad(CoordinatorService.describeTabletServers(ctx), 0, 1, 1, 1, 1);
    }

    @Test
    void testNoLeaderBucketAttributesLeadershipToNoServer() {
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

        DescribeTabletServersResponse resp = CoordinatorService.describeTabletServers(ctx);

        assertLoad(resp, 0, 1, 1, 0, 0);
        assertLoad(resp, 1, 1, 1, 0, 0);
    }

    @Test
    void testEvacuatedServerReportsZeroReplicas() {
        // only servers 0 and 1 host replicas; server 2 is evacuated and safe to remove
        TableBucket tb = new TableBucket(1L, 0);
        ctx.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1));
        ctx.putBucketLeaderAndIsr(
                tb, new LeaderAndIsr(0, 1, Arrays.asList(0, 1), Collections.emptyList(), 0, 1));

        assertLoad(CoordinatorService.describeTabletServers(ctx), 2, 0, 0, 0, 0);
    }

    @Test
    void testDeadServerStillAssignedIsReported() {
        // server 5 is not live but still has an assigned replica; it must be visible so
        // that a scale-in gate does not treat the cluster as fully drained
        TableBucket tb = new TableBucket(1L, 0);
        ctx.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1, 5));
        ctx.putBucketLeaderAndIsr(
                tb, new LeaderAndIsr(0, 1, Arrays.asList(0, 1), Collections.emptyList(), 0, 1));

        assertLoad(CoordinatorService.describeTabletServers(ctx), 5, 1, 0, 0, 0);
    }

    @Test
    void testPartitionedTableBucket() {
        TableBucket tb = new TableBucket(1L, 100L, 0);
        ctx.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1));
        ctx.putBucketLeaderAndIsr(
                tb,
                new LeaderAndIsr(
                        0, 1, Collections.singletonList(0), Collections.emptyList(), 0, 1));

        DescribeTabletServersResponse resp = CoordinatorService.describeTabletServers(ctx);

        assertLoad(resp, 0, 1, 1, 1, 1);
        assertLoad(resp, 1, 1, 0, 0, 0);
    }

    @Test
    void testPerServerSumsReconcileWithClusterHealth() {
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
        ctx.addPendingLeaderActivation(tb3);

        DescribeTabletServersResponse resp = CoordinatorService.describeTabletServers(ctx);
        GetClusterHealthResponse health = CoordinatorService.computeClusterHealth(ctx);

        int numReplicas = 0;
        int inSyncReplicas = 0;
        int numLeaderReplicas = 0;
        int activeLeaderReplicas = 0;
        for (PbTabletServerLoad load : resp.getTabletServersList()) {
            numReplicas += load.getNumReplicas();
            inSyncReplicas += load.getInSyncReplicas();
            numLeaderReplicas += load.getNumLeaderReplicas();
            activeLeaderReplicas += load.getActiveLeaderReplicas();
        }

        assertThat(numReplicas).isEqualTo(health.getNumReplicas());
        assertThat(inSyncReplicas).isEqualTo(health.getInSyncReplicas());
        assertThat(numLeaderReplicas).isEqualTo(health.getNumLeaderReplicas());
        assertThat(activeLeaderReplicas).isEqualTo(health.getActiveLeaderReplicas());
    }

    private static void assertLoad(
            DescribeTabletServersResponse resp,
            int serverId,
            int numReplicas,
            int inSyncReplicas,
            int numLeaderReplicas,
            int activeLeaderReplicas) {
        PbTabletServerLoad load =
                resp.getTabletServersList().stream()
                        .filter(l -> l.getServerId() == serverId)
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "no load reported for tablet server " + serverId));
        assertThat(load.getNumReplicas())
                .as("numReplicas of server %s", serverId)
                .isEqualTo(numReplicas);
        assertThat(load.getInSyncReplicas())
                .as("inSyncReplicas of server %s", serverId)
                .isEqualTo(inSyncReplicas);
        assertThat(load.getNumLeaderReplicas())
                .as("numLeaderReplicas of server %s", serverId)
                .isEqualTo(numLeaderReplicas);
        assertThat(load.getActiveLeaderReplicas())
                .as("activeLeaderReplicas of server %s", serverId)
                .isEqualTo(activeLeaderReplicas);
    }

    private static ServerInfo makeServerInfo(int id) {
        return new ServerInfo(
                id,
                "RACK" + id,
                Endpoint.fromListenersString("CLIENT://host" + id + ":9124"),
                ServerType.TABLET_SERVER);
    }
}

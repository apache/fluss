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
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.rpc.messages.GetClusterHealthResponse;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CoordinatorService#computeClusterHealth}. */
class ClusterHealthTest {

    private CoordinatorContext ctx;

    @BeforeEach
    void setUp() {
        ctx = new CoordinatorContext(ZkEpoch.INITIAL_EPOCH);
        ctx.setLiveTabletServers(
                Arrays.asList(makeServerInfo(0), makeServerInfo(1), makeServerInfo(2)));
    }

    @Test
    void testEmptyCluster() {
        GetClusterHealthResponse resp = CoordinatorService.computeClusterHealth(ctx);

        assertThat(resp.getNumReplicas()).isEqualTo(0);
        assertThat(resp.getInSyncReplicas()).isEqualTo(0);
        assertThat(resp.getNumLeaderReplicas()).isEqualTo(0);
        assertThat(resp.getActiveLeaderReplicas()).isEqualTo(0);
        assertThat(resp.getStatus()).isEqualTo(0 /* GREEN */);
    }

    @Test
    void testGreenAllInSyncAllLeadersActive() {
        TableBucket tb = new TableBucket(1L, 0);
        ctx.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1, 2));
        ctx.putBucketLeaderAndIsr(
                tb, new LeaderAndIsr(0, 1, Arrays.asList(0, 1, 2), Collections.emptyList(), 0, 1));

        GetClusterHealthResponse resp = CoordinatorService.computeClusterHealth(ctx);

        assertThat(resp.getNumReplicas()).isEqualTo(3);
        assertThat(resp.getInSyncReplicas()).isEqualTo(3);
        assertThat(resp.getNumLeaderReplicas()).isEqualTo(1);
        assertThat(resp.getActiveLeaderReplicas()).isEqualTo(1);
        assertThat(resp.getStatus()).isEqualTo(0 /* GREEN */);
    }

    @Test
    void testYellowIsrIncompleteButLeadersActive() {
        TableBucket tb = new TableBucket(1L, 0);
        ctx.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1, 2));
        ctx.putBucketLeaderAndIsr(
                tb, new LeaderAndIsr(0, 1, Arrays.asList(0, 1), Collections.emptyList(), 0, 1));

        GetClusterHealthResponse resp = CoordinatorService.computeClusterHealth(ctx);

        assertThat(resp.getNumReplicas()).isEqualTo(3);
        assertThat(resp.getInSyncReplicas()).isEqualTo(2);
        assertThat(resp.getNumLeaderReplicas()).isEqualTo(1);
        assertThat(resp.getActiveLeaderReplicas()).isEqualTo(1);
        assertThat(resp.getStatus()).isEqualTo(1 /* YELLOW */);
    }

    @Test
    void testRedLeaderInactive() {
        TableBucket tb = new TableBucket(1L, 0);
        ctx.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1, 2));
        ctx.putBucketLeaderAndIsr(
                tb, new LeaderAndIsr(0, 1, Arrays.asList(0, 1, 2), Collections.emptyList(), 0, 1));
        ctx.addPendingLeaderActivation(tb);

        GetClusterHealthResponse resp = CoordinatorService.computeClusterHealth(ctx);

        assertThat(resp.getNumReplicas()).isEqualTo(3);
        assertThat(resp.getInSyncReplicas()).isEqualTo(3);
        assertThat(resp.getNumLeaderReplicas()).isEqualTo(1);
        assertThat(resp.getActiveLeaderReplicas()).isEqualTo(0);
        assertThat(resp.getStatus()).isEqualTo(2 /* RED */);
    }

    @Test
    void testRedNoLeader() {
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

        GetClusterHealthResponse resp = CoordinatorService.computeClusterHealth(ctx);

        assertThat(resp.getActiveLeaderReplicas()).isEqualTo(0);
        assertThat(resp.getStatus()).isEqualTo(2 /* RED */);
    }

    @Test
    void testRedLeaderOnDeadServer() {
        TableBucket tb = new TableBucket(1L, 0);
        ctx.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1, 5));
        ctx.putBucketLeaderAndIsr(
                tb, new LeaderAndIsr(5, 1, Arrays.asList(0, 1, 5), Collections.emptyList(), 0, 1));

        GetClusterHealthResponse resp = CoordinatorService.computeClusterHealth(ctx);

        assertThat(resp.getActiveLeaderReplicas()).isEqualTo(0);
        assertThat(resp.getStatus()).isEqualTo(2 /* RED */);
    }

    @Test
    void testNoLeaderAndIsrCountsAsRedAndZeroIsr() {
        TableBucket tb = new TableBucket(1L, 0);
        ctx.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1));

        GetClusterHealthResponse resp = CoordinatorService.computeClusterHealth(ctx);

        assertThat(resp.getNumReplicas()).isEqualTo(2);
        assertThat(resp.getInSyncReplicas()).isEqualTo(0);
        assertThat(resp.getNumLeaderReplicas()).isEqualTo(1);
        assertThat(resp.getActiveLeaderReplicas()).isEqualTo(0);
        assertThat(resp.getStatus()).isEqualTo(2 /* RED */);
    }

    @Test
    void testTransitionRedToGreenAfterMarkActive() {
        TableBucket tb = new TableBucket(1L, 0);
        ctx.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1));
        ctx.putBucketLeaderAndIsr(
                tb, new LeaderAndIsr(0, 1, Arrays.asList(0, 1), Collections.emptyList(), 0, 1));
        ctx.addPendingLeaderActivation(tb);

        assertThat(CoordinatorService.computeClusterHealth(ctx).getStatus()).isEqualTo(2 /* RED */);

        ctx.clearPendingLeaderActivation(tb);

        assertThat(CoordinatorService.computeClusterHealth(ctx).getStatus())
                .isEqualTo(0 /* GREEN */);
    }

    @Test
    void testMultipleBucketsMixed() {
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

        GetClusterHealthResponse resp = CoordinatorService.computeClusterHealth(ctx);

        assertThat(resp.getNumReplicas()).isEqualTo(6);
        assertThat(resp.getInSyncReplicas()).isEqualTo(5);
        assertThat(resp.getNumLeaderReplicas()).isEqualTo(3);
        assertThat(resp.getActiveLeaderReplicas()).isEqualTo(3);
        assertThat(resp.getStatus()).isEqualTo(1 /* YELLOW */);
    }

    @Test
    void testPartitionedTableBucket() {
        TableBucket tb = new TableBucket(1L, 100L, 0);
        ctx.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1));
        ctx.putBucketLeaderAndIsr(
                tb,
                new LeaderAndIsr(
                        0, 1, Collections.singletonList(0), Collections.emptyList(), 0, 1));

        GetClusterHealthResponse resp = CoordinatorService.computeClusterHealth(ctx);

        assertThat(resp.getNumReplicas()).isEqualTo(2);
        assertThat(resp.getInSyncReplicas()).isEqualTo(1);
        assertThat(resp.getNumLeaderReplicas()).isEqualTo(1);
        assertThat(resp.getActiveLeaderReplicas()).isEqualTo(1);
        assertThat(resp.getStatus()).isEqualTo(1 /* YELLOW */);
    }

    @Test
    void testInactiveLeaderClearedOnTableRemoval() {
        TableBucket tb = new TableBucket(1L, 0);
        ctx.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1));
        ctx.putBucketLeaderAndIsr(
                tb, new LeaderAndIsr(0, 1, Arrays.asList(0, 1), Collections.emptyList(), 0, 1));
        ctx.addPendingLeaderActivation(tb);

        assertThat(ctx.getPendingLeaderActivationBuckets()).contains(tb);

        ctx.removeTable(1L);

        assertThat(ctx.getPendingLeaderActivationBuckets()).doesNotContain(tb);
    }

    @Test
    void testRedWithInactiveLeaderAndIncompleteIsr() {
        TableBucket tb1 = new TableBucket(1L, 0);
        TableBucket tb2 = new TableBucket(1L, 1);

        ctx.updateBucketReplicaAssignment(tb1, Arrays.asList(0, 1));
        ctx.updateBucketReplicaAssignment(tb2, Arrays.asList(0, 1));

        ctx.putBucketLeaderAndIsr(
                tb1,
                new LeaderAndIsr(
                        0, 1, Collections.singletonList(0), Collections.emptyList(), 0, 1));
        ctx.putBucketLeaderAndIsr(
                tb2, new LeaderAndIsr(1, 1, Arrays.asList(0, 1), Collections.emptyList(), 0, 1));

        ctx.addPendingLeaderActivation(tb2);

        GetClusterHealthResponse resp = CoordinatorService.computeClusterHealth(ctx);

        assertThat(resp.getNumReplicas()).isEqualTo(4);
        assertThat(resp.getInSyncReplicas()).isEqualTo(3);
        assertThat(resp.getActiveLeaderReplicas()).isEqualTo(1);
        assertThat(resp.getStatus()).isEqualTo(2 /* RED */);
    }

    @Test
    void testInactiveLeaderClearedOnPartitionRemoval() {
        TableBucket tb = new TableBucket(1L, 100L, 0);
        ctx.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1));
        ctx.putBucketLeaderAndIsr(
                tb, new LeaderAndIsr(0, 1, Arrays.asList(0, 1), Collections.emptyList(), 0, 1));
        ctx.addPendingLeaderActivation(tb);

        assertThat(ctx.getPendingLeaderActivationBuckets()).contains(tb);

        ctx.removePartition(new TablePartition(1L, 100L));

        assertThat(ctx.getPendingLeaderActivationBuckets()).doesNotContain(tb);
    }

    @Test
    void testFollowerNotificationDoesNotMarkInactive() {
        // Simulate sendNotifyLeaderAndIsrRequest: when NotifyLeaderAndIsr is sent
        // to a server for a bucket where it is a follower, the bucket should NOT
        // be marked inactive. Only the server that is the leader gets marked.
        TableBucket tb = new TableBucket(1L, 0);
        ctx.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1));
        ctx.putBucketLeaderAndIsr(
                tb, new LeaderAndIsr(0, 1, Arrays.asList(0, 1), Collections.emptyList(), 0, 1));

        // Sending to server 1 (follower): leader=0, serverId=1
        // Follower does not match leader, so addPendingLeaderActivation is NOT called.
        assertThat(ctx.isLeaderActive(tb)).isTrue();
        assertThat(CoordinatorService.computeClusterHealth(ctx).getStatus())
                .isEqualTo(0 /* GREEN */);

        // Sending to server 0 (leader): leader=0, serverId=0 — matches, so mark pending.
        ctx.addPendingLeaderActivation(tb);
        assertThat(ctx.isLeaderActive(tb)).isFalse();
        assertThat(CoordinatorService.computeClusterHealth(ctx).getStatus()).isEqualTo(2 /* RED */);
    }

    @Test
    void testLeaderChangedBetweenSendAndResponseStaysInactive() {
        // Simulate processNotifyLeaderAndIsrResponseReceivedEvent: if the leader
        // changed between send and response, the responding server is no longer
        // the leader, so the bucket must stay inactive.
        TableBucket tb = new TableBucket(1L, 0);
        ctx.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1, 2));
        ctx.putBucketLeaderAndIsr(
                tb, new LeaderAndIsr(0, 1, Arrays.asList(0, 1, 2), Collections.emptyList(), 0, 1));
        ctx.addPendingLeaderActivation(tb);

        // Simulate: leader changed from server 0 to server 1 before server 0 responds
        ctx.putBucketLeaderAndIsr(
                tb, new LeaderAndIsr(1, 2, Arrays.asList(0, 1, 2), Collections.emptyList(), 0, 1));

        // Server 0 responds successfully — but it's no longer the leader
        int respondingServerId = 0;
        ctx.getBucketLeaderAndIsr(tb)
                .ifPresent(
                        lai -> {
                            if (lai.leader() == respondingServerId) {
                                ctx.clearPendingLeaderActivation(tb);
                            }
                        });

        // Bucket must stay inactive because the responding server (0) != current leader (1)
        assertThat(ctx.isLeaderActive(tb)).isFalse();
        assertThat(CoordinatorService.computeClusterHealth(ctx).getStatus()).isEqualTo(2 /* RED */);

        // Now server 1 (the actual leader) responds → bucket becomes active
        int actualLeaderServerId = 1;
        ctx.getBucketLeaderAndIsr(tb)
                .ifPresent(
                        lai -> {
                            if (lai.leader() == actualLeaderServerId) {
                                ctx.clearPendingLeaderActivation(tb);
                            }
                        });

        assertThat(ctx.isLeaderActive(tb)).isTrue();
        assertThat(CoordinatorService.computeClusterHealth(ctx).getStatus())
                .isEqualTo(0 /* GREEN */);
    }

    private static ServerInfo makeServerInfo(int id) {
        return new ServerInfo(
                id,
                "RACK" + id,
                Endpoint.fromListenersString("CLIENT://host" + id + ":9124"),
                ServerType.TABLET_SERVER);
    }
}

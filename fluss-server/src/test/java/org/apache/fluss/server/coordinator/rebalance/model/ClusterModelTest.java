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

package org.apache.fluss.server.coordinator.rebalance.model;

import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ClusterModel}. */
public class ClusterModelTest {
    private SortedSet<ServerModel> servers;
    private ServerModel server0;
    private ServerModel server1;
    private ServerModel server2;
    private ServerModel server3;

    @BeforeEach
    public void setup() {
        servers = new TreeSet<>();
        server0 = new ServerModel(0, "rack0", false);
        server1 = new ServerModel(1, "rack1", false);
        server2 = new ServerModel(2, "rack2", false);
        server3 = new ServerModel(3, "rack0", true);
        servers.add(server0);
        servers.add(server1);
        servers.add(server2);
        servers.add(server3);
    }

    @Test
    void testClusterModel() {
        ClusterModel clusterModel = new ClusterModel(servers);
        assertThat(clusterModel.aliveServers()).containsOnly(server0, server1, server2);
        assertThat(clusterModel.offlineServers()).containsOnly(server3);
        assertThat(clusterModel.servers()).containsOnly(server0, server1, server2, server3);
        assertThat(clusterModel.bucket(new TableBucket(1, 0))).isNull();
        assertThat(clusterModel.numReplicas()).isEqualTo(0);
        assertThat(clusterModel.numLeaderReplicas()).isEqualTo(0);
        assertThat(clusterModel.rack("rack0").rack()).isEqualTo("rack0");
        assertThat(clusterModel.server(0)).isEqualTo(server0);
        assertThat(clusterModel.server(5)).isNull();

        // Test create replicas.
        clusterModel.createReplica(0, new TableBucket(1, 0), 0, true);
        clusterModel.createReplica(1, new TableBucket(1, 0), 1, false);
        clusterModel.createReplica(2, new TableBucket(1, 0), 2, false);
        clusterModel.createReplica(0, new TableBucket(2, 0L, 0), 0, true);
        clusterModel.createReplica(1, new TableBucket(2, 0L, 0), 1, false);
        clusterModel.createReplica(1, new TableBucket(2, 1L, 0), 0, true);

        assertThat(clusterModel.numReplicas()).isEqualTo(6);
        assertThat(clusterModel.numLeaderReplicas()).isEqualTo(3);
        assertThat(clusterModel.tables()).containsOnly(1L, 2L);
        assertThat(clusterModel.getBucketsByTable()).hasSize(2);

        // test get replica distribution.
        Map<TableBucket, List<Integer>> replicaDistribution = clusterModel.getReplicaDistribution();
        assertThat(replicaDistribution).hasSize(3);
        assertThat(replicaDistribution.get(new TableBucket(1, 0))).contains(0, 1, 2);
        assertThat(replicaDistribution.get(new TableBucket(2, 0L, 0))).contains(0, 1);
        assertThat(replicaDistribution.get(new TableBucket(2, 1L, 0))).contains(1);

        // test get leader distribution.
        Map<TableBucket, Integer> leaderDistribution = clusterModel.getLeaderDistribution();
        assertThat(leaderDistribution).hasSize(3);
        assertThat(leaderDistribution.get(new TableBucket(1, 0))).isEqualTo(0);
        assertThat(leaderDistribution.get(new TableBucket(2, 0L, 0))).isEqualTo(0);
        assertThat(leaderDistribution.get(new TableBucket(2, 1L, 0))).isEqualTo(1);
    }

    @Test
    void testRelocateLeadership() {
        TableBucket tb0 = new TableBucket(1, 0);
        ClusterModel clusterModel = new ClusterModel(servers);
        clusterModel.createReplica(0, tb0, 0, true);
        clusterModel.createReplica(1, tb0, 1, false);
        clusterModel.createReplica(2, tb0, 2, false);

        // try to relocate leadership from server 0 to server 1
        assertThat(clusterModel.relocateLeadership(tb0, 0, 1)).isTrue();
        ReplicaModel leaderReplica = clusterModel.bucket(tb0).leader();
        assertThat(leaderReplica).isNotNull();
        assertThat(leaderReplica.server().id()).isEqualTo(1);

        // try to relocate leadership from server 0 to server 2. As 0 is not leader, this operation
        // will return false.
        assertThat(clusterModel.relocateLeadership(tb0, 0, 2)).isFalse();

        assertThatThrownBy(() -> clusterModel.relocateLeadership(tb0, 1, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Cannot relocate leadership of bucket TableBucket{tableId=1, bucket=0} "
                                + "from server 1 to server 1 because the destination replica is a leader.");

        assertThatThrownBy(() -> clusterModel.relocateLeadership(tb0, 1, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Requested replica 5 is not a replica of bucket TableBucket{tableId=1, bucket=0}");
    }

    @Test
    void testRelocateReplica() {
        TableBucket tb0 = new TableBucket(1, 0);
        ClusterModel clusterModel = new ClusterModel(servers);
        clusterModel.createReplica(0, tb0, 0, true);
        clusterModel.createReplica(1, tb0, 1, false);

        BucketModel bucket = clusterModel.bucket(tb0);
        assertThat(bucket).isNotNull();
        assertThat(bucket.replica(0)).isNotNull();
        assertThat(bucket.replica(1)).isNotNull();
        assertThatThrownBy(() -> bucket.replica(2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Requested replica 2 is not a replica of bucket TableBucket{tableId=1, bucket=0}");
        clusterModel.relocateReplica(tb0, 1, 2);
        assertThat(bucket.replica(0)).isNotNull();
        assertThat(bucket.replica(2)).isNotNull();
        assertThatThrownBy(() -> bucket.replica(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Requested replica 1 is not a replica of bucket TableBucket{tableId=1, bucket=0}");
    }
}

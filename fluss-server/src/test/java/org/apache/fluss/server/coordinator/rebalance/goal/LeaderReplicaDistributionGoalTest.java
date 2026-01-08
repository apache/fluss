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

package org.apache.fluss.server.coordinator.rebalance.goal;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModel;
import org.apache.fluss.server.coordinator.rebalance.model.ServerModel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.fluss.server.coordinator.rebalance.RebalanceTestUtils.addBucket;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LeaderReplicaDistributionGoal}. */
public class LeaderReplicaDistributionGoalTest {
    private SortedSet<ServerModel> servers;

    @BeforeEach
    public void setup() {
        servers = new TreeSet<>();
        ServerModel server0 = new ServerModel(0, "rack0", false);
        ServerModel server1 = new ServerModel(1, "rack1", false);
        ServerModel server2 = new ServerModel(2, "rack2", false);
        ServerModel server3 = new ServerModel(3, "rack0", false);
        servers.add(server0);
        servers.add(server1);
        servers.add(server2);
        servers.add(server3);
    }

    @Test
    void testDoOptimize() {
        LeaderReplicaDistributionGoal goal = new LeaderReplicaDistributionGoal();
        ClusterModel clusterModel = new ClusterModel(servers);

        // before optimize:
        // for 18 buckets, the assignment: 0,1
        // for 18 buckets, the assignment: 1,0
        // the leader replica ratio of servers is 18:18:0:0, the avg buckets per server is 9
        for (int i = 0; i < 18; i++) {
            addBucket(clusterModel, new TableBucket(0, i), Arrays.asList(0, 1));
            addBucket(clusterModel, new TableBucket(1, i), Arrays.asList(1, 0));
        }

        Map<Integer, Integer> serverIdToLeaderReplicaNumber = getServerIdToLeaderReplicaNumber();
        assertThat(serverIdToLeaderReplicaNumber.get(0)).isEqualTo(18);
        assertThat(serverIdToLeaderReplicaNumber.get(1)).isEqualTo(18);
        assertThat(serverIdToLeaderReplicaNumber.get(2)).isEqualTo(0);
        assertThat(serverIdToLeaderReplicaNumber.get(3)).isEqualTo(0);

        goal.optimize(clusterModel, new HashSet<>());

        serverIdToLeaderReplicaNumber = getServerIdToLeaderReplicaNumber();
        assertThat(serverIdToLeaderReplicaNumber.get(0)).isEqualTo(10);
        assertThat(serverIdToLeaderReplicaNumber.get(1)).isEqualTo(8);
        assertThat(serverIdToLeaderReplicaNumber.get(2)).isEqualTo(10);
        assertThat(serverIdToLeaderReplicaNumber.get(3)).isEqualTo(8);
    }

    @Test
    void testDoOptimizeWithOfflineServer() {
        ServerModel server4 = new ServerModel(4, "rack0", true);
        servers.add(server4);

        LeaderReplicaDistributionGoal goal = new LeaderReplicaDistributionGoal();
        ClusterModel clusterModel = new ClusterModel(servers);

        // before optimize:
        // for 18 buckets, the assignment: 0,1
        // for 18 buckets, the assignment: 1,0
        // for 4 buckets, the assignment: 4,0,1
        // the leader replica ratio of servers is 18:18:0:0:4, the avg buckets per server is 8
        for (int i = 0; i < 18; i++) {
            addBucket(clusterModel, new TableBucket(0, i), Arrays.asList(0, 1));
            addBucket(clusterModel, new TableBucket(1, i), Arrays.asList(1, 0));
        }

        for (int i = 0; i < 4; i++) {
            addBucket(clusterModel, new TableBucket(2, i), Arrays.asList(4, 2, 1));
        }

        Map<Integer, Integer> serverIdToLeaderReplicaNumber = getServerIdToLeaderReplicaNumber();
        assertThat(serverIdToLeaderReplicaNumber.get(0)).isEqualTo(18);
        assertThat(serverIdToLeaderReplicaNumber.get(1)).isEqualTo(18);
        assertThat(serverIdToLeaderReplicaNumber.get(2)).isEqualTo(0);
        assertThat(serverIdToLeaderReplicaNumber.get(3)).isEqualTo(0);
        assertThat(serverIdToLeaderReplicaNumber.get(4)).isEqualTo(4);

        goal.optimize(clusterModel, new HashSet<>());

        serverIdToLeaderReplicaNumber = getServerIdToLeaderReplicaNumber();
        assertThat(serverIdToLeaderReplicaNumber.get(0)).isEqualTo(9);
        assertThat(serverIdToLeaderReplicaNumber.get(1)).isEqualTo(11);
        assertThat(serverIdToLeaderReplicaNumber.get(2)).isEqualTo(9);
        assertThat(serverIdToLeaderReplicaNumber.get(3)).isEqualTo(11);
        assertThat(serverIdToLeaderReplicaNumber.get(4)).isEqualTo(0);
    }

    private Map<Integer, Integer> getServerIdToLeaderReplicaNumber() {
        Map<Integer, Integer> idToLeaderReplicaNumber = new HashMap<>();
        for (ServerModel server : servers) {
            idToLeaderReplicaNumber.put(server.id(), server.leaderReplicas().size());
        }
        return idToLeaderReplicaNumber;
    }
}

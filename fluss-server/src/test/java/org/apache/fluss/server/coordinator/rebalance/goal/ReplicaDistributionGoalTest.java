/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModelStats;
import org.apache.fluss.server.coordinator.rebalance.model.ServerModel;
import org.apache.fluss.server.coordinator.rebalance.model.StatisticType;

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

/** Test for {@link ReplicaDistributionGoal}. */
public class ReplicaDistributionGoalTest {
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
        ReplicaDistributionGoal goal = new ReplicaDistributionGoal();
        ClusterModel clusterModel = new ClusterModel(servers);
        TableBucket t1b0 = new TableBucket(1, 0);
        TableBucket t1b1 = new TableBucket(1, 1);

        addBucket(clusterModel, t1b0, Arrays.asList(0, 1, 3));
        addBucket(clusterModel, t1b1, Arrays.asList(0, 1, 2));
        for (int i = 0; i < 11; i++) {
            addBucket(clusterModel, new TableBucket(2, i), Arrays.asList(0, 1));
        }

        ClusterModelStats clusterStats = clusterModel.getClusterStats();
        Map<StatisticType, Number> replicaStats = clusterStats.replicaStats();
        assertThat(replicaStats.get(StatisticType.AVG)).isEqualTo(7.0);

        goal.optimize(clusterModel, new HashSet<>());

        Map<Integer, Integer> serverIdToReplicaNumber = getServerIdToReplicaNumber(clusterModel);
        assertThat(serverIdToReplicaNumber.get(0)).isEqualTo(8);
        assertThat(serverIdToReplicaNumber.get(1)).isEqualTo(8);
        assertThat(serverIdToReplicaNumber.get(2)).isEqualTo(6);
        assertThat(serverIdToReplicaNumber.get(3)).isEqualTo(6);
    }

    @Test
    void testDoOptimizeWithOfflineServer() {
        ServerModel server4 = new ServerModel(4, "rack0", true); // Offline
        servers.add(server4);

        ReplicaDistributionGoal goal = new ReplicaDistributionGoal();
        ClusterModel clusterModel = new ClusterModel(servers);
        TableBucket t1b0 = new TableBucket(1, 0);
        TableBucket t1b1 = new TableBucket(1, 1);

        // Total: 45 replicas. Server 4 is offline.
        addBucket(clusterModel, t1b0, Arrays.asList(0, 1, 3));
        addBucket(clusterModel, t1b1, Arrays.asList(0, 1, 2));
        for (int i = 0; i < 13; i++) {
            addBucket(clusterModel, new TableBucket(2, i), Arrays.asList(0, 1, 4));
        }

        goal.optimize(clusterModel, new HashSet<>());

        Map<Integer, Integer> serverIdToReplicaNumber = getServerIdToReplicaNumber(clusterModel);

        // 1. Ensure the offline server (Server 4) is completely empty.
        assertThat(serverIdToReplicaNumber.get(4)).isEqualTo(0);

        // 2. Skew check: Max load should drop from 15 to 13.
        // We check <= 13 because placement constraints prevent a perfect 11/12 split.
        for (int i = 0; i < 4; i++) {
            assertThat(serverIdToReplicaNumber.get(i))
                    .as("Server " + i + " load is too high")
                    .isLessThanOrEqualTo(13);
        }

        // 3. Consistency check: Total replica count must still be 45.
        int total = serverIdToReplicaNumber.values().stream().mapToInt(Integer::intValue).sum();
        assertThat(total).isEqualTo(45);
    }

    private Map<Integer, Integer> getServerIdToReplicaNumber(ClusterModel clusterModel) {
        Map<Integer, Integer> idToReplicaNumber = new HashMap<>();
        for (ServerModel server : clusterModel.servers()) {
            idToReplicaNumber.put(server.id(), server.replicas().size());
        }
        return idToReplicaNumber;
    }
}

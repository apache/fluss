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

import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.fluss.server.coordinator.rebalance.RebalanceTestUtils.addBucket;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

/** Tests for the {@link ClusterModelStats}. */
public class ClusterModelStatsTest {
    private SortedSet<ServerModel> servers;

    @BeforeEach
    public void setup() {
        servers = new TreeSet<>();
        ServerModel server0 = new ServerModel(0, "rack0", false);
        ServerModel server1 = new ServerModel(1, "rack1", false);
        servers.add(server0);
        servers.add(server1);
    }

    @Test
    void testPopulate() {
        ClusterModel clusterModel = new ClusterModel(servers);
        addBucket(clusterModel, new TableBucket(0, 0), Arrays.asList(0, 1));
        addBucket(clusterModel, new TableBucket(1, 0), Arrays.asList(0, 1));
        addBucket(clusterModel, new TableBucket(2, 0), Arrays.asList(0, 1));

        ClusterModelStats clusterModelStats = clusterModel.getClusterStats();
        assertThat(clusterModelStats.numServers()).isEqualTo(2);
        assertThat(clusterModelStats.numReplicasInCluster()).isEqualTo(6);

        assertThat(clusterModelStats.replicaStats().get(StatisticType.AVG).doubleValue())
                .isEqualTo(3.0);
        assertThat(clusterModelStats.replicaStats().get(StatisticType.MAX).intValue()).isEqualTo(3);
        assertThat(clusterModelStats.replicaStats().get(StatisticType.MIN).intValue()).isEqualTo(3);
        assertThat(clusterModelStats.replicaStats().get(StatisticType.ST_DEV).doubleValue())
                .isEqualTo(0.0);

        assertThat(clusterModelStats.leaderReplicaStats().get(StatisticType.AVG).doubleValue())
                .isEqualTo(1.5);
        assertThat(clusterModelStats.leaderReplicaStats().get(StatisticType.MAX).intValue())
                .isEqualTo(3);
        assertThat(clusterModelStats.leaderReplicaStats().get(StatisticType.MIN).intValue())
                .isEqualTo(0);
        assertThat(clusterModelStats.leaderReplicaStats().get(StatisticType.ST_DEV).doubleValue())
                .isEqualTo(1.5);
    }

    @Test
    void testEmptyCluster() {
        ClusterModel clusterModel = new ClusterModel(new TreeSet<>());
        ClusterModelStats clusterModelStats = clusterModel.getClusterStats();
        assertThat(clusterModelStats.numServers()).isEqualTo(0);
        assertThat(clusterModelStats.numReplicasInCluster()).isEqualTo(0);
    }

    @Test
    void testSkewedCluster() {
        SortedSet<ServerModel> skewedServers = new TreeSet<>();
        ServerModel server0 = new ServerModel(0, "rack0", false);
        ServerModel server1 = new ServerModel(1, "rack1", false);
        skewedServers.add(server0);
        skewedServers.add(server1);

        ClusterModel clusterModel = new ClusterModel(skewedServers);
        // Server 0 has 10 replicas, Server 1 has 0 replicas
        for (int i = 0; i < 10; i++) {
            addBucket(clusterModel, new TableBucket(i, 0), Arrays.asList(0));
        }

        ClusterModelStats clusterModelStats = clusterModel.getClusterStats();
        assertThat(clusterModelStats.numServers()).isEqualTo(2);
        assertThat(clusterModelStats.numReplicasInCluster()).isEqualTo(10);

        // Avg = 5.0
        assertThat(clusterModelStats.replicaStats().get(StatisticType.AVG).doubleValue())
                .isEqualTo(5.0);
        // Max = 10
        assertThat(clusterModelStats.replicaStats().get(StatisticType.MAX).intValue())
                .isEqualTo(10);
        // Min = 0
        assertThat(clusterModelStats.replicaStats().get(StatisticType.MIN).intValue()).isEqualTo(0);
        // StDev = sqrt(((10-5)^2 + (0-5)^2) / 2) = sqrt((25 + 25) / 2) = sqrt(25) = 5.0
        assertThat(clusterModelStats.replicaStats().get(StatisticType.ST_DEV).doubleValue())
                .isCloseTo(5.0, offset(0.001));
    }
}

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
import org.apache.fluss.server.coordinator.rebalance.model.BucketModel;
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModel;
import org.apache.fluss.server.coordinator.rebalance.model.ReplicaModel;
import org.apache.fluss.server.coordinator.rebalance.model.ServerModel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PreferredLeaderElectionGoal}. */
public class PreferredLeaderElectionGoalTest {
    private SortedSet<ServerModel> servers;

    @BeforeEach
    public void setup() {
        servers = new TreeSet<>();
        ServerModel server0 = new ServerModel(0, "rack0", true);
        ServerModel server1 = new ServerModel(1, "rack1", true);
        ServerModel server2 = new ServerModel(2, "rack2", true);
        servers.add(server0);
        servers.add(server1);
        servers.add(server2);
    }

    @Test
    void testDoOptimize() {
        PreferredLeaderElectionGoal goal = new PreferredLeaderElectionGoal();
        ClusterModel clusterModel = new ClusterModel(servers);
        TableBucket t1b0 = new TableBucket(1, 0);
        TableBucket t1b1 = new TableBucket(1, 1);
        TableBucket t1b2 = new TableBucket(1, 2);
        // init clusterModel with three buckets, and the leader of two buckets is not in preferred
        // leader.
        // before optimize:
        // t1b0: assignment: 0, 1, 2, preferred leader: 0, current leader: 0
        // t1b1: assignment: 1, 0, 2, preferred leader: 1, current leader: 2
        // t1b2: assignment: 2, 0, 1, preferred leader: 2, current leader: 0
        clusterModel.createReplica(0, t1b0, 0, true);
        clusterModel.createReplica(1, t1b0, 1, false);
        clusterModel.createReplica(2, t1b0, 2, false);

        clusterModel.createReplica(1, t1b1, 0, false);
        clusterModel.createReplica(0, t1b1, 1, false);
        clusterModel.createReplica(2, t1b1, 2, true);

        clusterModel.createReplica(2, t1b2, 0, false);
        clusterModel.createReplica(0, t1b2, 1, true);
        clusterModel.createReplica(1, t1b2, 2, false);

        assertThat(clusterModel.bucket(t1b0))
                .isNotNull()
                .extracting(BucketModel::leader)
                .isNotNull()
                .extracting(ReplicaModel::server)
                .extracting(ServerModel::id)
                .isEqualTo(0);

        assertThat(clusterModel.bucket(t1b1))
                .isNotNull()
                .extracting(BucketModel::leader)
                .isNotNull()
                .extracting(ReplicaModel::server)
                .extracting(ServerModel::id)
                .isEqualTo(2);

        assertThat(clusterModel.bucket(t1b2))
                .isNotNull()
                .extracting(BucketModel::leader)
                .isNotNull()
                .extracting(ReplicaModel::server)
                .extracting(ServerModel::id)
                .isEqualTo(0);

        // do optimize.
        goal.optimize(clusterModel, new HashSet<>());

        // check optimized result:
        // after optimize:
        // t1b0: assignment: 0, 1, 2, preferred leader: 0, current leader: 0
        // t1b1: assignment: 1, 0, 2, preferred leader: 1, current leader: 1
        // t1b2: assignment: 2, 0, 1, preferred leader: 2, current leader: 2
        assertThat(clusterModel.bucket(t1b0))
                .isNotNull()
                .extracting(BucketModel::leader)
                .isNotNull()
                .extracting(ReplicaModel::server)
                .extracting(ServerModel::id)
                .isEqualTo(0);

        assertThat(clusterModel.bucket(t1b1))
                .isNotNull()
                .extracting(BucketModel::leader)
                .isNotNull()
                .extracting(ReplicaModel::server)
                .extracting(ServerModel::id)
                .isEqualTo(1);

        assertThat(clusterModel.bucket(t1b2))
                .isNotNull()
                .extracting(BucketModel::leader)
                .isNotNull()
                .extracting(ReplicaModel::server)
                .extracting(ServerModel::id)
                .isEqualTo(2);
    }
}

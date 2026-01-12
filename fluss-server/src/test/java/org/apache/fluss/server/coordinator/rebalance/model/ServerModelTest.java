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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ServerModel}. */
public class ServerModelTest {

    @Test
    void testServerModel() {
        ServerModel serverModel = new ServerModel(0, "rack0", false);
        assertThat(serverModel.id()).isEqualTo(0);
        assertThat(serverModel.rack()).isEqualTo("rack0");
        assertThat(serverModel.isOfflineTagged()).isFalse();

        // put some replicas.
        TableBucket t1b0 = new TableBucket(1L, 0);
        TableBucket t1b1 = new TableBucket(1L, 1);
        TableBucket t1b2 = new TableBucket(1L, 2);
        TableBucket t2b0 = new TableBucket(2L, 0);
        TableBucket t3p0b0 = new TableBucket(3L, 0L, 0);
        TableBucket t3p0b1 = new TableBucket(3L, 0L, 1);
        TableBucket t3p1b0 = new TableBucket(3L, 1L, 0);
        serverModel.putReplica(t1b0, new ReplicaModel(t1b0, serverModel, true));
        serverModel.putReplica(t1b1, new ReplicaModel(t1b1, serverModel, false));
        serverModel.putReplica(t1b2, new ReplicaModel(t1b2, serverModel, false));
        serverModel.putReplica(t2b0, new ReplicaModel(t2b0, serverModel, true));
        serverModel.putReplica(t3p0b0, new ReplicaModel(t3p0b0, serverModel, true));
        serverModel.putReplica(t3p0b1, new ReplicaModel(t3p0b1, serverModel, false));
        serverModel.putReplica(t3p1b0, new ReplicaModel(t3p1b0, serverModel, false));

        assertThat(serverModel.replicas()).hasSize(7);
        assertThat(serverModel.numLeaderReplicas()).isEqualTo(3);
        assertThat(serverModel.tables()).containsExactly(1L, 2L, 3L);

        // make t1b0 as follower and make t1b1 as leader.
        assertThat(serverModel.replica(t1b0).isLeader()).isTrue();
        assertThat(serverModel.replica(t1b1).isLeader()).isFalse();
        serverModel.makeFollower(t1b0);
        serverModel.makeLeader(t1b1);
        assertThat(serverModel.replica(t1b0).isLeader()).isFalse();
        assertThat(serverModel.replica(t1b1).isLeader()).isTrue();

        // make t3p0b0 as follower and make t3p0b1 as leader.
        assertThat(serverModel.replica(t3p0b0).isLeader()).isTrue();
        assertThat(serverModel.replica(t3p0b1).isLeader()).isFalse();
        serverModel.makeFollower(t3p0b0);
        serverModel.makeLeader(t3p0b1);
        assertThat(serverModel.replica(t3p0b0).isLeader()).isFalse();
        assertThat(serverModel.replica(t3p0b1).isLeader()).isTrue();

        // remove replica t2b0 and t3p1b0.
        serverModel.removeReplica(t2b0);
        serverModel.removeReplica(t3p1b0);
        assertThat(serverModel.replicas()).hasSize(5);
        assertThat(serverModel.numLeaderReplicas()).isEqualTo(2);
        assertThat(serverModel.tables()).containsExactly(1L, 3L);
    }

    @Test
    void testToString() {
        ServerModel serverModel = new ServerModel(0, "rack0", true);
        assertThat(serverModel.toString())
                .isEqualTo("ServerModel[id=0,rack=rack0,isAlive=true,replicaCount=0]");

        serverModel.putReplica(
                new TableBucket(1L, 0),
                new ReplicaModel(new TableBucket(1L, 0), serverModel, false));
        assertThat(serverModel.toString())
                .isEqualTo("ServerModel[id=0,rack=rack0,isAlive=true,replicaCount=1]");
    }

    @Test
    void testEquals() {
        ServerModel serverModel1 = new ServerModel(0, "rack0", true);
        ServerModel serverModel2 = new ServerModel(0, "rack0", true);
        // Same ID and same metadata -> Equal
        assertThat(serverModel1).isEqualTo(serverModel2);

        ServerModel serverModel3 = new ServerModel(0, "rack1", false);
        // Same ID but different rack/status -> Still Equal (as requested by maintainer)
        assertThat(serverModel1).isEqualTo(serverModel3);

        ServerModel serverModel4 = new ServerModel(1, "rack0", true);
        // Different ID -> Not Equal
        assertThat(serverModel1).isNotEqualTo(serverModel4);
    }

    @Test
    void testHashCode() {
        ServerModel serverModel1 = new ServerModel(0, "rack0", true);
        ServerModel serverModel2 = new ServerModel(0, "rack1", false);
        // Same ID must result in same HashCode
        assertThat(serverModel1.hashCode()).isEqualTo(serverModel2.hashCode());

        ServerModel serverModel3 = new ServerModel(1, "rack0", true);
        // Different ID results in different HashCode
        assertThat(serverModel1.hashCode()).isNotEqualTo(serverModel3.hashCode());
    }

    @Test
    void testCompareTo() {
        // order by server Id.
        ServerModel serverModel1 = new ServerModel(0, "rack0", true);
        ServerModel serverModel2 = new ServerModel(1, "rack1", true);
        assertThat(serverModel1.compareTo(serverModel2)).isEqualTo(-1);
    }
}

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

/** Test for {@link ReplicaModel}. */
public class ReplicaModelTest {

    @Test
    void testReplicaModel() {
        ReplicaModel replicaModel =
                new ReplicaModel(new TableBucket(1L, 0), new ServerModel(1, "rack1", false), false);
        assertThat(replicaModel.tableBucket()).isEqualTo(new TableBucket(1L, 0));
        assertThat(replicaModel.isLeader()).isFalse();
        assertThat(replicaModel.server().id()).isEqualTo(1);
        assertThat(replicaModel.originalServer().id()).isEqualTo(1);

        // make this replica as leader.
        replicaModel.makeLeader();
        assertThat(replicaModel.isLeader()).isTrue();

        // make as follower again.
        replicaModel.makeFollower();
        assertThat(replicaModel.isLeader()).isFalse();

        // set server.
        replicaModel.setServer(new ServerModel(2, "rack2", false));
        assertThat(replicaModel.server().id()).isEqualTo(2);
        assertThat(replicaModel.originalServer().id()).isEqualTo(1);
    }

    @Test
    void testToString() {
        ReplicaModel replicaModel =
                new ReplicaModel(new TableBucket(1L, 0), new ServerModel(1, "rack1", false), false);
        assertThat(replicaModel.toString())
                .isEqualTo(
                        "ReplicaModel[TableBucket=TableBucket{tableId=1, bucket=0},isLeader=false,rack=rack1,server=1,originalServer=1]");

        replicaModel.makeLeader();
        replicaModel.setServer(new ServerModel(2, "rack2", false));
        assertThat(replicaModel.toString())
                .isEqualTo(
                        "ReplicaModel[TableBucket=TableBucket{tableId=1, bucket=0},isLeader=true,rack=rack2,server=2,originalServer=1]");
    }

    @Test
    void testEquals() {
        ReplicaModel replicaModel1 =
                new ReplicaModel(new TableBucket(1L, 0), new ServerModel(1, "rack1", false), false);
        ReplicaModel replicaModel2 =
                new ReplicaModel(new TableBucket(1L, 0), new ServerModel(1, "rack1", false), false);
        assertThat(replicaModel1).isEqualTo(replicaModel2);

        replicaModel1.setServer(new ServerModel(2, "rack2", false));
        assertThat(replicaModel1).isEqualTo(replicaModel2);

        replicaModel1.setLeadership(true);
        assertThat(replicaModel1).isEqualTo(replicaModel2);
    }
}

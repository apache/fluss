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

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link BucketModel}. */
public class BucketModelTest {

    @Test
    void testBucketModel() {
        BucketModel bucketModel =
                new BucketModel(
                        new TableBucket(1L, 0),
                        Collections.singleton(new ServerModel(0, "rack0", false)));
        assertThat(bucketModel.tableBucket()).isEqualTo(new TableBucket(1L, 0));
        assertThat(bucketModel.leader()).isNull();
        assertThat(bucketModel.bucketServers()).isEmpty();
        assertThat(bucketModel.replicas()).isEmpty();
        assertThat(bucketModel.canAssignReplicaToServer(new ServerModel(0, "rack0", false)))
                .isFalse();

        // add a leader replica.
        ReplicaModel replicaModel1 =
                new ReplicaModel(new TableBucket(1L, 0), new ServerModel(1, "rack1", true), true);
        bucketModel.addLeader(replicaModel1, 0);
        assertThat(bucketModel.leader()).isNotNull();
        assertThat(bucketModel.leader().tableBucket()).isEqualTo(new TableBucket(1L, 0));

        // add a leader replica again will throw exception.
        assertThatThrownBy(
                        () ->
                                bucketModel.addLeader(
                                        new ReplicaModel(
                                                new TableBucket(1L, 0),
                                                new ServerModel(1, "rack1", false),
                                                true),
                                        0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Bucket TableBucket{tableId=1, bucket=0} already has a leader replica "
                                + "ReplicaModel[TableBucket=TableBucket{tableId=1, bucket=0},isLeader=true,rack=rack1,server=1,originalServer=1]. "
                                + "Cannot add a new leader replica ReplicaModel[TableBucket=TableBucket{tableId=1, bucket=0},isLeader=true,rack=rack1,server=1,originalServer=1].");

        // add a follower replica.
        ReplicaModel replicaModel2 =
                new ReplicaModel(new TableBucket(1L, 0), new ServerModel(2, "rack2", false), false);
        bucketModel.addFollower(replicaModel2, 1);
        ReplicaModel replicaModel3 =
                new ReplicaModel(new TableBucket(1L, 0), new ServerModel(3, "rack3", false), false);
        bucketModel.addFollower(replicaModel3, 2);

        assertThat(bucketModel.replicas()).contains(replicaModel1, replicaModel2, replicaModel3);
        assertThat(bucketModel.replica(1)).isEqualTo(replicaModel1);
        assertThat(bucketModel.replica(2)).isEqualTo(replicaModel2);
        assertThat(bucketModel.replica(3)).isEqualTo(replicaModel3);

        // change 2 to leader.
        bucketModel.relocateLeadership(replicaModel2);
        assertThat(bucketModel.leader()).isEqualTo(replicaModel2);
    }
}

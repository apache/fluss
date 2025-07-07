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

package org.apache.fluss.server.entity;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import java.util.List;

/** The table bucket data of {@link NotifyLeaderAndIsrRequest}. */
public final class NotifyLeaderAndIsrData {
    private final PhysicalTablePath physicalTablePath;
    private final TableBucket tableBucket;
    private final List<Integer> replicas;
    private final LeaderAndIsr leaderAndIsr;

    public NotifyLeaderAndIsrData(
            PhysicalTablePath physicalTablePath,
            TableBucket tableBucket,
            List<Integer> replicas,
            LeaderAndIsr leaderAndIsr) {
        this.physicalTablePath = physicalTablePath;
        this.tableBucket = tableBucket;
        this.replicas = replicas;
        this.leaderAndIsr = leaderAndIsr;
    }

    public PhysicalTablePath getPhysicalTablePath() {
        return physicalTablePath;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public int getLeader() {
        return leaderAndIsr.leader();
    }

    public int getLeaderEpoch() {
        return leaderAndIsr.leaderEpoch();
    }

    public List<Integer> getReplicas() {
        return replicas;
    }

    public int[] getReplicasArray() {
        return replicas.stream().mapToInt(Integer::intValue).toArray();
    }

    public int getCoordinatorEpoch() {
        return leaderAndIsr.coordinatorEpoch();
    }

    public int getBucketEpoch() {
        return leaderAndIsr.bucketEpoch();
    }

    public List<Integer> getIsr() {
        return leaderAndIsr.isr();
    }

    public int[] getIsrArray() {
        return leaderAndIsr.isr().stream().mapToInt(Integer::intValue).toArray();
    }

    public LeaderAndIsr getLeaderAndIsr() {
        return leaderAndIsr;
    }
}

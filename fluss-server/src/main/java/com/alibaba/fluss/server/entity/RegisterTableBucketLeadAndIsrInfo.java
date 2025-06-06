/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.entity;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;

import javax.annotation.Nullable;

import java.util.List;

/** The data for register LeaderAndIsr in zk. */
public class RegisterTableBucketLeadAndIsrInfo {
    private final TableBucket tableBucket;
    private final LeaderAndIsr leaderAndIsr;
    @Nullable private final String partitionName;
    private final List<Integer> liveReplicas;

    public RegisterTableBucketLeadAndIsrInfo(
            TableBucket tableBucket,
            LeaderAndIsr leaderAndIsr,
            @Nullable String partitionName,
            List<Integer> liveReplicas) {
        this.tableBucket = tableBucket;
        this.leaderAndIsr = leaderAndIsr;
        this.partitionName = partitionName;
        this.liveReplicas = liveReplicas;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public LeaderAndIsr getLeaderAndIsr() {
        return leaderAndIsr;
    }

    @Nullable
    public String getPartitionName() {
        return partitionName;
    }

    public List<Integer> getLiveReplicas() {
        return liveReplicas;
    }
}

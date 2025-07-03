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

package com.alibaba.fluss.server.entity;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The collection of {@link com.alibaba.fluss.server.entity.RegisterTableBucketLeadAndIsrInfo} for a
 * specific table or partition.
 */
public class BatchRegisterLeadAndIsr {
    private final long tableId;
    @Nullable private final Long partitionId;
    private final List<RegisterTableBucketLeadAndIsrInfo> registerList;

    public BatchRegisterLeadAndIsr(long tableId, @Nullable Long partitionId) {
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.registerList = new ArrayList<>();
    }

    public void add(
            TableBucket tableBucket,
            LeaderAndIsr leaderAndIsr,
            @Nullable String partitionName,
            List<Integer> liveReplicas) {
        // check the tableBucket has the same tableId and partitionId.
        // add it to registerList.
        if (tableBucket.getTableId() == tableId
                && Objects.equals(partitionId, tableBucket.getPartitionId())) {
            registerList.add(
                    new RegisterTableBucketLeadAndIsrInfo(
                            tableBucket, leaderAndIsr, partitionName, liveReplicas));
        } else {
            throw new IllegalArgumentException(
                    "Try to add a bucket with different tableId or partitionId in collection when try to batch register to Zookeeper."
                            + "batch tableId="
                            + tableId
                            + " batch partitionId:"
                            + partitionId
                            + " current tableId="
                            + tableBucket.getTableId()
                            + " current partitionId="
                            + tableBucket.getPartitionId());
        }
    }

    public List<RegisterTableBucketLeadAndIsrInfo> getRegisterList() {
        return registerList;
    }
}

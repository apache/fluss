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
import com.alibaba.fluss.rpc.messages.StopReplicaRequest;

/** The data to stop replica for request {@link StopReplicaRequest}. */
public class StopReplicaData {
    private final TableBucket tableBucket;
    private final boolean delete;
    private final int coordinatorEpoch;
    private final int leaderEpoch;

    public StopReplicaData(
            TableBucket tableBucket, boolean delete, int coordinatorEpoch, int leaderEpoch) {
        this.tableBucket = tableBucket;
        this.delete = delete;
        this.coordinatorEpoch = coordinatorEpoch;
        this.leaderEpoch = leaderEpoch;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public boolean isDelete() {
        return delete;
    }

    public int getCoordinatorEpoch() {
        return coordinatorEpoch;
    }

    public int getLeaderEpoch() {
        return leaderEpoch;
    }
}

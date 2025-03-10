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
import com.alibaba.fluss.metadata.TableBucketReplica;
import com.alibaba.fluss.rpc.entity.ResultForBucket;
import com.alibaba.fluss.rpc.messages.StopReplicaRequest;
import com.alibaba.fluss.rpc.protocol.ApiError;

/**
 * The result of delete bucket replica for a bucket replica in request {@link StopReplicaRequest}
 * with delete = true.
 */
public class DeleteReplicaResultForBucket extends ResultForBucket {

    private final int replica;

    public DeleteReplicaResultForBucket(TableBucket tableBucket, int replica) {
        this(tableBucket, replica, ApiError.NONE);
    }

    public DeleteReplicaResultForBucket(TableBucket tableBucket, int replica, ApiError error) {
        super(tableBucket, error);
        this.replica = replica;
    }

    public int getReplica() {
        return replica;
    }

    public TableBucketReplica getTableBucketReplica() {
        return new TableBucketReplica(tableBucket, replica);
    }
}

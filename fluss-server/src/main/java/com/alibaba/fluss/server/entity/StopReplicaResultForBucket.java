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
import com.alibaba.fluss.rpc.entity.ResultForBucket;
import com.alibaba.fluss.rpc.messages.StopReplicaRequest;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.rpc.protocol.Errors;

import javax.annotation.Nullable;

/** Stop replica result for bucket of request {@link StopReplicaRequest}. */
public class StopReplicaResultForBucket extends ResultForBucket {
    public StopReplicaResultForBucket(TableBucket tableBucket) {
        super(tableBucket);
    }

    public StopReplicaResultForBucket(
            TableBucket tableBucket, Errors errorCode, @Nullable String errorMessage) {
        super(tableBucket, new ApiError(errorCode, errorMessage));
    }

    public StopReplicaResultForBucket(TableBucket tableBucket, ApiError error) {
        super(tableBucket, error);
    }
}

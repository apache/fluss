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
import com.alibaba.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import com.alibaba.fluss.rpc.protocol.ApiError;

/**
 * The result of notify bucket leader and lsr for a bucket in request {@link
 * NotifyLeaderAndIsrRequest} with delete = true.
 */
public class NotifyLeaderAndIsrResultForBucket extends ResultForBucket {

    public NotifyLeaderAndIsrResultForBucket(TableBucket tableBucket) {
        super(tableBucket);
    }

    public NotifyLeaderAndIsrResultForBucket(TableBucket tableBucket, ApiError error) {
        super(tableBucket, error);
    }
}

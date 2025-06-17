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
import com.alibaba.fluss.rpc.messages.AdjustIsrRequest;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;

/** The bucket result of each table bucket for {@link AdjustIsrRequest}. */
public class AdjustIsrResultForBucket extends ResultForBucket {
    LeaderAndIsr leaderAndIsr;

    public AdjustIsrResultForBucket(TableBucket tableBucket, LeaderAndIsr leaderAndIsr) {
        this(tableBucket, leaderAndIsr, ApiError.NONE);
    }

    public AdjustIsrResultForBucket(TableBucket tableBucket, ApiError error) {
        this(tableBucket, new LeaderAndIsr(-1, 0), error);
    }

    private AdjustIsrResultForBucket(
            TableBucket tableBucket, LeaderAndIsr leaderAndIsr, ApiError error) {
        super(tableBucket, error);
        this.leaderAndIsr = leaderAndIsr;
    }

    public LeaderAndIsr leaderAndIsr() {
        return leaderAndIsr;
    }
}

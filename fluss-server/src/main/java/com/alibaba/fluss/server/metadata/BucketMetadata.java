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

package com.alibaba.fluss.server.metadata;

import java.util.List;

/** This entity used to describe the bucket metadata. */
public class BucketMetadata {
    private final int bucketId;
    private final int leaderId;
    private final int leaderEpoch;
    private final List<Integer> replicas;

    public BucketMetadata(int bucketId, int leaderId, int leaderEpoch, List<Integer> replicas) {
        this.bucketId = bucketId;
        this.leaderId = leaderId;
        this.leaderEpoch = leaderEpoch;
        this.replicas = replicas;
    }

    public int getBucketId() {
        return bucketId;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public int getLeaderEpoch() {
        return leaderEpoch;
    }

    public List<Integer> getReplicas() {
        return replicas;
    }
}

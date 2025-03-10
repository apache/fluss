/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.client.write;

import com.alibaba.fluss.cluster.Cluster;

import javax.annotation.Nullable;

/**
 * An abstract bucket assigner to assign a bucket dynamically. The bucket id determined during
 * sending to Fluss cluster by the status of cluster and write batch accumulation.
 */
abstract class DynamicBucketAssigner implements BucketAssigner {

    @Override
    public int assignBucket(@Nullable byte[] bucketKey, Cluster cluster) {
        return assignBucket(cluster);
    }

    public abstract int assignBucket(Cluster cluster);
}

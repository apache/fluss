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

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.cluster.Cluster;

import javax.annotation.Nullable;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * An abstract bucket assigner to assign a row to a bucket statically. Different from {@link
 * DynamicBucketAssigner}, it won't be affected by status of cluster and others during sending.
 */
@Internal
abstract class StaticBucketAssigner implements BucketAssigner {

    protected abstract int assignBucket(byte[] bucketKey);

    @Override
    public int assignBucket(@Nullable byte[] bucketKey, Cluster cluster) {
        checkNotNull(bucketKey);
        return assignBucket(bucketKey);
    }

    @Override
    public boolean abortIfBatchFull() {
        return false;
    }

    @Override
    public void onNewBatch(Cluster cluster, int prevBucketId) {
        // do nothing
    }
}

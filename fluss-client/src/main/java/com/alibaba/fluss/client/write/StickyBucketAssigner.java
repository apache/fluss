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

package com.alibaba.fluss.client.write;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.cluster.BucketLocation;
import com.alibaba.fluss.cluster.Cluster;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.utils.MathUtils;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The bucket assigner with sticky strategy. The assigned bucket id maybe changed only if one new
 * batch created in record accumulator. Otherwise, we will always return the same bucket id.
 */
@Internal
public class StickyBucketAssigner extends DynamicBucketAssigner {

    private final PhysicalTablePath physicalTablePath;
    private final AtomicInteger currentBucketId;

    public StickyBucketAssigner(PhysicalTablePath physicalTablePath) {
        this.physicalTablePath = physicalTablePath;
        this.currentBucketId = new AtomicInteger(-1);
    }

    @Override
    public int assignBucket(Cluster cluster) {
        int bucketId = currentBucketId.get();
        if (bucketId < 0) {
            // initialize the currentBucketId
            return nextBucket(cluster, bucketId);
        }
        return bucketId;
    }

    @Override
    public boolean abortIfBatchFull() {
        return true;
    }

    @Override
    public void onNewBatch(Cluster cluster, int prevBucketId) {
        nextBucket(cluster, prevBucketId);
    }

    private int nextBucket(Cluster cluster, int preBucketId) {
        int oldBucket = currentBucketId.get();
        int newBucket = oldBucket;
        // Check that the current sticky bucket for the table is either not set or that the
        // bucket that triggered the new batch matches the sticky bucket that needs to be
        // changed.
        if (oldBucket < 0 || oldBucket == preBucketId) {
            List<BucketLocation> availableBuckets =
                    cluster.getAvailableBucketsForPhysicalTablePath(physicalTablePath);
            if (availableBuckets.isEmpty()) {
                int random = MathUtils.toPositive(ThreadLocalRandom.current().nextInt());
                newBucket = random % cluster.getBucketCount(physicalTablePath.getTablePath());
            } else if (availableBuckets.size() == 1) {
                newBucket = availableBuckets.get(0).getBucketId();
            } else {
                while (newBucket < 0 || newBucket == oldBucket) {
                    int random = MathUtils.toPositive(ThreadLocalRandom.current().nextInt());
                    newBucket =
                            availableBuckets.get(random % availableBuckets.size()).getBucketId();
                }
            }

            // Only change the sticky partition if it is null or prevPartition matches the current
            // sticky partition.
            if (oldBucket < 0) {
                currentBucketId.set(newBucket);
            } else {
                currentBucketId.compareAndSet(preBucketId, newBucket);
            }
            return currentBucketId.get();
        }

        return currentBucketId.get();
    }
}

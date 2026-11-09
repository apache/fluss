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

package org.apache.fluss.client.write;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.utils.MathUtils;

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
    public int assignBucket(Cluster cluster, int bucketCount) {
        int bucketId = currentBucketId.get();
        if (bucketId < 0 || bucketId >= bucketCount) {
            // initialize the currentBucketId
            return nextBucket(cluster, bucketCount, bucketId);
        }
        return bucketId;
    }

    @Override
    public boolean abortIfBatchFull() {
        return true;
    }

    @Override
    public void onNewBatch(Cluster cluster, int bucketCount, int prevBucketId) {
        nextBucket(cluster, bucketCount, prevBucketId);
    }

    private int nextBucket(Cluster cluster, int bucketCount, int preBucketId) {
        int oldBucket = currentBucketId.get();
        int newBucket = oldBucket;
        // Check that the current sticky bucket for the table is either not set or that the
        // bucket that triggered the new batch matches the sticky bucket that needs to be
        // changed.
        if (oldBucket < 0 || oldBucket >= bucketCount || oldBucket == preBucketId) {
            List<BucketLocation> availableBuckets =
                    cluster.getAvailableBucketsForPhysicalTablePath(physicalTablePath);
            int random = MathUtils.toPositive(ThreadLocalRandom.current().nextInt());
            // Use a bounded search: metadata may contain buckets outside the temporary count,
            // or the old bucket may be the only available one within the current count.
            for (int i = 0; i < availableBuckets.size(); i++) {
                int index = (random % availableBuckets.size() + i) % availableBuckets.size();
                int candidate = availableBuckets.get(index).getBucketId();
                if (candidate < bucketCount && candidate != oldBucket) {
                    newBucket = candidate;
                    break;
                }
            }
            if (availableBuckets.isEmpty() || newBucket < 0 || newBucket >= bucketCount) {
                newBucket = random % bucketCount;
            }

            // Only change the sticky partition if it is null or prevPartition matches the current
            // sticky partition.
            if (oldBucket < 0) {
                currentBucketId.set(newBucket);
            } else {
                currentBucketId.compareAndSet(oldBucket, newBucket);
            }
            return currentBucketId.get();
        }

        return currentBucketId.get();
    }
}

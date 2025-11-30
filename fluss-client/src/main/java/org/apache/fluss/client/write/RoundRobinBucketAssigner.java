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
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/** The bucket assigner use round-robin strategy. */
@Internal
public class RoundRobinBucketAssigner extends DynamicBucketAssigner {
    private final PhysicalTablePath physicalTablePath;
    private final int bucketNumber;
    private final AtomicInteger counter = new AtomicInteger(new Random().nextInt());

    public RoundRobinBucketAssigner(PhysicalTablePath physicalTablePath, int bucketNumber) {
        this.physicalTablePath = physicalTablePath;
        this.bucketNumber = bucketNumber;
    }

    @Override
    public int assignBucket(Cluster cluster) {
        int nextValue = counter.getAndIncrement();
        List<BucketLocation> bucketsForTable =
                cluster.getAvailableBucketsForPhysicalTablePath(physicalTablePath);
        if (!bucketsForTable.isEmpty()) {
            int bucket = MathUtils.toPositive(nextValue) % bucketsForTable.size();
            return bucketsForTable.get(bucket).getBucketId();
        } else {
            // no buckets are available, give a non-available bucket.
            return MathUtils.toPositive(nextValue) % bucketNumber;
        }
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

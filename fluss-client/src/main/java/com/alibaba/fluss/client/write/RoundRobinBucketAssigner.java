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
import com.alibaba.fluss.cluster.BucketLocation;
import com.alibaba.fluss.cluster.Cluster;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.utils.MathUtils;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/** The bucket assigner use round-robin strategy. */
@Internal
public class RoundRobinBucketAssigner extends DynamicBucketAssigner {
    private final PhysicalTablePath physicalTablePath;
    private final AtomicInteger counter = new AtomicInteger(new Random().nextInt());

    public RoundRobinBucketAssigner(PhysicalTablePath physicalTablePath) {
        this.physicalTablePath = physicalTablePath;
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
            return MathUtils.toPositive(nextValue)
                    % cluster.getBucketCount(physicalTablePath.getTablePath());
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

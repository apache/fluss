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

import org.apache.fluss.cluster.Cluster;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** A bucket assigner whose routing can be controlled by tests. */
class TestingBucketAssigner implements BucketAssigner {
    private volatile int bucketId;

    void setBucketId(int bucketId) {
        this.bucketId = bucketId;
    }

    @Override
    public int assignBucket(@Nullable byte[] bucketKey, Cluster cluster, int bucketCount) {
        int assignedBucket = bucketId;
        checkArgument(
                assignedBucket >= 0 && assignedBucket < bucketCount,
                "Bucket id %s is outside bucket count %s.",
                assignedBucket,
                bucketCount);
        return assignedBucket;
    }

    @Override
    public boolean abortIfBatchFull() {
        return false;
    }

    @Override
    public void onNewBatch(Cluster cluster, int bucketCount, int prevBucketId) {}
}

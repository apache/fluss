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

package org.apache.fluss.flink.source.lookup;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.utils.MathUtils;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Maps each Fluss bucket to the lookup subtasks that hold a full cache of that bucket.
 *
 * <p>With at least as many buckets as subtasks, a bucket has a single owner, {@code bucketId %
 * parallelism}. With fewer buckets, each subtask owns exactly one bucket, {@code subtaskId %
 * numBuckets}, and lookup keys for that bucket are distributed among its replica subtasks.
 */
@Internal
public class FullCacheBucketAssignment implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int numBuckets;

    /** Creates an assignment for a positive number of buckets. */
    public FullCacheBucketAssignment(int numBuckets) {
        checkArgument(numBuckets > 0, "numBuckets must be positive, but was %s.", numBuckets);
        this.numBuckets = numBuckets;
    }

    /**
     * Selects a lookup subtask that owns the given bucket.
     *
     * @param bucketId Fluss bucket containing the lookup key
     * @param keyHash hash of the lookup key, used to distribute keys among bucket replicas
     * @param parallelism number of lookup subtasks
     * @return the target subtask id
     */
    public int route(int bucketId, int keyHash, int parallelism) {
        checkBucketId(bucketId);
        checkParallelism(parallelism);
        if (numBuckets >= parallelism) {
            return bucketId % parallelism;
        }

        // Bucket owners are bucketId, bucketId + numBuckets, ... below parallelism.
        int replicaCount = (parallelism - 1 - bucketId) / numBuckets + 1;
        int replicaIndex = MathUtils.toPositive(keyHash) % replicaCount;
        return bucketId + replicaIndex * numBuckets;
    }

    /**
     * Returns all buckets cached by a lookup subtask.
     *
     * @param subtaskId lookup subtask id
     * @param parallelism number of lookup subtasks
     * @return immutable set of bucket ids
     */
    public Set<Integer> ownedBuckets(int subtaskId, int parallelism) {
        checkParallelism(parallelism);
        checkArgument(
                subtaskId >= 0 && subtaskId < parallelism,
                "subtaskId must be in [0, %s), but was %s.",
                parallelism,
                subtaskId);

        if (numBuckets < parallelism) {
            return Collections.singleton(subtaskId % numBuckets);
        }

        Set<Integer> buckets = new LinkedHashSet<>();
        for (long bucketId = subtaskId; bucketId < numBuckets; bucketId += parallelism) {
            buckets.add((int) bucketId);
        }
        return Collections.unmodifiableSet(buckets);
    }

    private void checkBucketId(int bucketId) {
        checkArgument(
                bucketId >= 0 && bucketId < numBuckets,
                "bucketId must be in [0, %s), but was %s.",
                numBuckets,
                bucketId);
    }

    private static void checkParallelism(int parallelism) {
        checkArgument(parallelism > 0, "parallelism must be positive, but was %s.", parallelism);
    }
}

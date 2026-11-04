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

import org.apache.fluss.flink.adapter.SupportsLookupCustomShuffleAdapter.InputDataPartitionerAdapter;

import org.apache.flink.table.data.RowData;

import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Bucket-affine input partitioner for a full lookup cache.
 *
 * <p>The delegated Fluss lookup partitioner is called with {@code numPartitions = numBuckets} to
 * obtain exactly the bucket id computed by the client-side bucket-key encoder and bucketing
 * function. Unlike remote and partial-cache lookups, the target lookup subtask is then selected
 * using {@link FullCacheBucketAssignment}, so it always owns that bucket.
 */
public class FullCacheLookupInputPartitioner implements InputDataPartitionerAdapter {

    private static final long serialVersionUID = 1L;

    private final FlussLookupInputPartitioner bucketPartitioner;
    private final int numBuckets;
    private final FullCacheBucketAssignment bucketAssignment;

    /** Creates a full-cache partitioner using the Fluss-compatible bucket partitioner. */
    public FullCacheLookupInputPartitioner(
            FlussLookupInputPartitioner bucketPartitioner, int numBuckets) {
        this.bucketPartitioner = checkNotNull(bucketPartitioner, "bucketPartitioner is null.");
        checkArgument(numBuckets > 0, "numBuckets must be positive, but was %s.", numBuckets);
        this.numBuckets = numBuckets;
        this.bucketAssignment = new FullCacheBucketAssignment(numBuckets);
    }

    @Override
    public int partition(RowData joinKeys, int numPartitions) {
        checkArgument(
                numPartitions > 0, "numPartitions must be positive, but was %s.", numPartitions);
        // Null keys do not match any dimension row; the left join still needs a target subtask.
        for (int i = 0; i < joinKeys.getArity(); i++) {
            if (joinKeys.isNullAt(i)) {
                return 0;
            }
        }

        int bucketId = bucketPartitioner.partition(joinKeys, numBuckets);
        int keyHash = numPartitions > numBuckets ? bucketPartitioner.hashLookupKey(joinKeys) : 0;
        return bucketAssignment.route(bucketId, keyHash, numPartitions);
    }

    /** Returns the buckets that the given lookup subtask must load into its full cache. */
    public Set<Integer> ownedBuckets(int subtaskId, int parallelism) {
        return bucketAssignment.ownedBuckets(subtaskId, parallelism);
    }

    @Override
    public boolean isDeterministic() {
        return bucketPartitioner.isDeterministic();
    }
}

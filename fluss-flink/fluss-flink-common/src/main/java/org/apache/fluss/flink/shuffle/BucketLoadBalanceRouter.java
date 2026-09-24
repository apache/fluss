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

package org.apache.fluss.flink.shuffle;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.utils.MathUtils;

import javax.annotation.Nullable;

import java.util.Arrays;

/**
 * Shuffle data by bucket key with even load distribution across all downstream subtasks.
 *
 * <p>Two callers share this router and must stay consistent:
 *
 * <ul>
 *   <li>The sink shuffle ({@code BucketLoadBalanceChannelComputer}) routes records to sink
 *       subtasks. It passes the encoded bucket key as both the bucket key and the hash key, so
 *       records with the same bucket key always land on the same subtask.
 *   <li>The lookup-join partitioner ({@code FlussLookupInputPartitioner}) routes probe rows to
 *       lookup subtasks. It passes the encoded bucket key for bucketing and the encoded lookup key
 *       for the intra-bucket slot hash, spreading different lookup keys of the same bucket across
 *       the bucket's subtask range.
 * </ul>
 *
 * <p>Routing algorithm: first compute the bucket ID via {@code BucketingFunction}, then select the
 * concrete subtask within the bucket's assigned slot range using the murmur hash of the hash key.
 * When the bucket count is evenly divisible by the channel count the hash is skipped entirely and
 * the result is simply {@code bucketId % numChannels}, matching the behaviour of the BUCKET
 * distribution mode.
 */
@Internal
public final class BucketLoadBalanceRouter {

    private final KeyEncoder bucketKeyEncoder;
    private final @Nullable KeyEncoder hashKeyEncoder;
    private final BucketingFunction bucketingFunction;
    private final int numBuckets;

    /**
     * Creates a router for the BUCKET_LOAD_BALANCE distribution strategy.
     *
     * @param bucketKeyEncoder encoder for the bucket key (determines bucket ID)
     * @param hashKeyEncoder encoder for the intra-bucket slot hash; may be {@code null} when the
     *     caller wants to use the bucket key itself as the hash key (e.g. sink shuffle)
     * @param bucketingFunction the bucketing function for computing the bucket ID
     * @param numBuckets total number of buckets
     */
    public BucketLoadBalanceRouter(
            KeyEncoder bucketKeyEncoder,
            @Nullable KeyEncoder hashKeyEncoder,
            BucketingFunction bucketingFunction,
            int numBuckets) {
        this.bucketKeyEncoder = bucketKeyEncoder;
        this.hashKeyEncoder = hashKeyEncoder;
        this.bucketingFunction = bucketingFunction;
        this.numBuckets = numBuckets;
    }

    /**
     * Routes a record to a downstream channel using bucket-ID + LCM-based slot assignment.
     *
     * <p>The record is first encoded by {@code bucketKeyEncoder} to obtain the bucket key. When
     * {@link #isEvenlyDivisible} holds, the result is simply {@code bucketId % numChannels} and the
     * hash key is never encoded. Otherwise the intra-bucket slot hash is computed from the hash key
     * (via {@code hashKeyEncoder} when present, otherwise the bucket key is reused).
     *
     * @param row the record to route
     * @param numChannels number of downstream channels (subtask parallelism)
     * @return channel index in {@code [0, numChannels)}
     */
    public int route(InternalRow row, int numChannels) {
        byte[] bucketKeyBytes = bucketKeyEncoder.encodeKey(row);
        int bucketId = bucketingFunction.bucketing(bucketKeyBytes, numBuckets);

        // Fast-path: when bucket count is an exact multiple of channel count, every bucket
        // maps to a single channel and no hash is needed.
        if (isEvenlyDivisible(numBuckets, numChannels)) {
            return bucketId % numChannels;
        }

        byte[] hashKeyBytes =
                hashKeyEncoder != null ? hashKeyEncoder.encodeKey(row) : bucketKeyBytes;

        int recordHash = MathUtils.murmurHash(Arrays.hashCode(hashKeyBytes));
        return selectSlot(bucketId, numBuckets, numChannels, recordHash);
    }

    private static int selectSlot(int bucketId, int numBuckets, int numChannels, int recordHash) {
        if (numChannels > numBuckets && numChannels % numBuckets == 0) {
            int candidateCount = numChannels / numBuckets;
            return (recordHash % candidateCount) * numBuckets + bucketId;
        }

        int gcd = greatestCommonDivisor(numBuckets, numChannels);
        int slotsPerBucket = numChannels / gcd;
        int slotsPerSubtask = numBuckets / gcd;
        int slotWithinBucket = recordHash % slotsPerBucket;
        long logicalSlot = (long) bucketId * slotsPerBucket + slotWithinBucket;
        return (int) (logicalSlot / slotsPerSubtask);
    }

    private static int greatestCommonDivisor(int first, int second) {
        while (second != 0) {
            int remainder = first % second;
            first = second;
            second = remainder;
        }
        return first;
    }

    private static boolean isEvenlyDivisible(int numBuckets, int numChannels) {
        return numBuckets >= numChannels && numBuckets % numChannels == 0;
    }
}

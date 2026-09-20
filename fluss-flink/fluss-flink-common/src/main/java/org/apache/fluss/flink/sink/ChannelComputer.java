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

package org.apache.fluss.flink.sink;

import java.io.Serializable;

/**
 * A utility class to compute which downstream channel a given record should be sent to before flink
 * sink.
 *
 * @param <T> type of record
 */
public interface ChannelComputer<T> extends Serializable {
    void setup(int numChannels);

    int channel(T record);

    /**
     * Determines whether partition name should be combined with bucket for sharding calculation.
     *
     * <p>When bucket number is not evenly divisible by channel count, using only bucket ID for
     * sharding can cause data skew. For example, with 3 buckets and 2 channels:
     *
     * <ul>
     *   <li>Channel 0: bucket 0, bucket 2 (from all partitions)
     *   <li>Channel 1: bucket 1 (from all partitions)
     * </ul>
     *
     * <p>By including partition name in the hash, buckets from different partitions are distributed
     * more evenly across channels.
     *
     * @param isPartitioned whether the table is partitioned
     * @param numBuckets number of buckets in the table
     * @param numChannels number of downstream channels (parallelism)
     * @return true if partition name should be included in sharding calculation
     */
    static boolean shouldCombinePartitionInSharding(
            boolean isPartitioned, int numBuckets, int numChannels) {
        return isPartitioned && numBuckets % numChannels != 0;
    }

    static int select(String partitionName, int bucket, int numChannels) {
        int startChannel = Math.abs(partitionName.hashCode()) % numChannels;
        return (startChannel + bucket) % numChannels;
    }

    static int select(int bucket, int numChannels) {
        return bucket % numChannels;
    }

    /**
     * Selects a channel using LCM-based logical slot assignment, ensuring even load distribution
     * across all channels regardless of whether the bucket count and channel count are evenly
     * divisible.
     *
     * <p>Three cases are handled:
     *
     * <ul>
     *   <li>{@code numBuckets >= numChannels && numBuckets % numChannels == 0}: simple {@code
     *       bucketId % numChannels}.
     *   <li>{@code numChannels > numBuckets && numChannels % numBuckets == 0}: disjoint
     *       round-robin, preserving the original assignment shape.
     *   <li>Otherwise: LCM-based logical slots — each bucket owns {@code numChannels / gcd}
     *       consecutive slots and each channel owns {@code numBuckets / gcd} consecutive slots. A
     *       hash within the bucket distributes records evenly across channels whose slot ranges
     *       overlap the bucket's range.
     * </ul>
     *
     * @param bucketId the bucket ID computed by {@code BucketingFunction}
     * @param numBuckets total number of buckets in the table
     * @param numChannels number of downstream channels (subtask parallelism)
     * @param recordHash hash of the record (e.g. murmur hash of the bucket key bytes)
     * @return the target channel index in {@code [0, numChannels)}
     */
    static int select(int bucketId, int numBuckets, int numChannels, int recordHash) {
        if (numBuckets >= numChannels && numBuckets % numChannels == 0) {
            return bucketId % numChannels;
        }

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

    static int greatestCommonDivisor(int first, int second) {
        while (second != 0) {
            int remainder = first % second;
            first = second;
            second = remainder;
        }
        return first;
    }
}

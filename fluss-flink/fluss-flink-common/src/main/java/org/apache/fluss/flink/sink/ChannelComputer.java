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
}

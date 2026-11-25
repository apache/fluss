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

package org.apache.fluss.client.initializer;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.config.ConsumerOffsetReset;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

/** An interface for users to specify the starting offset. */
public interface OffsetsInitializer extends Serializable {

    /**
     * Get the initial offsets for the given fluss buckets. These offsets will be used as starting
     * offsets of the fluss buckets.
     *
     * @param partitionName the partition name of the buckets if they are partitioned. Otherwise,
     *     null.
     * @param buckets the fluss buckets to get the starting offsets.
     * @param bucketOffsetsRetriever a helper to retrieve information of the fluss buckets.
     * @return A mapping from fluss bucket to their offsets to start scanning from.
     */
    Map<Integer, Long> getBucketOffsets(
            @Nullable String partitionName,
            Collection<Integer> buckets,
            BucketOffsetsRetriever bucketOffsetsRetriever);

    /**
     * An interface that provides necessary information to the {@link OffsetsInitializer} to get the
     * initial offsets of the fluss buckets.
     */
    interface BucketOffsetsRetriever {
        Map<Integer, Long> latestOffsets(
                @Nullable String partitionName, Collection<Integer> buckets);

        Map<Integer, Long> earliestOffsets(
                @Nullable String partitionName, Collection<Integer> buckets);

        Map<Integer, Long> offsetsFromTimestamp(
                @Nullable String partitionName, Collection<Integer> buckets, long timestamp);

        /**
         * Fetch the last committed offset of {@code groupId} for each of {@code buckets}. Buckets
         * without a committed offset (never-committed) are absent from the returned map — the
         * caller decides the fallback behavior.
         *
         * @param groupId the consumer group whose committed offsets should be fetched
         * @param partitionName the partition name of the buckets if they are partitioned.
         *     Otherwise, null.
         * @param buckets the buckets whose committed offsets should be fetched
         * @return a mapping from bucket to its committed offset; buckets without a committed offset
         *     are absent
         */
        Map<Integer, Long> committedOffsets(
                String groupId, @Nullable String partitionName, Collection<Integer> buckets);
    }

    // --------------- factory methods ---------------

    /**
     * Get an {@link OffsetsInitializer} which initializes the offsets to the earliest available
     * offsets of each bucket.
     *
     * @return an {@link OffsetsInitializer} which initializes the offsets to the earliest available
     *     offsets.
     */
    static OffsetsInitializer earliest() {
        return new EarliestOffsetsInitializer();
    }

    /**
     * Get an {@link OffsetsInitializer} which initializes the offsets to the latest offsets of each
     * bucket.
     *
     * @return an {@link OffsetsInitializer} which initializes the offsets to the latest offsets.
     */
    static OffsetsInitializer latest() {
        return new LatestOffsetsInitializer();
    }

    /**
     * Get an {@link OffsetsInitializer} which performs a full snapshot on the table upon first
     * startup, and continue to read log with the offset to the snapshot.
     *
     * <p>If the table to read is a log table, the full snapshot means reading from the earliest log
     * offset which means "full" OffsetsInitializer equal to the {@link #earliest()}
     * OffsetsInitializer. If the table to read is a primary key table, the full snapshot means
     * reading the latest snapshot which materializes all changes on the table.
     *
     * @return an {@link OffsetsInitializer} which initializes the offsets to snapshot offsets.
     */
    static OffsetsInitializer full() {
        return new SnapshotOffsetsInitializer();
    }

    /**
     * Get an {@link OffsetsInitializer} which initializes the offsets in each bucket so that the
     * initialized offset is the offset of the first record batch whose commit timestamp is greater
     * than or equals the given timestamp (milliseconds).
     *
     * @param timestamp the timestamp (milliseconds) to start the scan.
     * @return an {@link OffsetsInitializer} which initializes the offsets based on the given
     *     timestamp.
     * @see Admin#listOffsets(TablePath, Collection, OffsetSpec)
     * @see Admin#listOffsets(TablePath, String, Collection, OffsetSpec)
     */
    static OffsetsInitializer timestamp(long timestamp) {
        return new TimestampOffsetsInitializer(timestamp);
    }

    /**
     * Get an {@link OffsetsInitializer} which initializes the offsets to the last committed offsets
     * of the given consumer group. Buckets that have no committed offset for this group follow the
     * default {@link ConsumerOffsetReset#EARLIEST} policy.
     *
     * @param groupId the consumer group whose committed offsets should be used as the starting
     *     offsets
     * @return an {@link OffsetsInitializer} which initializes the offsets to the committed offsets
     */
    static OffsetsInitializer committed(String groupId) {
        return committed(groupId, ConsumerOffsetReset.EARLIEST);
    }

    /**
     * Same as {@link #committed(String)} but uses the given reset policy for buckets that have no
     * committed offset yet.
     *
     * @param groupId the consumer group whose committed offsets should be used as the starting
     *     offsets
     * @param resetPolicy policy used for any bucket without a committed offset
     * @return an {@link OffsetsInitializer} which initializes the offsets to the committed offsets
     */
    static OffsetsInitializer committed(String groupId, ConsumerOffsetReset resetPolicy) {
        return new CommittedOffsetsInitializer(groupId, resetPolicy);
    }
}

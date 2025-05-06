/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.flink.source.enumerator.initializer;

import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.admin.OffsetSpec;
import com.alibaba.fluss.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

/** An interface for users to specify the starting offset of a {@link SourceSplitBase}. */
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
}

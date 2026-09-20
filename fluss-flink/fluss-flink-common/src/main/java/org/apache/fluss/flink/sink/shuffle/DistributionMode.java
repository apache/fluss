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

package org.apache.fluss.flink.sink.shuffle;

/** Distribution mode for sink shuffling. */
public enum DistributionMode {
    /**
     * Uses Flink's default shuffle strategy:
     *
     * <p>Typically uses FORWARD when the sink parallelism matches the upstream parallelism Uses
     * ROUND_ROBIN when parallelisms differ.
     */
    NONE,

    /**
     * Automatically chooses the best mode based on the table type:
     *
     * <p>Uses BUCKET mode for Primary Key Tables and Log Tables with bucket keys maximize
     * throughput.
     *
     * <p>Uses NONE for Log Tables without bucket keys.
     */
    AUTO,

    /**
     * Shuffle data by bucket ID before writing to sink. This groups data with the same bucket ID to
     * be processed by the same task, which improves client processing efficiency and reduces
     * resource consumption.
     *
     * <p>Characteristics:
     *
     * <p>Particularly recommended for Primary Key tables as it can significantly improve
     * throughput. For Log Tables, bucket shuffle only takes effect when the 'bucket.key' is
     * defined.
     *
     * <p>Note: When sink parallelism exceeds the number of buckets, some sink tasks may remain idle
     * without receiving data.
     */
    BUCKET,

    /**
     * Dynamically adjusts shuffle strategy based on partition key traffic patterns. This mode
     * monitors data distribution and adjusts the shuffle behavior to balance the load.
     *
     * <p>Characteristics:
     *
     * <p>Only supported for partitioned Log Tables (not supported for Primary Key tables now). Use
     * this mode when data is highly skewed across partitions or when there are many partitions.
     *
     * <p>Note: This mode has overhead costs including data statistics collection and additional
     * shuffle operations.
     */
    PARTITION_DYNAMIC,

    /**
     * Shuffle data by bucket key with even load distribution across all downstream subtasks.
     *
     * <p>Unlike {@link #BUCKET} which maps each bucket to exactly one subtask (potentially leaving
     * some subtasks idle), this mode uses an LCM-based logical slot assignment to ensure every
     * subtask receives data.
     *
     * <p>Routing algorithm: first compute bucket ID via {@code BucketingFunction}, then select the
     * concrete subtask within the bucket's assigned slot range using the bucket key's murmur hash.
     * Records with the same bucket key always route to the same subtask, making this mode safe for
     * Primary Key tables (merge semantics preserved).
     *
     * <p>Characteristics:
     *
     * <p>Requires 'bucket.key' to be defined. Suitable for tables where intra-bucket ordering is
     * not required but even load distribution matters.
     */
    BUCKET_LOAD_BALANCE
}

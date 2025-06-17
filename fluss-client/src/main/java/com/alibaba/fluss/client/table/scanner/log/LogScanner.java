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

package com.alibaba.fluss.client.table.scanner.log;

import com.alibaba.fluss.annotation.PublicEvolving;

import java.time.Duration;

/**
 * The scanner is used to scan log data of specify table from Fluss.
 *
 * @since 0.1
 */
@PublicEvolving
public interface LogScanner extends AutoCloseable {

    /**
     * The earliest offset to fetch from. Fluss uses "-2" to indicate fetching from log start
     * offset.
     */
    long EARLIEST_OFFSET = -2L;

    /**
     * Poll log data from tablet server.
     *
     * <p>On each poll, scanner will try to use the last scanned offset as the starting offset and
     * fetch sequentially. The latest offset will be cached in memory, and if there is no offset
     * cached in memory, scanner will start either from the earliest offset or from the latest
     * offset for each bucket according to the policy.
     *
     * @param timeout the timeout to poll.
     * @return the result of poll.
     * @throws java.lang.IllegalStateException if the scanner is not subscribed to any buckets to
     *     read from.
     */
    ScanRecords poll(Duration timeout);

    /**
     * Subscribe to the given table bucket in given offset dynamically. If the table bucket is
     * already subscribed, the offset will be updated.
     *
     * <p>Please use {@link #subscribe(long, int, long)} to subscribe a partitioned table.
     *
     * @param bucket the table bucket to subscribe.
     * @param offset the offset to start from.
     * @throws java.lang.IllegalStateException if the table is a partitioned table.
     */
    void subscribe(int bucket, long offset);

    /**
     * Subscribe to the given table buckets from beginning dynamically. If the table bucket is
     * already subscribed, the start offset will be updated.
     *
     * <p>It equals to call {@link #subscribe(int, long)} with offset {@link #EARLIEST_OFFSET} for
     * the subscribed bucket.
     *
     * <p>Please use {@link #subscribeFromBeginning(long, int)} to subscribe a partitioned table.
     *
     * @param bucket the table bucket to subscribe.
     * @throws java.lang.IllegalStateException if the table is a partitioned table.
     */
    default void subscribeFromBeginning(int bucket) {
        subscribe(bucket, EARLIEST_OFFSET);
    }

    /**
     * Subscribe to the given partitioned table bucket in given offset dynamically. If the table
     * bucket is already subscribed, the offset will be updated.
     *
     * <p>Please use {@link #subscribe(int, long)} to subscribe a non-partitioned table.
     *
     * @param partitionId the partition id of the table partition to subscribe.
     * @param bucket the table bucket to subscribe.
     * @param offset the offset to start from.
     * @throws java.lang.IllegalStateException if the table is a non-partitioned table.
     */
    void subscribe(long partitionId, int bucket, long offset);

    /**
     * Unsubscribe from the given bucket of given partition dynamically.
     *
     * @param partitionId the partition id of the table partition to unsubscribe.
     * @param bucket the table bucket to unsubscribe.
     * @throws java.lang.IllegalStateException if the table is a non-partitioned table.
     */
    void unsubscribe(long partitionId, int bucket);

    /**
     * Subscribe to the given partitioned table bucket from beginning dynamically. If the table
     * bucket is already subscribed, the start offset will be updated.
     *
     * <p>It equals to call {@link #subscribe(long, int, long)} with offset {@link #EARLIEST_OFFSET}
     * for the subscribed bucket.
     *
     * <p>Please use {@link #subscribeFromBeginning(int)} to subscribe a non-partitioned table.
     *
     * @param bucket the table bucket to subscribe.
     * @throws java.lang.IllegalStateException if the table is a non-partitioned table.
     */
    default void subscribeFromBeginning(long partitionId, int bucket) {
        subscribe(partitionId, bucket, EARLIEST_OFFSET);
    }

    /**
     * Wake up the log scanner in case the fetcher thread in log scanner is blocking in {@link
     * #poll(Duration timeout)}.
     */
    void wakeup();
}

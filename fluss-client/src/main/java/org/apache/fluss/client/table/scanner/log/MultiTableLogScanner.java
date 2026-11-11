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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.admin.OffsetMetadata;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import java.time.Duration;
import java.util.Map;

/**
 * Reads log data from multiple tables through a single client. All subscribed buckets (across
 * different tables) are polled fairly and returned in one {@link MultiTableRecords} result.
 *
 * <p>Unlike {@link LogScanner} which is bound to a single table, {@code MultiTableLogScanner}
 * allows subscribing to buckets across different tables. Per-table projection and filter pushdown
 * are configured on the {@link org.apache.fluss.client.table.scanner.MultiTableScan} builder, not
 * on {@code subscribe}.
 *
 * <p>{@code MultiTableLogScanner} is NOT thread-safe (consistent with {@link LogScanner}). It is
 * the responsibility of the user to ensure that multithreaded access is properly synchronized.
 *
 * <p>With automatic commits enabled, both {@link #poll(Duration)} and {@link #close()} submit the
 * current consumed offsets asynchronously when the configured interval has elapsed. Closing waits
 * for outstanding commits using {@code client.request-timeout}. To confirm the final positions have
 * been committed, call {@link #commitSync()} before closing. See {@link OffsetCommitCallback} for
 * callback delivery.
 *
 * @since 0.7
 */
@PublicEvolving
public interface MultiTableLogScanner extends AutoCloseable {

    /**
     * The earliest offset to fetch from. Fluss uses "-2" to indicate fetching from log start
     * offset.
     */
    long EARLIEST_OFFSET = LogScanner.EARLIEST_OFFSET;

    /**
     * The latest offset to fetch to. Fluss uses this to indicate the default stopping offset for
     * unbounded Fluss sources.
     */
    long NO_STOPPING_OFFSET = LogScanner.NO_STOPPING_OFFSET;

    /**
     * Poll log data from all subscribed table buckets.
     *
     * <p>On each poll, the scanner will try to use the last scanned offset as the starting offset
     * and fetch sequentially for each subscribed bucket. The result may contain records from
     * multiple tables.
     *
     * <p>The timeout is shared by committed-offset lookup and waiting for fetched records.
     *
     * @param timeout the timeout to poll.
     * @return the result of poll, organized by {@link TablePath} and {@link
     *     org.apache.fluss.metadata.TableBucket}.
     * @throws IllegalStateException if the scanner is not subscribed to any buckets.
     * @throws org.apache.fluss.exception.LogDecodeException if a subscribed bucket has an
     *     undecodable batch. See {@link
     *     org.apache.fluss.exception.LogDecodeException#getNextOffset()} for the offset after the
     *     failed batch.
     */
    MultiTableRecords poll(Duration timeout);

    // ---- non-partitioned tables ----

    /** Subscribe to the given non-partitioned table bucket starting at {@code offset}. */
    void subscribe(TablePath tablePath, int bucket, long offset);

    /**
     * Subscribe to the given non-partitioned table bucket starting from the consumer group's
     * committed offset. If no offset has been committed for the bucket, the configured {@code
     * client.consumer.auto-offset-reset} policy (default {@code earliest}) is applied.
     *
     * @param tablePath the table to subscribe.
     * @param bucket the table bucket to subscribe.
     * @throws org.apache.fluss.exception.InvalidConsumerGroupIdException if no consumer group id is
     *     configured.
     */
    void subscribeFromCommittedOffset(TablePath tablePath, int bucket);

    /** Subscribe to the given non-partitioned table bucket starting from the earliest offset. */
    default void subscribeFromBeginning(TablePath tablePath, int bucket) {
        subscribe(tablePath, bucket, EARLIEST_OFFSET);
    }

    /** Unsubscribe from the given non-partitioned table bucket. */
    void unsubscribe(TablePath tablePath, int bucket);

    // ---- partitioned tables ----

    /** Subscribe to the given partitioned table bucket starting at {@code offset}. */
    void subscribe(TablePath tablePath, long partitionId, int bucket, long offset);

    /**
     * Subscribe to the given partitioned table bucket starting from the consumer group's committed
     * offset. If no offset has been committed for the bucket, the configured {@code
     * client.consumer.auto-offset-reset} policy (default {@code earliest}) is applied.
     *
     * @param tablePath the table to subscribe.
     * @param partitionId the partition id of the table partition to subscribe.
     * @param bucket the table bucket to subscribe.
     * @throws org.apache.fluss.exception.InvalidConsumerGroupIdException if no consumer group id is
     *     configured.
     */
    void subscribeFromCommittedOffset(TablePath tablePath, long partitionId, int bucket);

    /** Subscribe to the given partitioned table bucket starting from the earliest offset. */
    default void subscribeFromBeginning(TablePath tablePath, long partitionId, int bucket) {
        subscribe(tablePath, partitionId, bucket, EARLIEST_OFFSET);
    }

    /** Unsubscribe from the given partitioned table bucket. */
    void unsubscribe(TablePath tablePath, long partitionId, int bucket);

    /**
     * Synchronously commits all offsets consumed so far by this scanner to its consumer group,
     * using the default API timeout.
     *
     * @throws org.apache.fluss.exception.TimeoutException if the commit does not finish in time.
     * @throws org.apache.fluss.exception.InvalidConsumerGroupIdException if no consumer group id is
     *     configured.
     */
    void commitSync();

    /**
     * Synchronously commits all offsets consumed so far by this scanner to its consumer group.
     *
     * @param timeout the maximum time to wait for the commit to finish.
     * @throws org.apache.fluss.exception.TimeoutException if the commit does not finish in time.
     * @throws org.apache.fluss.exception.InvalidConsumerGroupIdException if no consumer group id is
     *     configured.
     */
    void commitSync(Duration timeout);

    /**
     * Synchronously commits the given offsets to this scanner's consumer group, using the default
     * API timeout.
     *
     * @param offsets the offsets to commit, keyed by table bucket.
     * @throws org.apache.fluss.exception.TimeoutException if the commit does not finish in time.
     * @throws org.apache.fluss.exception.InvalidConsumerGroupIdException if no consumer group id is
     *     configured.
     */
    void commitSync(Map<TableBucket, OffsetMetadata> offsets);

    /**
     * Synchronously commits the given offsets to this scanner's consumer group.
     *
     * @param offsets the offsets to commit, keyed by table bucket.
     * @param timeout the maximum time to wait for the commit to finish.
     * @throws org.apache.fluss.exception.TimeoutException if the commit does not finish in time.
     * @throws org.apache.fluss.exception.InvalidConsumerGroupIdException if no consumer group id is
     *     configured.
     */
    void commitSync(Map<TableBucket, OffsetMetadata> offsets, Duration timeout);

    /**
     * Asynchronously commits all offsets consumed so far by this scanner to its consumer group. The
     * scanner logs failures at error level when it processes the commit result.
     *
     * @throws org.apache.fluss.exception.InvalidConsumerGroupIdException if no consumer group id is
     *     configured.
     */
    void commitAsync();

    /**
     * Asynchronously commits all offsets consumed so far by this scanner to its consumer group.
     *
     * @param callback the callback to invoke on a later scanner call, or {@code null} to log
     *     failures at error level when the scanner processes the result.
     * @throws org.apache.fluss.exception.InvalidConsumerGroupIdException if no consumer group id is
     *     configured.
     */
    void commitAsync(OffsetCommitCallback callback);

    /**
     * Asynchronously commits the given offsets to this scanner's consumer group.
     *
     * @param offsets the offsets to commit, keyed by table bucket. The request and callback use
     *     this map directly. Pass a copy if you plan to modify the original.
     * @param callback the callback to invoke on a later scanner call, or {@code null} to log
     *     failures at error level when the scanner processes the result.
     * @throws org.apache.fluss.exception.InvalidConsumerGroupIdException if no consumer group id is
     *     configured.
     */
    void commitAsync(Map<TableBucket, OffsetMetadata> offsets, OffsetCommitCallback callback);

    /**
     * Wake up the log scanner in case the fetcher thread in the log scanner is blocking in {@link
     * #poll(Duration)}.
     */
    void wakeup();
}

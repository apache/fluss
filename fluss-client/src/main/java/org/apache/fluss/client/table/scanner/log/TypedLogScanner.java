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

import java.time.Duration;
import java.util.Map;

/**
 * A typed scanner is used to scan log data as POJOs of specify table from Fluss.
 *
 * @param <T> the type of the POJO
 * @since 0.6
 */
@PublicEvolving
public interface TypedLogScanner<T> extends AutoCloseable {

    /**
     * Poll log data from tablet server.
     *
     * @param timeout the timeout to poll.
     * @return the result of poll.
     */
    TypedScanRecords<T> poll(Duration timeout);

    /**
     * Subscribe to the given table bucket from beginning dynamically. If the table bucket is
     * already subscribed, the start offset will be updated.
     *
     * @param bucket the table bucket to subscribe.
     */
    void subscribeFromBeginning(int bucket);

    /**
     * Subscribe to the given partitioned table bucket from beginning dynamically. If the table
     * bucket is already subscribed, the start offset will be updated.
     *
     * @param partitionId the partition id of the table partition to subscribe.
     * @param bucket the table bucket to subscribe.
     */
    void subscribeFromBeginning(long partitionId, int bucket);

    /**
     * Subscribe to the given non-partitioned table bucket starting from the consumer group's
     * committed offset. If no offset has been committed for the bucket, the configured {@code
     * client.consumer.auto-offset-reset} policy (default {@code earliest}) is applied.
     *
     * @param bucket the table bucket to subscribe.
     * @throws org.apache.fluss.exception.InvalidConsumerGroupIdException if no consumer group id is
     *     configured.
     */
    void subscribeFromCommittedOffset(int bucket);

    /**
     * Subscribe to the given partitioned table bucket starting from the consumer group's committed
     * offset. If no offset has been committed for the bucket, the configured {@code
     * client.consumer.auto-offset-reset} policy (default {@code earliest}) is applied.
     *
     * @param partitionId the partition id of the table partition to subscribe.
     * @param bucket the table bucket to subscribe.
     * @throws org.apache.fluss.exception.InvalidConsumerGroupIdException if no consumer group id is
     *     configured.
     */
    void subscribeFromCommittedOffset(long partitionId, int bucket);

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
     * Asynchronously commits all offsets consumed so far by this scanner to its consumer group.
     * Commit failures are logged at error level.
     *
     * @throws org.apache.fluss.exception.InvalidConsumerGroupIdException if no consumer group id is
     *     configured.
     */
    void commitAsync();

    /**
     * Asynchronously commits all offsets consumed so far by this scanner to its consumer group.
     *
     * @param callback invoked after the commit finishes; if {@code null}, failures are logged at
     *     error level.
     * @throws org.apache.fluss.exception.InvalidConsumerGroupIdException if no consumer group id is
     *     configured.
     */
    void commitAsync(OffsetCommitCallback callback);

    /**
     * Asynchronously commits the given offsets to this scanner's consumer group.
     *
     * @param offsets the offsets to commit, keyed by table bucket.
     * @param callback invoked after the commit finishes; if {@code null}, failures are logged at
     *     error level.
     * @throws org.apache.fluss.exception.InvalidConsumerGroupIdException if no consumer group id is
     *     configured.
     */
    void commitAsync(Map<TableBucket, OffsetMetadata> offsets, OffsetCommitCallback callback);

    /**
     * Subscribe to the given table bucket in given offset dynamically. If the table bucket is
     * already subscribed, the offset will be updated.
     *
     * @param bucket the table bucket to subscribe.
     * @param offset the offset to start from.
     */
    void subscribe(int bucket, long offset);

    /**
     * Subscribe to the given partitioned table bucket in given offset dynamically. If the table
     * bucket is already subscribed, the offset will be updated.
     *
     * @param partitionId the partition id of the table partition to subscribe.
     * @param bucket the table bucket to subscribe.
     * @param offset the offset to start from.
     */
    void subscribe(long partitionId, int bucket, long offset);

    /**
     * Unsubscribe from the given bucket of given partition dynamically.
     *
     * @param partitionId the partition id of the table partition to unsubscribe.
     * @param bucket the table bucket to unsubscribe.
     */
    void unsubscribe(long partitionId, int bucket);

    /**
     * Wake up the log scanner in case the fetcher thread in log scanner is blocking in {@link
     * #poll(Duration timeout)}.
     */
    void wakeup();
}

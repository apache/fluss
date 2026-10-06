/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.fluss.client.table.scanner.log.LogScanner.NO_STOPPING_OFFSET;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LogScannerStatus}. */
public class LogScannerStatusTest {

    private static final TableBucket TABLE_BUCKET = new TableBucket(1L, 0);

    @Test
    void testAssignBoundedBucket() {
        LogScannerStatus scannerStatus = new LogScannerStatus();

        scannerStatus.assignScanBucket(TABLE_BUCKET, 10L, 20L);

        assertThat(scannerStatus.getBucketOffset(TABLE_BUCKET)).isEqualTo(10L);
        assertThat(scannerStatus.getBucketStoppingOffset(TABLE_BUCKET)).isEqualTo(20L);
        assertThat(scannerStatus.hasReachedStoppingOffset(TABLE_BUCKET)).isFalse();
    }

    @Test
    void testResubscribeUpdatesStoppingOffset() {
        LogScannerStatus scannerStatus = new LogScannerStatus();

        scannerStatus.assignScanBucket(TABLE_BUCKET, 10L, 20L);
        scannerStatus.assignScanBucket(TABLE_BUCKET, 20L, 30L);

        assertThat(scannerStatus.getBucketOffset(TABLE_BUCKET)).isEqualTo(20L);
        assertThat(scannerStatus.getBucketStoppingOffset(TABLE_BUCKET)).isEqualTo(30L);
        assertThat(scannerStatus.hasReachedStoppingOffset(TABLE_BUCKET)).isFalse();
    }

    @Test
    void testResubscribeAsUnboundedClearsStoppingOffset() {
        LogScannerStatus scannerStatus = new LogScannerStatus();

        scannerStatus.assignScanBucket(TABLE_BUCKET, 10L, 20L);
        scannerStatus.assignScanBucket(TABLE_BUCKET, 20L, NO_STOPPING_OFFSET);

        assertThat(scannerStatus.getBucketOffset(TABLE_BUCKET)).isEqualTo(20L);
        assertThat(scannerStatus.getBucketStoppingOffset(TABLE_BUCKET))
                .isEqualTo(NO_STOPPING_OFFSET);
        assertThat(scannerStatus.hasReachedStoppingOffset(TABLE_BUCKET)).isFalse();
    }

    @Test
    void testBoundedRecordsLag() {
        LogScannerStatus scannerStatus = new LogScannerStatus();

        scannerStatus.assignScanBucket(TABLE_BUCKET, 90L, 100L);
        scannerStatus.updateHighWatermark(TABLE_BUCKET, 1000L);

        assertThat(scannerStatus.recordsLag()).isEqualTo(10L);
    }

    @Test
    void testUnboundedRecordsLag() {
        LogScannerStatus scannerStatus = new LogScannerStatus();

        scannerStatus.assignScanBucket(TABLE_BUCKET, 90L, NO_STOPPING_OFFSET);
        scannerStatus.updateHighWatermark(TABLE_BUCKET, 1000L);

        assertThat(scannerStatus.recordsLag()).isEqualTo(910L);
    }

    @Test
    void testEmptyBoundedRangeIsImmediatelyFinished() {
        LogScannerStatus scannerStatus = new LogScannerStatus();

        scannerStatus.assignScanBucket(TABLE_BUCKET, 10L, 10L);

        assertThat(scannerStatus.hasReachedStoppingOffset(TABLE_BUCKET)).isTrue();
        assertThat(scannerStatus.hasPendingFinishedBuckets()).isTrue();

        assertThat(scannerStatus.drainFinishedBuckets()).containsExactly(TABLE_BUCKET);
        assertThat(scannerStatus.hasPendingFinishedBuckets()).isFalse();
    }

    @Test
    void testReachingStoppingOffsetReportsCompletionOnce() {
        LogScannerStatus scannerStatus = new LogScannerStatus();

        scannerStatus.assignScanBucket(TABLE_BUCKET, 10L, 20L);

        scannerStatus.updateOffset(TABLE_BUCKET, 19L);
        assertThat(scannerStatus.hasReachedStoppingOffset(TABLE_BUCKET)).isFalse();
        assertThat(scannerStatus.hasPendingFinishedBuckets()).isFalse();

        scannerStatus.updateOffset(TABLE_BUCKET, 20L);
        assertThat(scannerStatus.hasReachedStoppingOffset(TABLE_BUCKET)).isTrue();
        assertThat(scannerStatus.drainFinishedBuckets()).containsExactly(TABLE_BUCKET);

        scannerStatus.updateOffset(TABLE_BUCKET, 20L);
        assertThat(scannerStatus.drainFinishedBuckets()).isEmpty();
    }

    @Test
    void testFinishedBucketIsNotFetchable() {
        LogScannerStatus scannerStatus = new LogScannerStatus();

        scannerStatus.assignScanBucket(TABLE_BUCKET, 10L, 20L);

        assertThat(scannerStatus.fetchableBuckets(ignored -> true)).containsExactly(TABLE_BUCKET);

        scannerStatus.updateOffset(TABLE_BUCKET, 20L);

        assertThat(scannerStatus.fetchableBuckets(ignored -> true)).isEmpty();
    }

    @Test
    void testResubscribeClearsPendingCompletion() {
        LogScannerStatus scannerStatus = new LogScannerStatus();

        scannerStatus.assignScanBucket(TABLE_BUCKET, 10L, 10L);
        assertThat(scannerStatus.hasPendingFinishedBuckets()).isTrue();

        scannerStatus.assignScanBucket(TABLE_BUCKET, 10L, 20L);

        assertThat(scannerStatus.hasPendingFinishedBuckets()).isFalse();
        assertThat(scannerStatus.hasReachedStoppingOffset(TABLE_BUCKET)).isFalse();
        assertThat(scannerStatus.getBucketStoppingOffset(TABLE_BUCKET)).isEqualTo(20L);
    }

    @Test
    void testUnassignClearsPendingCompletion() {
        LogScannerStatus scannerStatus = new LogScannerStatus();

        scannerStatus.assignScanBucket(TABLE_BUCKET, 10L, 10L);
        assertThat(scannerStatus.hasPendingFinishedBuckets()).isTrue();

        scannerStatus.unassignScanBuckets(Collections.singletonList(TABLE_BUCKET));

        assertThat(scannerStatus.hasPendingFinishedBuckets()).isFalse();
        assertThat(scannerStatus.getBucketOffset(TABLE_BUCKET)).isNull();
    }

    @Test
    void testFinishedBucketDoesNotBlockOtherBuckets() {
        LogScannerStatus scannerStatus = new LogScannerStatus();
        TableBucket first = new TableBucket(1L, 0);
        TableBucket second = new TableBucket(1L, 1);

        scannerStatus.assignScanBucket(first, 0L, 5L);
        scannerStatus.assignScanBucket(second, 0L, 10L);

        scannerStatus.updateOffset(first, 5L);

        assertThat(scannerStatus.fetchableBuckets(ignored -> true)).containsExactly(second);
        assertThat(scannerStatus.drainFinishedBuckets()).containsExactly(first);
    }

    @Test
    void testResolveBoundedEarliestBeyondStoppingOffset() {
        LogScannerStatus scannerStatus = new LogScannerStatus();

        scannerStatus.assignScanBucket(TABLE_BUCKET, LogScanner.EARLIEST_OFFSET, 50L);

        Long resolved =
                scannerStatus.resolveBoundedStartingOffset(
                        TABLE_BUCKET, LogScanner.EARLIEST_OFFSET, 100L);

        assertThat(resolved).isEqualTo(50L);
        assertThat(scannerStatus.getBucketOffset(TABLE_BUCKET)).isEqualTo(50L);
        assertThat(scannerStatus.hasReachedStoppingOffset(TABLE_BUCKET)).isTrue();
        assertThat(scannerStatus.hasPendingFinishedBuckets()).isTrue();
        assertThat(scannerStatus.drainFinishedBuckets()).containsExactly(TABLE_BUCKET);
    }

    @Test
    void testResolveBoundedEarliestBeforeStoppingOffset() {
        LogScannerStatus scannerStatus = new LogScannerStatus();

        scannerStatus.assignScanBucket(TABLE_BUCKET, LogScanner.EARLIEST_OFFSET, 150L);

        Long resolved =
                scannerStatus.resolveBoundedStartingOffset(
                        TABLE_BUCKET, LogScanner.EARLIEST_OFFSET, 100L);

        assertThat(resolved).isEqualTo(100L);
        assertThat(scannerStatus.getBucketOffset(TABLE_BUCKET)).isEqualTo(100L);
        assertThat(scannerStatus.hasReachedStoppingOffset(TABLE_BUCKET)).isFalse();
        assertThat(scannerStatus.hasPendingFinishedBuckets()).isFalse();
    }

    @Test
    void testResolveBoundedEarliestIgnoresStaleRequest() {
        LogScannerStatus scannerStatus = new LogScannerStatus();
        scannerStatus.assignScanBucket(TABLE_BUCKET, LogScanner.EARLIEST_OFFSET, 50L);
        // Resubscribe before the old EARLIEST response arrives.
        scannerStatus.assignScanBucket(TABLE_BUCKET, 10L, 50L);
        Long resolved =
                scannerStatus.resolveBoundedStartingOffset(
                        TABLE_BUCKET, LogScanner.EARLIEST_OFFSET, 20L);

        assertThat(resolved).isNull();
        assertThat(scannerStatus.getBucketOffset(TABLE_BUCKET)).isEqualTo(10L);
        assertThat(scannerStatus.hasReachedStoppingOffset(TABLE_BUCKET)).isFalse();
        assertThat(scannerStatus.hasPendingFinishedBuckets()).isFalse();
    }

    @Test
    void testFinishedBoundedSubscriptionsAreNotActive() {
        LogScannerStatus scannerStatus = new LogScannerStatus();
        TableBucket first = new TableBucket(1L, 0);
        TableBucket second = new TableBucket(1L, 1);

        scannerStatus.assignScanBucket(first, 0L, 5L);
        scannerStatus.assignScanBucket(second, 10L, 20L);
        assertThat(scannerStatus.hasActiveSubscriptions()).isTrue();

        scannerStatus.updateOffset(first, 5L);
        assertThat(scannerStatus.hasActiveSubscriptions()).isTrue();

        scannerStatus.updateOffset(second, 20L);
        assertThat(scannerStatus.hasActiveSubscriptions()).isFalse();
    }

    @Test
    void testUnboundedSubscriptionRemainsActiveAfterBoundedSubscriptionFinishes() {
        LogScannerStatus scannerStatus = new LogScannerStatus();
        TableBucket bounded = new TableBucket(1L, 0);
        TableBucket unbounded = new TableBucket(1L, 1);

        scannerStatus.assignScanBucket(bounded, 0L, 5L);
        scannerStatus.assignScanBucket(unbounded, 0L, NO_STOPPING_OFFSET);
        scannerStatus.updateOffset(bounded, 5L);

        assertThat(scannerStatus.hasReachedStoppingOffset(bounded)).isTrue();
        assertThat(scannerStatus.hasActiveSubscriptions()).isTrue();
    }

    @Test
    void testResubscribeReactivatesFinishedBucket() {
        LogScannerStatus scannerStatus = new LogScannerStatus();

        scannerStatus.assignScanBucket(TABLE_BUCKET, 0L, 5L);
        scannerStatus.updateOffset(TABLE_BUCKET, 5L);
        assertThat(scannerStatus.hasActiveSubscriptions()).isFalse();

        scannerStatus.assignScanBucket(TABLE_BUCKET, 5L, 10L);
        assertThat(scannerStatus.hasActiveSubscriptions()).isTrue();
    }
}

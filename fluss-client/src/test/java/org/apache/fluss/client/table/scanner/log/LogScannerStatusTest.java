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

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LogScannerStatus}. */
class LogScannerStatusTest {

    @Test
    void testBoundedCompletionReportedOnlyOnce() {
        LogScannerStatus status = new LogScannerStatus();
        TableBucket bucket = new TableBucket(1L, 0);
        status.assignBoundedScanBucket(bucket, 10L, 20L);

        assertThat(status.drainFinishedBuckets()).isEmpty();
        assertThat(status.updateOffsetIfCurrent(bucket, 10L, 20L, 20L)).isTrue();
        assertThat(status.getBucketOffset(bucket)).isEqualTo(20L);
        assertThat(status.drainFinishedBuckets()).containsExactly(bucket);
        // A finished bucket must not be advanced again.
        assertThat(status.updateOffsetIfCurrent(bucket, 20L, 20L, 25L)).isFalse();
        assertThat(status.drainFinishedBuckets()).isEmpty();
        assertThat(status.allSubscribedBucketsFinished()).isTrue();
    }

    @Test
    void testEmptyBoundedRange() {
        LogScannerStatus status = new LogScannerStatus();
        TableBucket bucket = new TableBucket(1L, 0);

        status.assignBoundedScanBucket(bucket, 100L, 100L);

        assertThat(status.fetchableBuckets(b -> true)).isEmpty();
        assertThat(status.drainFinishedBuckets()).containsExactly(bucket);
        assertThat(status.drainFinishedBuckets()).isEmpty();
    }

    @Test
    void testResubscriptionClearsPendingCompletion() {
        LogScannerStatus status = new LogScannerStatus();
        TableBucket bucket = new TableBucket(1L, 0);
        status.assignBoundedScanBucket(bucket, 100L, 100L);
        // Replace the completed subscription before polling.
        status.assignBoundedScanBucket(bucket, 100L, 200L);

        assertThat(status.drainFinishedBuckets()).isEmpty();
        assertThat(status.getBucketOffset(bucket)).isEqualTo(100L);
        assertThat(status.allSubscribedBucketsFinished()).isFalse();
    }

    @Test
    void testBoundedReplacedByUnbounded() {
        LogScannerStatus status = new LogScannerStatus();
        TableBucket bucket = new TableBucket(1L, 0);
        status.assignBoundedScanBucket(bucket, 100L, 100L);
        status.assignScanBuckets(Collections.singletonMap(bucket, 50L));

        assertThat(status.drainFinishedBuckets()).isEmpty();
        assertThat(status.fetchableBuckets(b -> true)).containsExactly(bucket);
    }

    @Test
    void testUnboundedReassignmentPreservesWatermark() {
        LogScannerStatus status = new LogScannerStatus();
        TableBucket bucket = new TableBucket(1L, 0);

        status.assignScanBuckets(Collections.singletonMap(bucket, 0L));
        status.updateHighWatermark(bucket, 100L);
        status.assignScanBuckets(Collections.singletonMap(bucket, 50L));

        assertThat(status.recordsLag()).isEqualTo(50L);
    }

    @Test
    void testResolveBoundedEarliestBeyondStop() {
        LogScannerStatus status = new LogScannerStatus();
        TableBucket bucket = new TableBucket(1L, 0);

        status.assignBoundedScanBucket(bucket, LogScanner.EARLIEST_OFFSET, 100L);

        assertThat(status.resolveBoundedStartingOffset(bucket, 100L, 150L)).isTrue();
        assertThat(status.getBucketOffset(bucket)).isEqualTo(100L);
        assertThat(status.drainFinishedBuckets()).containsExactly(bucket);
    }

    @Test
    void testOldStoppingOffsetIsRejected() {
        LogScannerStatus status = new LogScannerStatus();
        TableBucket bucket = new TableBucket(1L, 0);

        status.assignBoundedScanBucket(bucket, 100L, 150L);
        status.assignBoundedScanBucket(bucket, 100L, 200L);

        assertThat(status.matchesRequestedRange(bucket, 100L, 150L)).isFalse();
        assertThat(status.matchesRequestedRange(bucket, 100L, 200L)).isTrue();
    }

    @Test
    void testNoSubscribedBucketsAreNotConsideredFinished() {
        LogScannerStatus status = new LogScannerStatus();

        assertThat(status.allSubscribedBucketsFinished()).isFalse();
    }
}

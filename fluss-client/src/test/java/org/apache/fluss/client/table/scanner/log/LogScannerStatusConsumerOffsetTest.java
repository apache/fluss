/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.fluss.client.admin.OffsetMetadata;
import org.apache.fluss.config.ConsumerOffsetReset;
import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests consumer-offset state transitions in {@link LogScannerStatus}. */
class LogScannerStatusConsumerOffsetTest {

    private static final TableBucket TABLE_BUCKET = new TableBucket(1L, null, 0);

    @Test
    void testCommittedOffsetBecomesFetchableAndCommittable() {
        LogScannerStatus status = new LogScannerStatus();
        status.assignFromCommittedOffset(TABLE_BUCKET);

        assertThat(status.missingCommittedOffsets()).containsExactly(TABLE_BUCKET);
        assertThat(status.fetchableBuckets(ignored -> true)).isEmpty();

        status.resolveCommittedOffsets(
                Collections.singletonMap(TABLE_BUCKET, new OffsetMetadata(42L)),
                ConsumerOffsetReset.EARLIEST);

        assertThat(status.missingCommittedOffsets()).isEmpty();
        assertThat(status.getBucketOffset(TABLE_BUCKET)).isEqualTo(42L);
        assertThat(status.allConsumed()).containsEntry(TABLE_BUCKET, new OffsetMetadata(42L));
    }

    @Test
    void testMissingCommittedOffsetUsesResetPolicy() {
        LogScannerStatus earliest = new LogScannerStatus();
        earliest.assignFromCommittedOffset(TABLE_BUCKET);
        earliest.resolveCommittedOffsets(Collections.emptyMap(), ConsumerOffsetReset.EARLIEST);
        assertThat(earliest.getBucketOffset(TABLE_BUCKET)).isEqualTo(LogScanner.EARLIEST_OFFSET);

        LogScannerStatus latest = new LogScannerStatus();
        latest.assignFromCommittedOffset(TABLE_BUCKET);
        latest.resolveCommittedOffsets(Collections.emptyMap(), ConsumerOffsetReset.LATEST);
        // Latest positions are resolved with ListOffsets instead of a fetchable sentinel.
        BucketScanStatus request = latest.beginOffsetResets(0L).get(TABLE_BUCKET);
        assertThat(request).isNotNull();
        assertThat(latest.fetchableBuckets(ignored -> true)).isEmpty();
        assertThat(latest.allConsumed()).isEmpty();
        latest.completeOffsetReset(TABLE_BUCKET, request, 7L);
        assertThat(latest.getBucketOffset(TABLE_BUCKET)).isEqualTo(7L);
    }

    @Test
    void testMissingCommittedOffsetWithNonePolicyFails() {
        LogScannerStatus status = new LogScannerStatus();
        status.assignFromCommittedOffset(TABLE_BUCKET);

        assertThatThrownBy(
                        () ->
                                status.resolveCommittedOffsets(
                                        Collections.emptyMap(), ConsumerOffsetReset.NONE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("reset policy is none")
                .hasMessageContaining(String.valueOf(TABLE_BUCKET));
    }

    @Test
    void testLateResetAfterResubscriptionAndRetryBackoff() {
        LogScannerStatus status = new LogScannerStatus();
        status.assignScanBuckets(Collections.singletonMap(TABLE_BUCKET, LogScanner.LATEST_OFFSET));
        BucketScanStatus oldRequest = status.beginOffsetResets(0L).get(TABLE_BUCKET);
        status.unassignScanBuckets(Collections.singletonList(TABLE_BUCKET));
        status.assignScanBuckets(Collections.singletonMap(TABLE_BUCKET, LogScanner.LATEST_OFFSET));
        BucketScanStatus current = status.beginOffsetResets(0L).get(TABLE_BUCKET);

        status.completeOffsetReset(TABLE_BUCKET, oldRequest, 100L);
        assertThat(
                        status.failOffsetReset(
                                TABLE_BUCKET, oldRequest, new IllegalStateException(), 0L))
                .isFalse();
        assertThat(status.getBucketOffset(TABLE_BUCKET)).isNull();
        assertThat(status.beginOffsetResets(10L)).isEmpty();

        assertThat(status.failOffsetReset(TABLE_BUCKET, current, null, 20L)).isTrue();
        assertThat(status.nextResetTimeMs()).isEqualTo(20L);
        assertThat(status.beginOffsetResets(19L)).isEmpty();
        assertThat(status.beginOffsetResets(20L)).containsOnlyKeys(TABLE_BUCKET);
        status.completeOffsetReset(TABLE_BUCKET, current, 50L);
        assertThat(status.allConsumed()).containsEntry(TABLE_BUCKET, new OffsetMetadata(50L));
    }

    @Test
    void testResetErrorIsReportedOnPollWithoutLosingOtherResets() {
        LogScannerStatus status = new LogScannerStatus();
        TableBucket other = new TableBucket(2L, 0);
        status.assignScanBuckets(Collections.singletonMap(TABLE_BUCKET, LogScanner.LATEST_OFFSET));
        BucketScanStatus request = status.beginOffsetResets(0L).get(TABLE_BUCKET);
        status.assignScanBuckets(Collections.singletonMap(other, LogScanner.LATEST_OFFSET));
        IllegalStateException failure = new IllegalStateException("reset failed");
        status.failOffsetReset(TABLE_BUCKET, request, failure, 0L);
        assertThatThrownBy(() -> status.beginOffsetResets(0L)).isSameAs(failure);
        assertThat(status.beginOffsetResets(0L)).containsOnlyKeys(TABLE_BUCKET, other);
    }

    @Test
    void testCommittedAssignmentSupersedesLatestReset() {
        LogScannerStatus status = new LogScannerStatus();
        status.assignScanBuckets(Collections.singletonMap(TABLE_BUCKET, LogScanner.LATEST_OFFSET));
        status.assignFromCommittedOffset(TABLE_BUCKET);
        status.resolveCommittedOffsets(
                Collections.singletonMap(TABLE_BUCKET, new OffsetMetadata(10L)),
                ConsumerOffsetReset.LATEST);

        assertThat(status.fetchableBuckets(ignored -> true)).containsExactly(TABLE_BUCKET);
        assertThat(status.allConsumed()).containsEntry(TABLE_BUCKET, new OffsetMetadata(10L));
    }

    @Test
    void testExplicitAssignmentOverridesPendingCommittedOffset() {
        LogScannerStatus status = new LogScannerStatus();
        status.assignFromCommittedOffset(TABLE_BUCKET);
        status.assignScanBuckets(Collections.singletonMap(TABLE_BUCKET, 7L));

        assertThat(status.missingCommittedOffsets()).isEmpty();
        assertThat(status.getBucketOffset(TABLE_BUCKET)).isEqualTo(7L);
        assertThat(status.allConsumed()).containsEntry(TABLE_BUCKET, new OffsetMetadata(7L));
    }
}

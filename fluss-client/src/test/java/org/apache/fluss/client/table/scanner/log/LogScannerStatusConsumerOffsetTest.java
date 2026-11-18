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
import org.apache.fluss.exception.NoOffsetForTableBucketException;
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
                Collections.singletonMap(TABLE_BUCKET, new OffsetMetadata(42L)), "earliest");

        assertThat(status.missingCommittedOffsets()).isEmpty();
        assertThat(status.getBucketOffset(TABLE_BUCKET)).isEqualTo(42L);
        assertThat(status.allConsumed()).containsEntry(TABLE_BUCKET, new OffsetMetadata(42L));
    }

    @Test
    void testMissingCommittedOffsetUsesResetPolicy() {
        LogScannerStatus earliest = new LogScannerStatus();
        earliest.assignFromCommittedOffset(TABLE_BUCKET);
        earliest.resolveCommittedOffsets(Collections.emptyMap(), "earliest");
        assertThat(earliest.getBucketOffset(TABLE_BUCKET)).isEqualTo(LogScanner.EARLIEST_OFFSET);

        LogScannerStatus latest = new LogScannerStatus();
        latest.assignFromCommittedOffset(TABLE_BUCKET);
        latest.resolveCommittedOffsets(Collections.emptyMap(), "latest");
        assertThat(latest.getBucketOffset(TABLE_BUCKET)).isEqualTo(LogScanner.LATEST_OFFSET);
    }

    @Test
    void testMissingCommittedOffsetWithNonePolicyFails() {
        LogScannerStatus status = new LogScannerStatus();
        status.assignFromCommittedOffset(TABLE_BUCKET);

        assertThatThrownBy(() -> status.resolveCommittedOffsets(Collections.emptyMap(), "none"))
                .isInstanceOf(NoOffsetForTableBucketException.class);
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

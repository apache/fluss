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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.log.FairBucketStatusMap;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** The status of a {@link LogScanner}. */
@ThreadSafe
@Internal
public class LogScannerStatus {
    private final FairBucketStatusMap<BucketScanStatus> bucketStatusMap;

    private final Set<TableBucket> pendingFinishedBuckets = new LinkedHashSet<>();

    public LogScannerStatus() {
        this.bucketStatusMap = new FairBucketStatusMap<>();
    }

    synchronized boolean prepareToPoll() {
        return bucketStatusMap.size() > 0;
    }

    synchronized void moveBucketToEnd(TableBucket tableBucket) {
        bucketStatusMap.moveToEnd(tableBucket);
    }

    /** Return the offset of the bucket, if the bucket have been unsubscribed, return null. */
    synchronized @Nullable Long getBucketOffset(TableBucket tableBucket) {
        BucketScanStatus bucketScanStatus = bucketStatus(tableBucket);
        if (bucketScanStatus == null) {
            return null;
        } else {
            return bucketScanStatus.getOffset();
        }
    }

    synchronized void updateHighWatermark(TableBucket tableBucket, long highWatermark) {
        bucketStatus(tableBucket).setHighWatermark(highWatermark);
    }

    synchronized void updateOffset(TableBucket tableBucket, long offset) {
        BucketScanStatus status = bucketStatus(tableBucket);
        boolean wasFinished = status.isFinished();
        long nextOffset =
                status.isBounded() ? Math.min(offset, status.getStoppingOffset()) : offset;
        status.setOffset(nextOffset);
        if (!wasFinished && status.isFinished()) {
            pendingFinishedBuckets.add(tableBucket);
        }
    }

    synchronized long recordsLag() {
        long recordsLag = 0L;
        for (BucketScanStatus bucketScanStatus : bucketStatusMap.bucketStatusMap().values()) {
            recordsLag += bucketScanStatus.recordsLag();
        }
        return recordsLag;
    }

    synchronized void assignScanBuckets(Map<TableBucket, Long> scanBucketAndOffsets) {
        for (Map.Entry<TableBucket, Long> entry : scanBucketAndOffsets.entrySet()) {
            assign(entry.getKey(), entry.getValue(), LogScanner.NO_STOPPING_OFFSET);
        }
    }

    synchronized void assignBoundedScanBucket(TableBucket bucket, long start, long stop) {
        checkArgument(
                start >= 0 || start == LogScanner.EARLIEST_OFFSET,
                "Starting offset must be nonnegative or EARLIEST_OFFSET.");
        checkArgument(stop >= 0, "Stopping offset must be nonnegative.");
        assign(bucket, start, stop);
    }

    private void assign(TableBucket bucket, long offset, long stoppingOffset) {
        BucketScanStatus status = bucketStatus(bucket);
        // Preserve the original unbounded reassignment behavior.
        if (stoppingOffset == LogScanner.NO_STOPPING_OFFSET
                && status != null
                && !status.isBounded()) {
            status.setOffset(offset);
        } else {
            status = new BucketScanStatus(offset, stoppingOffset);
        }
        bucketStatusMap.update(bucket, status);
        pendingFinishedBuckets.remove(bucket);
        if (status.isFinished()) {
            pendingFinishedBuckets.add(bucket);
        }
    }

    synchronized void unassignScanBuckets(List<TableBucket> buckets) {
        for (TableBucket bucket : buckets) {
            bucketStatusMap.remove(bucket);
            pendingFinishedBuckets.remove(bucket);
        }
    }

    synchronized List<TableBucket> fetchableBuckets(Predicate<TableBucket> isAvailable) {
        // Since this is in the hot-path for fetching, we do this instead of using java.util.stream
        // API
        List<TableBucket> result = new ArrayList<>();
        bucketStatusMap.forEach(
                (tableBucket, bucketScanStatus) -> {
                    if (!bucketScanStatus.isFinished() && isAvailable.test(tableBucket)) {
                        result.add(tableBucket);
                    }
                });
        return result;
    }

    synchronized boolean allSubscribedBucketsFinished() {
        Map<TableBucket, BucketScanStatus> buckets = bucketStatusMap.bucketStatusMap();
        if (buckets.isEmpty()) {
            return false;
        }
        for (BucketScanStatus status : buckets.values()) {
            if (!status.isFinished()) {
                return false;
            }
        }
        return true;
    }

    synchronized Set<TableBucket> drainFinishedBuckets() {
        Set<TableBucket> finished = new LinkedHashSet<>(pendingFinishedBuckets);
        pendingFinishedBuckets.clear();
        return finished;
    }

    @Nullable
    synchronized SubscriptionSnapshot subscriptionSnapshot(TableBucket bucket) {
        BucketScanStatus status = bucketStatus(bucket);
        return status == null
                ? null
                : new SubscriptionSnapshot(status.getOffset(), status.getStoppingOffset());
    }

    /**
     * Checks that the current subscription still has the offset and stopping offset captured for a
     * request.
     */
    synchronized boolean matchesRequestedRange(
            TableBucket bucket, long expectedOffset, long expectedStoppingOffset) {
        BucketScanStatus status = bucketStatus(bucket);

        return status != null
                && status.getOffset() == expectedOffset
                && status.getStoppingOffset() == expectedStoppingOffset;
    }

    /** Checks the stopping offset independently of the current consumed position. */
    synchronized boolean matchesStoppingOffset(TableBucket bucket, long expectedStoppingOffset) {
        BucketScanStatus status = bucketStatus(bucket);
        return status != null && status.getStoppingOffset() == expectedStoppingOffset;
    }

    /**
     * Advances only when the current read position and stopping offset still match the expected
     * values.
     */
    synchronized boolean updateOffsetIfCurrent(
            TableBucket bucket, long expectedOffset, long expectedStoppingOffset, long nextOffset) {
        if (!matchesRequestedRange(bucket, expectedOffset, expectedStoppingOffset)) {
            return false;
        }
        if (bucketStatus(bucket).isFinished()) {
            return false;
        }
        updateOffset(bucket, nextOffset);
        return true;
    }

    synchronized boolean updateHighWatermarkIfCurrent(
            TableBucket bucket,
            long expectedOffset,
            long expectedStoppingOffset,
            long highWatermark) {
        if (!matchesRequestedRange(bucket, expectedOffset, expectedStoppingOffset)) {
            return false;
        }
        updateHighWatermark(bucket, highWatermark);
        return true;
    }

    /**
     * Resolves bounded EARLIEST using the original Fluss log start offset returned by the server.
     */
    synchronized boolean resolveBoundedStartingOffset(
            TableBucket bucket, long expectedStoppingOffset, long startingOffset) {
        checkArgument(startingOffset >= 0, "Resolved starting offset must be nonnegative.");
        BucketScanStatus status = bucketStatus(bucket);
        if (status == null
                || !status.isBounded()
                || status.isFinished()
                || status.getOffset() != LogScanner.EARLIEST_OFFSET
                || status.getStoppingOffset() != expectedStoppingOffset) {
            return false;
        }
        updateOffset(bucket, startingOffset);
        return true;
    }

    static final class SubscriptionSnapshot {
        final long offset;
        final long stoppingOffset;

        SubscriptionSnapshot(long offset, long stoppingOffset) {
            this.offset = offset;
            this.stoppingOffset = stoppingOffset;
        }

        boolean isBounded() {
            return stoppingOffset != LogScanner.NO_STOPPING_OFFSET;
        }
    }

    private BucketScanStatus bucketStatus(TableBucket tableBucket) {
        return bucketStatusMap.statusValue(tableBucket);
    }
}

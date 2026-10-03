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
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

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
        BucketScanStatus bucketScanStatus = bucketStatus(tableBucket);

        boolean hadReachedStoppingOffset = bucketScanStatus.hasReachedStoppingOffset();
        bucketScanStatus.setOffset(offset);

        if (!hadReachedStoppingOffset && bucketScanStatus.hasReachedStoppingOffset()) {
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
            assignScanBucket(entry.getKey(), entry.getValue(), LogScanner.NO_STOPPING_OFFSET);
        }
    }

    synchronized void assignScanBucket(
            TableBucket tableBucket, long startingOffset, long stoppingOffset) {
        // A new subscription must not inherit an undelivered completion event from
        // the previous subscription of the same bucket.
        pendingFinishedBuckets.remove(tableBucket);

        BucketScanStatus bucketScanStatus = bucketStatus(tableBucket);
        if (bucketScanStatus == null) {
            bucketScanStatus = new BucketScanStatus(startingOffset);
        } else {
            bucketScanStatus.setOffset(startingOffset);
        }
        bucketScanStatus.setStoppingOffset(stoppingOffset);
        bucketStatusMap.update(tableBucket, bucketScanStatus);

        if (bucketScanStatus.hasReachedStoppingOffset()) {
            pendingFinishedBuckets.add(tableBucket);
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
                ((tableBucket, bucketScanStatus) -> {
                    if (!bucketScanStatus.hasReachedStoppingOffset()
                            && isAvailable.test(tableBucket)) {
                        result.add(tableBucket);
                    }
                }));
        return result;
    }

    @Nullable
    synchronized Long resolveBoundedStartingOffset(
            TableBucket tableBucket, long requestedOffset, long resolvedEarliestOffset) {
        checkArgument(
                requestedOffset == LogScanner.EARLIEST_OFFSET,
                "Only EARLIEST_OFFSET can be resolved, but requested offset was %s.",
                requestedOffset);

        checkArgument(
                resolvedEarliestOffset >= 0,
                "Resolved EARLIEST offset must be non-negative, but was %s.",
                resolvedEarliestOffset);

        BucketScanStatus bucketScanStatus = bucketStatus(tableBucket);

        if (bucketScanStatus == null || bucketScanStatus.getOffset() != requestedOffset) {
            return null;
        }

        checkState(
                bucketScanStatus.isBounded(),
                "Cannot resolve an EARLIEST starting offset for unbounded bucket %s.",
                tableBucket);

        long logicalOffset = Math.min(resolvedEarliestOffset, bucketScanStatus.getStoppingOffset());

        updateOffset(tableBucket, logicalOffset);

        return logicalOffset;
    }

    synchronized long getBucketStoppingOffset(TableBucket tableBucket) {
        BucketScanStatus bucketScanStatus = bucketStatus(tableBucket);
        return bucketScanStatus == null
                ? LogScanner.NO_STOPPING_OFFSET
                : bucketScanStatus.getStoppingOffset();
    }

    synchronized boolean hasReachedStoppingOffset(TableBucket tableBucket) {
        BucketScanStatus bucketScanStatus = bucketStatus(tableBucket);
        return bucketScanStatus != null && bucketScanStatus.hasReachedStoppingOffset();
    }

    synchronized boolean hasPendingFinishedBuckets() {
        return !pendingFinishedBuckets.isEmpty();
    }

    synchronized Set<TableBucket> drainFinishedBuckets() {
        if (pendingFinishedBuckets.isEmpty()) {
            return Collections.emptySet();
        }

        Set<TableBucket> finishedBuckets = new LinkedHashSet<>(pendingFinishedBuckets);
        pendingFinishedBuckets.clear();
        return finishedBuckets;
    }

    private BucketScanStatus bucketStatus(TableBucket tableBucket) {
        return bucketStatusMap.statusValue(tableBucket);
    }
}

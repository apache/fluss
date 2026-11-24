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
import org.apache.fluss.client.admin.OffsetMetadata;
import org.apache.fluss.config.ConsumerOffsetReset;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.log.FairBucketStatusMap;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.apache.fluss.client.table.scanner.log.BucketScanStatus.FetchState.AWAITING_COMMITTED_OFFSET;
import static org.apache.fluss.client.table.scanner.log.BucketScanStatus.FetchState.AWAITING_RESET;
import static org.apache.fluss.client.table.scanner.log.BucketScanStatus.FetchState.FETCHING;

/** The status of a {@link LogScanner}. */
@ThreadSafe
@Internal
public class LogScannerStatus {
    private final FairBucketStatusMap<BucketScanStatus> bucketStatusMap;
    // Earliest time at which poll needs to check for resets. Reassignment may make it fire early.
    private long nextResetTimeMs = Long.MAX_VALUE;
    private long assignmentVersion;

    public LogScannerStatus() {
        this.bucketStatusMap = new FairBucketStatusMap<>();
    }

    synchronized boolean prepareToPoll() {
        return bucketStatusMap.size() > 0;
    }

    synchronized void moveBucketToEnd(TableBucket tableBucket) {
        bucketStatusMap.moveToEnd(tableBucket);
    }

    /** Returns the fetch position, or null if the bucket is unassigned or awaiting a position. */
    synchronized @Nullable Long getBucketOffset(TableBucket tableBucket) {
        return getBucketOffset(tableBucket, assignmentVersion);
    }

    synchronized long assignmentVersion() {
        return assignmentVersion;
    }

    /** Ignores data fetched before the bucket's most recent subscription. */
    synchronized @Nullable Long getBucketOffset(TableBucket tableBucket, long requestVersion) {
        BucketScanStatus bucketScanStatus = bucketStatus(tableBucket);
        if (bucketScanStatus == null
                || bucketScanStatus.state() != FETCHING
                || bucketScanStatus.assignmentVersion() > requestVersion) {
            return null;
        } else {
            return bucketScanStatus.getOffset();
        }
    }

    synchronized void updateHighWatermark(TableBucket tableBucket, long highWatermark) {
        bucketStatus(tableBucket).setHighWatermark(highWatermark);
    }

    synchronized void updateOffset(TableBucket tableBucket, long offset) {
        bucketStatus(tableBucket).setOffset(offset);
    }

    synchronized long recordsLag() {
        long recordsLag = 0L;
        for (BucketScanStatus bucketScanStatus : bucketStatusMap.bucketStatusMap().values()) {
            recordsLag += bucketScanStatus.recordsLag();
        }
        return recordsLag;
    }

    synchronized void assignFromCommittedOffset(TableBucket tableBucket) {
        bucketStatusMap.update(tableBucket, new BucketScanStatus(++assignmentVersion));
    }

    synchronized Set<TableBucket> missingCommittedOffsets() {
        Set<TableBucket> missing = new HashSet<>();
        bucketStatusMap.forEach(
                (bucket, status) -> {
                    if (status.state() == AWAITING_COMMITTED_OFFSET) {
                        missing.add(bucket);
                    }
                });
        return missing;
    }

    synchronized void resolveCommittedOffsets(
            Map<TableBucket, OffsetMetadata> committedOffsets, ConsumerOffsetReset resetPolicy) {
        Set<TableBucket> unresolved = new HashSet<>();
        bucketStatusMap.forEach(
                (bucket, status) -> {
                    if (status.state() != AWAITING_COMMITTED_OFFSET) {
                        return;
                    }
                    OffsetMetadata committed = committedOffsets.get(bucket);
                    if (committed != null) {
                        status.setOffset(committed.getOffset());
                    } else if (resetPolicy == ConsumerOffsetReset.EARLIEST) {
                        // Fetch resolves the earliest sentinel on the server.
                        status.setOffset(LogScanner.EARLIEST_OFFSET);
                    } else if (resetPolicy == ConsumerOffsetReset.LATEST) {
                        status.setOffset(LogScanner.LATEST_OFFSET);
                        nextResetTimeMs = 0L;
                    } else {
                        unresolved.add(bucket);
                    }
                });
        if (!unresolved.isEmpty()) {
            throw new IllegalStateException(
                    "No committed offset for table buckets and the reset policy is none: "
                            + unresolved);
        }
    }

    /** Claims eligible resets. Each status instance identifies one subscription to a bucket. */
    synchronized Map<TableBucket, BucketScanStatus> beginOffsetResets(long nowMs) {
        if (nowMs < nextResetTimeMs) {
            return Collections.emptyMap();
        }
        Map<TableBucket, BucketScanStatus> resets = new LinkedHashMap<>();
        long nextRetry = Long.MAX_VALUE;
        for (Map.Entry<TableBucket, BucketScanStatus> entry :
                bucketStatusMap.bucketStatusMap().entrySet()) {
            BucketScanStatus status = entry.getValue();
            if (status.state() == AWAITING_RESET) {
                status.maybeThrowResetError();
                if (status.nextAllowedResetMs() <= nowMs) {
                    resets.put(entry.getKey(), status);
                } else {
                    nextRetry = Math.min(nextRetry, status.nextAllowedResetMs());
                }
            }
        }
        resets.values().forEach(BucketScanStatus::markResetPending);
        nextResetTimeMs = nextRetry;
        return resets;
    }

    synchronized void completeOffsetReset(
            TableBucket bucket, BucketScanStatus assignment, long offset) {
        if (bucketStatus(bucket) == assignment) {
            assignment.setOffset(offset);
        }
    }

    synchronized boolean failOffsetReset(
            TableBucket bucket,
            BucketScanStatus assignment,
            @Nullable Throwable error,
            long retryAtMs) {
        if (bucketStatus(bucket) != assignment) {
            return false;
        }
        assignment.resetFailed(error, retryAtMs);
        nextResetTimeMs = Math.min(nextResetTimeMs, retryAtMs);
        return true;
    }

    synchronized long nextResetTimeMs() {
        return nextResetTimeMs;
    }

    /** Returns a snapshot of the next offsets for subscribed buckets with a known position. */
    public synchronized Map<TableBucket, OffsetMetadata> allConsumed() {
        Map<TableBucket, OffsetMetadata> offsets = new LinkedHashMap<>();
        bucketStatusMap.forEach(
                (tableBucket, status) -> {
                    if (status.state() == FETCHING && status.getOffset() >= 0) {
                        offsets.put(tableBucket, new OffsetMetadata(status.getOffset()));
                    }
                });
        return offsets;
    }

    synchronized void assignScanBuckets(Map<TableBucket, Long> scanBucketAndOffsets) {
        scanBucketAndOffsets.forEach(
                (bucket, offset) -> {
                    bucketStatusMap.update(
                            bucket, new BucketScanStatus(++assignmentVersion, offset));
                    if (offset == LogScanner.LATEST_OFFSET) {
                        nextResetTimeMs = 0L;
                    }
                });
    }

    synchronized void unassignScanBuckets(List<TableBucket> buckets) {
        buckets.forEach(bucketStatusMap::remove);
    }

    synchronized List<TableBucket> fetchableBuckets(Predicate<TableBucket> isAvailable) {
        // Since this is in the hot-path for fetching, we do this instead of using java.util.stream
        // API
        List<TableBucket> result = new ArrayList<>();
        bucketStatusMap.forEach(
                ((tableBucket, bucketScanStatus) -> {
                    if (bucketScanStatus.state() == FETCHING && isAvailable.test(tableBucket)) {
                        result.add(tableBucket);
                    }
                }));
        return result;
    }

    private BucketScanStatus bucketStatus(TableBucket tableBucket) {
        return bucketStatusMap.statusValue(tableBucket);
    }
}

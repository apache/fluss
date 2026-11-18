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
import org.apache.fluss.exception.NoOffsetForTableBucketException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.log.FairBucketStatusMap;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/** The status of a {@link LogScanner}. */
@ThreadSafe
@Internal
public class LogScannerStatus {
    private final FairBucketStatusMap<BucketScanStatus> bucketStatusMap;
    private final Set<TableBucket> awaitingCommittedOffsets = new HashSet<>();

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
        BucketScanStatus status = bucketStatusMap.statusValue(tableBucket);
        if (status == null) {
            status = new BucketScanStatus(0L);
        }
        bucketStatusMap.update(tableBucket, status);
        awaitingCommittedOffsets.add(tableBucket);
    }

    synchronized Set<TableBucket> missingCommittedOffsets() {
        return Collections.unmodifiableSet(new HashSet<>(awaitingCommittedOffsets));
    }

    synchronized void resolveCommittedOffsets(
            Map<TableBucket, OffsetMetadata> committedOffsets, String resetPolicy) {
        Set<TableBucket> unresolved = new HashSet<>();
        Iterator<TableBucket> pending = awaitingCommittedOffsets.iterator();
        while (pending.hasNext()) {
            TableBucket tableBucket = pending.next();
            OffsetMetadata committed = committedOffsets.get(tableBucket);
            long offset;
            if (committed != null) {
                offset = committed.getOffset();
            } else if ("earliest".equals(resetPolicy)) {
                offset = LogScanner.EARLIEST_OFFSET;
            } else if ("latest".equals(resetPolicy)) {
                offset = LogScanner.LATEST_OFFSET;
            } else {
                unresolved.add(tableBucket);
                continue;
            }
            bucketStatusMap.statusValue(tableBucket).setOffset(offset);
            pending.remove();
        }
        if (!unresolved.isEmpty()) {
            throw new NoOffsetForTableBucketException(unresolved);
        }
    }

    synchronized Map<TableBucket, OffsetMetadata> allConsumed() {
        Map<TableBucket, OffsetMetadata> offsets = new LinkedHashMap<>();
        bucketStatusMap.forEach(
                (tableBucket, status) -> {
                    if (!awaitingCommittedOffsets.contains(tableBucket)
                            && status.getOffset() >= 0) {
                        offsets.put(tableBucket, new OffsetMetadata(status.getOffset()));
                    }
                });
        return offsets;
    }

    synchronized void assignScanBuckets(Map<TableBucket, Long> scanBucketAndOffsets) {
        for (Map.Entry<TableBucket, Long> entry : scanBucketAndOffsets.entrySet()) {
            TableBucket scanBucket = entry.getKey();
            long offset = entry.getValue();
            BucketScanStatus bucketScanStatus = bucketStatusMap.statusValue(scanBucket);
            if (bucketScanStatus == null) {
                bucketScanStatus = new BucketScanStatus(offset);
            } else {
                bucketScanStatus.setOffset(offset);
            }
            bucketStatusMap.update(scanBucket, bucketScanStatus);
            awaitingCommittedOffsets.remove(scanBucket);
        }
    }

    synchronized void unassignScanBuckets(List<TableBucket> buckets) {
        for (TableBucket bucket : buckets) {
            bucketStatusMap.remove(bucket);
            awaitingCommittedOffsets.remove(bucket);
        }
    }

    synchronized List<TableBucket> fetchableBuckets(Predicate<TableBucket> isAvailable) {
        // Since this is in the hot-path for fetching, we do this instead of using java.util.stream
        // API
        List<TableBucket> result = new ArrayList<>();
        bucketStatusMap.forEach(
                ((tableBucket, bucketScanStatus) -> {
                    if (!awaitingCommittedOffsets.contains(tableBucket)
                            && isAvailable.test(tableBucket)) {
                        result.add(tableBucket);
                    }
                }));
        return result;
    }

    private BucketScanStatus bucketStatus(TableBucket tableBucket) {
        return bucketStatusMap.statusValue(tableBucket);
    }
}

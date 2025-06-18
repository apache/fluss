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

package com.alibaba.fluss.client.table.scanner.log;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.utils.log.FairBucketStatusMap;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/** The status of a {@link LogScanner}. */
@ThreadSafe
@Internal
public class LogScannerStatus {
    private final FairBucketStatusMap<BucketScanStatus> bucketStatusMap;

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
        }
    }

    synchronized void unassignScanBuckets(List<TableBucket> buckets) {
        for (TableBucket bucket : buckets) {
            bucketStatusMap.remove(bucket);
        }
    }

    synchronized List<TableBucket> fetchableBuckets(Predicate<TableBucket> isAvailable) {
        // Since this is in the hot-path for fetching, we do this instead of using java.util.stream
        // API
        List<TableBucket> result = new ArrayList<>();
        bucketStatusMap.forEach(
                ((tableBucket, bucketScanStatus) -> {
                    if (isAvailable.test(tableBucket)) {
                        result.add(tableBucket);
                    }
                }));
        return result;
    }

    private BucketScanStatus bucketStatus(TableBucket tableBucket) {
        return bucketStatusMap.statusValue(tableBucket);
    }
}

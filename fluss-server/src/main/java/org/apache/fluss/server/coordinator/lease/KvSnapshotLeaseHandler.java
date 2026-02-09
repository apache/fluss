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

package org.apache.fluss.server.coordinator.lease;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.zk.data.lease.KvSnapshotTableLease;
import org.apache.fluss.utils.MapUtils;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/** handler of kv snapshot lease. */
@NotThreadSafe
public class KvSnapshotLeaseHandler {
    private long expirationTime;

    /** A map from table id to kv snapshot lease for one table. */
    private final Map<Long, KvSnapshotTableLease> tableIdToTableLease;

    public KvSnapshotLeaseHandler(long expirationTime) {
        this(expirationTime, MapUtils.newConcurrentHashMap());
    }

    public KvSnapshotLeaseHandler(
            long expirationTime, Map<Long, KvSnapshotTableLease> tableIdToTableLease) {
        this.expirationTime = expirationTime;
        this.tableIdToTableLease = tableIdToTableLease;
    }

    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public Map<Long, KvSnapshotTableLease> getTableIdToTableLease() {
        return tableIdToTableLease;
    }

    /**
     * Acquire a bucket to the lease id. If the bucket array already exists but is smaller than
     * maxBucketNum, the array will be dynamically expanded to {@code maxBucketNum}.
     *
     * @param tableBucket table bucket
     * @param snapshotId snapshot id
     * @param maxBucketNum the max bucket num
     * @return the original registered snapshotId. if -1 means the bucket is new registered
     */
    public long acquireBucket(TableBucket tableBucket, long snapshotId, int maxBucketNum) {
        Long[] bucketSnapshot;
        Long partitionId = tableBucket.getPartitionId();
        long tableId = tableBucket.getTableId();
        int bucketId = tableBucket.getBucket();
        if (partitionId == null) {
            // For none-partitioned table.
            KvSnapshotTableLease tableLease =
                    tableIdToTableLease.computeIfAbsent(
                            tableId,
                            k -> {
                                Long[] array = new Long[maxBucketNum];
                                Arrays.fill(array, -1L);
                                return new KvSnapshotTableLease(tableId, array);
                            });
            bucketSnapshot = tableLease.getBucketSnapshots();
            // Dynamically expand the array if the maxBucketNum exceeds the current array size.
            // This can happen when new buckets are added to an existing table.
            if (bucketSnapshot != null && maxBucketNum > bucketSnapshot.length) {
                bucketSnapshot = expandArray(bucketSnapshot, maxBucketNum);
                tableLease.setBucketSnapshots(bucketSnapshot);
            }
        } else {
            // For partitioned table.

            // first add partition to table.
            KvSnapshotTableLease tableLease =
                    tableIdToTableLease.computeIfAbsent(
                            tableId, k -> new KvSnapshotTableLease(tableId));
            Map<Long, Long[]> partitionSnapshots = tableLease.getPartitionSnapshots();
            // then add bucket to partition.
            bucketSnapshot =
                    partitionSnapshots.computeIfAbsent(
                            partitionId,
                            k -> {
                                Long[] array = new Long[maxBucketNum];
                                Arrays.fill(array, -1L);
                                return array;
                            });
            // Dynamically expand the array if the maxBucketNum exceeds the current array size.
            if (maxBucketNum > bucketSnapshot.length) {
                bucketSnapshot = expandArray(bucketSnapshot, maxBucketNum);
                partitionSnapshots.put(partitionId, bucketSnapshot);
            }
        }

        if (bucketSnapshot == null) {
            throw new IllegalArgumentException(
                    "Bucket snapshot array is null. This may indicate a conflict between "
                            + "partitioned and non-partitioned usage for the same table ID.");
        }

        long originalSnapshotId = bucketSnapshot[bucketId];
        bucketSnapshot[bucketId] = snapshotId;
        return originalSnapshotId;
    }

    /**
     * Release a bucket from the lease id.
     *
     * @param tableBucket table bucket
     * @return the snapshot id of the unregistered bucket, or -1 if the bucket was never registered
     *     or the bucket id exceeds the current array size
     */
    public long releaseBucket(TableBucket tableBucket) {
        Long[] bucketIndex;
        long tableId = tableBucket.getTableId();
        Long partitionId = tableBucket.getPartitionId();
        int bucketId = tableBucket.getBucket();
        KvSnapshotTableLease tableLease = tableIdToTableLease.get(tableId);
        if (partitionId == null) {
            // For none-partitioned table.
            bucketIndex = tableLease.getBucketSnapshots();
        } else {
            // For partitioned table.
            bucketIndex = tableLease.getBucketSnapshots(partitionId);
        }

        Long snapshotId = -1L;
        if (bucketIndex != null) {
            // The bucket id exceeds the current array size, meaning it was never registered
            // under this lease. Return -1 directly.
            if (bucketId >= bucketIndex.length) {
                return -1L;
            }

            snapshotId = bucketIndex[bucketId];
            bucketIndex[bucketId] = -1L;

            boolean needRemove = true;
            for (Long bucket : bucketIndex) {
                if (bucket != -1L) {
                    needRemove = false;
                    break;
                }
            }

            if (needRemove) {
                if (partitionId == null) {
                    tableIdToTableLease.remove(tableId);
                } else {
                    Map<Long, Long[]> partitionSnapshots = tableLease.getPartitionSnapshots();
                    partitionSnapshots.remove(partitionId);
                    if (partitionSnapshots.isEmpty()) {
                        tableIdToTableLease.remove(tableId);
                    }
                }
            }
        }
        return snapshotId;
    }

    public boolean isEmpty() {
        return tableIdToTableLease.isEmpty();
    }

    /**
     * Expand the given array to the specified new size, filling new slots with -1L.
     *
     * @param original the original array
     * @param newSize the desired new size (must be greater than original.length)
     * @return a new expanded array with original values preserved
     */
    private Long[] expandArray(Long[] original, int newSize) {
        Long[] expanded = Arrays.copyOf(original, newSize);
        Arrays.fill(expanded, original.length, newSize, -1L);
        return expanded;
    }

    public int getLeasedSnapshotCount() {
        int count = 0;
        for (KvSnapshotTableLease tableLease : tableIdToTableLease.values()) {
            count += tableLease.getLeasedSnapshotCount();
        }
        return count;
    }

    @Override
    public String toString() {
        return "KvSnapshotLease{"
                + "expirationTime="
                + expirationTime
                + ", tableIdToTableLease="
                + tableIdToTableLease
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KvSnapshotLeaseHandler)) {
            return false;
        }
        KvSnapshotLeaseHandler that = (KvSnapshotLeaseHandler) o;
        return expirationTime == that.expirationTime
                && Objects.equals(tableIdToTableLease, that.tableIdToTableLease);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expirationTime, tableIdToTableLease);
    }
}

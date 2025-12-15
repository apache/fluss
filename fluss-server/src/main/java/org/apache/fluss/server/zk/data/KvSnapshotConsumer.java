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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.MapUtils;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** The zkNode data of kv snapshot consumer. */
@NotThreadSafe
public class KvSnapshotConsumer {
    private final long expirationTime;

    /**
     * mapping from table id to sorted snapshot ids for none-partitioned table, the value is a list
     * of consumed snapshot ids sorted by bucket id.
     */
    private final Map<Long, Long[]> tableIdToSnapshots;

    /** tableId to partition ids for partitioned table. */
    private final Map<Long, Set<Long>> tableIdToPartitions;

    /**
     * mapping from partition id to sorted snapshot ids for partitioned table, the value is a list
     * of consumed snapshot ids sorted by bucket id.
     */
    private final Map<Long, Long[]> partitionIdToSnapshots;

    public KvSnapshotConsumer(long expirationTime) {
        this(
                expirationTime,
                MapUtils.newConcurrentHashMap(),
                MapUtils.newConcurrentHashMap(),
                MapUtils.newConcurrentHashMap());
    }

    public KvSnapshotConsumer(
            long expirationTime,
            Map<Long, Long[]> tableIdToSnapshots,
            Map<Long, Set<Long>> tableIdToPartitions,
            Map<Long, Long[]> partitionIdToSnapshots) {
        this.expirationTime = expirationTime;
        this.tableIdToSnapshots = tableIdToSnapshots;
        this.tableIdToPartitions = tableIdToPartitions;
        this.partitionIdToSnapshots = partitionIdToSnapshots;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public Map<Long, Long[]> getTableIdToSnapshots() {
        return tableIdToSnapshots;
    }

    public Map<Long, Set<Long>> getTableIdToPartitions() {
        return tableIdToPartitions;
    }

    public Map<Long, Long[]> getPartitionIdToSnapshots() {
        return partitionIdToSnapshots;
    }

    /**
     * Register a bucket to the consumer.
     *
     * @param tableBucket table bucket
     * @param snapshotId snapshot id
     * @param bucketNum bucket number of this table or partition
     * @return true if this operation is update, false if this operation is insert
     */
    public boolean registerBucket(TableBucket tableBucket, long snapshotId, int bucketNum) {
        Long[] bucketIndex;
        Long partitionId = tableBucket.getPartitionId();
        long tableId = tableBucket.getTableId();
        int bucketId = tableBucket.getBucket();
        if (partitionId == null) {
            // For none-partitioned table.
            bucketIndex =
                    tableIdToSnapshots.computeIfAbsent(
                            tableId,
                            k -> {
                                Long[] array = new Long[bucketNum];
                                Arrays.fill(array, -1L);
                                return array;
                            });
        } else {
            // For partitioned table.

            // first add partition to table.
            Set<Long> partitions =
                    tableIdToPartitions.computeIfAbsent(tableId, k -> new HashSet<>());
            partitions.add(partitionId);

            // then add bucket to partition.
            bucketIndex =
                    partitionIdToSnapshots.computeIfAbsent(
                            partitionId,
                            k -> {
                                Long[] array = new Long[bucketNum];
                                Arrays.fill(array, -1L);
                                return array;
                            });
        }

        if (bucketIndex.length != bucketNum) {
            throw new IllegalArgumentException(
                    "The input bucket number is not equal to the bucket number of the table.");
        }
        boolean isUpdate = bucketIndex[bucketId] != -1L;
        bucketIndex[bucketId] = snapshotId;
        return isUpdate;
    }

    /**
     * Unregister a bucket from the consumer.
     *
     * @param tableBucket table bucket
     * @return the snapshot id of the unregistered bucket
     */
    public long unregisterBucket(TableBucket tableBucket) {
        Long[] bucketIndex;
        long tableId = tableBucket.getTableId();
        Long partitionId = tableBucket.getPartitionId();
        int bucketId = tableBucket.getBucket();
        if (partitionId == null) {
            // For none-partitioned table.
            bucketIndex = tableIdToSnapshots.get(tableId);
        } else {
            // For partitioned table.
            bucketIndex = partitionIdToSnapshots.get(partitionId);
        }

        Long snapshotId = -1L;
        if (bucketIndex != null) {
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
                    tableIdToSnapshots.remove(tableId);
                } else {
                    tableIdToPartitions.remove(tableId);
                    partitionIdToSnapshots.remove(partitionId);
                }
            }
        }
        return snapshotId;
    }

    public boolean isEmpty() {
        return tableIdToSnapshots.isEmpty()
                && tableIdToPartitions.isEmpty()
                && partitionIdToSnapshots.isEmpty();
    }

    public int getConsumedSnapshotCount() {
        int count = 0;
        for (Long[] buckets : tableIdToSnapshots.values()) {
            for (Long bucket : buckets) {
                if (bucket != -1L) {
                    count++;
                }
            }
        }

        for (Long[] buckets : partitionIdToSnapshots.values()) {
            for (Long bucket : buckets) {
                if (bucket != -1L) {
                    count++;
                }
            }
        }
        return count;
    }

    @Override
    public String toString() {
        String tableSnapshotsStr = formatLongArrayMap(tableIdToSnapshots);
        String partitionSnapshotsStr = formatLongArrayMap(partitionIdToSnapshots);

        return "KvSnapshotConsumer{"
                + "expirationTime="
                + expirationTime
                + ", tableIdToSnapshots="
                + tableSnapshotsStr
                + ", tableIdToPartitions="
                + tableIdToPartitions
                + ", partitionIdToSnapshots="
                + partitionSnapshotsStr
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        KvSnapshotConsumer that = (KvSnapshotConsumer) o;
        if (expirationTime != that.expirationTime) {
            return false;
        }

        if (!deepEqualsMapOfArrays(this.tableIdToSnapshots, that.tableIdToSnapshots)) {
            return false;
        }

        if (!tableIdToPartitions.equals(that.tableIdToPartitions)) {
            return false;
        }
        return deepEqualsMapOfArrays(this.partitionIdToSnapshots, that.partitionIdToSnapshots);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(expirationTime);
        result = 31 * result + deepHashCodeMapOfArrays(tableIdToSnapshots);
        result = 31 * result + Objects.hashCode(tableIdToPartitions);
        result = 31 * result + deepHashCodeMapOfArrays(partitionIdToSnapshots);
        return result;
    }

    private static String formatLongArrayMap(Map<Long, Long[]> map) {
        if (map == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<Long, Long[]> entry : map.entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(entry.getKey()).append("=").append(Arrays.toString(entry.getValue()));
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }

    private static boolean deepEqualsMapOfArrays(Map<Long, Long[]> map1, Map<Long, Long[]> map2) {
        if (map1 == map2) {
            return true;
        }
        if (map1 == null || map2 == null || map1.size() != map2.size()) {
            return false;
        }

        for (Map.Entry<Long, Long[]> entry : map1.entrySet()) {
            Long key = entry.getKey();
            Long[] value1 = entry.getValue();
            Long[] value2 = map2.get(key);

            if (value2 == null) {
                return false;
            }

            if (!Arrays.equals(value1, value2)) {
                return false;
            }
        }
        return true;
    }

    private static int deepHashCodeMapOfArrays(Map<Long, Long[]> map) {
        if (map == null) {
            return 0;
        }
        int hash = 0;
        for (Map.Entry<Long, Long[]> entry : map.entrySet()) {
            Long key = entry.getKey();
            Long[] value = entry.getValue();
            // Combine key hash and array content hash
            hash += Objects.hashCode(key) ^ Arrays.hashCode(value);
        }
        return hash;
    }
}

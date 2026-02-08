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

package org.apache.fluss.server.zk.data.lease;

import org.apache.fluss.utils.MapUtils;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/** The lease of kv snapshot for a table. */
@NotThreadSafe
public class KvSnapshotTableLease {
    private final long tableId;
    private @Nullable Long[] bucketSnapshots;
    private final ConcurrentHashMap<Long, Long[]> partitionSnapshots;

    public KvSnapshotTableLease(long tableId) {
        this(tableId, null, MapUtils.newConcurrentHashMap());
    }

    public KvSnapshotTableLease(long tableId, Long[] bucketSnapshots) {
        this(tableId, bucketSnapshots, MapUtils.newConcurrentHashMap());
    }

    public KvSnapshotTableLease(long tableId, ConcurrentHashMap<Long, Long[]> partitionSnapshots) {
        this(tableId, null, partitionSnapshots);
    }

    public KvSnapshotTableLease(
            long tableId,
            @Nullable Long[] bucketSnapshots,
            ConcurrentHashMap<Long, Long[]> partitionSnapshots) {
        this.tableId = tableId;
        this.bucketSnapshots = bucketSnapshots;
        this.partitionSnapshots = partitionSnapshots;
    }

    public long getTableId() {
        return tableId;
    }

    public @Nullable Long[] getBucketSnapshots() {
        return bucketSnapshots;
    }

    public void setBucketSnapshots(@Nullable Long[] bucketSnapshots) {
        this.bucketSnapshots = bucketSnapshots;
    }

    public @Nullable Long[] getBucketSnapshots(long partitionId) {
        return partitionSnapshots.get(partitionId);
    }

    public Map<Long, Long[]> getPartitionSnapshots() {
        return partitionSnapshots;
    }

    public void addPartitionSnapshots(long partitionId, Long[] snapshots) {
        if (bucketSnapshots != null) {
            throw new IllegalStateException("This is an none partition table lease.");
        }
        partitionSnapshots.put(partitionId, snapshots);
    }

    public int getLeasedSnapshotCount() {
        int count = 0;
        if (bucketSnapshots != null) {
            for (Long snapshot : bucketSnapshots) {
                if (snapshot != -1L) {
                    count++;
                }
            }
        } else {
            for (Long[] snapshots : partitionSnapshots.values()) {
                for (Long snapshot : snapshots) {
                    if (snapshot != -1L) {
                        count++;
                    }
                }
            }
        }
        return count;
    }

    @Override
    public String toString() {
        String partitionSnapshotsStr = formatLongArrayMap(partitionSnapshots);
        return "KvSnapshotTableLease{"
                + "tableId="
                + tableId
                + ", bucketSnapshots="
                + Arrays.toString(bucketSnapshots)
                + ", partitionSnapshots="
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
        KvSnapshotTableLease that = (KvSnapshotTableLease) o;
        return tableId == that.tableId
                && Arrays.equals(bucketSnapshots, that.bucketSnapshots)
                && deepEqualsMapOfArrays(partitionSnapshots, that.partitionSnapshots);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(tableId);
        result = 31 * result + Arrays.hashCode(bucketSnapshots);
        result = 31 * result + deepHashCodeMapOfArrays(partitionSnapshots);
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
            hash = 31 * hash + (Objects.hashCode(key) ^ Arrays.hashCode(value));
        }
        return hash;
    }
}

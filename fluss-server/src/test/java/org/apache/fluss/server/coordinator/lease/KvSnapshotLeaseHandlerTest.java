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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link KvSnapshotLeaseHandler}. */
public class KvSnapshotLeaseHandlerTest {

    @Test
    void testConstructorAndGetters() {
        long expirationTime = 1000L;
        KvSnapshotLeaseHandler kvSnapshotLeasehandle = new KvSnapshotLeaseHandler(expirationTime);

        assertThat(kvSnapshotLeasehandle.getExpirationTime()).isEqualTo(expirationTime);
        assertThat(kvSnapshotLeasehandle.getTableIdToTableLease()).isEmpty();
        assertThat(kvSnapshotLeasehandle.getLeasedSnapshotCount()).isEqualTo(0);
    }

    @Test
    void testRegisterBucketForNonPartitionedTable() {
        KvSnapshotLeaseHandler lease = new KvSnapshotLeaseHandler(1000L);
        long tableId = 1L;
        int bucketId = 0;

        long originalSnapshot = acquireBucket(lease, new TableBucket(tableId, bucketId), 123L);

        assertThat(originalSnapshot).isEqualTo(-1L);
        assertThat(lease.getTableIdToTableLease()).containsKey(tableId);
        KvSnapshotTableLease tableLease = lease.getTableIdToTableLease().get(tableId);
        Long[] bucketSnapshots = tableLease.getBucketSnapshots();
        assertThat(bucketSnapshots).isNotNull();
        assertThat(bucketSnapshots).hasSize(1);
        assertThat(bucketSnapshots[bucketId]).isEqualTo(123L);

        // Register again same bucket → should be update
        originalSnapshot = acquireBucket(lease, new TableBucket(tableId, bucketId), 456L);
        assertThat(originalSnapshot).isEqualTo(123L);
        tableLease = lease.getTableIdToTableLease().get(tableId);
        bucketSnapshots = tableLease.getBucketSnapshots();
        assertThat(bucketSnapshots).isNotNull();
        assertThat(bucketSnapshots[bucketId]).isEqualTo(456L);
    }

    @Test
    void testAcquireBucketDynamicExpansionForNonPartitionedTable() {
        KvSnapshotLeaseHandler lease = new KvSnapshotLeaseHandler(1000L);
        long tableId = 1L;

        // Acquire bucket 0 → creates array of size 1
        acquireBucket(lease, new TableBucket(tableId, 0), 100L);
        Long[] bucketSnapshots = lease.getTableIdToTableLease().get(tableId).getBucketSnapshots();
        assertThat(bucketSnapshots).hasSize(1);
        assertThat(bucketSnapshots[0]).isEqualTo(100L);

        // Acquire bucket 2 → array should expand from size 1 to size 3
        long originalSnapshot = acquireBucket(lease, new TableBucket(tableId, 2), 200L);
        assertThat(originalSnapshot).isEqualTo(-1L);
        bucketSnapshots = lease.getTableIdToTableLease().get(tableId).getBucketSnapshots();
        assertThat(bucketSnapshots).hasSize(3);
        assertThat(bucketSnapshots[0]).isEqualTo(100L);
        assertThat(bucketSnapshots[1]).isEqualTo(-1L);
        assertThat(bucketSnapshots[2]).isEqualTo(200L);

        // Acquire bucket 1 → no expansion needed, just fill in
        originalSnapshot = acquireBucket(lease, new TableBucket(tableId, 1), 150L);
        assertThat(originalSnapshot).isEqualTo(-1L);
        bucketSnapshots = lease.getTableIdToTableLease().get(tableId).getBucketSnapshots();
        assertThat(bucketSnapshots).hasSize(3);
        assertThat(bucketSnapshots[1]).isEqualTo(150L);
    }

    @Test
    void testAcquireBucketDynamicExpansionForPartitionedTable() {
        KvSnapshotLeaseHandler lease = new KvSnapshotLeaseHandler(1000L);
        long tableId = 1L;
        long partitionId = 1000L;

        // Acquire partition 1000, bucket 0 → creates array of size 1
        acquireBucket(lease, new TableBucket(tableId, partitionId, 0), 100L);
        Long[] partitionBuckets =
                lease.getTableIdToTableLease()
                        .get(tableId)
                        .getPartitionSnapshots()
                        .get(partitionId);
        assertThat(partitionBuckets).hasSize(1);
        assertThat(partitionBuckets[0]).isEqualTo(100L);

        // Acquire partition 1000, bucket 2 → array should expand from size 1 to size 3
        long originalSnapshot =
                acquireBucket(lease, new TableBucket(tableId, partitionId, 2), 200L);
        assertThat(originalSnapshot).isEqualTo(-1L);
        partitionBuckets =
                lease.getTableIdToTableLease()
                        .get(tableId)
                        .getPartitionSnapshots()
                        .get(partitionId);
        assertThat(partitionBuckets).hasSize(3);
        assertThat(partitionBuckets[0]).isEqualTo(100L);
        assertThat(partitionBuckets[1]).isEqualTo(-1L);
        assertThat(partitionBuckets[2]).isEqualTo(200L);
    }

    @Test
    void testRegisterBucketForPartitionedTable() {
        KvSnapshotLeaseHandler lease = new KvSnapshotLeaseHandler(1000L);
        long tableId = 1L;

        long originalSnapshot = acquireBucket(lease, new TableBucket(tableId, 1000L, 0), 111L);
        assertThat(originalSnapshot).isEqualTo(-1L);
        // Acquire bucket 1 for partition 1000 → array expands from size 1 to size 2
        originalSnapshot = acquireBucket(lease, new TableBucket(tableId, 1000L, 1), 122L);
        assertThat(originalSnapshot).isEqualTo(-1L);
        originalSnapshot = acquireBucket(lease, new TableBucket(tableId, 1001L, 0), 122L);
        assertThat(originalSnapshot).isEqualTo(-1L);

        Map<Long, KvSnapshotTableLease> tableIdToTableLease = lease.getTableIdToTableLease();
        assertThat(tableIdToTableLease).containsKey(tableId);
        KvSnapshotTableLease tableLease = tableIdToTableLease.get(tableId);
        Map<Long, Long[]> partitionSnapshots = tableLease.getPartitionSnapshots();
        assertThat(partitionSnapshots).containsKeys(1000L, 1001L);
        assertThat(partitionSnapshots.get(1000L)[0]).isEqualTo(111L);
        assertThat(partitionSnapshots.get(1000L)[1]).isEqualTo(122L);
        // Partition 1001 only has bucket 0, so its array size is 1
        assertThat(partitionSnapshots.get(1001L)).hasSize(1);
        assertThat(partitionSnapshots.get(1001L)[0]).isEqualTo(122L);

        // test update.
        originalSnapshot = acquireBucket(lease, new TableBucket(tableId, 1000L, 0), 222L);
        assertThat(originalSnapshot).isEqualTo(111L);
        assertThat(partitionSnapshots.get(1000L)[0]).isEqualTo(222L);
    }

    @Test
    void testReleaseBucket() {
        KvSnapshotLeaseHandler lease = new KvSnapshotLeaseHandler(1000L);
        long tableId = 1L;

        // Register
        acquireBucket(lease, new TableBucket(tableId, 0), 123L);
        assertThat(lease.getLeasedSnapshotCount()).isEqualTo(1);

        // Unregister
        long snapshotId = releaseBucket(lease, new TableBucket(tableId, 0));
        assertThat(snapshotId).isEqualTo(123L);
        assertThat(lease.getLeasedSnapshotCount()).isEqualTo(0);
        assertThat(lease.isEmpty()).isTrue();
    }

    @Test
    void testReleaseBucketExceedingArraySize() {
        KvSnapshotLeaseHandler lease = new KvSnapshotLeaseHandler(1000L);
        long tableId = 1L;

        // Register bucket 0 only → array size is 1
        acquireBucket(lease, new TableBucket(tableId, 0), 100L);
        assertThat(lease.getLeasedSnapshotCount()).isEqualTo(1);

        // Release bucket 5 which exceeds the array size → should return -1
        long snapshotId = releaseBucket(lease, new TableBucket(tableId, 5));
        assertThat(snapshotId).isEqualTo(-1L);
        // The original lease should remain unchanged
        assertThat(lease.getLeasedSnapshotCount()).isEqualTo(1);
        assertThat(lease.isEmpty()).isFalse();
    }

    @Test
    void testReleaseBucketExceedingArraySizeForPartitionedTable() {
        KvSnapshotLeaseHandler lease = new KvSnapshotLeaseHandler(1000L);
        long tableId = 1L;
        long partitionId = 1000L;

        // Register partition 1000, bucket 0 only → array size is 1
        acquireBucket(lease, new TableBucket(tableId, partitionId, 0), 100L);
        assertThat(lease.getLeasedSnapshotCount()).isEqualTo(1);

        // Release partition 1000, bucket 3 which exceeds the array size → should return -1
        long snapshotId = releaseBucket(lease, new TableBucket(tableId, partitionId, 3));
        assertThat(snapshotId).isEqualTo(-1L);
        // The original lease should remain unchanged
        assertThat(lease.getLeasedSnapshotCount()).isEqualTo(1);
        assertThat(lease.isEmpty()).isFalse();
    }

    @Test
    void testGetLeasedSnapshotCount() {
        KvSnapshotLeaseHandler lease = new KvSnapshotLeaseHandler(1000L);

        // Non-partitioned
        acquireBucket(lease, new TableBucket(1L, 0), 100L);
        acquireBucket(lease, new TableBucket(1L, 1), 101L);

        // Partitioned
        acquireBucket(lease, new TableBucket(2L, 20L, 0), 200L);
        acquireBucket(lease, new TableBucket(2L, 21L, 1), 201L);

        assertThat(lease.getLeasedSnapshotCount()).isEqualTo(4);

        // Unregister one
        releaseBucket(lease, new TableBucket(1L, 0));
        assertThat(lease.getLeasedSnapshotCount()).isEqualTo(3);
    }

    @Test
    void testEqualsAndHashCode() {
        KvSnapshotLeaseHandler lease = new KvSnapshotLeaseHandler(1000L);
        assertThat(lease).isEqualTo(lease);
        assertThat(lease.hashCode()).isEqualTo(lease.hashCode());

        KvSnapshotLeaseHandler c1 = new KvSnapshotLeaseHandler(1000L);
        KvSnapshotLeaseHandler c2 = new KvSnapshotLeaseHandler(2000L);
        assertThat(c1).isNotEqualTo(c2);

        // Create two leases with same logical content but different array objects
        Map<Long, KvSnapshotTableLease> map1 = new HashMap<>();
        ConcurrentHashMap<Long, Long[]> partitionSnapshots1 = MapUtils.newConcurrentHashMap();
        partitionSnapshots1.put(2001L, new Long[] {100L, -1L});
        partitionSnapshots1.put(2002L, new Long[] {-1L, 101L});
        map1.put(1L, new KvSnapshotTableLease(1L, new Long[] {100L, -1L}, partitionSnapshots1));
        Map<Long, KvSnapshotTableLease> map2 = new HashMap<>();
        ConcurrentHashMap<Long, Long[]> partitionSnapshots2 = MapUtils.newConcurrentHashMap();
        partitionSnapshots2.put(2001L, new Long[] {100L, -1L});
        partitionSnapshots2.put(2002L, new Long[] {-1L, 101L});
        map2.put(1L, new KvSnapshotTableLease(1L, new Long[] {100L, -1L}, partitionSnapshots2));
        c1 = new KvSnapshotLeaseHandler(1000L, map1);
        c2 = new KvSnapshotLeaseHandler(1000L, map2);
        assertThat(c1).isEqualTo(c2);
        assertThat(c1.hashCode()).isEqualTo(c2.hashCode());

        // different array content.
        map1 = new HashMap<>();
        map1.put(1L, new KvSnapshotTableLease(1L, new Long[] {100L, -1L}));
        map2 = new HashMap<>();
        map2.put(1L, new KvSnapshotTableLease(1L, new Long[] {200L, -1L}));
        c1 = new KvSnapshotLeaseHandler(1000L, map1);
        c2 = new KvSnapshotLeaseHandler(1000L, map2);
        assertThat(c1).isNotEqualTo(c2);
    }

    @Test
    void testToString() {
        KvSnapshotLeaseHandler lease = new KvSnapshotLeaseHandler(1000L);
        acquireBucket(lease, new TableBucket(1L, 0), 100L);
        acquireBucket(lease, new TableBucket(1L, 1), 101L);
        acquireBucket(lease, new TableBucket(2L, 0L, 0), 200L);
        acquireBucket(lease, new TableBucket(2L, 1L, 1), 201L);
        assertThat(lease.toString())
                .isEqualTo(
                        "KvSnapshotLease{expirationTime=1000, tableIdToTableLease={"
                                + "1=KvSnapshotTableLease{tableId=1, bucketSnapshots=[100, 101], partitionSnapshots={}}, "
                                + "2=KvSnapshotTableLease{tableId=2, bucketSnapshots=null, partitionSnapshots={"
                                + "0=[200], 1=[-1, 201]}}}}");
    }

    private long acquireBucket(KvSnapshotLeaseHandler lease, TableBucket tb, long kvSnapshotId) {
        return lease.acquireBucket(tb, kvSnapshotId);
    }

    private long releaseBucket(KvSnapshotLeaseHandler lease, TableBucket tb) {
        return lease.releaseBucket(tb);
    }
}

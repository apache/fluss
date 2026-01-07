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

import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link KvSnapshotLease}. */
public class KvSnapshotLeaseTest {

    private static final int NUM_BUCKET = 2;

    @Test
    void testConstructorAndGetters() {
        long expirationTime = 1000L;
        KvSnapshotLease kvSnapshotLease = new KvSnapshotLease(expirationTime);

        assertThat(kvSnapshotLease.getExpirationTime()).isEqualTo(expirationTime);
        assertThat(kvSnapshotLease.getTableIdToTableLease()).isEmpty();
        assertThat(kvSnapshotLease.getLeasedSnapshotCount()).isEqualTo(0);
    }

    @Test
    void testRegisterBucketForNonPartitionedTable() {
        KvSnapshotLease lease = new KvSnapshotLease(1000L);
        long tableId = 1L;
        int bucketId = 0;

        long originalSnapshot = acquireBucket(lease, new TableBucket(tableId, bucketId), 123L);

        assertThat(originalSnapshot).isEqualTo(-1L);
        assertThat(lease.getTableIdToTableLease()).containsKey(tableId);
        KvSnapshotTableLease tableLease = lease.getTableIdToTableLease().get(tableId);
        Long[] bucketSnapshots = tableLease.getBucketSnapshots();
        assertThat(bucketSnapshots).isNotNull();
        assertThat(bucketSnapshots).hasSize(NUM_BUCKET);
        assertThat(bucketSnapshots[bucketId]).isEqualTo(123L);
        assertThat(bucketSnapshots[1]).isEqualTo(-1L);

        // Register again same bucket → should be update
        originalSnapshot = acquireBucket(lease, new TableBucket(tableId, bucketId), 456L);
        assertThat(originalSnapshot).isEqualTo(123L);
        tableLease = lease.getTableIdToTableLease().get(tableId);
        bucketSnapshots = tableLease.getBucketSnapshots();
        assertThat(bucketSnapshots).isNotNull();
        assertThat(bucketSnapshots[bucketId]).isEqualTo(456L);
    }

    @Test
    void testIllegalBucketNum() {
        // Currently, for the same table, the bucket num should be the same.
        KvSnapshotLease lease = new KvSnapshotLease(1000L);
        long tableId = 1L;
        int bucketId = 0;

        lease.acquireBucket(new TableBucket(tableId, bucketId), 123L, 10);
        assertThatThrownBy(() -> lease.acquireBucket(new TableBucket(tableId, bucketId), 456L, 20))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Bucket index is null, or input bucket number is not equal to the bucket "
                                + "number of the table.");
    }

    @Test
    void testRegisterBucketForPartitionedTable() {
        KvSnapshotLease lease = new KvSnapshotLease(1000L);
        long tableId = 1L;

        long originalSnapshot = acquireBucket(lease, new TableBucket(tableId, 1000L, 0), 111L);
        assertThat(originalSnapshot).isEqualTo(-1L);
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
        assertThat(partitionSnapshots.get(1001L)[0]).isEqualTo(122L);
        assertThat(partitionSnapshots.get(1001L)[1]).isEqualTo(-1L);

        // test update.
        originalSnapshot = acquireBucket(lease, new TableBucket(tableId, 1000L, 0), 222L);
        assertThat(originalSnapshot).isEqualTo(111L);
        assertThat(partitionSnapshots.get(1000L)[0]).isEqualTo(222L);
    }

    @Test
    void testReleaseBucket() {
        KvSnapshotLease lease = new KvSnapshotLease(1000L);
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
    void testGetLeasedSnapshotCount() {
        KvSnapshotLease lease = new KvSnapshotLease(1000L);

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
        KvSnapshotLease lease = new KvSnapshotLease(1000L);
        assertThat(lease).isEqualTo(lease);
        assertThat(lease.hashCode()).isEqualTo(lease.hashCode());

        KvSnapshotLease c1 = new KvSnapshotLease(1000L);
        KvSnapshotLease c2 = new KvSnapshotLease(2000L);
        assertThat(c1).isNotEqualTo(c2);

        // Create two leases with same logical content but different array objects
        Map<Long, KvSnapshotTableLease> map1 = new HashMap<>();
        Map<Long, Long[]> partitionSnapshots1 = new HashMap<>();
        partitionSnapshots1.put(2001L, new Long[] {100L, -1L});
        partitionSnapshots1.put(2002L, new Long[] {-1L, 101L});
        map1.put(1L, new KvSnapshotTableLease(1L, new Long[] {100L, -1L}, partitionSnapshots1));
        Map<Long, KvSnapshotTableLease> map2 = new HashMap<>();
        Map<Long, Long[]> partitionSnapshots2 = new HashMap<>();
        partitionSnapshots2.put(2001L, new Long[] {100L, -1L});
        partitionSnapshots2.put(2002L, new Long[] {-1L, 101L});
        map2.put(1L, new KvSnapshotTableLease(1L, new Long[] {100L, -1L}, partitionSnapshots2));
        c1 = new KvSnapshotLease(1000L, map1);
        c2 = new KvSnapshotLease(1000L, map2);
        assertThat(c1).isEqualTo(c2);
        assertThat(c1.hashCode()).isEqualTo(c2.hashCode());

        // different array content.
        map1 = new HashMap<>();
        map1.put(1L, new KvSnapshotTableLease(1L, new Long[] {100L, -1L}));
        map2 = new HashMap<>();
        map2.put(1L, new KvSnapshotTableLease(1L, new Long[] {200L, -1L}));
        c1 = new KvSnapshotLease(1000L, map1);
        c2 = new KvSnapshotLease(1000L, map2);
        assertThat(c1).isNotEqualTo(c2);
    }

    @Test
    void testToString() {
        KvSnapshotLease lease = new KvSnapshotLease(1000L);
        acquireBucket(lease, new TableBucket(1L, 0), 100L);
        acquireBucket(lease, new TableBucket(1L, 1), 101L);
        acquireBucket(lease, new TableBucket(2L, 0L, 0), 200L);
        acquireBucket(lease, new TableBucket(2L, 1L, 1), 201L);
        assertThat(lease.toString())
                .isEqualTo(
                        "KvSnapshotLease{expirationTime=1000, tableIdToTableLease={"
                                + "1=KvSnapshotTableLease{tableId=1, bucketSnapshots=[100, 101], partitionSnapshots={}}, "
                                + "2=KvSnapshotTableLease{tableId=2, bucketSnapshots=null, partitionSnapshots={"
                                + "0=[200, -1], 1=[-1, 201]}}}}");
    }

    private long acquireBucket(KvSnapshotLease lease, TableBucket tb, long kvSnapshotId) {
        return lease.acquireBucket(tb, kvSnapshotId, NUM_BUCKET);
    }

    private long releaseBucket(KvSnapshotLease lease, TableBucket tb) {
        return lease.releaseBucket(tb);
    }
}

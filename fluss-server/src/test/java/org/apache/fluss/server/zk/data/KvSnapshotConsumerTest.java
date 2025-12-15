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

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link KvSnapshotConsumer}. */
public class KvSnapshotConsumerTest {

    private static final int NUM_BUCKET = 2;

    @Test
    void testConstructorAndGetters() {
        long expirationTime = 1000L;
        KvSnapshotConsumer consumer = new KvSnapshotConsumer(expirationTime);

        assertThat(consumer.getExpirationTime()).isEqualTo(expirationTime);
        assertThat(consumer.getTableIdToSnapshots()).isEmpty();
        assertThat(consumer.getTableIdToPartitions()).isEmpty();
        assertThat(consumer.getPartitionIdToSnapshots()).isEmpty();
    }

    @Test
    void testRegisterBucketForNonPartitionedTable() {
        KvSnapshotConsumer consumer = new KvSnapshotConsumer(1000L);
        long tableId = 1L;
        int bucketId = 0;

        boolean isUpdate = registerBucket(consumer, new TableBucket(tableId, bucketId), 123L);

        assertThat(isUpdate).isFalse();
        assertThat(consumer.getTableIdToSnapshots()).containsKey(tableId);
        Long[] snapshots = consumer.getTableIdToSnapshots().get(tableId);
        assertThat(snapshots).hasSize(NUM_BUCKET);
        assertThat(snapshots[bucketId]).isEqualTo(123L);
        assertThat(snapshots[1]).isEqualTo(-1L);

        // Register again same bucket → should be update
        boolean isUpdate2 = registerBucket(consumer, new TableBucket(tableId, bucketId), 456L);
        assertThat(isUpdate2).isTrue();
        assertThat(consumer.getTableIdToSnapshots().get(tableId)[bucketId]).isEqualTo(456L);
    }

    @Test
    void testIllegalBucketNum() {
        // Currently, for the same table, the bucket num should be the same.
        KvSnapshotConsumer consumer = new KvSnapshotConsumer(1000L);
        long tableId = 1L;
        int bucketId = 0;

        consumer.registerBucket(new TableBucket(tableId, bucketId), 123L, 10);
        assertThatThrownBy(
                        () -> consumer.registerBucket(new TableBucket(tableId, bucketId), 456L, 20))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "The input bucket number is not equal to the bucket number of the table.");
    }

    @Test
    void testRegisterBucketForPartitionedTable() {
        KvSnapshotConsumer consumer = new KvSnapshotConsumer(1000L);
        long tableId = 1L;

        boolean isUpdate = registerBucket(consumer, new TableBucket(tableId, 1000L, 0), 111L);
        assertThat(isUpdate).isFalse();
        isUpdate = registerBucket(consumer, new TableBucket(tableId, 1000L, 1), 122L);
        assertThat(isUpdate).isFalse();
        isUpdate = registerBucket(consumer, new TableBucket(tableId, 1001L, 0), 122L);
        assertThat(isUpdate).isFalse();

        assertThat(consumer.getTableIdToPartitions()).containsKey(tableId);
        Set<Long> partitions = consumer.getTableIdToPartitions().get(tableId);
        assertThat(partitions).contains(1000L, 1001L);

        Map<Long, Long[]> partitionIdToSnapshots = consumer.getPartitionIdToSnapshots();
        assertThat(partitionIdToSnapshots).containsKeys(1000L, 1001L);
        assertThat(partitionIdToSnapshots.get(1000L)[0]).isEqualTo(111L);
        assertThat(partitionIdToSnapshots.get(1000L)[1]).isEqualTo(122L);
        assertThat(partitionIdToSnapshots.get(1001L)[0]).isEqualTo(122L);
        assertThat(partitionIdToSnapshots.get(1001L)[1]).isEqualTo(-1L);

        // test update.
        isUpdate = registerBucket(consumer, new TableBucket(tableId, 1000L, 0), 222L);
        assertThat(isUpdate).isTrue();
        assertThat(partitionIdToSnapshots.get(1000L)[0]).isEqualTo(222L);
    }

    @Test
    void testUnregisterBucket() {
        KvSnapshotConsumer consumer = new KvSnapshotConsumer(1000L);
        long tableId = 1L;

        // Register
        registerBucket(consumer, new TableBucket(tableId, 0), 123L);
        assertThat(consumer.getConsumedSnapshotCount()).isEqualTo(1);

        // Unregister
        long snapshotId = unregisterBucket(consumer, new TableBucket(tableId, 0));
        assertThat(snapshotId).isEqualTo(123L);
        assertThat(consumer.getConsumedSnapshotCount()).isEqualTo(0);
        assertThat(consumer.isEmpty()).isTrue();
    }

    @Test
    void testGetConsumedSnapshotCount() {
        KvSnapshotConsumer consumer = new KvSnapshotConsumer(1000L);

        // Non-partitioned
        registerBucket(consumer, new TableBucket(1L, 0), 100L);
        registerBucket(consumer, new TableBucket(1L, 1), 101L);

        // Partitioned
        registerBucket(consumer, new TableBucket(2L, 20L, 0), 200L);
        registerBucket(consumer, new TableBucket(2L, 21L, 1), 201L);

        assertThat(consumer.getConsumedSnapshotCount()).isEqualTo(4);

        // Unregister one
        unregisterBucket(consumer, new TableBucket(1L, 0));
        assertThat(consumer.getConsumedSnapshotCount()).isEqualTo(3);
    }

    @Test
    void testEqualsAndHashCode() {
        KvSnapshotConsumer consumer = new KvSnapshotConsumer(1000L);
        assertThat(consumer).isEqualTo(consumer);
        assertThat(consumer.hashCode()).isEqualTo(consumer.hashCode());

        KvSnapshotConsumer c1 = new KvSnapshotConsumer(1000L);
        KvSnapshotConsumer c2 = new KvSnapshotConsumer(2000L);
        assertThat(c1).isNotEqualTo(c2);

        // Create two consumers with same logical content but different array objects
        Map<Long, Long[]> map1 = MapUtils.newConcurrentHashMap();
        map1.put(1L, new Long[] {100L, -1L});
        Map<Long, Long[]> map2 = MapUtils.newConcurrentHashMap();
        map2.put(1L, new Long[] {100L, -1L});
        c1 = new KvSnapshotConsumer(1000L, map1, Collections.emptyMap(), Collections.emptyMap());
        c2 = new KvSnapshotConsumer(1000L, map2, Collections.emptyMap(), Collections.emptyMap());
        assertThat(c1).isEqualTo(c2);
        assertThat(c1.hashCode()).isEqualTo(c2.hashCode());

        // different array content.
        map1 = MapUtils.newConcurrentHashMap();
        map1.put(1L, new Long[] {100L, -1L});
        map2 = MapUtils.newConcurrentHashMap();
        map2.put(1L, new Long[] {200L, -1L});
        c1 = new KvSnapshotConsumer(1000L, map1, Collections.emptyMap(), Collections.emptyMap());
        c2 = new KvSnapshotConsumer(1000L, map2, Collections.emptyMap(), Collections.emptyMap());
        assertThat(c1).isNotEqualTo(c2);
    }

    @Test
    void testToString() {
        KvSnapshotConsumer consumer = new KvSnapshotConsumer(1000L);
        registerBucket(consumer, new TableBucket(1L, 0), 100L);
        registerBucket(consumer, new TableBucket(1L, 1), 101L);
        registerBucket(consumer, new TableBucket(2L, 0L, 0), 200L);
        registerBucket(consumer, new TableBucket(2L, 1L, 1), 201L);
        assertThat(consumer.toString())
                .isEqualTo(
                        "KvSnapshotConsumer{expirationTime=1000, "
                                + "tableIdToSnapshots={1=[100, 101]}, "
                                + "tableIdToPartitions={2=[0, 1]}, "
                                + "partitionIdToSnapshots={0=[200, -1], 1=[-1, 201]}}");
    }

    private boolean registerBucket(KvSnapshotConsumer consumer, TableBucket tb, long kvSnapshotId) {
        return consumer.registerBucket(tb, kvSnapshotId, NUM_BUCKET);
    }

    private long unregisterBucket(KvSnapshotConsumer consumer, TableBucket tb) {
        return consumer.unregisterBucket(tb);
    }
}

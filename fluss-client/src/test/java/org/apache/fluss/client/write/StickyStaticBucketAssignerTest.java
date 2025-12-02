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

package org.apache.fluss.client.write;

import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link StickyBucketAssigner}. */
class StickyStaticBucketAssignerTest {
    ServerNode node1 = new ServerNode(1, "localhost", 90, ServerType.TABLET_SERVER, "rack1");
    ServerNode node2 = new ServerNode(2, "localhost", 91, ServerType.TABLET_SERVER, "rack2");
    ServerNode node3 = new ServerNode(3, "localhost", 92, ServerType.TABLET_SERVER, "rack3");
    private final int[] serverNodes = new int[] {node1.id(), node2.id(), node3.id()};
    private final BucketLocation bucket1 =
            new BucketLocation(
                    DATA1_PHYSICAL_TABLE_PATH, DATA1_TABLE_ID, 0, node1.id(), serverNodes);
    private final BucketLocation bucket2 =
            new BucketLocation(
                    DATA1_PHYSICAL_TABLE_PATH, DATA1_TABLE_ID, 1, node1.id(), serverNodes);
    private final BucketLocation bucket3 =
            new BucketLocation(
                    DATA1_PHYSICAL_TABLE_PATH, DATA1_TABLE_ID, 2, node2.id(), serverNodes);

    @Test
    void testSticky() {
        // init cluster.
        Cluster cluster = updateCluster(Arrays.asList(bucket1, bucket2, bucket3));
        StickyBucketAssigner stickyBucketAssigner =
                new StickyBucketAssigner(DATA1_PHYSICAL_TABLE_PATH, 3);
        int bucketId = stickyBucketAssigner.assignBucket(cluster);
        assertThat(bucketId >= 0 && bucketId < 3).isTrue();

        for (int i = 0; i < 10; i++) {
            int newBucketId = stickyBucketAssigner.assignBucket(cluster);
            assertThat(newBucketId >= 0 && newBucketId < 3).isTrue();
            assertThat(newBucketId).isEqualTo(bucketId);
        }

        // on new batch.
        stickyBucketAssigner.onNewBatch(cluster, bucketId);
        int newBucketId = stickyBucketAssigner.assignBucket(cluster);
        assertThat(newBucketId >= 0 && newBucketId < 3).isTrue();
        assertThat(newBucketId).isNotEqualTo(bucketId);

        for (int i = 0; i < 100; i++) {
            int prevBucketId = stickyBucketAssigner.assignBucket(cluster);
            stickyBucketAssigner.onNewBatch(cluster, bucketId);
            int nextBucketId = stickyBucketAssigner.assignBucket(cluster);
            assertThat(prevBucketId).isEqualTo(nextBucketId);
        }
    }

    @Test
    void testBucketIdShouldNotChange() {
        // init cluster.
        Cluster cluster = updateCluster(Arrays.asList(bucket1, bucket2, bucket3));
        StickyBucketAssigner stickyBucketAssigner =
                new StickyBucketAssigner(DATA1_PHYSICAL_TABLE_PATH, 3);
        int bucketId = stickyBucketAssigner.assignBucket(cluster);
        for (int i = 0; i < 3; i++) {
            if (i != bucketId) {
                // If the preBucketId != currentBucketId, the bucket id should not change.
                stickyBucketAssigner.onNewBatch(cluster, i);
                int newBucketId = stickyBucketAssigner.assignBucket(cluster);
                assertThat(newBucketId).isEqualTo(bucketId);
            }
        }
    }

    @Test
    void testOnlyOneAvailableBuckets() {
        // init cluster.
        Cluster cluster = updateCluster(Collections.singletonList(bucket1));
        StickyBucketAssigner stickyBucketAssigner =
                new StickyBucketAssigner(DATA1_PHYSICAL_TABLE_PATH, 3);
        int bucketId = stickyBucketAssigner.assignBucket(cluster);

        for (int i = 0; i < 100; i++) {
            // If there is only one available bucket, the bucket id should not change.
            stickyBucketAssigner.onNewBatch(cluster, bucketId);
            assertThat(stickyBucketAssigner.assignBucket(cluster)).isEqualTo(bucketId);
        }
    }

    @Test
    void testAvailableBucketsTest() {
        PhysicalTablePath tp1 = PhysicalTablePath.of(TablePath.of("db1", "table1"));
        PhysicalTablePath tp2 = PhysicalTablePath.of(TablePath.of("db1", "table2"));
        PhysicalTablePath tp3 = PhysicalTablePath.of(TablePath.of("db1", "table3"));
        List<BucketLocation> allBuckets =
                Arrays.asList(
                        new BucketLocation(tp1, 150001L, 1, null, serverNodes),
                        new BucketLocation(tp1, 150001L, 2, node3.id(), serverNodes),
                        new BucketLocation(tp2, 150002L, 0, null, serverNodes),
                        new BucketLocation(tp2, 150002L, 1, node1.id(), serverNodes),
                        new BucketLocation(tp3, 150003L, 0, null, serverNodes));
        Cluster cluster = updateCluster(allBuckets);

        // Assure we never choose bucket 1 for tp1 because it is unavailable.
        StickyBucketAssigner stickyBucketAssigner = new StickyBucketAssigner(tp1, 3);
        int bucketForTp1 = stickyBucketAssigner.assignBucket(cluster);
        assertThat(bucketForTp1).isNotEqualTo(1);
        for (int i = 0; i < 100; i++) {
            stickyBucketAssigner.onNewBatch(cluster, bucketForTp1);
            assertThat(stickyBucketAssigner.assignBucket(cluster)).isNotEqualTo(1);
        }

        // Assure we always choose bucket 1 for tp2.
        stickyBucketAssigner = new StickyBucketAssigner(tp2, 3);
        int bucketForTp2 = stickyBucketAssigner.assignBucket(cluster);
        assertThat(bucketForTp2).isEqualTo(1);
        for (int i = 0; i < 100; i++) {
            stickyBucketAssigner.onNewBatch(cluster, bucketForTp2);
            assertThat(stickyBucketAssigner.assignBucket(cluster)).isEqualTo(1);
        }

        // Assure that we can still choose one bucket even if there are no available buckets.
        stickyBucketAssigner = new StickyBucketAssigner(tp3, 3);
        int bucketForTp3 = stickyBucketAssigner.assignBucket(cluster);
        assertThat(bucketForTp3).isIn(0, 1, 2);
        stickyBucketAssigner.onNewBatch(cluster, bucketForTp3);
        assertThat(stickyBucketAssigner.assignBucket(cluster)).isIn(0, 1, 2);
    }

    @Test
    void testMultiThreadToCallOnNewBatch() {
        Cluster cluster = updateCluster(Arrays.asList(bucket1, bucket2, bucket3));
        StickyBucketAssigner stickyBucketAssigner =
                new StickyBucketAssigner(PhysicalTablePath.of(DATA1_TABLE_PATH), 3);
        int bucketId = stickyBucketAssigner.assignBucket(cluster);
        Queue<Integer> bucketIds = new ConcurrentLinkedQueue<>();
        Thread[] threads = new Thread[100];
        for (int i = 0; i < 100; i++) {
            threads[i] =
                    new Thread(
                            () -> {
                                stickyBucketAssigner.onNewBatch(cluster, bucketId);
                                int newBucketId = stickyBucketAssigner.assignBucket(cluster);
                                bucketIds.add(newBucketId);
                            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        assertThat(bucketIds)
                .hasSize(100)
                .allMatch(id -> id != bucketId)
                .allMatch(id -> Objects.equals(id, bucketIds.peek()));
    }

    private Cluster updateCluster(List<BucketLocation> bucketLocations) {
        Map<Integer, ServerNode> aliveTabletServersById = new HashMap<>();
        aliveTabletServersById.put(node1.id(), node1);
        aliveTabletServersById.put(node2.id(), node2);
        aliveTabletServersById.put(node3.id(), node3);

        Map<PhysicalTablePath, List<BucketLocation>> bucketsByPath = new HashMap<>();
        Map<TablePath, Long> tableIdByPath = new HashMap<>();
        Map<TablePath, TableInfo> tableInfoByPath = new HashMap<>();
        bucketLocations.forEach(
                bucketLocation -> {
                    PhysicalTablePath physicalTablePath = bucketLocation.getPhysicalTablePath();
                    bucketsByPath
                            .computeIfAbsent(physicalTablePath, k -> new ArrayList<>())
                            .add(bucketLocation);
                    tableIdByPath.put(
                            bucketLocation.getPhysicalTablePath().getTablePath(),
                            bucketLocation.getTableBucket().getTableId());
                    tableInfoByPath.put(
                            physicalTablePath.getTablePath(),
                            TableInfo.of(
                                    physicalTablePath.getTablePath(),
                                    bucketLocation.getTableBucket().getTableId(),
                                    1,
                                    TableDescriptor.builder()
                                            .schema(DATA1_SCHEMA)
                                            .distributedBy(3)
                                            .build(),
                                    System.currentTimeMillis(),
                                    System.currentTimeMillis()));
                });

        return new Cluster(
                aliveTabletServersById,
                new ServerNode(0, "localhost", 89, ServerType.COORDINATOR),
                bucketsByPath,
                tableIdByPath,
                Collections.emptyMap(),
                tableInfoByPath);
    }
}

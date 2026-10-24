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

package org.apache.fluss.server.metadata;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.cluster.TabletServerInfo;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.fluss.record.TestData.DATA1_PARTITIONED_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.server.metadata.PartitionMetadata.DELETED_PARTITION_ID;
import static org.apache.fluss.server.metadata.PartitionMetadata.DELETED_PARTITION_NAME;
import static org.apache.fluss.server.metadata.TableMetadata.DELETED_TABLE_ID;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.NO_LEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link TabletServerMetadataCache}. */
public class TabletServerMetadataCacheTest {

    private TabletServerMetadataCache serverMetadataCache;
    private ServerInfo coordinatorServer;
    private Set<ServerInfo> aliveTableServers;

    private final TablePath partitionedTablePath =
            TablePath.of("test_db_1", "test_partition_table_1");
    private final long partitionTableId = 150002L;
    private final long partitionId1 = 15L;
    private final String partitionName1 = "p1";
    private final long partitionId2 = 16L;
    private final String partitionName2 = "p2";
    private final TableInfo partitionTableInfo =
            TableInfo.of(
                    partitionedTablePath,
                    partitionTableId,
                    0,
                    DATA1_PARTITIONED_TABLE_DESCRIPTOR,
                    DEFAULT_REMOTE_DATA_DIR,
                    100L,
                    100L);
    private List<TableMetadata> tableMetadataList;
    private List<PartitionMetadata> partitionMetadataList;
    private final List<BucketMetadata> initialBucketMetadata =
            Arrays.asList(
                    new BucketMetadata(0, 0, 0, Arrays.asList(0, 1, 2)),
                    new BucketMetadata(1, NO_LEADER, 0, Arrays.asList(1, 0, 2)));
    private final List<BucketMetadata> changedBucket1BucketMetadata =
            Collections.singletonList(new BucketMetadata(1, 1, 0, Arrays.asList(1, 0, 2)));
    private final List<BucketMetadata> afterChangeBucketMetadata =
            Arrays.asList(
                    new BucketMetadata(0, 0, 0, Arrays.asList(0, 1, 2)),
                    new BucketMetadata(1, 1, 0, Arrays.asList(1, 0, 2)));

    @BeforeEach
    public void setup() {
        serverMetadataCache =
                new TabletServerMetadataCache(
                        new TestingMetadataManager(
                                Arrays.asList(DATA1_TABLE_INFO, partitionTableInfo)));
        coordinatorServer =
                new ServerInfo(
                        0,
                        null,
                        Endpoint.fromListenersString(
                                "CLIENT://localhost:99,INTERNAL://localhost:100"),
                        ServerType.COORDINATOR);
        aliveTableServers =
                new HashSet<>(
                        Arrays.asList(
                                new ServerInfo(
                                        0,
                                        "rack0",
                                        Endpoint.fromListenersString(
                                                "CLIENT://localhost:101, INTERNAL://localhost:102"),
                                        ServerType.TABLET_SERVER),
                                new ServerInfo(
                                        1,
                                        "rack1",
                                        Endpoint.fromListenersString("INTERNAL://localhost:103"),
                                        ServerType.TABLET_SERVER),
                                new ServerInfo(
                                        2,
                                        "rack2",
                                        Endpoint.fromListenersString("INTERNAL://localhost:104"),
                                        ServerType.TABLET_SERVER)));
        tableMetadataList =
                Arrays.asList(
                        new TableMetadata(DATA1_TABLE_INFO, initialBucketMetadata),
                        new TableMetadata(partitionTableInfo, Collections.emptyList()));

        partitionMetadataList =
                Arrays.asList(
                        new PartitionMetadata(
                                partitionTableId,
                                partitionName1,
                                partitionId1,
                                initialBucketMetadata),
                        new PartitionMetadata(
                                partitionTableId,
                                partitionName2,
                                partitionId2,
                                initialBucketMetadata));
    }

    @Test
    void testUpdateClusterMetadataRequest() {
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        tableMetadataList,
                        partitionMetadataList));
        assertThat(serverMetadataCache.getCoordinatorServer("CLIENT"))
                .isEqualTo(coordinatorServer.node("CLIENT"));
        assertThat(serverMetadataCache.getCoordinatorServer("INTERNAL"))
                .isEqualTo(coordinatorServer.node("INTERNAL"));
        assertThat(serverMetadataCache.isAliveTabletServer(0)).isTrue();
        assertThat(serverMetadataCache.getAllAliveTabletServers("CLIENT").size()).isEqualTo(1);
        assertThat(serverMetadataCache.getAllAliveTabletServers("INTERNAL").size()).isEqualTo(3);
        assertThat(serverMetadataCache.getAliveTabletServerInfos())
                .containsExactlyInAnyOrder(
                        new TabletServerInfo(0, "rack0"),
                        new TabletServerInfo(1, "rack1"),
                        new TabletServerInfo(2, "rack2"));

        assertThat(serverMetadataCache.getTablePath(DATA1_TABLE_ID).get())
                .isEqualTo(DATA1_TABLE_PATH);
        assertThat(serverMetadataCache.getTablePath(partitionTableId).get())
                .isEqualTo(TablePath.of("test_db_1", "test_partition_table_1"));

        assertTableMetadataEquals(DATA1_TABLE_ID, DATA1_TABLE_INFO, initialBucketMetadata);

        assertPartitionMetadataEquals(
                partitionId1,
                partitionTableId,
                partitionId1,
                partitionName1,
                initialBucketMetadata);
        assertPartitionMetadataEquals(
                partitionId2,
                partitionTableId,
                partitionId2,
                partitionName2,
                initialBucketMetadata);

        ServerMetadataSnapshot original = serverMetadataCache.getSnapshot();
        // test partial update bucket info as setting NO_LEADER to 1 for bucketId = 1
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.singletonList(
                                new TableMetadata(DATA1_TABLE_INFO, changedBucket1BucketMetadata)),
                        Collections.singletonList(
                                new PartitionMetadata(
                                        partitionTableId,
                                        partitionName1,
                                        partitionId1,
                                        changedBucket1BucketMetadata))));
        assertTableMetadataEquals(DATA1_TABLE_ID, DATA1_TABLE_INFO, afterChangeBucketMetadata);

        assertPartitionMetadataEquals(
                partitionId1,
                partitionTableId,
                partitionId1,
                partitionName1,
                afterChangeBucketMetadata);
        assertPartitionMetadataEquals(
                partitionId2,
                partitionTableId,
                partitionId2,
                partitionName2,
                initialBucketMetadata);

        assertThat(original.getBucketMetadataForTable(DATA1_TABLE_ID).values())
                .containsExactlyInAnyOrderElementsOf(initialBucketMetadata);
        assertThat(original.getBucketMetadataForPartition(partitionId1).values())
                .containsExactlyInAnyOrderElementsOf(initialBucketMetadata);
        assertThatThrownBy(() -> original.getBucketMetadataForTable(DATA1_TABLE_ID).clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> original.getBucketMetadataForPartition(partitionId1).clear())
                .isInstanceOf(UnsupportedOperationException.class);

        // test delete one table.
        Set<Long> deletedTableIds =
                serverMetadataCache.updateClusterMetadata(
                        new ClusterMetadata(
                                coordinatorServer,
                                aliveTableServers,
                                Collections.singletonList(
                                        new TableMetadata(
                                                TableInfo.of(
                                                        DATA1_TABLE_PATH,
                                                        DELETED_TABLE_ID, // mark this table as
                                                        // deletion.
                                                        1,
                                                        DATA1_TABLE_DESCRIPTOR,
                                                        DEFAULT_REMOTE_DATA_DIR,
                                                        System.currentTimeMillis(),
                                                        System.currentTimeMillis()),
                                                changedBucket1BucketMetadata)),
                                Collections.emptyList()));
        assertThat(deletedTableIds).containsExactly(DATA1_TABLE_ID);
        assertThat(serverMetadataCache.getTablePath(DATA1_TABLE_ID)).isEmpty();

        // test delete one partition.
        deletedTableIds =
                serverMetadataCache.updateClusterMetadata(
                        new ClusterMetadata(
                                coordinatorServer,
                                aliveTableServers,
                                Collections.emptyList(),
                                Collections.singletonList(
                                        new PartitionMetadata(
                                                partitionTableId,
                                                partitionName1,
                                                DELETED_PARTITION_ID, // mark this partition as
                                                // deletion.
                                                Collections.emptyList()))));
        assertThat(deletedTableIds).isEmpty();
        assertThat(serverMetadataCache.getPhysicalTablePath(partitionId1)).isEmpty();
        assertPartitionMetadataEquals(
                partitionId2,
                partitionTableId,
                partitionId2,
                partitionName2,
                initialBucketMetadata);
    }

    @Test
    void testContainsTableBucket() {
        tableMetadataList =
                Collections.singletonList(
                        new TableMetadata(DATA1_TABLE_INFO, initialBucketMetadata));
        partitionMetadataList =
                Collections.singletonList(
                        new PartitionMetadata(
                                partitionTableId,
                                partitionName1,
                                partitionId1,
                                initialBucketMetadata));
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        tableMetadataList,
                        partitionMetadataList));

        assertThat(
                        serverMetadataCache.contains(
                                new TableBucket(
                                        DATA1_TABLE_INFO.getTableId(),
                                        initialBucketMetadata.get(0).getBucketId())))
                .isTrue();
        assertThat(
                        serverMetadataCache.contains(
                                new TableBucket(
                                        DATA1_TABLE_INFO.getTableId(),
                                        1L,
                                        initialBucketMetadata.get(0).getBucketId())))
                .isFalse();
        assertThat(
                        serverMetadataCache.contains(
                                new TableBucket(DATA1_TABLE_INFO.getTableId(), Integer.MAX_VALUE)))
                .isFalse();

        assertThat(
                        serverMetadataCache.contains(
                                new TableBucket(
                                        partitionTableId,
                                        partitionId1,
                                        initialBucketMetadata.get(0).getBucketId())))
                .isTrue();
        assertThat(
                        serverMetadataCache.contains(
                                new TableBucket(
                                        partitionTableId,
                                        initialBucketMetadata.get(0).getBucketId())))
                .isFalse();
        assertThat(
                        serverMetadataCache.contains(
                                new TableBucket(partitionTableId, partitionId1, Integer.MAX_VALUE)))
                .isFalse();
    }

    private void assertTableMetadataEquals(
            long tableId,
            TableInfo expectedTableInfo,
            List<BucketMetadata> expectedBucketMetadataList) {
        TablePath tablePath = serverMetadataCache.getTablePath(tableId).get();
        TableMetadata tableMetadata = serverMetadataCache.getTableMetadata(tablePath).get();
        assertThat(tableMetadata.getTableInfo()).isEqualTo(expectedTableInfo);
        assertThat(tableMetadata.getBucketMetadataList())
                .hasSameElementsAs(expectedBucketMetadataList);
    }

    @Test
    void testPartitionBucketCountRemovedOnDelete() {
        // Seed both partitions with explicit per-partition bucket counts.
        int explicitBucketCount = 8;
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        tableMetadataList,
                        Arrays.asList(
                                new PartitionMetadata(
                                        partitionTableId,
                                        partitionName1,
                                        partitionId1,
                                        initialBucketMetadata,
                                        explicitBucketCount),
                                new PartitionMetadata(
                                        partitionTableId,
                                        partitionName2,
                                        partitionId2,
                                        initialBucketMetadata,
                                        explicitBucketCount))));
        // The explicit count must be exposed via the updateClusterMetadata path (not the
        // merged bucket-metadata-list size); the delete/re-add asserts below build on this.
        assertThat(
                        serverMetadataCache
                                .getPartitionMetadata(
                                        PhysicalTablePath.of(partitionedTablePath, partitionName1))
                                .get()
                                .getBucketCount())
                .isEqualTo(explicitBucketCount);

        // Delete partition1 via DELETED_PARTITION_ID (partitionId marks deletion).
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.emptyList(),
                        Collections.singletonList(
                                new PartitionMetadata(
                                        partitionTableId,
                                        partitionName1,
                                        DELETED_PARTITION_ID,
                                        Collections.emptyList()))));
        assertThat(
                        serverMetadataCache.getPartitionMetadata(
                                PhysicalTablePath.of(partitionedTablePath, partitionName1)))
                .isEmpty();

        // Re-add partition1 WITHOUT an explicit bucketCount. The cache must NOT return the
        // stale 8; the DELETED_PARTITION_ID path must have removed the prior entry from the
        // partitionBucketCounts map so the fallback (bucketMetadataList.size()) applies.
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.emptyList(),
                        Collections.singletonList(
                                new PartitionMetadata(
                                        partitionTableId,
                                        partitionName1,
                                        partitionId1,
                                        initialBucketMetadata))));
        assertThat(
                        serverMetadataCache
                                .getPartitionMetadata(
                                        PhysicalTablePath.of(partitionedTablePath, partitionName1))
                                .get()
                                .getBucketCount())
                .isEqualTo(initialBucketMetadata.size());

        // Delete partition2 via DELETED_PARTITION_NAME (partitionName marks deletion).
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.emptyList(),
                        Collections.singletonList(
                                new PartitionMetadata(
                                        partitionTableId,
                                        DELETED_PARTITION_NAME,
                                        partitionId2,
                                        Collections.emptyList()))));
        assertThat(
                        serverMetadataCache.getPartitionMetadata(
                                PhysicalTablePath.of(partitionedTablePath, partitionName2)))
                .isEmpty();

        // Re-add partition2 WITHOUT explicit; the DELETED_PARTITION_NAME path must have also
        // cleared partitionBucketCounts, so fallback (list size) applies rather than stale 8.
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.emptyList(),
                        Collections.singletonList(
                                new PartitionMetadata(
                                        partitionTableId,
                                        partitionName2,
                                        partitionId2,
                                        initialBucketMetadata))));
        assertThat(
                        serverMetadataCache
                                .getPartitionMetadata(
                                        PhysicalTablePath.of(partitionedTablePath, partitionName2))
                                .get()
                                .getBucketCount())
                .isEqualTo(initialBucketMetadata.size());
    }

    @Test
    void testUpdatePartitionMetadataPropagatesExplicitBucketCount() {
        // Seed table metadata: updatePartitionMetadata bails out if the tableId is unknown.
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        tableMetadataList,
                        Collections.emptyList()));

        // Route via the single-partition updatePartitionMetadata path (distinct from
        // updateClusterMetadata). The explicit bucketCount must be applied to the cache.
        int explicitBucketCount = 8;
        serverMetadataCache.updatePartitionMetadata(
                new PartitionMetadata(
                        partitionTableId,
                        partitionName1,
                        partitionId1,
                        initialBucketMetadata,
                        explicitBucketCount));
        assertThat(
                        serverMetadataCache
                                .getPartitionMetadata(
                                        PhysicalTablePath.of(partitionedTablePath, partitionName1))
                                .get()
                                .getBucketCount())
                .isEqualTo(explicitBucketCount);
    }

    private void assertPartitionMetadataEquals(
            long partitionId,
            long expectedTableId,
            long expectedPartitionId,
            String expectedPartitionName,
            List<BucketMetadata> expectedBucketMetadataList) {
        String actualPartitionName =
                serverMetadataCache.getPhysicalTablePath(partitionId).get().getPartitionName();
        assertThat(actualPartitionName).isEqualTo(expectedPartitionName);
        PartitionMetadata partitionMetadata =
                serverMetadataCache
                        .getPartitionMetadata(
                                PhysicalTablePath.of(partitionedTablePath, actualPartitionName))
                        .get();
        assertThat(partitionMetadata.getTableId()).isEqualTo(expectedTableId);
        assertThat(partitionMetadata.getPartitionId()).isEqualTo(expectedPartitionId);
        assertThat(partitionMetadata.getPartitionName()).isEqualTo(actualPartitionName);
        assertThat(partitionMetadata.getBucketMetadataList())
                .hasSameElementsAs(expectedBucketMetadataList);
    }

    // Tests merged from TabletServerMetadataCacheExtendedTest - specific metadata operations

    @Test
    void testUpdateTableMetadata() {
        // Given: a table metadata using existing test data
        TableMetadata tableMetadata = new TableMetadata(DATA1_TABLE_INFO, initialBucketMetadata);

        // When: update table metadata
        serverMetadataCache.updateTableMetadata(tableMetadata);

        // Then: verify the table metadata is cached correctly
        assertThat(serverMetadataCache.getTablePath(DATA1_TABLE_ID)).isPresent();
        assertThat(serverMetadataCache.getTablePath(DATA1_TABLE_ID).get())
                .isEqualTo(DATA1_TABLE_PATH);

        TableMetadata cachedMetadata = serverMetadataCache.getTableMetadata(DATA1_TABLE_PATH).get();
        assertThat(cachedMetadata).isNotNull();
        assertThat(cachedMetadata.getTableInfo()).isEqualTo(DATA1_TABLE_INFO);
        assertThat(cachedMetadata.getBucketMetadataList()).hasSameElementsAs(initialBucketMetadata);
    }

    @Test
    void testGetCachedBucketMetadata() {
        assertThat(serverMetadataCache.getCachedBucketMetadata(DATA1_TABLE_PATH, 0)).isEmpty();

        serverMetadataCache.updateTableMetadata(
                new TableMetadata(DATA1_TABLE_INFO, initialBucketMetadata));

        assertThat(serverMetadataCache.getCachedBucketMetadata(DATA1_TABLE_PATH, 0))
                .contains(initialBucketMetadata.get(0));
        assertThat(serverMetadataCache.getCachedBucketMetadata(DATA1_TABLE_PATH, 1))
                .contains(initialBucketMetadata.get(1));
        assertThat(serverMetadataCache.getCachedBucketMetadata(DATA1_TABLE_PATH, 2)).isEmpty();
        assertThat(
                        serverMetadataCache.getCachedBucketMetadata(
                                TablePath.of("unknown_db", "unknown_table"), 0))
                .isEmpty();
    }

    @Test
    void testUpdateTableMetadataMergeWithExisting() {
        // Given: existing table metadata in cache
        TableMetadata existingMetadata = new TableMetadata(DATA1_TABLE_INFO, initialBucketMetadata);
        serverMetadataCache.updateTableMetadata(existingMetadata);

        // When: update with new bucket metadata
        List<BucketMetadata> newBucketMetadata =
                Arrays.asList(
                        new BucketMetadata(0, 1, 1, Arrays.asList(1, 2, 3)),
                        new BucketMetadata(2, 2, 0, Arrays.asList(2, 3, 4)));
        TableMetadata newMetadata = new TableMetadata(DATA1_TABLE_INFO, newBucketMetadata);
        serverMetadataCache.updateTableMetadata(newMetadata);

        // Then: verify the new metadata replaces the old one
        TableMetadata cachedMetadata = serverMetadataCache.getTableMetadata(DATA1_TABLE_PATH).get();
        assertThat(cachedMetadata.getBucketMetadataList()).hasSameElementsAs(newBucketMetadata);
        assertThat(cachedMetadata.getBucketMetadataList())
                .doesNotContainAnyElementsOf(initialBucketMetadata);
    }

    @Test
    void testUpdatePartitionMetadata() {
        // Given: table metadata exists in cache (using existing partition table)
        TableMetadata tableMetadata = new TableMetadata(partitionTableInfo, initialBucketMetadata);
        serverMetadataCache.updateTableMetadata(tableMetadata);

        // When: update partition metadata
        String partitionName = "p_new";
        long partitionId = 2001L;
        PhysicalTablePath partitionPath = PhysicalTablePath.of(partitionedTablePath, partitionName);
        PartitionMetadata partitionMetadata =
                new PartitionMetadata(
                        partitionTableId, partitionName, partitionId, initialBucketMetadata);

        serverMetadataCache.updatePartitionMetadata(partitionMetadata);

        // Then: verify the partition metadata is cached correctly
        assertThat(serverMetadataCache.getPhysicalTablePath(partitionId)).isPresent();
        assertThat(serverMetadataCache.getPhysicalTablePath(partitionId).get())
                .isEqualTo(partitionPath);

        PartitionMetadata cachedMetadata =
                serverMetadataCache.getPartitionMetadata(partitionPath).get();
        assertThat(cachedMetadata).isNotNull();
        assertThat(cachedMetadata.getTableId()).isEqualTo(partitionTableId);
        assertThat(cachedMetadata.getPartitionId()).isEqualTo(partitionId);
        assertThat(cachedMetadata.getPartitionName()).isEqualTo(partitionName);
        assertThat(cachedMetadata.getBucketMetadataList()).hasSameElementsAs(initialBucketMetadata);
    }

    @Test
    void testUpdatePartitionMetadataWithoutTable() {
        // Given: no table metadata in cache
        String partitionName = "p_no_table";
        long partitionId = 2002L;
        PhysicalTablePath partitionPath = PhysicalTablePath.of(partitionedTablePath, partitionName);
        PartitionMetadata partitionMetadata =
                new PartitionMetadata(
                        partitionTableId, partitionName, partitionId, initialBucketMetadata);

        // When: update partition metadata without table
        serverMetadataCache.updatePartitionMetadata(partitionMetadata);

        // Then: partition metadata should not be cached
        assertThat(serverMetadataCache.getPhysicalTablePath(partitionId)).isEmpty();
        assertThat(serverMetadataCache.getPartitionMetadata(partitionPath)).isEmpty();
    }

    @Test
    void testUpdatePartitionMetadataMergeWithExisting() {
        // Given: existing partition metadata in cache (using existing partition table)
        TableMetadata tableMetadata = new TableMetadata(partitionTableInfo, initialBucketMetadata);
        serverMetadataCache.updateTableMetadata(tableMetadata);

        String partitionName = "p_merge";
        long partitionId = 2003L;
        PhysicalTablePath partitionPath = PhysicalTablePath.of(partitionedTablePath, partitionName);
        PartitionMetadata existingPartitionMetadata =
                new PartitionMetadata(
                        partitionTableId, partitionName, partitionId, initialBucketMetadata);
        serverMetadataCache.updatePartitionMetadata(existingPartitionMetadata);

        // When: update with new bucket metadata
        List<BucketMetadata> newBucketMetadata =
                Arrays.asList(new BucketMetadata(0, 1, 1, Arrays.asList(1, 2, 3)));
        PartitionMetadata newPartitionMetadata =
                new PartitionMetadata(
                        partitionTableId, partitionName, partitionId, newBucketMetadata);
        serverMetadataCache.updatePartitionMetadata(newPartitionMetadata);

        // Then: verify the new metadata replaces the old one
        PartitionMetadata cachedMetadata =
                serverMetadataCache.getPartitionMetadata(partitionPath).get();
        assertThat(cachedMetadata.getBucketMetadataList()).hasSameElementsAs(newBucketMetadata);
        assertThat(cachedMetadata.getBucketMetadataList())
                .doesNotContainAnyElementsOf(initialBucketMetadata);
    }

    @Test
    void testConcurrentUpdateTableMetadata() throws InterruptedException {
        // Given: multiple threads updating table metadata with existing test data
        int threadCount = 10;
        Thread[] threads = new Thread[threadCount];

        // When: concurrent updates
        for (int i = 0; i < threadCount; i++) {
            threads[i] =
                    new Thread(
                            () -> {
                                // Use a different bucket configuration for each thread
                                List<BucketMetadata> threadBucketMetadata =
                                        Arrays.asList(
                                                new BucketMetadata(
                                                        0,
                                                        0,
                                                        Thread.currentThread().hashCode() % 3,
                                                        Arrays.asList(0, 1, 2)));
                                TableMetadata tableMetadata =
                                        new TableMetadata(DATA1_TABLE_INFO, threadBucketMetadata);
                                serverMetadataCache.updateTableMetadata(tableMetadata);
                            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        // Then: verify the table path is still available (last update wins)
        assertThat(serverMetadataCache.getTablePath(DATA1_TABLE_ID)).isPresent();
        assertThat(serverMetadataCache.getTablePath(DATA1_TABLE_ID).get())
                .isEqualTo(DATA1_TABLE_PATH);
    }

    @Test
    void testMetadataViewUsesOneSnapshotWithoutLoadingSchema() {
        // A null MetadataManager makes any table/schema read fail immediately.
        TabletServerMetadataCache cache = new TabletServerMetadataCache(null);
        cache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.singletonList(
                                kafkaTable(DATA1_TABLE_ID, "true", initialBucketMetadata)),
                        Collections.emptyList()));
        MetadataView original = cache.getMetadataView("CLIENT", path -> true);
        cache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        Collections.emptySet(),
                        Collections.singletonList(
                                kafkaTable(DATA1_TABLE_ID, "false", changedBucket1BucketMetadata)),
                        Collections.emptyList()));
        MetadataView current = cache.getMetadataView("CLIENT", path -> true);
        assertThat(original.getTabletServers()).hasSize(1);
        assertThat(current.getTabletServers()).isEmpty();
        assertThat(original.getTable(DATA1_TABLE_PATH).get().isKafkaEnabled()).isTrue();
        assertThat(current.getTable(DATA1_TABLE_ID).get().isKafkaEnabled()).isFalse();
        assertThat(original.getTable(DATA1_TABLE_ID).get().getBuckets())
                .containsExactlyInAnyOrderElementsOf(initialBucketMetadata);
        assertThat(current.getTable(DATA1_TABLE_ID).get().getBuckets())
                .containsExactlyInAnyOrderElementsOf(afterChangeBucketMetadata);
        MetadataView hidden = cache.getMetadataView("CLIENT", path -> false);
        assertThat(hidden.getTable(DATA1_TABLE_ID)).isEmpty();
        assertThat(hidden.getTable(DATA1_TABLE_PATH)).isEmpty();
    }

    @Test
    void testMetadataViewRetainsParsedBooleanWithoutProperties() {
        TabletServerMetadataCache cache = new TabletServerMetadataCache(null);
        TableMetadata table = kafkaTable(DATA1_TABLE_ID, "TrUe", initialBucketMetadata);
        Configuration properties = table.getTableInfo().getProperties();
        cache.updateTableMetadata(table);
        MetadataView.Table original =
                cache.getMetadataView("CLIENT", path -> true).getTable(DATA1_TABLE_ID).get();
        assertThat(original.isKafkaEnabled()).isTrue();

        // Removing the property replaces the flag with its default, but keeps the known table.
        properties.removeKey(ConfigOptions.TABLE_KAFKA_ENABLED.key());
        cache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.singletonList(table),
                        Collections.emptyList()));
        assertThat(cache.getMetadataView("CLIENT", path -> true).getTable(DATA1_TABLE_ID))
                .hasValueSatisfying(current -> assertThat(current.isKafkaEnabled()).isFalse());
        assertThat(original.isKafkaEnabled()).isTrue();

        properties.setString(ConfigOptions.TABLE_KAFKA_ENABLED.key(), "invalid");
        cache.updateTableMetadata(table);
        assertThat(cache.getMetadataView("CLIENT", path -> true).getTable(DATA1_TABLE_ID))
                .hasValueSatisfying(current -> assertThat(current.isKafkaEnabled()).isFalse());
    }

    @Test
    void testConcurrentUpdatesPublishRoutingAndPropertiesTogether() throws Exception {
        TabletServerMetadataCache cache = new TabletServerMetadataCache(null);
        ClusterMetadata enabled =
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.singletonList(
                                kafkaTable(DATA1_TABLE_ID, "true", initialBucketMetadata)),
                        Collections.emptyList());
        ClusterMetadata disabled =
                new ClusterMetadata(
                        coordinatorServer,
                        Collections.emptySet(),
                        Collections.singletonList(
                                kafkaTable(DATA1_TABLE_ID, "false", afterChangeBucketMetadata)),
                        Collections.emptyList());
        cache.updateClusterMetadata(enabled);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> writer =
                    executor.submit(
                            () -> {
                                start.await();
                                for (int i = 0; i < 1000; i++) {
                                    cache.updateClusterMetadata(i % 2 == 0 ? disabled : enabled);
                                }
                                return null;
                            });
            Future<?> reader =
                    executor.submit(
                            () -> {
                                start.await();
                                for (int i = 0; i < 1000; i++) {
                                    MetadataView view =
                                            cache.getMetadataView("CLIENT", path -> true);
                                    MetadataView.Table table = view.getTable(DATA1_TABLE_ID).get();
                                    boolean isEnabled = table.isKafkaEnabled();
                                    assertThat(view.getTabletServers()).hasSize(isEnabled ? 1 : 0);
                                    assertThat(table.getBuckets())
                                            .containsExactlyInAnyOrderElementsOf(
                                                    isEnabled
                                                            ? initialBucketMetadata
                                                            : afterChangeBucketMetadata);
                                }
                                return null;
                            });
            start.countDown();
            writer.get();
            reader.get();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testStaleCacheAsideLoadCannotUndoAlterOrDrop() {
        TabletServerMetadataCache cache = new TabletServerMetadataCache(null);
        TableMetadata enabled = kafkaTable(DATA1_TABLE_ID, "true", initialBucketMetadata);
        cache.updateTableMetadata(Collections.singletonList(enabled), cache.getSnapshot());
        ServerMetadataSnapshot beforeAlter = cache.getSnapshot();
        cache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.singletonList(
                                kafkaTable(DATA1_TABLE_ID, "false", Collections.emptyList())),
                        Collections.emptyList()));
        cache.updateTableMetadata(Collections.singletonList(enabled), beforeAlter);
        assertThat(cache.getMetadataView("CLIENT", path -> true).getTable(DATA1_TABLE_ID))
                .hasValueSatisfying(table -> assertThat(table.isKafkaEnabled()).isFalse());
        ServerMetadataSnapshot beforeDrop = cache.getSnapshot();
        cache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.singletonList(
                                kafkaTable(DELETED_TABLE_ID, "false", Collections.emptyList())),
                        Collections.emptyList()));
        cache.updateTableMetadata(Collections.singletonList(enabled), beforeDrop);
        assertThat(cache.getSnapshot().getTableId(DATA1_TABLE_PATH).isPresent()).isFalse();
        assertThat(cache.getMetadataView("CLIENT", path -> true).getTablePaths()).isEmpty();

        // Reusing the same path must discard the previous ID and its cached properties.
        cache.updateTableMetadata(enabled);
        cache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.singletonList(
                                kafkaTable(DATA1_TABLE_ID + 1, "false", initialBucketMetadata)),
                        Collections.emptyList()));
        assertThat(cache.getSnapshot().getTablePath(DATA1_TABLE_ID)).isEmpty();
        MetadataView recreated = cache.getMetadataView("CLIENT", path -> true);
        assertThat(recreated.getTable(DATA1_TABLE_ID)).isEmpty();
        assertThat(recreated.getTable(DATA1_TABLE_PATH))
                .hasValueSatisfying(
                        table -> {
                            assertThat(table.getTableId()).isEqualTo(DATA1_TABLE_ID + 1);
                            assertThat(table.isKafkaEnabled()).isFalse();
                        });
        cache.clearTableMetadata();
        assertThat(cache.getMetadataView("CLIENT", path -> true).getTablePaths()).isEmpty();
    }

    private TableMetadata kafkaTable(long tableId, String enabled, List<BucketMetadata> buckets) {
        TableDescriptor descriptor =
                TableDescriptor.builder(DATA1_TABLE_DESCRIPTOR)
                        .property(ConfigOptions.TABLE_KAFKA_ENABLED.key(), enabled)
                        .build();
        return new TableMetadata(
                TableInfo.of(
                        DATA1_TABLE_PATH, tableId, 0, descriptor, DEFAULT_REMOTE_DATA_DIR, 0, 0),
                buckets);
    }

    @Test
    void testClearTableMetadata() {
        // Given: table and partition metadata in cache (using existing test data)
        TableMetadata tableMetadata = new TableMetadata(DATA1_TABLE_INFO, initialBucketMetadata);
        serverMetadataCache.updateTableMetadata(tableMetadata);

        TableMetadata partitionTableMetadata =
                new TableMetadata(partitionTableInfo, initialBucketMetadata);
        serverMetadataCache.updateTableMetadata(partitionTableMetadata);

        String partitionName = "p_clear";
        long partitionId = 2004L;
        PhysicalTablePath partitionPath = PhysicalTablePath.of(partitionedTablePath, partitionName);
        PartitionMetadata partitionMetadata =
                new PartitionMetadata(
                        partitionTableId, partitionName, partitionId, initialBucketMetadata);
        serverMetadataCache.updatePartitionMetadata(partitionMetadata);

        // When: clear table metadata
        serverMetadataCache.clearTableMetadata();

        // Then: verify all table metadata is cleared
        assertThat(serverMetadataCache.getTablePath(DATA1_TABLE_ID)).isEmpty();
        assertThat(serverMetadataCache.getTableMetadata(DATA1_TABLE_PATH)).isEmpty();
        assertThat(serverMetadataCache.getTablePath(partitionTableId)).isEmpty();
        assertThat(serverMetadataCache.getTableMetadata(partitionedTablePath)).isEmpty();
        assertThat(serverMetadataCache.getPhysicalTablePath(partitionId)).isEmpty();
        assertThat(serverMetadataCache.getPartitionMetadata(partitionPath)).isEmpty();
    }

    private static final class TestingMetadataManager extends MetadataManager {

        private final Map<TablePath, TableInfo> tableInfoMap = new HashMap<>();

        public TestingMetadataManager(List<TableInfo> tableInfos) {
            super(
                    null,
                    new Configuration(),
                    new LakeCatalogDynamicLoader(new Configuration(), null, true));
            tableInfos.forEach(tableInfo -> tableInfoMap.put(tableInfo.getTablePath(), tableInfo));
        }

        @Override
        public TableInfo getTable(TablePath tablePath) throws TableNotExistException {
            TableInfo tableInfo = tableInfoMap.get(tablePath);
            if (tableInfo == null) {
                throw new TableNotExistException("Table '" + tablePath + "' does not exist.");
            }
            return tableInfo;
        }
    }
}

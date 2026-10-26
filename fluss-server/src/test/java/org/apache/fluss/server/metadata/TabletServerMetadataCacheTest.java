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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.record.TestData.DATA1_PARTITIONED_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.server.metadata.PartitionMetadata.DELETED_PARTITION_ID;
import static org.apache.fluss.server.metadata.PartitionMetadata.DELETED_PARTITION_NAME;
import static org.apache.fluss.server.metadata.TableMetadata.DELETED_TABLE_ID;
import static org.apache.fluss.server.metadata.TableMetadata.DELETED_TABLE_PATH;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.NO_LEADER;
import static org.assertj.core.api.Assertions.assertThat;

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
                Arrays.asList(
                        new TableMetadata(DATA1_TABLE_INFO, initialBucketMetadata),
                        new TableMetadata(partitionTableInfo, Collections.emptyList()));
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

    @Test
    void testStaleTableDeletionDoesNotRemoveRecreatedTable() {
        long recreatedTableId = DATA1_TABLE_ID + 1;
        TableInfo recreatedTableInfo =
                TableInfo.of(
                        DATA1_TABLE_PATH,
                        recreatedTableId,
                        0,
                        DATA1_TABLE_DESCRIPTOR,
                        DEFAULT_REMOTE_DATA_DIR,
                        100L,
                        100L);

        // Register the original table.
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.singletonList(
                                new TableMetadata(DATA1_TABLE_INFO, initialBucketMetadata)),
                        Collections.emptyList()));

        // Recreate a table with the same path but a different table ID.
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.singletonList(
                                new TableMetadata(recreatedTableInfo, initialBucketMetadata)),
                        Collections.emptyList()));

        assertThat(serverMetadataCache.getTablePath(recreatedTableId)).contains(DATA1_TABLE_PATH);
        assertThat(serverMetadataCache.contains(new TableBucket(recreatedTableId, 0))).isTrue();

        // The delayed deletion still identifies the original table ID.
        TableInfo deletion =
                TableInfo.of(
                        DELETED_TABLE_PATH,
                        DATA1_TABLE_ID,
                        0,
                        DATA1_TABLE_DESCRIPTOR,
                        DEFAULT_REMOTE_DATA_DIR,
                        -1L,
                        -1L);

        Set<Long> deletedTableIds =
                serverMetadataCache.updateClusterMetadata(
                        new ClusterMetadata(
                                coordinatorServer,
                                aliveTableServers,
                                Collections.singletonList(
                                        new TableMetadata(deletion, Collections.emptyList())),
                                Collections.emptyList()));

        assertThat(deletedTableIds).containsExactly(DATA1_TABLE_ID);
        assertThat(serverMetadataCache.getTablePath(DATA1_TABLE_ID)).isEmpty();

        // The recreated table must retain its mapping and bucket metadata.
        assertThat(serverMetadataCache.getTablePath(recreatedTableId)).contains(DATA1_TABLE_PATH);
        assertThat(serverMetadataCache.contains(new TableBucket(recreatedTableId, 0))).isTrue();
        assertThat(serverMetadataCache.getBucketCountEpoch(recreatedTableId)).isPresent();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testTableAndPartitionRecreationWithStaleDeletion(boolean deletionFirst) {
        long recreatedTableId = partitionTableId + 100L;
        long recreatedPartitionId = partitionId1 + 100L;

        TableInfo recreatedTableInfo =
                TableInfo.of(
                        partitionedTablePath,
                        recreatedTableId,
                        0,
                        DATA1_PARTITIONED_TABLE_DESCRIPTOR,
                        DEFAULT_REMOTE_DATA_DIR,
                        100L,
                        100L);

        TableInfo tableDeletion =
                TableInfo.of(
                        DELETED_TABLE_PATH,
                        partitionTableId,
                        0,
                        DATA1_PARTITIONED_TABLE_DESCRIPTOR,
                        DEFAULT_REMOTE_DATA_DIR,
                        -1L,
                        -1L);

        TableMetadata newTable = new TableMetadata(recreatedTableInfo, Collections.emptyList());
        TableMetadata oldTableDeletion = new TableMetadata(tableDeletion, Collections.emptyList());

        PartitionMetadata newPartition =
                new PartitionMetadata(
                        recreatedTableId,
                        partitionName1,
                        recreatedPartitionId,
                        initialBucketMetadata,
                        2);
        PartitionMetadata oldPartitionDeletion =
                new PartitionMetadata(
                        partitionTableId,
                        DELETED_PARTITION_NAME,
                        partitionId1,
                        Collections.emptyList());

        // Start with the original table and partition registered.
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.singletonList(
                                new TableMetadata(partitionTableInfo, Collections.emptyList())),
                        Collections.singletonList(
                                new PartitionMetadata(
                                        partitionTableId,
                                        partitionName1,
                                        partitionId1,
                                        initialBucketMetadata,
                                        2))));

        List<TableMetadata> tableUpdates =
                deletionFirst
                        ? Arrays.asList(oldTableDeletion, newTable)
                        : Arrays.asList(newTable, oldTableDeletion);

        List<PartitionMetadata> partitionUpdates =
                deletionFirst
                        ? Arrays.asList(oldPartitionDeletion, newPartition)
                        : Arrays.asList(newPartition, oldPartitionDeletion);

        Set<Long> deletedTableIds =
                serverMetadataCache.updateClusterMetadata(
                        new ClusterMetadata(
                                coordinatorServer,
                                aliveTableServers,
                                tableUpdates,
                                partitionUpdates));

        PhysicalTablePath partitionPath =
                PhysicalTablePath.of(partitionedTablePath, partitionName1);

        assertThat(deletedTableIds).containsExactly(partitionTableId);
        assertThat(serverMetadataCache.getTablePath(partitionTableId)).isEmpty();
        assertThat(serverMetadataCache.getPhysicalTablePath(partitionId1)).isEmpty();

        assertThat(serverMetadataCache.getTablePath(recreatedTableId))
                .contains(partitionedTablePath);
        assertThat(serverMetadataCache.getPhysicalTablePath(recreatedPartitionId))
                .contains(partitionPath);

        assertThat(
                        serverMetadataCache.contains(
                                new TableBucket(recreatedTableId, recreatedPartitionId, 0)))
                .isTrue();
        assertThat(serverMetadataCache.contains(new TableBucket(partitionTableId, partitionId1, 0)))
                .isFalse();
    }

    @Test
    void testUpdateTableMetadataRemovesPreviousPath() {
        TablePath renamedPath = TablePath.of(DATA1_TABLE_PATH.getDatabaseName(), "renamed_table");
        TableInfo renamedTableInfo =
                TableInfo.of(
                        renamedPath,
                        DATA1_TABLE_ID,
                        0,
                        DATA1_TABLE_DESCRIPTOR,
                        DEFAULT_REMOTE_DATA_DIR,
                        100L,
                        100L);

        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.singletonList(
                                new TableMetadata(DATA1_TABLE_INFO, initialBucketMetadata)),
                        Collections.emptyList()));

        serverMetadataCache.updateTableMetadata(
                new TableMetadata(renamedTableInfo, initialBucketMetadata));
        assertThat(serverMetadataCache.getTablePath(DATA1_TABLE_ID)).contains(renamedPath);
        assertThat(serverMetadataCache.getTableMetadata(DATA1_TABLE_PATH)).isEmpty();
    }

    @Test
    void testTableAndPartitionDeletionInSameMetadataUpdate() {
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.singletonList(
                                new TableMetadata(partitionTableInfo, Collections.emptyList())),
                        Collections.singletonList(
                                new PartitionMetadata(
                                        partitionTableId,
                                        partitionName1,
                                        partitionId1,
                                        initialBucketMetadata,
                                        2))));

        assertThat(serverMetadataCache.getTablePath(partitionTableId))
                .contains(partitionedTablePath);
        assertThat(serverMetadataCache.getPhysicalTablePath(partitionId1))
                .contains(PhysicalTablePath.of(partitionedTablePath, partitionName1));
        TableInfo tableDeletion =
                TableInfo.of(
                        DELETED_TABLE_PATH,
                        partitionTableId,
                        0,
                        DATA1_PARTITIONED_TABLE_DESCRIPTOR,
                        DEFAULT_REMOTE_DATA_DIR,
                        -1L,
                        -1L);
        PartitionMetadata partitionDeletion =
                new PartitionMetadata(
                        partitionTableId,
                        DELETED_PARTITION_NAME,
                        partitionId1,
                        Collections.emptyList());
        Set<Long> deletedTableIds =
                serverMetadataCache.updateClusterMetadata(
                        new ClusterMetadata(
                                coordinatorServer,
                                aliveTableServers,
                                Collections.singletonList(
                                        new TableMetadata(tableDeletion, Collections.emptyList())),
                                Collections.singletonList(partitionDeletion)));
        assertThat(deletedTableIds).containsExactly(partitionTableId);
        assertThat(serverMetadataCache.getTablePath(partitionTableId)).isEmpty();
        assertThat(serverMetadataCache.getPhysicalTablePath(partitionId1)).isEmpty();
        assertThat(serverMetadataCache.contains(new TableBucket(partitionTableId, partitionId1, 0)))
                .isFalse();
    }

    @Test
    void testStalePartitionDeletionDoesNotRemoveRecreatedPartition() {
        long recreatedPartitionId = partitionId1 + 100L;
        PhysicalTablePath partitionPath =
                PhysicalTablePath.of(partitionedTablePath, partitionName1);
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.singletonList(
                                new TableMetadata(partitionTableInfo, Collections.emptyList())),
                        Collections.singletonList(
                                new PartitionMetadata(
                                        partitionTableId,
                                        partitionName1,
                                        partitionId1,
                                        initialBucketMetadata,
                                        2))));
        // Recreate the same partition name with a new partition ID.
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.emptyList(),
                        Collections.singletonList(
                                new PartitionMetadata(
                                        partitionTableId,
                                        partitionName1,
                                        recreatedPartitionId,
                                        initialBucketMetadata,
                                        2))));

        assertThat(serverMetadataCache.getPhysicalTablePath(recreatedPartitionId))
                .contains(partitionPath);
        // The delayed deletion refers to the original partition ID.
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.emptyList(),
                        Collections.singletonList(
                                new PartitionMetadata(
                                        partitionTableId,
                                        DELETED_PARTITION_NAME,
                                        partitionId1,
                                        Collections.emptyList()))));

        assertThat(serverMetadataCache.getPhysicalTablePath(partitionId1)).isEmpty();
        assertThat(serverMetadataCache.getPhysicalTablePath(recreatedPartitionId))
                .contains(partitionPath);
        assertThat(
                        serverMetadataCache.contains(
                                new TableBucket(partitionTableId, recreatedPartitionId, 0)))
                .isTrue();
        assertThat(serverMetadataCache.getPartitionMetadata(partitionPath)).isPresent();
    }

    @Test
    void testUpdatePartitionMetadataRemovesPreviousPath() {
        serverMetadataCache.updateTableMetadata(
                new TableMetadata(partitionTableInfo, Collections.emptyList()));
        serverMetadataCache.updatePartitionMetadata(
                new PartitionMetadata(
                        partitionTableId, partitionName1, partitionId1, initialBucketMetadata));
        String renamedPartitionName = "renamed_partition";
        serverMetadataCache.updatePartitionMetadata(
                new PartitionMetadata(
                        partitionTableId,
                        renamedPartitionName,
                        partitionId1,
                        initialBucketMetadata));
        PhysicalTablePath oldPath = PhysicalTablePath.of(partitionedTablePath, partitionName1);
        PhysicalTablePath newPath =
                PhysicalTablePath.of(partitionedTablePath, renamedPartitionName);
        assertThat(serverMetadataCache.getPhysicalTablePath(partitionId1)).contains(newPath);
        assertThat(serverMetadataCache.getPartitionMetadata(oldPath)).isEmpty();
        assertThat(serverMetadataCache.getPartitionMetadata(newPath)).isPresent();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRecreatedTableDoesNotExposePreviousPartitions(boolean deleteOldTableFirst) {
        long recreatedTableId = partitionTableId + 100L;
        PhysicalTablePath partitionPath =
                PhysicalTablePath.of(partitionedTablePath, partitionName1);
        registerOriginalPartitionedTable();
        assertThat(serverMetadataCache.getPartitionMetadata(partitionPath)).isPresent();
        assertThat(serverMetadataCache.getPhysicalTablePath(partitionId1)).contains(partitionPath);
        if (deleteOldTableFirst) {
            TableInfo deletion =
                    TableInfo.of(
                            DELETED_TABLE_PATH,
                            partitionTableId,
                            0,
                            DATA1_PARTITIONED_TABLE_DESCRIPTOR,
                            DEFAULT_REMOTE_DATA_DIR,
                            -1L,
                            -1L);

            serverMetadataCache.updateClusterMetadata(
                    new ClusterMetadata(
                            coordinatorServer,
                            aliveTableServers,
                            Collections.singletonList(
                                    new TableMetadata(deletion, Collections.emptyList())),
                            Collections.emptyList()));
        }
        // Recreate the table before deleting the old partition.
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.singletonList(
                                new TableMetadata(
                                        recreatedPartitionedTableInfo(recreatedTableId),
                                        Collections.emptyList())),
                        Collections.emptyList()));

        assertThat(serverMetadataCache.getTablePath(recreatedTableId))
                .contains(partitionedTablePath);
        assertThat(serverMetadataCache.getPartitionMetadata(partitionPath)).isEmpty();
        assertThat(serverMetadataCache.getPhysicalTablePath(partitionId1)).isEmpty();
        assertThat(serverMetadataCache.contains(new TableBucket(partitionTableId, partitionId1, 0)))
                .isTrue();
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.emptyList(),
                        Collections.singletonList(
                                new PartitionMetadata(
                                        partitionTableId,
                                        DELETED_PARTITION_NAME,
                                        partitionId1,
                                        Collections.emptyList()))));

        assertThat(serverMetadataCache.contains(new TableBucket(partitionTableId, partitionId1, 0)))
                .isFalse();
        assertThat(serverMetadataCache.getPartitionMetadata(partitionPath)).isEmpty();
        assertThat(serverMetadataCache.getTablePath(recreatedTableId))
                .contains(partitionedTablePath);
    }

    @Test
    void testRecreatedTableAndPartitionReplaceOldPartitionMapping() {
        long recreatedTableId = partitionTableId + 100L;
        long recreatedPartitionId = partitionId1 + 100L;
        PhysicalTablePath partitionPath =
                PhysicalTablePath.of(partitionedTablePath, partitionName1);
        registerOriginalPartitionedTable();
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.singletonList(
                                new TableMetadata(
                                        recreatedPartitionedTableInfo(recreatedTableId),
                                        Collections.emptyList())),
                        Collections.singletonList(
                                new PartitionMetadata(
                                        recreatedTableId,
                                        partitionName1,
                                        recreatedPartitionId,
                                        initialBucketMetadata,
                                        2))));

        PartitionMetadata metadata = serverMetadataCache.getPartitionMetadata(partitionPath).get();

        assertThat(metadata.getTableId()).isEqualTo(recreatedTableId);
        assertThat(metadata.getPartitionId()).isEqualTo(recreatedPartitionId);
        assertThat(metadata.getBucketCount()).isEqualTo(2);
        assertThat(serverMetadataCache.getPhysicalTablePath(partitionId1)).isEmpty();
        assertThat(serverMetadataCache.getPhysicalTablePath(recreatedPartitionId))
                .contains(partitionPath);
        // A delayed deletion for the old partition must leave the new one intact.
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.emptyList(),
                        Collections.singletonList(
                                new PartitionMetadata(
                                        partitionTableId,
                                        DELETED_PARTITION_NAME,
                                        partitionId1,
                                        Collections.emptyList()))));

        PartitionMetadata current = serverMetadataCache.getPartitionMetadata(partitionPath).get();
        assertThat(current.getTableId()).isEqualTo(recreatedTableId);
        assertThat(current.getPartitionId()).isEqualTo(recreatedPartitionId);
        assertThat(
                        serverMetadataCache.contains(
                                new TableBucket(recreatedTableId, recreatedPartitionId, 0)))
                .isTrue();
    }

    @Test
    void testSingleTableUpdateInvalidatesOldPartitionsButKeepsNewOnRefresh() {
        long recreatedTableId = partitionTableId + 100L;
        long recreatedPartitionId = partitionId1 + 100L;
        PhysicalTablePath partitionPath =
                PhysicalTablePath.of(partitionedTablePath, partitionName1);
        registerOriginalPartitionedTable();
        TableInfo recreatedTableInfo = recreatedPartitionedTableInfo(recreatedTableId);
        serverMetadataCache.updateTableMetadata(
                new TableMetadata(recreatedTableInfo, Collections.emptyList()));

        assertThat(serverMetadataCache.getPartitionMetadata(partitionPath)).isEmpty();
        assertThat(serverMetadataCache.getPhysicalTablePath(partitionId1)).isEmpty();

        serverMetadataCache.updatePartitionMetadata(
                new PartitionMetadata(
                        recreatedTableId,
                        partitionName1,
                        recreatedPartitionId,
                        initialBucketMetadata,
                        2));
        PartitionMetadata newPartition =
                serverMetadataCache.getPartitionMetadata(partitionPath).get();
        assertThat(newPartition.getTableId()).isEqualTo(recreatedTableId);
        assertThat(newPartition.getPartitionId()).isEqualTo(recreatedPartitionId);
        // Refreshing the same table must not invalidate its own partitions.
        serverMetadataCache.updateTableMetadata(
                new TableMetadata(recreatedTableInfo, Collections.emptyList()));
        PartitionMetadata refreshedPartition =
                serverMetadataCache.getPartitionMetadata(partitionPath).get();
        assertThat(refreshedPartition.getTableId()).isEqualTo(recreatedTableId);
        assertThat(refreshedPartition.getPartitionId()).isEqualTo(recreatedPartitionId);
        assertThat(
                        serverMetadataCache.contains(
                                new TableBucket(recreatedTableId, recreatedPartitionId, 0)))
                .isTrue();
    }

    private void registerOriginalPartitionedTable() {
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.singletonList(
                                new TableMetadata(partitionTableInfo, Collections.emptyList())),
                        Collections.singletonList(
                                new PartitionMetadata(
                                        partitionTableId,
                                        partitionName1,
                                        partitionId1,
                                        initialBucketMetadata,
                                        2))));
    }

    private TableInfo recreatedPartitionedTableInfo(long tableId) {
        return TableInfo.of(
                partitionedTablePath,
                tableId,
                0,
                DATA1_PARTITIONED_TABLE_DESCRIPTOR,
                DEFAULT_REMOTE_DATA_DIR,
                100L,
                100L);
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

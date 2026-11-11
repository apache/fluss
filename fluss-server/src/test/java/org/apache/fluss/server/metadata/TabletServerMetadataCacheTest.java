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
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.zk.data.PartitionRegistration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import static org.apache.fluss.server.metadata.TableMetadata.DELETED_TABLE_PATH;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.NO_LEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link TabletServerMetadataCache}. */
public class TabletServerMetadataCacheTest {

    private TestingMetadataManager metadataManager;
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
                    new BucketMetadata(1, NO_LEADER, 0, Arrays.asList(1, 0, 2)),
                    new BucketMetadata(2, null, 0, Arrays.asList(2, 0, 1)));
    private final List<BucketMetadata> changedBucket1BucketMetadata =
            Collections.singletonList(new BucketMetadata(1, 1, 0, Arrays.asList(1, 0, 2)));
    private final List<BucketMetadata> afterChangeBucketMetadata =
            Arrays.asList(
                    new BucketMetadata(0, 0, 0, Arrays.asList(0, 1, 2)),
                    new BucketMetadata(1, 1, 0, Arrays.asList(1, 0, 2)),
                    new BucketMetadata(2, null, 0, Arrays.asList(2, 0, 1)));

    @BeforeEach
    public void setup() {
        metadataManager =
                new TestingMetadataManager(Arrays.asList(DATA1_TABLE_INFO, partitionTableInfo));
        serverMetadataCache = new TabletServerMetadataCache(metadataManager);
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
                                initialBucketMetadata,
                                initialBucketMetadata.size()),
                        new PartitionMetadata(
                                partitionTableId,
                                partitionName2,
                                partitionId2,
                                initialBucketMetadata,
                                initialBucketMetadata.size()));
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
                                        changedBucket1BucketMetadata,
                                        null))));
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
                                                Collections.emptyList(),
                                                null))));
        assertThat(deletedTableIds).isEmpty();
        assertThat(serverMetadataCache.getPhysicalTablePath(partitionId1)).isEmpty();
        assertPartitionMetadataEquals(
                partitionId2,
                partitionTableId,
                partitionId2,
                partitionName2,
                initialBucketMetadata);
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 3})
    void testInvalidBucketIdsAreCacheMisses(int invalidBucketId) {
        List<BucketMetadata> buckets = bucketMetadata(3);
        buckets.set(2, new BucketMetadata(invalidBucketId, 0, 0, Arrays.asList(0, 1, 2)));
        serverMetadataCache.updateTableMetadata(new TableMetadata(DATA1_TABLE_INFO, buckets));
        assertThat(serverMetadataCache.getTableMetadata(DATA1_TABLE_PATH)).isEmpty();

        serverMetadataCache.updateTableMetadata(
                new TableMetadata(partitionTableInfo, Collections.emptyList()));
        serverMetadataCache.updatePartitionMetadata(
                new PartitionMetadata(partitionTableId, partitionName1, partitionId1, buckets, 3));
        assertThat(
                        serverMetadataCache.getPartitionMetadata(
                                PhysicalTablePath.of(partitionedTablePath, partitionName1)))
                .isEmpty();
    }

    @Test
    void testPartitionUsesItsOwnBucketCount() {
        serverMetadataCache.updateTableMetadata(
                new TableMetadata(partitionTableInfo, Collections.emptyList()));
        serverMetadataCache.updatePartitionMetadata(
                new PartitionMetadata(
                        partitionTableId, partitionName1, partitionId1, bucketMetadata(1), 1));
        assertThat(serverMetadataCache.getTableMetadata(partitionedTablePath))
                .hasValueSatisfying(
                        metadata -> assertThat(metadata.getBucketMetadataList()).isEmpty());
        assertThat(
                        serverMetadataCache.getPartitionMetadata(
                                PhysicalTablePath.of(partitionedTablePath, partitionName1)))
                .hasValueSatisfying(
                        metadata -> {
                            assertThat(metadata.getBucketCount()).isEqualTo(1);
                            assertThat(metadata.getBucketMetadataList()).hasSize(1);
                        });
    }

    @Test
    void testPartitionWithoutExplicitBucketCountIsCacheMiss() {
        serverMetadataCache.updateTableMetadata(
                new TableMetadata(partitionTableInfo, Collections.emptyList()));
        serverMetadataCache.updatePartitionMetadata(
                new PartitionMetadata(
                        partitionTableId,
                        partitionName1,
                        partitionId1,
                        initialBucketMetadata,
                        null));
        assertThat(
                        serverMetadataCache.getPartitionMetadata(
                                PhysicalTablePath.of(partitionedTablePath, partitionName1)))
                .isEmpty();
    }

    @Test
    void testContainsTableBucket() {
        tableMetadataList =
                Collections.singletonList(
                        new TableMetadata(DATA1_TABLE_INFO, initialBucketMetadata));
        long otherPartitionTableId = partitionTableId + 1;
        partitionMetadataList =
                Arrays.asList(
                        new PartitionMetadata(
                                partitionTableId,
                                partitionName1,
                                partitionId1,
                                initialBucketMetadata,
                                null),
                        new PartitionMetadata(
                                otherPartitionTableId,
                                partitionName1,
                                partitionId2,
                                initialBucketMetadata,
                                null));
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
        assertThat(
                        serverMetadataCache.contains(
                                new TableBucket(
                                        otherPartitionTableId,
                                        partitionId2,
                                        initialBucketMetadata.get(0).getBucketId())))
                .isTrue();
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
        List<BucketMetadata> buckets = bucketMetadata(explicitBucketCount);
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
                                        buckets,
                                        explicitBucketCount),
                                new PartitionMetadata(
                                        partitionTableId,
                                        partitionName2,
                                        partitionId2,
                                        buckets,
                                        explicitBucketCount))));
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
                                        Collections.emptyList(),
                                        null))));
        assertThat(
                        serverMetadataCache.getPartitionMetadata(
                                PhysicalTablePath.of(partitionedTablePath, partitionName1)))
                .isEmpty();

        // Re-adding without a count must not reuse the deleted partition's count.
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
                                        initialBucketMetadata,
                                        null))));
        assertThat(
                        serverMetadataCache.getPartitionMetadata(
                                PhysicalTablePath.of(partitionedTablePath, partitionName1)))
                .isEmpty();
        assertThat(
                        serverMetadataCache
                                .getSnapshot()
                                .getPartitionBucketCount(
                                        new TablePartition(partitionTableId, partitionId1)))
                .isNull();

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
                                        Collections.emptyList(),
                                        null))));
        assertThat(
                        serverMetadataCache.getPartitionMetadata(
                                PhysicalTablePath.of(partitionedTablePath, partitionName2)))
                .isEmpty();

        // The name-based deletion must also discard the partition's count.
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
                                        initialBucketMetadata,
                                        null))));
        assertThat(
                        serverMetadataCache.getPartitionMetadata(
                                PhysicalTablePath.of(partitionedTablePath, partitionName2)))
                .isEmpty();
        assertThat(
                        serverMetadataCache
                                .getSnapshot()
                                .getPartitionBucketCount(
                                        new TablePartition(partitionTableId, partitionId2)))
                .isNull();
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
                        bucketMetadata(explicitBucketCount),
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
        assertThat(serverMetadataCache.getCachedBucketMetadata(DATA1_TABLE_PATH, 3)).isEmpty();
        assertThat(
                        serverMetadataCache.getCachedBucketMetadata(
                                TablePath.of("unknown_db", "unknown_table"), 0))
                .isEmpty();
    }

    @Test
    void testGetTableMetadataIgnoresStaleTableIdOfRecreatedTable() {
        // Given: the cache still maps the path to a dropped table with two buckets, while ZK
        // already holds a re-created table with a new id under the same path
        TableInfo recreatedTableInfo =
                TableInfo.of(
                        DATA1_TABLE_PATH,
                        DATA1_TABLE_ID + 1,
                        0,
                        DATA1_TABLE_DESCRIPTOR,
                        DEFAULT_REMOTE_DATA_DIR,
                        100L,
                        100L);
        serverMetadataCache =
                new TabletServerMetadataCache(
                        new TestingMetadataManager(Collections.singletonList(recreatedTableInfo)));
        serverMetadataCache.updateTableMetadata(
                new TableMetadata(DATA1_TABLE_INFO, initialBucketMetadata));

        // Then: the new table info must not be served with the dropped table's buckets
        assertThat(serverMetadataCache.getTableMetadata(DATA1_TABLE_PATH)).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(TableTombstone.class)
    void testDroppedTableRemovesPartitions(TableTombstone tombstone) {
        ClusterMetadata initialUpdate =
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        tableMetadataList,
                        partitionMetadataList);
        serverMetadataCache.updateClusterMetadata(initialUpdate);
        PhysicalTablePath partitionPath =
                PhysicalTablePath.of(partitionedTablePath, partitionName1);
        assertThat(serverMetadataCache.getPartitionMetadata(partitionPath)).isPresent();
        long replacementPartitionId = partitionId1 + 50;
        PartitionMetadata replacementPartition =
                new PartitionMetadata(
                        partitionTableId,
                        partitionName1,
                        replacementPartitionId,
                        initialBucketMetadata,
                        initialBucketMetadata.size());
        ClusterMetadata replacementUpdate =
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        tableMetadataList,
                        Collections.singletonList(replacementPartition));
        serverMetadataCache.updateClusterMetadata(replacementUpdate);
        serverMetadataCache.updateClusterMetadata(initialUpdate);
        assertThat(serverMetadataCache.getPartitionMetadata(partitionPath))
                .map(PartitionMetadata::getPartitionId)
                .hasValue(replacementPartitionId);

        // Older Coordinators also drop the partitions by name.
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.singletonList(tableTombstone(tombstone)),
                        tombstone == TableTombstone.BY_PATH_WITHOUT_ID
                                ? Arrays.asList(
                                        legacyPartitionTombstone(partitionName1),
                                        legacyPartitionTombstone(partitionName2))
                                : Collections.emptyList()));

        List<Long> droppedPartitionIds =
                Arrays.asList(partitionId1, partitionId2, replacementPartitionId);
        assertDroppedTableRemoved(droppedPartitionIds);

        serverMetadataCache.updateClusterMetadata(initialUpdate);
        serverMetadataCache.updateClusterMetadata(replacementUpdate);
        assertDroppedTableRemoved(droppedPartitionIds);

        long recreatedTableId = partitionTableId + 1;
        long recreatedPartitionId = replacementPartitionId + 1;
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.singletonList(
                                new TableMetadata(
                                        TableInfo.of(
                                                partitionedTablePath,
                                                recreatedTableId,
                                                0,
                                                DATA1_PARTITIONED_TABLE_DESCRIPTOR,
                                                DEFAULT_REMOTE_DATA_DIR,
                                                100L,
                                                100L),
                                        Collections.emptyList())),
                        Collections.singletonList(
                                new PartitionMetadata(
                                        recreatedTableId,
                                        partitionName1,
                                        recreatedPartitionId,
                                        initialBucketMetadata,
                                        initialBucketMetadata.size()))));
        assertThat(serverMetadataCache.getPartitionMetadata(partitionPath))
                .map(PartitionMetadata::getPartitionId)
                .hasValue(recreatedPartitionId);
        assertThat(metadataManager.partitionRegistrationReads).isZero();
    }

    @Test
    void testLateUpdateCannotRestoreTableDroppedBeforeCached() {
        assertThat(
                        serverMetadataCache.updateClusterMetadata(
                                new ClusterMetadata(
                                        coordinatorServer,
                                        aliveTableServers,
                                        Collections.singletonList(
                                                tableTombstone(TableTombstone.BY_PATH)),
                                        Collections.emptyList())))
                .containsExactly(partitionTableId);
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        tableMetadataList,
                        partitionMetadataList));
        assertDroppedTableRemoved(Arrays.asList(partitionId1, partitionId2));
        assertThat(serverMetadataCache.getTableId(DATA1_TABLE_PATH)).hasValue(DATA1_TABLE_ID);

        long recreatedTableId = partitionTableId + 1;
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.singletonList(
                                new TableMetadata(
                                        TableInfo.of(
                                                partitionedTablePath,
                                                recreatedTableId,
                                                0,
                                                DATA1_PARTITIONED_TABLE_DESCRIPTOR,
                                                DEFAULT_REMOTE_DATA_DIR,
                                                100L,
                                                100L),
                                        Collections.emptyList())),
                        Collections.emptyList()));
        assertThat(serverMetadataCache.getTableId(partitionedTablePath)).hasValue(recreatedTableId);
    }

    private TableMetadata tableTombstone(TableTombstone tombstone) {
        boolean byTableId = tombstone == TableTombstone.BY_ID;
        TablePath decodedDeletedTablePath =
                TablePath.of(
                        DELETED_TABLE_PATH.getDatabaseName(), DELETED_TABLE_PATH.getTableName());
        TableInfo tableInfo =
                TableInfo.of(
                        byTableId ? decodedDeletedTablePath : partitionedTablePath,
                        byTableId ? partitionTableId : DELETED_TABLE_ID,
                        0,
                        DATA1_PARTITIONED_TABLE_DESCRIPTOR,
                        DEFAULT_REMOTE_DATA_DIR,
                        -1L,
                        -1L);
        return new TableMetadata(
                tableInfo,
                Collections.emptyList(),
                tombstone == TableTombstone.BY_PATH ? partitionTableId : null);
    }

    private enum TableTombstone {
        BY_ID,
        BY_PATH,
        /** Sent by older Coordinators, without the ID of the dropped table. */
        BY_PATH_WITHOUT_ID
    }

    private void assertDroppedTableRemoved(List<Long> partitionIds) {
        assertThat(serverMetadataCache.getTableId(partitionedTablePath)).isEmpty();
        assertThat(serverMetadataCache.getTablePath(partitionTableId)).isEmpty();
        assertThat(
                        serverMetadataCache.getPartitionMetadata(
                                PhysicalTablePath.of(partitionedTablePath, partitionName1)))
                .isEmpty();
        assertThat(
                        serverMetadataCache.getPartitionMetadata(
                                PhysicalTablePath.of(partitionedTablePath, partitionName2)))
                .isEmpty();
        assertPartitionsRemoved(partitionIds);
        assertThat(serverMetadataCache.getSnapshot().getPartitionBucketCounts()).isEmpty();
    }

    @Test
    void testLateUpdateCannotRestoreDroppedPartition() {
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        tableMetadataList,
                        Collections.emptyList()));
        PhysicalTablePath partitionPath1 =
                PhysicalTablePath.of(partitionedTablePath, partitionName1);
        PhysicalTablePath partitionPath2 =
                PhysicalTablePath.of(partitionedTablePath, partitionName2);
        PartitionMetadata tombstone2 =
                new PartitionMetadata(
                        partitionTableId,
                        DELETED_PARTITION_NAME,
                        partitionId2,
                        Collections.emptyList(),
                        null);
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.emptyList(),
                        Arrays.asList(tombstone2, partitionMetadataList.get(0))));
        assertThat(serverMetadataCache.getPartitionMetadata(partitionPath1))
                .map(PartitionMetadata::getPartitionId)
                .hasValue(partitionId1);

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
                                        changedBucket1BucketMetadata,
                                        initialBucketMetadata.size()),
                                partitionMetadataList.get(1))));
        assertThat(serverMetadataCache.getPartitionMetadata(partitionPath2)).isEmpty();
        assertPartitionsRemoved(Collections.singletonList(partitionId2));
        assertBucketMetadata(partitionPath1, afterChangeBucketMetadata);

        String partitionName0 = "p0";
        PhysicalTablePath partitionPath0 =
                PhysicalTablePath.of(partitionedTablePath, partitionName0);
        long partitionId0 = partitionId1 - 1;
        updatePartition(partitionName0, partitionId0, initialBucketMetadata);
        assertThat(serverMetadataCache.getPartitionMetadata(partitionPath0)).isEmpty();
        serverMetadataCache.updatePartitionMetadata(
                Collections.singletonList(
                        new PartitionMetadata(
                                partitionTableId,
                                partitionName0,
                                partitionId0,
                                initialBucketMetadata,
                                initialBucketMetadata.size())),
                serverMetadataCache.getSnapshot());
        updatePartition(partitionName0, partitionId0, changedBucket1BucketMetadata);
        assertBucketMetadata(partitionPath0, afterChangeBucketMetadata);

        long recreatedPartitionId = partitionId2 + 100;
        serverMetadataCache.updatePartitionMetadata(
                Collections.singletonList(
                        new PartitionMetadata(
                                partitionTableId,
                                partitionName2,
                                recreatedPartitionId,
                                initialBucketMetadata,
                                initialBucketMetadata.size())),
                serverMetadataCache.getSnapshot());
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.emptyList(),
                        Collections.singletonList(tombstone2)));
        updatePartition(partitionName2, recreatedPartitionId, changedBucket1BucketMetadata);
        assertBucketMetadata(partitionPath2, afterChangeBucketMetadata);
    }

    /**
     * Older Coordinators identify a dropped partition by name, so the ID cached for the name is
     * recorded as dropped. A duplicate of the tombstone finds nothing, but checks only cache hits
     * of its own path against ZK.
     */
    @Test
    void testLateUpdateAfterLegacyPartitionTombstone() {
        ClusterMetadata update =
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        tableMetadataList,
                        partitionMetadataList);
        serverMetadataCache.updateClusterMetadata(update);
        PhysicalTablePath partitionPath2 =
                PhysicalTablePath.of(partitionedTablePath, partitionName2);

        updatePartitions(Collections.singletonList(legacyPartitionTombstone(partitionName1)));
        serverMetadataCache.updateClusterMetadata(update);
        assertPartitionsRemoved(Collections.singletonList(partitionId1));

        updatePartitions(Collections.singletonList(legacyPartitionTombstone(partitionName1)));
        assertThat(serverMetadataCache.getPartitionMetadata(partitionPath2)).isPresent();
        assertThat(metadataManager.partitionRegistrationReads).isZero();
    }

    /**
     * If a tombstone of an older Coordinator finds no cached object, a late update of the object is
     * applied, so cache hits at its path are checked against ZK until the Coordinator epoch
     * advances, which removes the object.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testLateUpdateAfterUnidentifiedLegacyTombstone(boolean dropTable) {
        int coordinatorEpoch = 0;
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        dropTable
                                ? Collections.singletonList(tableMetadataList.get(0))
                                : tableMetadataList,
                        Collections.emptyList()),
                coordinatorEpoch);
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        dropTable
                                ? Collections.singletonList(
                                        tableTombstone(TableTombstone.BY_PATH_WITHOUT_ID))
                                : Collections.emptyList(),
                        dropTable
                                ? Collections.emptyList()
                                : Collections.singletonList(
                                        legacyPartitionTombstone(partitionName1))),
                coordinatorEpoch);
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        tableMetadataList,
                        partitionMetadataList),
                coordinatorEpoch);
        PhysicalTablePath partitionPath1 =
                PhysicalTablePath.of(partitionedTablePath, partitionName1);
        PhysicalTablePath partitionPath2 =
                PhysicalTablePath.of(partitionedTablePath, partitionName2);
        assertThat(serverMetadataCache.getSnapshot().getPartitionId(partitionPath1))
                .hasValue(partitionId1);
        metadataManager.registerPartition(partitionPath2, partitionTableId, partitionId2);

        assertThat(
                        serverMetadataCache.getPartitionsMetadata(
                                Arrays.asList(partitionPath1, partitionPath2)))
                .containsOnlyKeys(partitionPath2);
        assertThat(metadataManager.partitionRegistrationReads).isOne();

        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.emptyList(),
                        Collections.emptyList()),
                coordinatorEpoch + 1);
        assertThat(serverMetadataCache.getSnapshot().getPartitionId(partitionPath1)).isEmpty();
        assertThat(serverMetadataCache.getTableId(partitionedTablePath).isPresent())
                .isEqualTo(!dropTable);
        assertThat(serverMetadataCache.getPartitionMetadata(partitionPath2).isPresent())
                .isEqualTo(!dropTable);
        assertThat(metadataManager.partitionRegistrationReads).isOne();
    }

    private PartitionMetadata legacyPartitionTombstone(String partitionName) {
        return new PartitionMetadata(
                partitionTableId,
                partitionName,
                DELETED_PARTITION_ID,
                Collections.emptyList(),
                null);
    }

    @Test
    void testDeletedIdWatermarksReclaimedWhenCoordinatorEpochAdvances() {
        int coordinatorEpoch = 0;
        int droppedTables = 1000;
        List<TableInfo> droppedTableInfos = new ArrayList<>();
        for (int i = 0; i < droppedTables; i++) {
            TableInfo tableInfo =
                    TableInfo.of(
                            TablePath.of("test_db_1", "dropped_table_" + i),
                            1000L + i,
                            0,
                            DATA1_TABLE_DESCRIPTOR,
                            DEFAULT_REMOTE_DATA_DIR,
                            100L,
                            100L);
            droppedTableInfos.add(tableInfo);
            updateTables(
                    coordinatorEpoch,
                    Collections.singletonList(
                            new TableMetadata(tableInfo, Collections.emptyList())));
            updateTables(
                    coordinatorEpoch,
                    Collections.singletonList(
                            new TableMetadata(
                                    TableInfo.of(
                                            tableInfo.getTablePath(),
                                            DELETED_TABLE_ID,
                                            0,
                                            DATA1_TABLE_DESCRIPTOR,
                                            DEFAULT_REMOTE_DATA_DIR,
                                            -1L,
                                            -1L),
                                    Collections.emptyList(),
                                    tableInfo.getTableId())));
        }
        assertThat(serverMetadataCache.getDeletedIdWatermarkCount()).isEqualTo(droppedTables);

        TableInfo firstDropped = droppedTableInfos.get(0);
        updateTables(
                coordinatorEpoch,
                Collections.singletonList(
                        new TableMetadata(firstDropped, Collections.emptyList())));
        assertThat(serverMetadataCache.getTableId(firstDropped.getTablePath())).isEmpty();

        updateTables(coordinatorEpoch + 1, Collections.emptyList());
        assertThat(serverMetadataCache.getDeletedIdWatermarkCount()).isZero();
    }

    private void updateTables(int coordinatorEpoch, List<TableMetadata> tables) {
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer, aliveTableServers, tables, Collections.emptyList()),
                coordinatorEpoch);
    }

    private void updatePartition(
            String partitionName, long partitionId, List<BucketMetadata> bucketMetadata) {
        updatePartitions(
                Collections.singletonList(
                        new PartitionMetadata(
                                partitionTableId,
                                partitionName,
                                partitionId,
                                bucketMetadata,
                                initialBucketMetadata.size())));
    }

    private void updatePartitions(List<PartitionMetadata> partitions) {
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer, aliveTableServers, Collections.emptyList(), partitions));
    }

    private void assertBucketMetadata(
            PhysicalTablePath partitionPath, List<BucketMetadata> bucketMetadata) {
        assertThat(serverMetadataCache.getPartitionMetadata(partitionPath))
                .hasValueSatisfying(
                        partition ->
                                assertThat(partition.getBucketMetadataList())
                                        .containsExactlyInAnyOrderElementsOf(bucketMetadata));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRecreatedTableRemovesPartitionsOfOldTable(boolean refillFromZk) {
        ClusterMetadata oldTableUpdate =
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        tableMetadataList,
                        partitionMetadataList);
        serverMetadataCache.updateClusterMetadata(oldTableUpdate);
        long replacementPartitionId = partitionId1 + 50;
        serverMetadataCache.updatePartitionMetadata(
                Collections.singletonList(
                        new PartitionMetadata(
                                partitionTableId,
                                partitionName1,
                                replacementPartitionId,
                                initialBucketMetadata,
                                initialBucketMetadata.size())),
                serverMetadataCache.getSnapshot());
        long recreatedTableId = partitionTableId + 1;
        TableInfo recreatedTableInfo =
                TableInfo.of(
                        partitionedTablePath,
                        recreatedTableId,
                        0,
                        DATA1_PARTITIONED_TABLE_DESCRIPTOR,
                        DEFAULT_REMOTE_DATA_DIR,
                        100L,
                        100L);
        TableMetadata recreatedTable =
                new TableMetadata(recreatedTableInfo, Collections.emptyList());
        long recreatedPartitionId = partitionId1 + 100;
        PartitionMetadata recreatedPartition =
                new PartitionMetadata(
                        recreatedTableId,
                        partitionName1,
                        recreatedPartitionId,
                        initialBucketMetadata,
                        initialBucketMetadata.size());
        PhysicalTablePath partitionPath1 =
                PhysicalTablePath.of(partitionedTablePath, partitionName1);
        PhysicalTablePath partitionPath2 =
                PhysicalTablePath.of(partitionedTablePath, partitionName2);
        if (refillFromZk) {
            serverMetadataCache.updateTableMetadata(
                    Collections.singletonList(recreatedTable), serverMetadataCache.getSnapshot());
            assertThat(serverMetadataCache.getTableId(partitionedTablePath))
                    .hasValue(recreatedTableId);
            assertThat(serverMetadataCache.getPartitionMetadata(partitionPath1)).isEmpty();
            serverMetadataCache.updatePartitionMetadata(
                    Collections.singletonList(recreatedPartition),
                    serverMetadataCache.getSnapshot());
        } else {
            serverMetadataCache.updateClusterMetadata(
                    new ClusterMetadata(
                            coordinatorServer,
                            aliveTableServers,
                            Collections.singletonList(recreatedTable),
                            Collections.singletonList(recreatedPartition)));
        }
        serverMetadataCache.updateClusterMetadata(oldTableUpdate);
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.singletonList(tableTombstone(TableTombstone.BY_PATH)),
                        Collections.emptyList()));

        assertPartitionsRemoved(Arrays.asList(partitionId1, partitionId2, replacementPartitionId));
        assertThat(serverMetadataCache.getPartitionMetadata(partitionPath1))
                .hasValueSatisfying(
                        partition -> {
                            assertThat(partition.getTableId()).isEqualTo(recreatedTableId);
                            assertThat(partition.getPartitionId()).isEqualTo(recreatedPartitionId);
                        });
        assertThat(serverMetadataCache.getPartitionMetadata(partitionPath2)).isEmpty();
        assertThat(serverMetadataCache.getSnapshot().getPartitionBucketCounts())
                .containsOnlyKeys(new TablePartition(recreatedTableId, recreatedPartitionId));
    }

    private void assertPartitionsRemoved(List<Long> partitionIds) {
        for (long partitionId : partitionIds) {
            assertThat(serverMetadataCache.getPhysicalTablePath(partitionId)).isEmpty();
            assertThat(serverMetadataCache.getSnapshot().getBucketMetadataForPartition(partitionId))
                    .isEmpty();
        }
    }

    @Test
    void testStaleCacheAsideLoadCannotUndoPartitionUpdate() {
        TabletServerMetadataCache cache = new TabletServerMetadataCache(null);
        cache.updateTableMetadata(new TableMetadata(partitionTableInfo, Collections.emptyList()));
        PhysicalTablePath partitionPath =
                PhysicalTablePath.of(partitionedTablePath, partitionName1);
        PartitionMetadata loadedPartition =
                new PartitionMetadata(
                        partitionTableId,
                        partitionName1,
                        partitionId1,
                        initialBucketMetadata,
                        initialBucketMetadata.size());

        ServerMetadataSnapshot beforeLeaderChange = cache.getSnapshot();
        cache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.emptyList(),
                        Collections.singletonList(
                                new PartitionMetadata(
                                        partitionTableId,
                                        partitionName1,
                                        partitionId1,
                                        afterChangeBucketMetadata,
                                        afterChangeBucketMetadata.size()))));
        cache.updatePartitionMetadata(
                Collections.singletonList(loadedPartition), beforeLeaderChange);
        assertThat(cache.getPartitionMetadata(partitionPath))
                .hasValueSatisfying(
                        partition ->
                                assertThat(partition.getBucketMetadataList())
                                        .containsExactlyInAnyOrderElementsOf(
                                                afterChangeBucketMetadata));

        ServerMetadataSnapshot beforeReplacement = cache.getSnapshot();
        long replacementPartitionId = partitionId1 + 100;
        cache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinatorServer,
                        aliveTableServers,
                        Collections.emptyList(),
                        Arrays.asList(
                                new PartitionMetadata(
                                        partitionTableId,
                                        partitionName1,
                                        replacementPartitionId,
                                        initialBucketMetadata,
                                        initialBucketMetadata.size()),
                                new PartitionMetadata(
                                        partitionTableId,
                                        DELETED_PARTITION_NAME,
                                        partitionId1,
                                        Collections.emptyList(),
                                        null))));
        cache.updatePartitionMetadata(
                Collections.singletonList(loadedPartition), beforeReplacement);
        assertThat(cache.getPartitionMetadata(partitionPath))
                .hasValueSatisfying(
                        partition ->
                                assertThat(partition.getPartitionId())
                                        .isEqualTo(replacementPartitionId));
        assertThat(cache.getSnapshot().getBucketMetadataForPartition(partitionId1)).isEmpty();
        assertThat(cache.getSnapshot().getPartitionBucketCounts())
                .containsOnlyKeys(new TablePartition(partitionTableId, replacementPartitionId));
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
                        new BucketMetadata(1, 2, 1, Arrays.asList(1, 2, 3)),
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
                        partitionTableId,
                        partitionName,
                        partitionId,
                        initialBucketMetadata,
                        initialBucketMetadata.size());

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
                        partitionTableId, partitionName, partitionId, initialBucketMetadata, null);

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
                        partitionTableId,
                        partitionName,
                        partitionId,
                        initialBucketMetadata,
                        initialBucketMetadata.size());
        serverMetadataCache.updatePartitionMetadata(existingPartitionMetadata);

        // When: update with new bucket metadata
        List<BucketMetadata> newBucketMetadata =
                Arrays.asList(new BucketMetadata(0, 1, 1, Arrays.asList(1, 2, 3)));
        PartitionMetadata newPartitionMetadata =
                new PartitionMetadata(
                        partitionTableId,
                        partitionName,
                        partitionId,
                        newBucketMetadata,
                        newBucketMetadata.size());
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

    private static List<BucketMetadata> bucketMetadata(int count) {
        List<BucketMetadata> buckets = new ArrayList<>();
        for (int bucketId = 0; bucketId < count; bucketId++) {
            buckets.add(new BucketMetadata(bucketId, 0, 0, Arrays.asList(0, 1, 2)));
        }
        return buckets;
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
                        partitionTableId, partitionName, partitionId, initialBucketMetadata, null);
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
        private final Map<PhysicalTablePath, PartitionRegistration> partitionRegistrations =
                new HashMap<>();
        private int partitionRegistrationReads;

        public TestingMetadataManager(List<TableInfo> tableInfos) {
            super(
                    null,
                    new Configuration(),
                    new LakeCatalogDynamicLoader(new Configuration(), null, true));
            tableInfos.forEach(tableInfo -> tableInfoMap.put(tableInfo.getTablePath(), tableInfo));
        }

        void registerPartition(PhysicalTablePath partitionPath, long tableId, long partitionId) {
            partitionRegistrations.put(
                    partitionPath, new PartitionRegistration(tableId, partitionId, null, null));
        }

        @Override
        public Map<String, PartitionRegistration> getPartitionRegistrations(
                TablePath tablePath, Collection<String> partitionNames) {
            partitionRegistrationReads++;
            Map<String, PartitionRegistration> result = new HashMap<>();
            for (String partitionName : partitionNames) {
                PartitionRegistration registration =
                        partitionRegistrations.get(PhysicalTablePath.of(tablePath, partitionName));
                if (registration != null) {
                    result.put(partitionName, registration);
                }
            }
            return result;
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

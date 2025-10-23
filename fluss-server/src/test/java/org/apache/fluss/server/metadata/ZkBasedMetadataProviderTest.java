/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.metadata;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.TestData;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ZkBasedMetadataProviderTest {
    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zookeeperClient;
    private static MetadataManager metadataManager;
    private static ZkBasedMetadataProvider metadataProvider;

    @BeforeAll
    static void beforeAll() {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
        metadataManager =
                new MetadataManager(
                        zookeeperClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));
        TabletServerMetadataCache metadataCache = new TabletServerMetadataCache(metadataManager);
        metadataProvider =
                new TabletServerMetadataProvider(zookeeperClient, metadataManager, metadataCache);
    }

    @AfterEach
    void afterEach() {
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupRoot();
    }

    @AfterAll
    static void afterAll() {
        zookeeperClient.close();
    }

    @Test
    void testGetTableMetadataFromZk() throws Exception {
        // Create Table
        TablePath tablePath = TablePath.of("test_db", "test_table");
        TableDescriptor desc = TestData.DATA1_TABLE_DESCRIPTOR.withReplicationFactor(1);
        TableAssignment tableAssignment =
                TableAssignment.builder()
                        .add(0, BucketAssignment.of(1, 2, 3))
                        .add(1, BucketAssignment.of(2, 3, 4))
                        .build();
        metadataManager.createDatabase("test_db", DatabaseDescriptor.EMPTY, true);
        long tableId = metadataManager.createTable(tablePath, desc, tableAssignment, false);

        // Create leader and isr for buckets
        TableBucket tableBucket0 = new TableBucket(tableId, 0);
        TableBucket tableBucket1 = new TableBucket(tableId, 1);

        LeaderAndIsr leaderAndIsr0 = new LeaderAndIsr(1, 10, Arrays.asList(1, 2, 3), 100, 1000);
        LeaderAndIsr leaderAndIsr1 = new LeaderAndIsr(2, 20, Arrays.asList(2, 3, 4), 200, 2000);

        zookeeperClient.registerLeaderAndIsr(tableBucket0, leaderAndIsr0);
        zookeeperClient.registerLeaderAndIsr(tableBucket1, leaderAndIsr1);

        List<TableMetadata> tablesMetadataFromZK =
                metadataProvider.getTablesMetadataFromZK(
                        Collections.singletonList(TablePath.of("test_db", "test_table")));
        assertThat(tablesMetadataFromZK).hasSize(1);

        TableMetadata tableMetadata = tablesMetadataFromZK.get(0);
        TableInfo tableInfo = tableMetadata.getTableInfo();
        assertThat(tableInfo.getTableId()).isEqualTo(tableId);
        assertThat(tableInfo.getTablePath()).isEqualTo(tablePath);
        assertThat(tableInfo.getSchema()).isEqualTo(desc.getSchema());

        List<BucketMetadata> bucketMetadataList = tableMetadata.getBucketMetadataList();
        assertThat(bucketMetadataList).hasSize(2);

        // Verify bucket metadata
        Map<Integer, BucketMetadata> bucketMap =
                bucketMetadataList.stream()
                        .collect(Collectors.toMap(BucketMetadata::getBucketId, b -> b));

        BucketMetadata bucket0Metadata = bucketMap.get(0);
        assertThat(extractLeaderFromBucketMetadata(bucket0Metadata)).isEqualTo(1);
        assertThat(bucket0Metadata.getReplicas()).containsExactly(1, 2, 3);

        BucketMetadata bucket1Metadata = bucketMap.get(1);
        assertThat(extractLeaderFromBucketMetadata(bucket1Metadata)).isEqualTo(2);
        assertThat(bucket1Metadata.getReplicas()).containsExactly(2, 3, 4);
    }

    @Test
    void testGetPartitionMetadataFromZk() throws Exception {
        // Prepare test data
        TablePath tablePath = TablePath.of("test_db", "test_partition_table");
        long tableId = 2001L;
        long partitionId = 1L;
        String partitionName = "p1";

        long currentMillis = System.currentTimeMillis();
        TableRegistration tableReg =
                createTestTableRegistration(tableId, "partition table", currentMillis);
        zookeeperClient.registerTable(tablePath, tableReg);

        // Create partition assignment
        Map<Integer, BucketAssignment> bucketAssignments = new HashMap<>();
        bucketAssignments.put(0, BucketAssignment.of(1, 2));
        bucketAssignments.put(1, BucketAssignment.of(2, 3));
        PartitionAssignment partitionAssignment =
                new PartitionAssignment(tableId, bucketAssignments);

        zookeeperClient.registerPartitionAssignmentAndMetadata(
                partitionId, partitionName, partitionAssignment, tablePath, tableId);

        // Create leader and isr for partition buckets
        TableBucket partitionBucket0 = new TableBucket(tableId, partitionId, 0);
        TableBucket partitionBucket1 = new TableBucket(tableId, partitionId, 1);

        LeaderAndIsr leaderAndIsr0 = new LeaderAndIsr(1, 10, Arrays.asList(1, 2), 100, 1000);
        LeaderAndIsr leaderAndIsr1 = new LeaderAndIsr(2, 20, Arrays.asList(2, 3), 200, 2000);

        zookeeperClient.registerLeaderAndIsr(partitionBucket0, leaderAndIsr0);
        zookeeperClient.registerLeaderAndIsr(partitionBucket1, leaderAndIsr1);

        // Test getPartitionMetadataFromZkAsync
        PhysicalTablePath partitionPath = PhysicalTablePath.of(tablePath, partitionName);

        List<PartitionMetadata> partitionsMetadataFromZK =
                metadataProvider.getPartitionsMetadataFromZK(Collections.singleton(partitionPath));

        assertThat(partitionsMetadataFromZK).hasSize(1);
        PartitionMetadata partitionMetadata = partitionsMetadataFromZK.get(0);

        assertThat(partitionMetadata.getTableId()).isEqualTo(tableId);
        assertThat(partitionMetadata.getPartitionName()).isEqualTo(partitionName);
        assertThat(partitionMetadata.getPartitionId()).isEqualTo(partitionId);

        List<BucketMetadata> bucketMetadataList = partitionMetadata.getBucketMetadataList();
        assertThat(bucketMetadataList).hasSize(2);

        Map<Integer, BucketMetadata> bucketMap =
                bucketMetadataList.stream()
                        .collect(Collectors.toMap(BucketMetadata::getBucketId, b -> b));

        BucketMetadata bucket0Metadata = bucketMap.get(0);
        assertThat(extractLeaderFromBucketMetadata(bucket0Metadata)).isEqualTo(1);
        assertThat(bucket0Metadata.getReplicas()).containsExactly(1, 2);

        BucketMetadata bucket1Metadata = bucketMap.get(1);
        assertThat(extractLeaderFromBucketMetadata(bucket1Metadata)).isEqualTo(2);
        assertThat(bucket1Metadata.getReplicas()).containsExactly(2, 3);
    }

    @Test
    void testBatchGetPartitionMetadataFromZkAsync() throws Exception {
        // Prepare test data with multiple tables and partitions
        TablePath tablePath1 = TablePath.of("test_db", "table1");
        TablePath tablePath2 = TablePath.of("test_db", "table2");
        long tableId1 = 3001L;
        long tableId2 = 3002L;

        long currentMillis = System.currentTimeMillis();
        TableRegistration tableReg1 =
                createTestTableRegistration(tableId1, "table1", currentMillis);
        TableRegistration tableReg2 =
                createTestTableRegistration(tableId2, "table2", currentMillis);

        zookeeperClient.registerTable(tablePath1, tableReg1);
        zookeeperClient.registerTable(tablePath2, tableReg2);

        // Create partitions for table1
        long partitionId1 = 11L;
        long partitionId2 = 12L;
        String partitionName1 = "p1";
        String partitionName2 = "p2";

        PartitionAssignment partitionAssignment1 =
                new PartitionAssignment(
                        tableId1, Collections.singletonMap(0, BucketAssignment.of(1, 2)));
        PartitionAssignment partitionAssignment2 =
                new PartitionAssignment(
                        tableId1, Collections.singletonMap(1, BucketAssignment.of(2, 3)));

        zookeeperClient.registerPartitionAssignmentAndMetadata(
                partitionId1, partitionName1, partitionAssignment1, tablePath1, tableId1);
        zookeeperClient.registerPartitionAssignmentAndMetadata(
                partitionId2, partitionName2, partitionAssignment2, tablePath1, tableId1);

        // Create partition for table2
        long partitionId3 = 21L;
        String partitionName3 = "p1";

        PartitionAssignment partitionAssignment3 =
                new PartitionAssignment(
                        tableId2, Collections.singletonMap(0, BucketAssignment.of(1, 3)));

        zookeeperClient.registerPartitionAssignmentAndMetadata(
                partitionId3, partitionName3, partitionAssignment3, tablePath2, tableId2);

        // Create leader and isr for all partition buckets
        TableBucket bucket1 = new TableBucket(tableId1, partitionId1, 0);
        TableBucket bucket2 = new TableBucket(tableId1, partitionId2, 1);
        TableBucket bucket3 = new TableBucket(tableId2, partitionId3, 0);

        zookeeperClient.registerLeaderAndIsr(
                bucket1, new LeaderAndIsr(1, 10, Arrays.asList(1, 2), 100, 1000));
        zookeeperClient.registerLeaderAndIsr(
                bucket2, new LeaderAndIsr(2, 20, Arrays.asList(2, 3), 200, 2000));
        zookeeperClient.registerLeaderAndIsr(
                bucket3, new LeaderAndIsr(1, 30, Arrays.asList(1, 3), 300, 3000));

        // Test getPartitionsMetadataFromZK
        List<PhysicalTablePath> partitionPaths =
                Arrays.asList(
                        PhysicalTablePath.of(tablePath1, partitionName1),
                        PhysicalTablePath.of(tablePath1, partitionName2),
                        PhysicalTablePath.of(tablePath2, partitionName3));
        List<PartitionMetadata> partitionsMetadataFromZK =
                metadataProvider.getPartitionsMetadataFromZK(partitionPaths);

        assertThat(partitionsMetadataFromZK).hasSize(3);

        // Verify partition metadata
        Map<Long, PartitionMetadata> partitionMap =
                partitionsMetadataFromZK.stream()
                        .collect(Collectors.toMap(PartitionMetadata::getPartitionId, p -> p));

        // Verify partition 1
        PartitionMetadata partition1Metadata = partitionMap.get(partitionId1);
        assertThat(partition1Metadata.getTableId()).isEqualTo(tableId1);
        assertThat(partition1Metadata.getPartitionName()).isEqualTo(partitionName1);
        assertThat(partition1Metadata.getBucketMetadataList()).hasSize(1);
        assertThat(partition1Metadata.getBucketMetadataList().get(0).getBucketId()).isEqualTo(0);
        assertThat(
                        extractLeaderFromBucketMetadata(
                                partition1Metadata.getBucketMetadataList().get(0)))
                .isEqualTo(1);

        // Verify partition 2
        PartitionMetadata partition2Metadata = partitionMap.get(partitionId2);
        assertThat(partition2Metadata.getTableId()).isEqualTo(tableId1);
        assertThat(partition2Metadata.getPartitionName()).isEqualTo(partitionName2);
        assertThat(partition2Metadata.getBucketMetadataList()).hasSize(1);
        assertThat(partition2Metadata.getBucketMetadataList().get(0).getBucketId()).isEqualTo(1);
        assertThat(
                        extractLeaderFromBucketMetadata(
                                partition2Metadata.getBucketMetadataList().get(0)))
                .isEqualTo(2);

        // Verify partition 3
        PartitionMetadata partition3Metadata = partitionMap.get(partitionId3);
        assertThat(partition3Metadata.getTableId()).isEqualTo(tableId2);
        assertThat(partition3Metadata.getPartitionName()).isEqualTo(partitionName3);
        assertThat(partition3Metadata.getBucketMetadataList()).hasSize(1);
        assertThat(partition3Metadata.getBucketMetadataList().get(0).getBucketId()).isEqualTo(0);
        assertThat(
                        extractLeaderFromBucketMetadata(
                                partition3Metadata.getBucketMetadataList().get(0)))
                .isEqualTo(1);
    }

    @Test
    void testBatchGetPartitionMetadataFromZkWithNonExistentPartition() {
        // Test error handling when partition doesn't exist
        PhysicalTablePath nonexistPath = PhysicalTablePath.of("test_db", "table1", "nonexist");
        assertThatThrownBy(
                        () ->
                                metadataProvider.getPartitionsMetadataFromZK(
                                        Collections.singleton(nonexistPath)))
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessageContaining(
                        "Table partition 'test_db.table1(p=nonexist)' does not exist");
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods for test data creation
    // --------------------------------------------------------------------------------------------

    private TableRegistration createTestTableRegistration(
            long tableId, String comment, long currentMillis) {
        Map<String, String> options = new HashMap<>();
        options.put("option-1", "100");
        return new TableRegistration(
                tableId,
                comment,
                Arrays.asList("a", "b"),
                new TableDescriptor.TableDistribution(3, Collections.singletonList("a")),
                options,
                Collections.emptyMap(),
                currentMillis,
                currentMillis);
    }

    /**
     * Helper method to extract leader ID from BucketMetadata, handling OptionalInt. Returns null if
     * leader is not present.
     */
    private static Integer extractLeaderFromBucketMetadata(BucketMetadata bucketMetadata) {
        return bucketMetadata.getLeaderId().isPresent()
                ? bucketMetadata.getLeaderId().getAsInt()
                : null;
    }
}

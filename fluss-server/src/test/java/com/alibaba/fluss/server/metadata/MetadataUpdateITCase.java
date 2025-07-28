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

package com.alibaba.fluss.server.metadata;

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.PartitionNotExistException;
import com.alibaba.fluss.exception.TableNotExistException;
import com.alibaba.fluss.metadata.PartitionSpec;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.gateway.CoordinatorGateway;
import com.alibaba.fluss.server.coordinator.MetadataManager;
import com.alibaba.fluss.server.tablet.TabletServer;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;
import com.alibaba.fluss.server.zk.data.TableAssignment;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.fluss.record.TestData.DATA1_PARTITIONED_TABLE_DESCRIPTOR;
import static com.alibaba.fluss.record.TestData.DATA1_SCHEMA;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.record.TestData.DATA2_TABLE_PATH;
import static com.alibaba.fluss.server.testutils.PartitionMetadataAssert.assertPartitionMetadata;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.createPartition;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newDropPartitionRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newDropTableRequest;
import static com.alibaba.fluss.server.testutils.TableMetadataAssert.assertTableMetadata;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT Case for metadata update. */
class MetadataUpdateITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(3).build();

    private CoordinatorGateway coordinatorGateway;
    private ServerNode coordinatorServerNode;
    private ZooKeeperClient zkClient;
    private MetadataManager metadataManager;

    @BeforeEach
    void setup() {
        coordinatorGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        coordinatorServerNode = FLUSS_CLUSTER_EXTENSION.getCoordinatorServerNode();
        zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        metadataManager =
                new MetadataManager(
                        FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), new Configuration());
    }

    @Test
    void testMetadataUpdateForServerStartAndStop() throws Exception {
        // get metadata and check it
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();

        Map<Long, TableContext> expectedTablePathById = new HashMap<>();
        // create non-partitioned table
        long tableId1 =
                createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR);
        expectedTablePathById.put(
                tableId1, new TableContext(false, false, DATA1_TABLE_PATH, tableId1, null));
        // create partitioned table
        long tableId2 =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        DATA2_TABLE_PATH,
                        DATA1_PARTITIONED_TABLE_DESCRIPTOR);
        expectedTablePathById.put(
                tableId2, new TableContext(false, true, DATA2_TABLE_PATH, tableId2, null));
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertUpdateMetadataEquals(
                                coordinatorServerNode,
                                3,
                                expectedTablePathById,
                                Collections.emptyMap()));

        // now, start one tablet server and check it
        FLUSS_CLUSTER_EXTENSION.startTabletServer(3);
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertUpdateMetadataEquals(
                                coordinatorServerNode,
                                4,
                                expectedTablePathById,
                                Collections.emptyMap()));

        // now, kill one tablet server and check it
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(1);
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertUpdateMetadataEquals(
                                coordinatorServerNode,
                                3,
                                expectedTablePathById,
                                Collections.emptyMap()));

        // check when coordinator start, it should send update metadata request
        // to all tablet servers

        // let's stop the coordinator server
        FLUSS_CLUSTER_EXTENSION.stopCoordinatorServer();
        // then kill one tablet server again
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(2);
        // let's start the coordinator server again;
        FLUSS_CLUSTER_EXTENSION.startCoordinatorServer();
        coordinatorServerNode = FLUSS_CLUSTER_EXTENSION.getCoordinatorServerNode();
        // check the metadata again
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertUpdateMetadataEquals(
                                coordinatorServerNode,
                                2,
                                expectedTablePathById,
                                Collections.emptyMap()));

        // add back tablet server2
        FLUSS_CLUSTER_EXTENSION.startTabletServer(2);
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertUpdateMetadataEquals(
                                coordinatorServerNode,
                                3,
                                expectedTablePathById,
                                Collections.emptyMap()));
    }

    @Test
    void testMetadataUpdateForTableCreateAndDrop() throws Exception {
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();
        Map<Long, TableContext> expectedTablePathById = new HashMap<>();
        assertUpdateMetadataEquals(
                coordinatorServerNode, 3, expectedTablePathById, Collections.emptyMap());

        long tableId =
                createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR);
        expectedTablePathById.put(
                tableId, new TableContext(false, false, DATA1_TABLE_PATH, tableId, null));
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertUpdateMetadataEquals(
                                coordinatorServerNode,
                                3,
                                expectedTablePathById,
                                Collections.emptyMap()));

        // create a partitioned table.
        TablePath partitionTablePath = TablePath.of("test_db_1", "test_partition_table_1");
        TableDescriptor partitionTableDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA)
                        .distributedBy(3)
                        .partitionedBy("b")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, false)
                        .build();
        long partitionedTableId =
                createTable(FLUSS_CLUSTER_EXTENSION, partitionTablePath, partitionTableDescriptor);
        expectedTablePathById.put(
                partitionedTableId,
                new TableContext(false, true, partitionTablePath, partitionedTableId, null));
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertUpdateMetadataEquals(
                                coordinatorServerNode,
                                3,
                                expectedTablePathById,
                                Collections.emptyMap()));

        // test drop table.
        coordinatorGateway.dropTable(
                newDropTableRequest(
                        DATA1_TABLE_PATH.getDatabaseName(),
                        DATA1_TABLE_PATH.getTableName(),
                        false));
        expectedTablePathById.put(
                tableId, new TableContext(true, false, DATA1_TABLE_PATH, tableId, null));
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertUpdateMetadataEquals(
                                coordinatorServerNode,
                                3,
                                expectedTablePathById,
                                Collections.emptyMap()));

        // test drop table.
        // TODO, this is a bug, which will cause CoordinatorServer cache contains residual
        // tablePath, trace by: https://github.com/alibaba/fluss/issues/898
        //        coordinatorGateway.dropTable(
        //                newDropTableRequest(
        //                        partitionTablePath.getDatabaseName(),
        //                        partitionTablePath.getTableName(),
        //                        false));
        //        expectedTablePathById.put(partitionedTableId, new TablePathTuple(true, true,
        // partitionTablePath,
        //        partitionedTableId, null));
        //        retry(
        //                Duration.ofMinutes(1),
        //                () ->
        //                        assertUpdateMetadataEquals(
        //                                coordinatorServerNode,
        //                                3,
        //                                expectedTablePathById,
        //                                Collections.emptyMap()));
    }

    @Test
    void testMetadataUpdateForPartitionCreateAndDrop() throws Exception {
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();
        Map<Long, TableContext> expectedTablePathById = new HashMap<>();
        Map<Long, TableContext> expectedPartitionNameById = new HashMap<>();
        assertUpdateMetadataEquals(
                coordinatorServerNode, 3, expectedTablePathById, Collections.emptyMap());

        // create a partitioned table.
        TablePath partitionTablePath = TablePath.of("test_db_1", "test_partition_table_1");
        TableDescriptor partitionTableDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA)
                        .distributedBy(3)
                        .partitionedBy("b")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, false)
                        .build();
        long partitionedTableId =
                createTable(FLUSS_CLUSTER_EXTENSION, partitionTablePath, partitionTableDescriptor);
        expectedTablePathById.put(
                partitionedTableId,
                new TableContext(false, true, partitionTablePath, partitionedTableId, null));
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertUpdateMetadataEquals(
                                coordinatorServerNode,
                                3,
                                expectedTablePathById,
                                Collections.emptyMap()));

        // create two new partitions.
        String partitionName1 = "b1";
        String partitionName2 = "b2";
        long partitionId1 =
                createPartition(
                        FLUSS_CLUSTER_EXTENSION,
                        partitionTablePath,
                        new PartitionSpec(Collections.singletonMap("b", partitionName1)),
                        false);
        long partitionId2 =
                createPartition(
                        FLUSS_CLUSTER_EXTENSION,
                        partitionTablePath,
                        new PartitionSpec(Collections.singletonMap("b", partitionName2)),
                        false);
        expectedPartitionNameById.put(
                partitionId1,
                new TableContext(
                        false, true, partitionTablePath, partitionedTableId, partitionName1));
        expectedPartitionNameById.put(
                partitionId2,
                new TableContext(
                        false, true, partitionTablePath, partitionedTableId, partitionName2));
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertUpdateMetadataEquals(
                                coordinatorServerNode,
                                3,
                                expectedTablePathById,
                                expectedPartitionNameById));

        // drop one partition.
        coordinatorGateway
                .dropPartition(
                        newDropPartitionRequest(
                                partitionTablePath,
                                new PartitionSpec(Collections.singletonMap("b", partitionName1)),
                                false))
                .get();
        expectedPartitionNameById.put(
                partitionId1,
                new TableContext(
                        true, true, partitionTablePath, partitionedTableId, partitionName1));
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertUpdateMetadataEquals(
                                coordinatorServerNode,
                                3,
                                expectedTablePathById,
                                expectedPartitionNameById));
    }

    private void assertUpdateMetadataEquals(
            ServerNode expectedCoordinatorServer,
            int expectedTabletServerSize,
            Map<Long, TableContext> expectedTablePathById,
            Map<Long, TableContext> expectedPartitionNameById) {
        ServerMetadataCache csMetadataCache =
                FLUSS_CLUSTER_EXTENSION.getCoordinatorServer().getMetadataCache();
        assertThat(csMetadataCache.getCoordinatorServer("FLUSS"))
                .isEqualTo(expectedCoordinatorServer);
        assertThat(csMetadataCache.getAliveTabletServerInfos().size())
                .isEqualTo(expectedTabletServerSize);

        List<TabletServerMetadataCache> metadataCacheList =
                FLUSS_CLUSTER_EXTENSION.getTabletServers().stream()
                        .map(TabletServer::getMetadataCache)
                        .collect(Collectors.toList());
        metadataCacheList.forEach(
                serverMetadataCache -> {
                    assertThat(serverMetadataCache.getCoordinatorServer("FLUSS"))
                            .isEqualTo(expectedCoordinatorServer);

                    assertThat(serverMetadataCache.getAliveTabletServerInfos().size())
                            .isEqualTo(expectedTabletServerSize);

                    expectedTablePathById.forEach(
                            (tableId, tableContext) -> {
                                if (!tableContext.isDeleted) {
                                    TablePath tablePath = tableContext.tablePath;
                                    assertThat(serverMetadataCache.getTablePath(tableId))
                                            .hasValue(tablePath);

                                    // check table info and bucket location and leader only for
                                    // non-partitioned table.
                                    if (!tableContext.isPartitionedTable) {
                                        TableMetadata tableMetadataFromZk =
                                                getTableMetadataFromZk(tablePath, tableId);
                                        assertTableMetadata(
                                                        serverMetadataCache.getTableMetadata(
                                                                tablePath))
                                                .isEqualTo(tableMetadataFromZk);
                                    }
                                } else {
                                    assertThat(serverMetadataCache.getTablePath(tableId))
                                            .isNotPresent();
                                    assertThatThrownBy(
                                                    () ->
                                                            serverMetadataCache.getTableMetadata(
                                                                    tableContext.tablePath))
                                            .isInstanceOf(TableNotExistException.class)
                                            .hasMessageContaining(
                                                    "Table '"
                                                            + tableContext.tablePath
                                                            + "' does not exist.");
                                }
                            });
                    expectedPartitionNameById.forEach(
                            (partitionId, tableContext) -> {
                                PhysicalTablePath physicalTablePath =
                                        PhysicalTablePath.of(
                                                tableContext.tablePath, tableContext.partitionName);
                                if (!tableContext.isDeleted) {
                                    assertThat(
                                                    serverMetadataCache.getPhysicalTablePath(
                                                            partitionId))
                                            .hasValue(physicalTablePath);

                                    PartitionMetadata partitionMetadataFromZk =
                                            getPartitionMetadataFromZk(
                                                    tableContext.tableId,
                                                    tableContext.partitionName,
                                                    partitionId);
                                    assertPartitionMetadata(
                                                    serverMetadataCache.getPartitionMetadata(
                                                            physicalTablePath))
                                            .isEqualTo(partitionMetadataFromZk);
                                } else {
                                    assertThat(
                                                    serverMetadataCache.getPhysicalTablePath(
                                                            partitionId))
                                            .isNotPresent();
                                    assertThatThrownBy(
                                                    () ->
                                                            serverMetadataCache
                                                                    .getPartitionMetadata(
                                                                            physicalTablePath))
                                            .isInstanceOf(PartitionNotExistException.class)
                                            .hasMessageContaining(
                                                    "Table partition '"
                                                            + physicalTablePath
                                                            + "' does not exist.");
                                }
                            });
                });
    }

    private TableMetadata getTableMetadataFromZk(TablePath tablePath, long tableId) {
        TableInfo tableInfo = metadataManager.getTable(tablePath);
        List<BucketMetadata> bucketMetadataList = new ArrayList<>();
        try {
            TableAssignment tableAssignment = zkClient.getTableAssignment(tableId).get();
            tableAssignment
                    .getBucketAssignments()
                    .forEach(
                            (bucketId, assignment) -> {
                                List<Integer> replicas = assignment.getReplicas();
                                try {
                                    LeaderAndIsr leaderAndIsr =
                                            zkClient.getLeaderAndIsr(
                                                            new TableBucket(tableId, bucketId))
                                                    .get();
                                    bucketMetadataList.add(
                                            new BucketMetadata(
                                                    bucketId,
                                                    leaderAndIsr.leader(),
                                                    leaderAndIsr.leaderEpoch(),
                                                    replicas));
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new TableMetadata(tableInfo, bucketMetadataList);
    }

    private PartitionMetadata getPartitionMetadataFromZk(
            long tableId, String partitionName, long partitionId) {
        List<BucketMetadata> bucketMetadataList = new ArrayList<>();
        try {
            TableAssignment tableAssignment = zkClient.getPartitionAssignment(partitionId).get();
            tableAssignment
                    .getBucketAssignments()
                    .forEach(
                            (bucketId, assignment) -> {
                                List<Integer> replicas = assignment.getReplicas();
                                try {
                                    LeaderAndIsr leaderAndIsr =
                                            zkClient.getLeaderAndIsr(
                                                            new TableBucket(
                                                                    tableId, partitionId, bucketId))
                                                    .get();
                                    bucketMetadataList.add(
                                            new BucketMetadata(
                                                    bucketId,
                                                    leaderAndIsr.leader(),
                                                    leaderAndIsr.leaderEpoch(),
                                                    replicas));
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });
            return new PartitionMetadata(tableId, partitionName, partitionId, bucketMetadataList);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class TableContext {
        final boolean isDeleted;
        final boolean isPartitionedTable;
        final TablePath tablePath;
        final long tableId;
        final @Nullable String partitionName;

        public TableContext(
                boolean isDeleted,
                boolean isPartitionedTable,
                TablePath tablePath,
                long tableId,
                @Nullable String partitionName) {
            this.isDeleted = isDeleted;
            this.isPartitionedTable = isPartitionedTable;
            this.tablePath = tablePath;
            this.tableId = tableId;
            this.partitionName = partitionName;
        }
    }
}

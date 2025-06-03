/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import com.alibaba.fluss.metadata.PartitionSpec;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.gateway.CoordinatorGateway;
import com.alibaba.fluss.server.tablet.TabletServer;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.fluss.record.TestData.DATA1_SCHEMA;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.createPartition;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newDropPartitionRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newDropTableRequest;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** IT Case for metadata update. */
class MetadataUpdateITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(3).build();

    private CoordinatorGateway coordinatorGateway;
    private ServerNode coordinatorServerNode;

    @BeforeEach
    void setup() {
        coordinatorGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        coordinatorServerNode = FLUSS_CLUSTER_EXTENSION.getCoordinatorServerNode();
    }

    @Test
    void testMetadataUpdateForServerStartAndStop() throws Exception {
        // get metadata and check it
        FLUSS_CLUSTER_EXTENSION.waitUtilAllGatewayHasSameMetadata();

        Map<Long, TablePath> expectedTablePathById = new HashMap<>();
        long tableId =
                createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR);
        expectedTablePathById.put(tableId, DATA1_TABLE_PATH);
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
        FLUSS_CLUSTER_EXTENSION.waitUtilAllGatewayHasSameMetadata();
        Map<Long, TablePath> expectedTablePathById = new HashMap<>();
        assertUpdateMetadataEquals(
                coordinatorServerNode, 3, expectedTablePathById, Collections.emptyMap());

        long tableId =
                createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR);
        expectedTablePathById.put(tableId, DATA1_TABLE_PATH);
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
        expectedTablePathById.put(partitionedTableId, partitionTablePath);
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
        expectedTablePathById.put(tableId, null);
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
        //        expectedTablePathById.put(partitionedTableId, null);
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
        FLUSS_CLUSTER_EXTENSION.waitUtilAllGatewayHasSameMetadata();
        Map<Long, TablePath> expectedTablePathById = new HashMap<>();
        Map<Long, String> expectedPartitionNameById = new HashMap<>();
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
        expectedTablePathById.put(partitionedTableId, partitionTablePath);
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
        expectedPartitionNameById.put(partitionId1, partitionName1);
        expectedPartitionNameById.put(partitionId2, partitionName2);
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
        expectedPartitionNameById.put(partitionId1, null);
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
            Map<Long, TablePath> expectedTablePathById,
            Map<Long, String> expectedPartitionNameById) {
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
                            (k, v) -> {
                                if (v != null) {
                                    assertThat(serverMetadataCache.getTablePath(k)).isPresent();
                                    assertThat(serverMetadataCache.getTablePath(k).get())
                                            .isEqualTo(v);
                                } else {
                                    assertThat(serverMetadataCache.getTablePath(k)).isNotPresent();
                                }
                            });
                    expectedPartitionNameById.forEach(
                            (k, v) -> {
                                if (v != null) {
                                    assertThat(serverMetadataCache.getPartitionName(k)).isPresent();
                                    assertThat(serverMetadataCache.getPartitionName(k).get())
                                            .isEqualTo(v);
                                } else {
                                    assertThat(serverMetadataCache.getPartitionName(k))
                                            .isNotPresent();
                                }
                            });
                });
    }
}

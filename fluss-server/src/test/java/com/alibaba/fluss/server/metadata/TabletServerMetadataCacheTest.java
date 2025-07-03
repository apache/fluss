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

import com.alibaba.fluss.cluster.Endpoint;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.cluster.TabletServerInfo;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.TableNotExistException;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.coordinator.MetadataManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.fluss.record.TestData.DATA1_PARTITIONED_TABLE_DESCRIPTOR;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_ID;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_INFO;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.server.metadata.PartitionMetadata.DELETED_PARTITION_ID;
import static com.alibaba.fluss.server.metadata.TableMetadata.DELETED_TABLE_ID;
import static com.alibaba.fluss.server.zk.data.LeaderAndIsr.NO_LEADER;
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
                                Arrays.asList(DATA1_TABLE_INFO, partitionTableInfo)),
                        null);
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
                                                System.currentTimeMillis(),
                                                System.currentTimeMillis()),
                                        changedBucket1BucketMetadata)),
                        Collections.emptyList()));
        assertThat(serverMetadataCache.getTablePath(DATA1_TABLE_ID)).isEmpty();

        // test delete one partition.
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
        assertThat(serverMetadataCache.getPhysicalTablePath(partitionId1)).isEmpty();
        assertPartitionMetadataEquals(
                partitionId2,
                partitionTableId,
                partitionId2,
                partitionName2,
                initialBucketMetadata);
    }

    private void assertTableMetadataEquals(
            long tableId,
            TableInfo expectedTableInfo,
            List<BucketMetadata> expectedBucketMetadataList) {
        TablePath tablePath = serverMetadataCache.getTablePath(tableId).get();
        TableMetadata tableMetadata = serverMetadataCache.getTableMetadata(tablePath);
        assertThat(tableMetadata.getTableInfo()).isEqualTo(expectedTableInfo);
        assertThat(tableMetadata.getBucketMetadataList())
                .hasSameElementsAs(expectedBucketMetadataList);
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
                serverMetadataCache.getPartitionMetadata(
                        PhysicalTablePath.of(partitionedTablePath, actualPartitionName));
        assertThat(partitionMetadata.getTableId()).isEqualTo(expectedTableId);
        assertThat(partitionMetadata.getPartitionId()).isEqualTo(expectedPartitionId);
        assertThat(partitionMetadata.getPartitionName()).isEqualTo(actualPartitionName);
        assertThat(partitionMetadata.getBucketMetadataList())
                .hasSameElementsAs(expectedBucketMetadataList);
    }

    private static final class TestingMetadataManager extends MetadataManager {

        private final Map<TablePath, TableInfo> tableInfoMap = new HashMap<>();

        public TestingMetadataManager(List<TableInfo> tableInfos) {
            super(null, new Configuration());
            tableInfos.forEach(tableInfo -> tableInfoMap.put(tableInfo.getTablePath(), tableInfo));
        }

        public TableInfo getTable(TablePath tablePath) throws TableNotExistException {
            return tableInfoMap.get(tablePath);
        }
    }
}

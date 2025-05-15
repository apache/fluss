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

import com.alibaba.fluss.cluster.Endpoint;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.cluster.TabletServerInfo;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.coordinator.CoordinatorContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.alibaba.fluss.record.TestData.DATA1_PARTITIONED_TABLE_DESCRIPTOR;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_ID;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_INFO;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CoordinatorServerMetadataCache}. */
public class CoordinatorServerMetadataCacheTest {
    private CoordinatorContext coordinatorContext;
    private CoordinatorServerMetadataCache serverMetadataCache;

    private ServerInfo coordinatorServer;
    private Set<ServerInfo> aliveTableServers;

    @BeforeEach
    public void setup() {
        coordinatorContext = new CoordinatorContext();
        serverMetadataCache = new CoordinatorServerMetadataCache(coordinatorContext);

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
    }

    @Test
    void testCoordinatorServerMetadataCache() {
        long partitionTableId = 150002L;
        long partitionId1 = 15L;
        String partitionName1 = "p1";
        long partitionId2 = 16L;
        String partitionName2 = "p2";

        coordinatorContext.setCoordinatorServerInfo(coordinatorServer);
        aliveTableServers.forEach(
                aliveTableServer -> coordinatorContext.addLiveTabletServer(aliveTableServer));

        coordinatorContext.putTablePath(DATA1_TABLE_ID, DATA1_TABLE_PATH);
        coordinatorContext.putTableInfo(DATA1_TABLE_INFO);

        TablePath partitionTablePath = TablePath.of("test_db_1", "test_partition_table_1");
        TableInfo partitionTableInfo =
                TableInfo.of(
                        partitionTablePath,
                        partitionTableId,
                        0,
                        DATA1_PARTITIONED_TABLE_DESCRIPTOR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());
        coordinatorContext.putTablePath(partitionTableId, partitionTablePath);
        coordinatorContext.putTableInfo(partitionTableInfo);

        coordinatorContext.putPartition(partitionId1, partitionName1);
        coordinatorContext.putPartition(partitionId2, partitionName2);

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
        assertThat(serverMetadataCache.getPartitionName(partitionId1).get())
                .isEqualTo(partitionName1);
        assertThat(serverMetadataCache.getPartitionName(partitionId2).get())
                .isEqualTo(partitionName2);
    }
}

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

package org.apache.fluss.client.metadata;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableOrPartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.server.coordinator.TestCoordinatorGateway;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.client.utils.MetadataUtils.sendMetadataRequestAndRebuildCluster;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.assertj.core.api.Assertions.assertThat;

/** IT test for update metadata of {@link MetadataUpdater}. */
class MetadataUpdaterITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(2).build();

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testPartialCacheThroughRpcAndClientRouting(boolean explicitCount) throws Exception {
        TablePath path = TablePath.of("fluss", "partial_routing_" + explicitCount);
        long tableId;
        try (Connection connection =
                ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig())) {
            connection.getAdmin().createTable(path, DATA1_TABLE_DESCRIPTOR, false).get();
            tableId = connection.getAdmin().getTableInfo(path).get().getTableId();
        }
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();
        TabletServer server = FLUSS_CLUSTER_EXTENSION.getTabletServers().iterator().next();
        TableMetadata full = server.getMetadataCache().getTableMetadata(path).get();
        server.getMetadataCache()
                .updateTableMetadata(
                        new TableMetadata(
                                full.getTableInfo(),
                                Collections.singletonList(full.getBucketMetadataList().get(0))));
        Cluster cluster =
                sendMetadataRequestAndRebuildCluster(
                        new TestCoordinatorGateway() {
                            @Override
                            public CompletableFuture<MetadataResponse> metadata(
                                    MetadataRequest request) {
                                return FLUSS_CLUSTER_EXTENSION
                                        .newTabletServerClientForNode(server.getServerId())
                                        .metadata(request)
                                        .thenApply(
                                                response -> {
                                                    assertThat(
                                                                    response.getTableMetadatasList()
                                                                            .get(0)
                                                                            .getBucketCount())
                                                            .isEqualTo(3);
                                                    if (!explicitCount) {
                                                        // Preserve the pre-extension response shape
                                                        // after the real RPC.
                                                        response.getTableMetadatasList()
                                                                .get(0)
                                                                .clearBucketCount();
                                                    }
                                                    return response;
                                                });
                            }
                        },
                        Collections.singleton(path));
        assertThat(cluster.getBucketCount(TableOrPartition.ofTable(tableId))).hasValue(3);
        assertThat(cluster.getBucketCountEpoch(tableId)).hasValue(0);
        assertThat(server.getMetadataCache().getTableMetadata(path).get().getBucketMetadataList())
                .hasSize(3);
    }

    @Test
    void testRebuildClusterNTimes() throws Exception {
        Configuration clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        Connection conn = ConnectionFactory.createConnection(clientConf);
        Admin admin = conn.getAdmin();
        TablePath tablePath = TablePath.of("fluss", "test");
        admin.createTable(tablePath, DATA1_TABLE_DESCRIPTOR, true).get();
        admin.close();
        conn.close();

        RpcClient rpcClient = FLUSS_CLUSTER_EXTENSION.getRpcClient();
        MetadataUpdater metadataUpdater = new MetadataUpdater(clientConf, rpcClient);
        // update metadata
        metadataUpdater.updateMetadata(Collections.singleton(tablePath), null, null);
        Cluster cluster = metadataUpdater.getCluster();

        // repeat 20K times to reproduce StackOverflowError if there is
        // any N levels UnmodifiableCollection
        for (int i = 0; i < 20000; i++) {
            cluster =
                    sendMetadataRequestAndRebuildCluster(
                            FLUSS_CLUSTER_EXTENSION.newCoordinatorClient(),
                            true,
                            cluster,
                            null,
                            null,
                            null);
        }
    }

    @Test
    void testUpdateWithEmptyMetadataResponse() throws Exception {
        RpcClient rpcClient = FLUSS_CLUSTER_EXTENSION.getRpcClient();
        MetadataUpdater metadataUpdater =
                new MetadataUpdater(FLUSS_CLUSTER_EXTENSION.getClientConfig(), rpcClient);

        // update metadata
        metadataUpdater.updateMetadata(null, null, null);
        Cluster cluster = metadataUpdater.getCluster();

        List<ServerNode> expectedServerNodes = FLUSS_CLUSTER_EXTENSION.getTabletServerNodes();
        assertThat(expectedServerNodes).hasSize(2);
        assertThat(cluster.getAliveTabletServerList()).isEqualTo(expectedServerNodes);

        // then, stop coordinator server, can still update metadata
        FLUSS_CLUSTER_EXTENSION.stopCoordinatorServer();
        metadataUpdater.updateMetadata(null, null, null);
        assertThat(cluster.getAliveTabletServerList()).isEqualTo(expectedServerNodes);

        // start a new tablet server, the tablet server will return empty metadata
        // response since no coordinator server to send newest metadata to the tablet server
        int newServerId = 2;
        FLUSS_CLUSTER_EXTENSION.startTabletServer(newServerId);

        // we mock a new cluster with only server 1 so that it'll only send request
        // to server 1, which will return empty resonate
        Cluster newCluster =
                new Cluster(
                        Collections.singletonMap(
                                newServerId,
                                FLUSS_CLUSTER_EXTENSION.getTabletServerNodes().get(newServerId)),
                        null,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());

        metadataUpdater = new MetadataUpdater(rpcClient, new Configuration(), newCluster);
        // shouldn't update metadata to empty since the empty metadata will be ignored
        metadataUpdater.updateMetadata(null, null, null);
        assertThat(metadataUpdater.getCluster().getAliveTabletServers())
                .isEqualTo(newCluster.getAliveTabletServers())
                .hasSize(1);

        // recover the coordinator
        FLUSS_CLUSTER_EXTENSION.startCoordinatorServer();
    }
}

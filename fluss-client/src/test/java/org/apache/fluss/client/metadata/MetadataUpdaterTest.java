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

import org.apache.fluss.client.utils.MetadataUtils;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableOrPartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.AdminReadOnlyGateway;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.PbPartitionMetadata;
import org.apache.fluss.rpc.messages.PbTableMetadata;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.server.coordinator.TestCoordinatorGateway;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.buildMetadataResponse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** UT Test for update metadata of {@link MetadataUpdater}. */
public class MetadataUpdaterTest {

    private static final ServerNode CS_NODE =
            new ServerNode(1, "localhost", 8080, ServerType.COORDINATOR);
    private static final ServerNode TS_NODE =
            new ServerNode(1, "localhost", 8080, ServerType.TABLET_SERVER);

    @Test
    void testInitializeClusterWithRetries() throws Exception {
        Configuration configuration = new Configuration();
        RpcClient rpcClient =
                RpcClient.create(configuration, TestingClientMetricGroup.newInstance());

        // retry lower than max retry count.
        AdminReadOnlyGateway gateway = new TestingAdminReadOnlyGateway(2);
        Cluster cluster =
                MetadataUpdater.tryToInitializeClusterWithRetries(rpcClient, CS_NODE, gateway, 3);
        assertThat(cluster).isNotNull();
        assertThat(cluster.getCoordinatorServer()).isEqualTo(CS_NODE);
        assertThat(cluster.getAliveTabletServerList()).containsExactly(TS_NODE);

        // retry higher than max retry count.
        AdminReadOnlyGateway gateway2 = new TestingAdminReadOnlyGateway(5);
        assertThatThrownBy(
                        () ->
                                MetadataUpdater.tryToInitializeClusterWithRetries(
                                        rpcClient, CS_NODE, gateway2, 3))
                .isInstanceOf(StaleMetadataException.class)
                .hasMessageContaining("The metadata is stale.");
    }

    @ParameterizedTest
    @CsvSource({"true, 0", "true, 3", "false, 0", "false, 3"})
    void testMetadataBucketCountCompatibility(boolean partialUpdate, int tableBucketCount)
            throws Exception {
        long tableId = 1L;
        long legacyPartitionId = 2L;
        long explicitPartitionId = 3L;
        long unassignedPartitionId = 4L;
        TablePath tablePath = TablePath.of("db", "table");

        MetadataResponse response = new MetadataResponse();
        response.addTabletServer()
                .setNodeId(TS_NODE.id())
                .setHost(TS_NODE.host())
                .setPort(TS_NODE.port());

        PbTableMetadata tableMetadata = response.addTableMetadata().setTableId(tableId);
        tableMetadata
                .setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        for (int bucketId = 0; bucketId < tableBucketCount; bucketId++) {
            tableMetadata.addBucketMetadata().setBucketId(bucketId);
        }

        PbPartitionMetadata explicitPartition =
                response.addPartitionMetadata()
                        .setTableId(tableId)
                        .setPartitionId(explicitPartitionId)
                        .setPartitionName("explicit")
                        .setBucketCount(4);
        explicitPartition.addBucketMetadata().setBucketId(0);
        explicitPartition.addBucketMetadata().setBucketId(1);

        PbPartitionMetadata legacyPartition =
                response.addPartitionMetadata()
                        .setTableId(tableId)
                        .setPartitionId(legacyPartitionId)
                        .setPartitionName("legacy");
        legacyPartition.addBucketMetadata().setBucketId(0);
        legacyPartition.addBucketMetadata().setBucketId(1);
        assertThat(legacyPartition.hasBucketCount()).isFalse();

        response.addPartitionMetadata()
                .setTableId(tableId)
                .setPartitionId(unassignedPartitionId)
                .setPartitionName("unassigned");

        Cluster originCluster =
                new Cluster(
                        Collections.singletonMap(TS_NODE.id(), TS_NODE),
                        CS_NODE,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        AdminReadOnlyGateway gateway =
                new TestCoordinatorGateway() {
                    @Override
                    public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
                        return CompletableFuture.completedFuture(response);
                    }
                };

        Cluster updatedCluster =
                MetadataUtils.sendMetadataRequestAndRebuildCluster(
                        gateway,
                        partialUpdate,
                        partialUpdate ? originCluster : null,
                        null,
                        null,
                        null);

        assertThat(updatedCluster.getBucketCount(TableOrPartition.ofTable(tableId))).hasValue(4);
        assertThat(updatedCluster.getBucketCount(TableOrPartition.ofPartition(legacyPartitionId)))
                .hasValue(2);
        assertThat(updatedCluster.getBucketCount(TableOrPartition.ofPartition(explicitPartitionId)))
                .hasValue(4);
        assertThat(
                        updatedCluster.getBucketCount(
                                TableOrPartition.ofPartition(unassignedPartitionId)))
                .isEmpty();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 2, 4, 8})
    void testPartitionMetadataOnlyIncreasesTableBucketCount(int initialTableBucketCount)
            throws Exception {
        long tableId = 1L;
        long partitionId = 2L;
        TablePath tablePath = TablePath.of("db", "table");
        TableOrPartition table = TableOrPartition.ofTable(tableId);
        Cluster cluster =
                new Cluster(
                        Collections.singletonMap(TS_NODE.id(), TS_NODE),
                        CS_NODE,
                        Collections.emptyMap(),
                        Collections.singletonMap(tablePath, tableId),
                        Collections.emptyMap(),
                        initialTableBucketCount == 0
                                ? Collections.emptyMap()
                                : Collections.singletonMap(table, initialTableBucketCount));

        int expectedTableBucketCount = initialTableBucketCount;
        for (int partitionBucketCount : new int[] {4, 2, 0, 6}) {
            MetadataResponse response = new MetadataResponse();
            response.addTabletServer()
                    .setNodeId(TS_NODE.id())
                    .setHost(TS_NODE.host())
                    .setPort(TS_NODE.port());
            // A partition-only refresh must also update the cached table-level count.
            response.addPartitionMetadata()
                    .setTableId(tableId)
                    .setPartitionId(partitionId)
                    .setPartitionName("p")
                    .setBucketCount(partitionBucketCount);
            AdminReadOnlyGateway gateway =
                    new TestCoordinatorGateway() {
                        @Override
                        public CompletableFuture<MetadataResponse> metadata(
                                MetadataRequest request) {
                            return CompletableFuture.completedFuture(response);
                        }
                    };

            Cluster previousCluster = cluster;
            cluster =
                    MetadataUtils.sendMetadataRequestAndRebuildCluster(
                            gateway, true, cluster, null, null, Collections.singleton(partitionId));

            if (expectedTableBucketCount == 0) {
                assertThat(previousCluster.getBucketCount(table)).isEmpty();
            } else {
                assertThat(previousCluster.getBucketCount(table))
                        .hasValue(expectedTableBucketCount);
            }
            expectedTableBucketCount = Math.max(expectedTableBucketCount, partitionBucketCount);
            assertThat(cluster.getBucketCount(table)).hasValue(expectedTableBucketCount);
            if (partitionBucketCount > 0) {
                // The partition keeps its actual count, even when smaller than the table count.
                assertThat(cluster.getBucketCount(TableOrPartition.ofPartition(partitionId)))
                        .hasValue(partitionBucketCount);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testStalePartitionMetadataRetriesCoordinator(boolean includesTable) {
        TablePath tablePath = TablePath.of("db", "recreated");
        PhysicalTablePath partitionPath = PhysicalTablePath.of(tablePath, "p");
        Cluster original = metadataCluster(tablePath, 2L);
        TestingMetadataRpcClient client =
                new TestingMetadataRpcClient(
                        tableAndPartitionMetadata(tablePath, includesTable ? 2L : 0L, 1L),
                        tableAndPartitionMetadata(tablePath, 2L, 2L));
        MetadataUpdater updater = new MetadataUpdater(client, new Configuration(), original);

        assertThat(updater.checkAndUpdatePartitionMetadata(partitionPath)).isTrue();
        assertThat(updater.getCluster().getTableId(tablePath)).contains(2L);
        assertThat(updater.getPartitionId(partitionPath)).contains(20L);
        assertThat(original.getPartitionId(partitionPath)).isEmpty();
        assertThat(client.nodes).containsExactly(TS_NODE, CS_NODE);
        assertThat(client.requests.get(1).getTablePathsList()).hasSize(1);
        assertThat(client.requests.get(1).getTablePathsList().get(0).getTableName())
                .isEqualTo(tablePath.getTableName());
    }

    @Test
    void testEmptyNodeMetadataKeepsPreviousClusterWithoutCoordinator() {
        Cluster original =
                new Cluster(
                        Collections.singletonMap(TS_NODE.id(), TS_NODE),
                        null,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        TestingMetadataRpcClient client =
                new TestingMetadataRpcClient(new MetadataResponse(), new MetadataResponse());
        MetadataUpdater updater = new MetadataUpdater(client, new Configuration(), original);

        updater.updateMetadata(null, null, null);

        assertThat(updater.getCluster()).isSameAs(original);
        assertThat(client.nodes).containsExactly(TS_NODE);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFailedCoordinatorRefreshDoesNotReportPartitionMissing(boolean networkFailure) {
        TablePath tablePath = TablePath.of("db", "recreated");
        Cluster original = metadataCluster(tablePath, 2L);
        MetadataResponse stale = tableAndPartitionMetadata(tablePath, 2L, 1L);
        TestingMetadataRpcClient client = new TestingMetadataRpcClient(stale, stale);
        client.failCoordinatorRequest = networkFailure;
        MetadataUpdater updater = new MetadataUpdater(client, new Configuration(), original);

        assertThatThrownBy(
                        () ->
                                updater.checkAndUpdatePartitionMetadata(
                                        PhysicalTablePath.of(tablePath, "p")))
                .isInstanceOf(StaleMetadataException.class);
        assertThat(updater.getCluster().getTableId(tablePath)).contains(2L);
        if (networkFailure) {
            assertThat(client.nodes).containsExactly(TS_NODE, CS_NODE, TS_NODE, CS_NODE);
        } else {
            assertThat(client.nodes).containsExactly(TS_NODE, CS_NODE);
        }
    }

    @Test
    void testTableRecreationRemovesClientPartitionMetadata() {
        TablePath tablePath = TablePath.of("db", "recreated");
        PhysicalTablePath oldPartition = PhysicalTablePath.of(tablePath, "old");
        Map<TableOrPartition, Integer> counts = new HashMap<>();
        counts.put(TableOrPartition.ofTable(1L), 10);
        counts.put(TableOrPartition.ofPartition(10L), 10);
        Cluster original =
                new Cluster(
                        Collections.singletonMap(TS_NODE.id(), TS_NODE),
                        CS_NODE,
                        Collections.singletonMap(oldPartition, Collections.emptyList()),
                        Collections.singletonMap(tablePath, 1L),
                        Collections.singletonMap(oldPartition, 10L),
                        counts);
        MetadataResponse fresh = tableAndPartitionMetadata(tablePath, 2L, 2L);
        TestingMetadataRpcClient client = new TestingMetadataRpcClient(fresh, fresh);
        MetadataUpdater updater = new MetadataUpdater(client, new Configuration(), original);
        updater.updateMetadata(Collections.singleton(tablePath), null, null);

        assertThat(updater.getCluster().getTableId(tablePath)).contains(2L);
        assertThat(updater.getCluster().getPartitionId(oldPartition)).isEmpty();
        assertThat(updater.getCluster().getBucketLocationsByPath()).doesNotContainKey(oldPartition);
        assertThat(updater.getCluster().getBucketCount(TableOrPartition.ofTable(1L))).isEmpty();
        assertThat(updater.getCluster().getBucketCount(TableOrPartition.ofPartition(10L)))
                .isEmpty();
        assertThat(original.getPartitionId(oldPartition)).contains(10L);
    }

    private static Cluster metadataCluster(TablePath tablePath, long tableId) {
        return new Cluster(
                Collections.singletonMap(TS_NODE.id(), TS_NODE), CS_NODE,
                Collections.emptyMap(), Collections.singletonMap(tablePath, tableId),
                Collections.emptyMap(), Collections.emptyMap());
    }

    private static MetadataResponse tableAndPartitionMetadata(
            TablePath tablePath, long tableId, long partitionTableId) {
        MetadataResponse response = new MetadataResponse();
        response.addTabletServer()
                .setNodeId(TS_NODE.id())
                .setHost(TS_NODE.host())
                .setPort(TS_NODE.port());
        response.setCoordinatorServer()
                .setNodeId(CS_NODE.id())
                .setHost(CS_NODE.host())
                .setPort(CS_NODE.port());
        if (tableId != 0) {
            response.addTableMetadata()
                    .setTableId(tableId)
                    .setTablePath()
                    .setDatabaseName(tablePath.getDatabaseName())
                    .setTableName(tablePath.getTableName());
        }
        response.addPartitionMetadata()
                .setTableId(partitionTableId)
                .setPartitionId(partitionTableId * 10)
                .setPartitionName("p")
                .setBucketCount(2);
        return response;
    }

    private static final class TestingMetadataRpcClient implements RpcClient {
        private final MetadataResponse tabletResponse;
        private final MetadataResponse coordinatorResponse;
        private boolean failCoordinatorRequest;
        private final List<ServerNode> nodes = new ArrayList<>();
        private final List<MetadataRequest> requests = new ArrayList<>();

        private TestingMetadataRpcClient(
                MetadataResponse tabletResponse, MetadataResponse coordinatorResponse) {
            this.tabletResponse = tabletResponse;
            this.coordinatorResponse = coordinatorResponse;
        }

        @Override
        public CompletableFuture<ApiMessage> sendRequest(
                ServerNode node, ApiKeys apiKey, ApiMessage request) {
            assertThat(apiKey).isEqualTo(ApiKeys.GET_METADATA);
            nodes.add(node);
            requests.add((MetadataRequest) request);
            if (node.equals(CS_NODE) && failCoordinatorRequest) {
                CompletableFuture<ApiMessage> failure = new CompletableFuture<>();
                failure.completeExceptionally(new NetworkException("Coordinator unavailable"));
                return failure;
            }
            MetadataRequest metadataRequest = (MetadataRequest) request;
            if (failCoordinatorRequest
                    && metadataRequest.getTablePathsCount() == 0
                    && metadataRequest.getPartitionsPathsCount() == 0
                    && metadataRequest.getPartitionsIdsCount() == 0) {
                return CompletableFuture.completedFuture(
                        buildMetadataResponse(
                                CS_NODE,
                                Collections.singleton(TS_NODE),
                                Collections.emptyList(),
                                Collections.emptyList()));
            }
            return CompletableFuture.completedFuture(
                    node.equals(CS_NODE) ? coordinatorResponse : tabletResponse);
        }

        @Override
        public boolean connect(ServerNode node) {
            return true;
        }

        @Override
        public boolean isReady(String serverUid) {
            return true;
        }

        @Override
        public CompletableFuture<Void> disconnect(String serverUid) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {}
    }

    private static final class TestingAdminReadOnlyGateway extends TestCoordinatorGateway {

        private final int maxRetryCount;
        private int retryCount;

        public TestingAdminReadOnlyGateway(int maxRetryCount) {
            this.maxRetryCount = maxRetryCount;
        }

        @Override
        public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
            retryCount++;
            if (retryCount <= maxRetryCount) {
                throw new StaleMetadataException("The metadata is stale.");
            } else {
                MetadataResponse metadataResponse =
                        buildMetadataResponse(
                                CS_NODE,
                                Collections.singleton(TS_NODE),
                                Collections.emptyList(),
                                Collections.emptyList());
                return CompletableFuture.completedFuture(metadataResponse);
            }
        }
    }
}

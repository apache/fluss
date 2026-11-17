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

import org.apache.fluss.client.admin.FlussAdmin;
import org.apache.fluss.client.metrics.TestingWriterMetricGroup;
import org.apache.fluss.client.utils.MetadataUtils;
import org.apache.fluss.client.utils.TestingRpcClient;
import org.apache.fluss.client.write.DynamicPartitionCreator;
import org.apache.fluss.client.write.WriteRecord;
import org.apache.fluss.client.write.WriterClient;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.exception.UnknownTableOrBucketException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TableOrPartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.AdminReadOnlyGateway;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.CreatePartitionResponse;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.apache.fluss.record.TestData.DATA1_PARTITIONED_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.buildMetadataResponse;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.utils.concurrent.FutureUtils.completedExceptionally;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** UT Test for update metadata of {@link MetadataUpdater}. */
public class MetadataUpdaterTest {

    private static final ServerNode CS_NODE =
            new ServerNode(1, "localhost", 8080, ServerType.COORDINATOR);
    private static final ServerNode TS_NODE =
            new ServerNode(1, "localhost", 8080, ServerType.TABLET_SERVER);

    private static final TablePath RECREATED_TABLE = TablePath.of("db", "recreated");
    private static final PhysicalTablePath PARTITION = PhysicalTablePath.of(RECREATED_TABLE, "p");

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testLegacyTableCountDoesNotDependOnLocations(boolean partialUpdate) throws Exception {
        MetadataResponse response = new MetadataResponse();
        response.addTabletServer().setNodeId(1).setHost("localhost").setPort(8080);
        PbTableMetadata table =
                response.addTableMetadata()
                        .setTableId(1)
                        .setTableJson(DATA1_TABLE_DESCRIPTOR.withBucketCount(3).toJsonBytes());
        table.setTablePath().setDatabaseName("db").setTableName("table");
        table.addBucketMetadata().setBucketId(0);
        AdminReadOnlyGateway gateway =
                new TestCoordinatorGateway() {
                    @Override
                    public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
                        return CompletableFuture.completedFuture(response);
                    }
                };
        Cluster cluster =
                MetadataUtils.sendMetadataRequestAndRebuildCluster(
                        gateway,
                        partialUpdate,
                        partialUpdate ? Cluster.empty() : null,
                        null,
                        null,
                        null);
        assertThat(cluster.getBucketCount(TableOrPartition.ofTable(1))).hasValue(3);
    }

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

    @Test
    void testRetryDisconnectsOnlyFailedBootstrapEndpoint() throws Exception {
        ServerNode failedNode = new ServerNode(-1, "bootstrap-a", 9001, ServerType.UNKNOWN);
        ServerNode otherNode = new ServerNode(-1, "bootstrap-b", 9002, ServerType.UNKNOWN);

        // Bootstrap endpoints share a UID despite having different addresses.
        assertThat(failedNode.uid()).isEqualTo(otherNode.uid());

        TestingRpcClient rpcClient =
                new TestingRpcClient(
                        (node, apiKey) -> {
                            throw new AssertionError("Requests should go through the test gateway");
                        });
        AdminReadOnlyGateway gateway = new TestingAdminReadOnlyGateway(1);

        // The first attempt fails and the next attempt succeeds.
        Cluster cluster =
                MetadataUpdater.tryToInitializeClusterWithRetries(
                        rpcClient, failedNode, gateway, 1);

        assertThat(cluster).isNotNull();

        assertThat(rpcClient.getDisconnectedNodes()).containsExactly(failedNode);
        assertThat(rpcClient.getDisconnectedServerUids()).isEmpty();
    }

    @Test
    void testInitializeClusterDisconnectsOnlyFailedBootstrapEndpoint() {
        Configuration configuration = new Configuration();
        configuration.set(
                ConfigOptions.BOOTSTRAP_SERVERS,
                Arrays.asList("localhost:19001", "localhost:19002"));

        ServerNode failedNode = new ServerNode(-1, "localhost", 19001, ServerType.UNKNOWN);
        ServerNode healthyNode = new ServerNode(-1, "localhost", 19002, ServerType.UNKNOWN);

        assertThat(failedNode.uid()).isEqualTo(healthyNode.uid());

        TestingRpcClient rpcClient =
                new TestingRpcClient(
                        (node, apiKey) -> {
                            assertThat(apiKey).isEqualTo(ApiKeys.GET_METADATA);
                            if (node.equals(failedNode)) {
                                throw new StaleMetadataException("Bootstrap endpoint unavailable");
                            }
                            assertThat(node).isEqualTo(healthyNode);
                            return CompletableFuture.completedFuture(
                                    buildMetadataResponse(
                                            CS_NODE,
                                            Collections.singleton(TS_NODE),
                                            Collections.emptyList(),
                                            Collections.emptyList()));
                        });

        MetadataUpdater updater = new MetadataUpdater(configuration, rpcClient);

        assertThat(updater.getCluster().getCoordinatorServer()).isEqualTo(CS_NODE);
        assertThat(updater.getCluster().getAliveTabletServerList()).containsExactly(TS_NODE);

        assertThat(rpcClient.getDisconnectedNodes()).containsExactly(failedNode);
        assertThat(rpcClient.getDisconnectedServerUids()).isEmpty();
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

        PbTableMetadata tableMetadata =
                response.addTableMetadata()
                        .setTableId(tableId)
                        .setTableJson(
                                DATA1_PARTITIONED_TABLE_DESCRIPTOR
                                        .withBucketCount(3)
                                        .toJsonBytes());
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

        assertThat(updatedCluster.getBucketCount(TableOrPartition.ofTable(tableId))).hasValue(3);
        assertThat(updatedCluster.getBucketCount(TableOrPartition.ofPartition(legacyPartitionId)))
                .hasValue(3);
        assertThat(updatedCluster.getBucketCount(TableOrPartition.ofPartition(explicitPartitionId)))
                .hasValue(4);
        assertThat(
                        updatedCluster.getBucketCount(
                                TableOrPartition.ofPartition(unassignedPartitionId)))
                .hasValue(3);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 2, 4, 8})
    void testPartitionMetadataDoesNotChangeTableDefault(int initialTableBucketCount)
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
                                : Collections.singletonMap(table, initialTableBucketCount),
                        Collections.emptyMap());

        int expectedTableBucketCount = initialTableBucketCount;
        for (int partitionBucketCount : new int[] {4, 2, 6}) {
            MetadataResponse response = new MetadataResponse();
            response.addTabletServer()
                    .setNodeId(TS_NODE.id())
                    .setHost(TS_NODE.host())
                    .setPort(TS_NODE.port());
            // A partition-only refresh cannot establish the current table default.
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
            if (expectedTableBucketCount == 0) {
                assertThat(cluster.getBucketCount(table)).isEmpty();
            } else {
                assertThat(cluster.getBucketCount(table)).hasValue(expectedTableBucketCount);
            }
            // The partition keeps its actual count, even when smaller than the table count.
            assertThat(cluster.getBucketCount(TableOrPartition.ofPartition(partitionId)))
                    .hasValue(partitionBucketCount);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testExplicitCountDoesNotDependOnAvailableLocations(boolean partialUpdate)
            throws Exception {
        MetadataResponse response = routingResponse(3, 0, true);
        response.getTableMetadatasList().get(0).getBucketMetadatasList().get(0).setLeaderId(1);
        Cluster cluster =
                updateRouting(response, partialUpdate ? Cluster.empty() : null, partialUpdate);
        assertThat(cluster.getBucketCount(TableOrPartition.ofTable(1))).hasValue(3);
        assertThat(
                        cluster.getAvailableBucketsForPhysicalTablePath(
                                PhysicalTablePath.of(TablePath.of("db", "table"))))
                .hasSize(1);
    }

    @Test
    void testCurrentServerWithoutTableCountSupportsMixedPartitions() throws Exception {
        MetadataResponse response = routingPartitionResponse(6, 1, false);
        response.addPartitionMetadata()
                .setTableId(1)
                .setPartitionId(2)
                .setPartitionName("old")
                .setBucketCount(3);
        response.addPartitionMetadata()
                .setTableId(1)
                .setPartitionId(3)
                .setPartitionName("new")
                .setBucketCount(6);
        Cluster cluster = updateRouting(response, null, false);
        assertThat(cluster.getBucketCount(TableOrPartition.ofTable(1))).hasValue(6);
        assertThat(cluster.getBucketCount(TableOrPartition.ofPartition(2))).hasValue(3);
        assertThat(cluster.getBucketCount(TableOrPartition.ofPartition(3))).hasValue(6);
    }

    @Test
    void testEpochOrdersGrowthAndShrinkageWithoutDroppingPartitionUpdates() throws Exception {
        Cluster cluster = updateRouting(routingPartitionResponse(3, 0, true), null, false);
        cluster = updateRouting(routingPartitionResponse(6, 1, true), cluster, true);
        cluster = updateRouting(routingPartitionResponse(2, 2, true), cluster, true);
        assertThat(cluster.getBucketCount(TableOrPartition.ofTable(1))).hasValue(2);
        MetadataResponse delayed = routingPartitionResponse(6, 1, false);
        delayed.addPartitionMetadata()
                .setTableId(1)
                .setPartitionId(2)
                .setPartitionName("old")
                .setBucketCount(6);
        cluster = updateRouting(delayed, cluster, true);
        assertThat(cluster.getBucketCount(TableOrPartition.ofTable(1))).hasValue(2);
        assertThat(cluster.getBucketCountEpoch(1)).hasValue(2);
        assertThat(cluster.getBucketCount(TableOrPartition.ofPartition(2))).hasValue(6);
        Cluster stable = cluster;
        assertThatThrownBy(() -> updateRouting(routingResponse(6, 2, true), stable, true))
                .hasCauseInstanceOf(StaleMetadataException.class);
    }

    @ParameterizedTest
    @CsvSource({"false, 0", "true, 0", "true, -1"})
    void testUnresolvedPartitionCountIsRejected(boolean explicit, int count) throws Exception {
        Cluster cluster = updateRouting(routingPartitionResponse(6, 1, true), null, false);
        MetadataResponse response = routingPartitionResponse(6, 1, true);
        PbPartitionMetadata partition =
                response.addPartitionMetadata()
                        .setTableId(1)
                        .setPartitionId(2)
                        .setPartitionName("p");
        if (explicit) {
            partition.setBucketCount(count);
        }
        assertThatThrownBy(() -> updateRouting(response, cluster, true))
                .hasCauseInstanceOf(StaleMetadataException.class);
    }

    @ParameterizedTest
    @CsvSource({"0, 0", "-1, 0", "3, -1"})
    void testInvalidExplicitLayoutIsNotTreatedAsLegacy(int count, long epoch) {
        MetadataResponse response = routingResponse(3, 0, true);
        response.getTableMetadatasList().get(0).setBucketCount(count).setBucketCountEpoch(epoch);
        assertThatThrownBy(() -> updateRouting(response, null, false))
                .hasCauseInstanceOf(StaleMetadataException.class);
    }

    @Test
    void testInvalidationRetainsEpochAgainstStaleTableInfo() throws Exception {
        TablePath path = TablePath.of("db", "table");
        Cluster cluster =
                updateRouting(routingResponse(3, 1, true), null, false)
                        .invalidPhysicalTableBucketMeta(
                                Collections.singleton(PhysicalTablePath.of(path)));
        TableInfo stale =
                TableInfo.of(path, 1, 0, DATA1_TABLE_DESCRIPTOR, DEFAULT_REMOTE_DATA_DIR, 0, 0);
        assertThatThrownBy(() -> cluster.getBucketCountOrFallback(stale, null))
                .isInstanceOf(org.apache.fluss.exception.InvalidBucketRoutingException.class);
        assertThat(cluster.getBucketCountEpoch(1)).hasValue(1);
    }

    private static MetadataResponse routingResponse(int count, long epoch, boolean explicit) {
        MetadataResponse response = new MetadataResponse();
        response.addTabletServer().setNodeId(1).setHost("localhost").setPort(8080);
        PbTableMetadata table =
                response.addTableMetadata()
                        .setTableId(1)
                        .setTableJson(DATA1_TABLE_DESCRIPTOR.withBucketCount(count).toJsonBytes())
                        .setBucketCountEpoch(epoch);
        table.setTablePath().setDatabaseName("db").setTableName("table");
        table.addBucketMetadata().setBucketId(0);
        if (explicit) {
            table.setBucketCount(count);
        }
        return response;
    }

    @Test
    void testPartitionOnlyLegacyResponseUsesCachedTableCount() throws Exception {
        MetadataResponse tableResponse = routingPartitionResponse(3, 0, false);
        tableResponse.getTableMetadatasList().get(0).clearBucketCountEpoch();
        Cluster cachedCluster = updateRouting(tableResponse, null, false);
        assertThat(cachedCluster.getBucketCount(TableOrPartition.ofTable(1))).hasValue(3);
        assertThat(cachedCluster.getBucketCountEpoch(1)).hasValue(0);

        MetadataResponse partitionResponse = new MetadataResponse();
        partitionResponse.addTabletServer().setNodeId(1).setHost("localhost").setPort(8080);
        partitionResponse
                .addPartitionMetadata()
                .setTableId(1)
                .setPartitionId(2)
                .setPartitionName("p")
                .addBucketMetadata()
                .setBucketId(0)
                .setLeaderId(1);

        Cluster updatedCluster = updateRouting(partitionResponse, cachedCluster, true);

        assertThat(updatedCluster.getBucketCount(TableOrPartition.ofTable(1))).hasValue(3);
        assertThat(updatedCluster.getBucketCountEpoch(1)).hasValue(0);
        assertThat(updatedCluster.getBucketCount(TableOrPartition.ofPartition(2))).hasValue(3);
        assertThat(
                        updatedCluster
                                .getBucketLocationsByPath()
                                .get(PhysicalTablePath.of("db", "table", "p")))
                .hasSize(1);
    }

    @Test
    void testExplicitCountWithoutEpochDoesNotEnableLegacyFallback() {
        MetadataResponse response = routingPartitionResponse(6, 1, true);
        response.getTableMetadatasList().get(0).clearBucketCountEpoch();
        response.addPartitionMetadata().setTableId(1).setPartitionId(2).setPartitionName("p");
        assertThatThrownBy(() -> updateRouting(response, null, false))
                .hasCauseInstanceOf(StaleMetadataException.class);
    }

    private static MetadataResponse routingPartitionResponse(
            int count, long epoch, boolean explicit) {
        MetadataResponse response = routingResponse(count, epoch, explicit);
        response.getTableMetadatasList()
                .get(0)
                .setTableJson(
                        DATA1_PARTITIONED_TABLE_DESCRIPTOR.withBucketCount(count).toJsonBytes());
        return response;
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testPartitionWithoutParentMetadataIsNotPublished(boolean partialUpdate) {
        MetadataResponse response = new MetadataResponse();
        response.addTabletServer().setNodeId(1).setHost("localhost").setPort(8080);
        response.addPartitionMetadata()
                .setTableId(1)
                .setPartitionId(2)
                .setPartitionName("p")
                .setBucketCount(3);
        assertThatThrownBy(
                        () ->
                                updateRouting(
                                        response,
                                        partialUpdate ? Cluster.empty() : null,
                                        partialUpdate))
                .hasCauseInstanceOf(StaleMetadataException.class);
    }

    private static Cluster updateRouting(MetadataResponse response, Cluster origin, boolean partial)
            throws Exception {
        return MetadataUtils.sendMetadataRequestAndRebuildCluster(
                new TestCoordinatorGateway() {
                    @Override
                    public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
                        return CompletableFuture.completedFuture(response);
                    }
                },
                partial,
                origin,
                null,
                null,
                null);
    }

    @ParameterizedTest
    @ValueSource(longs = {0L, 2L, 1L})
    void testStalePartitionMetadataRetriesTabletWithParent(long responseTableId) {
        // The response can omit the parent, include the current parent, or be entirely stale.
        Cluster original = clusterWithRecreatedTable();
        TestingMetadataRpcClient client =
                new TestingMetadataRpcClient(
                        tableAndPartitionMetadata(responseTableId, 1L),
                        tableAndPartitionMetadata(2L, 2L));
        MetadataUpdater updater = new MetadataUpdater(client, new Configuration(), original);

        assertThat(updater.checkAndUpdatePartitionMetadata(PARTITION)).isTrue();
        assertThat(updater.getCluster().getTableId(RECREATED_TABLE)).contains(2L);
        assertThat(updater.getPartitionId(PARTITION)).contains(20L);
        assertThat(original.getPartitionId(PARTITION)).isEmpty();
        assertThat(client.nodes).containsExactly(TS_NODE, TS_NODE);
        assertThat(client.requests.get(0).getTablePathsList()).isEmpty();
        assertThat(client.requests.get(1).getPartitionsPathsList())
                .hasSameSizeAs(client.requests.get(0).getPartitionsPathsList())
                .hasSize(1);
        assertThat(client.requests.get(1).getTablePathsList()).hasSize(1);
        assertThat(client.requests.get(1).getTablePathsList().get(0).getTableName())
                .isEqualTo(RECREATED_TABLE.getTableName());
    }

    @Test
    void testTabletReportedConflictRetriesAnotherTablet() {
        ServerNode other = new ServerNode(2, "localhost", 8081, ServerType.TABLET_SERVER);
        Map<Integer, ServerNode> tablets = new HashMap<>();
        tablets.put(TS_NODE.id(), TS_NODE);
        tablets.put(other.id(), other);
        Cluster original =
                new Cluster(
                        tablets,
                        CS_NODE,
                        Collections.emptyMap(),
                        Collections.singletonMap(RECREATED_TABLE, 2L),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        TestingMetadataRpcClient client =
                new TestingMetadataRpcClient(
                        new MetadataResponse(), tableAndPartitionMetadata(2L, 2L));
        client.firstFailure = new UnknownTableOrBucketException("Parent replaced");
        MetadataUpdater updater = new MetadataUpdater(client, new Configuration(), original);

        assertThat(updater.checkAndUpdatePartitionMetadata(PARTITION)).isTrue();
        assertThat(client.nodes).containsExactlyInAnyOrder(TS_NODE, other);
        MetadataRequest retry = client.requests.get(1);
        assertThat(retry.getTablePathsCount()).isOne();
        assertThat(retry.getPartitionsPathsCount()).isOne();
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
    void testFailedTabletRecoveryDoesNotReportPartitionMissing(boolean networkFailure) {
        Cluster original = clusterWithRecreatedTable();
        MetadataResponse stale = tableAndPartitionMetadata(2L, 1L);
        TestingMetadataRpcClient client = new TestingMetadataRpcClient(stale, stale);
        if (networkFailure) {
            client.recoveryFailure = new NetworkException("Tablet unavailable");
        }
        MetadataUpdater updater = new MetadataUpdater(client, new Configuration(), original);

        assertThatThrownBy(() -> updater.checkAndUpdatePartitionMetadata(PARTITION))
                .isInstanceOf(StaleMetadataException.class);
        assertThat(updater.getCluster().getTableId(RECREATED_TABLE)).contains(2L);
        assertThat(client.nodes).containsExactly(TS_NODE, TS_NODE);
    }

    @Test
    void testPartitionRecoveryTimeoutPreservesConflict() {
        Cluster original = clusterWithRecreatedTable();
        MetadataResponse stale = tableAndPartitionMetadata(0L, 1L);
        TestingMetadataRpcClient client = new TestingMetadataRpcClient(stale, stale);
        TimeoutException timeout = new TimeoutException();
        client.recoveryFailure = timeout;
        MetadataUpdater updater = new MetadataUpdater(client, new Configuration(), original);

        assertThatThrownBy(() -> updater.checkAndUpdatePartitionMetadata(PARTITION))
                .isInstanceOf(StaleMetadataException.class)
                .hasCause(timeout);
        assertThat(client.nodes).containsExactly(TS_NODE, TS_NODE);
        assertThat(updater.getCluster()).isSameAs(original);
    }

    @Test
    void testMissingPartitionDuringRecovery() {
        MetadataResponse stale = tableAndPartitionMetadata(0L, 1L);
        TestingMetadataRpcClient client = new TestingMetadataRpcClient(stale, stale);
        client.recoveryFailure = new PartitionNotExistException("Partition was deleted");
        MetadataUpdater updater =
                new MetadataUpdater(client, new Configuration(), clusterWithRecreatedTable());

        assertThatThrownBy(() -> updater.checkAndUpdatePartitionMetadata(PARTITION))
                .isSameAs(client.recoveryFailure);
        assertThat(client.nodes).containsExactly(TS_NODE, TS_NODE);
    }

    @Test
    void testPersistentTableIdConflictDoesNotCreatePartition() {
        TableInfo table = partitionedTableForWrite();
        TestingMetadataRpcClient rpc =
                new TestingMetadataRpcClient(
                        tableAndPartitionMetadata(0L, 1L),
                        tableAndPartitionMetadata(table.getTableId(), 1L));
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.CLIENT_WRITER_DYNAMIC_CREATE_PARTITION_ENABLED, true);
        conf.set(ConfigOptions.CLIENT_WRITER_ENABLE_IDEMPOTENCE, false);
        MetadataUpdater updater = new MetadataUpdater(rpc, conf, clusterWithRecreatedTable());
        List<ApiKeys> adminRequests = new ArrayList<>();
        try (FlussAdmin admin = recordingPartitionAdmin(updater, adminRequests)) {
            WriterClient writer =
                    new WriterClient(conf, updater, TestingWriterMetricGroup.newInstance(), admin);
            try {
                WriteRecord record =
                        WriteRecord.forArrowAppend(table, PARTITION, row(1, "p"), null);
                assertThatThrownBy(() -> writer.send(record, (bucket, offset, error) -> {}))
                        .isInstanceOf(StaleMetadataException.class);
                assertThat(adminRequests).isEmpty();
                assertThat(rpc.requests).hasSize(2);
                assertThat(rpc.requests.get(0).getTablePathsCount()).isZero();
                assertThat(rpc.requests.get(1).getTablePathsCount()).isOne();
            } finally {
                writer.close(Duration.ZERO);
            }
        }
    }

    @Test
    void testExplicitlyMissingPartitionTriggersCreation() {
        TableInfo table = partitionedTableForWrite();
        TestingRpcClient client =
                new TestingRpcClient(
                        (node, api) ->
                                completedExceptionally(
                                        new PartitionNotExistException(
                                                "Partition p does not exist")));
        MetadataUpdater updater =
                new MetadataUpdater(client, new Configuration(), clusterWithRecreatedTable());
        List<ApiKeys> adminRequests = new ArrayList<>();
        try (FlussAdmin admin = recordingPartitionAdmin(updater, adminRequests)) {
            DynamicPartitionCreator creator =
                    new DynamicPartitionCreator(
                            updater,
                            admin,
                            true,
                            error -> {
                                throw new AssertionError(error);
                            });
            creator.checkAndCreatePartitionAsync(PARTITION, table);
            assertThat(adminRequests).containsExactly(ApiKeys.CREATE_PARTITION);
        }
    }

    private static TableInfo partitionedTableForWrite() {
        return TableInfo.of(
                RECREATED_TABLE,
                2L,
                0,
                TableDescriptor.builder(DATA1_TABLE_DESCRIPTOR).partitionedBy("b").build(),
                DEFAULT_REMOTE_DATA_DIR,
                0,
                0);
    }

    private static FlussAdmin recordingPartitionAdmin(
            MetadataUpdater updater, List<ApiKeys> requests) {
        return new FlussAdmin(
                new TestingRpcClient(
                        (node, api) -> {
                            requests.add(api);
                            return CompletableFuture.completedFuture(new CreatePartitionResponse());
                        }),
                updater);
    }

    @Test
    void testTableRecreationRemovesClientPartitionMetadata() {
        PhysicalTablePath oldPartition = PhysicalTablePath.of(RECREATED_TABLE, "old");
        Map<TableOrPartition, Integer> counts = new HashMap<>();
        counts.put(TableOrPartition.ofTable(1L), 10);
        counts.put(TableOrPartition.ofPartition(10L), 10);
        Cluster original =
                new Cluster(
                        Collections.singletonMap(TS_NODE.id(), TS_NODE),
                        CS_NODE,
                        Collections.singletonMap(oldPartition, Collections.emptyList()),
                        Collections.singletonMap(RECREATED_TABLE, 1L),
                        Collections.singletonMap(oldPartition, 10L),
                        counts,
                        Collections.singletonMap(1L, 0L));
        MetadataResponse fresh = tableAndPartitionMetadata(2L, 2L);
        TestingMetadataRpcClient client = new TestingMetadataRpcClient(fresh, fresh);
        MetadataUpdater updater = new MetadataUpdater(client, new Configuration(), original);
        updater.updateMetadata(Collections.singleton(RECREATED_TABLE), null, null);

        assertThat(updater.getCluster().getTableId(RECREATED_TABLE)).contains(2L);
        assertThat(updater.getCluster().getPartitionId(oldPartition)).isEmpty();
        assertThat(updater.getCluster().getBucketLocationsByPath()).doesNotContainKey(oldPartition);
        assertThat(updater.getCluster().getBucketCount(TableOrPartition.ofTable(1L))).isEmpty();
        assertThat(updater.getCluster().getBucketCount(TableOrPartition.ofPartition(10L)))
                .isEmpty();
        assertThat(updater.getCluster().getBucketCountEpoch(1L)).isEmpty();
        assertThat(original.getPartitionId(oldPartition)).contains(10L);
    }

    private static Cluster clusterWithRecreatedTable() {
        return new Cluster(
                Collections.singletonMap(TS_NODE.id(), TS_NODE),
                CS_NODE,
                Collections.emptyMap(),
                Collections.singletonMap(RECREATED_TABLE, 2L),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap());
    }

    private static MetadataResponse tableAndPartitionMetadata(long tableId, long partitionTableId) {
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
                    .setBucketCount(2)
                    .setBucketCountEpoch(0)
                    .setTablePath()
                    .setDatabaseName(RECREATED_TABLE.getDatabaseName())
                    .setTableName(RECREATED_TABLE.getTableName());
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
        private final MetadataResponse recoveryResponse;
        private Exception firstFailure;
        private Exception recoveryFailure;
        private final List<ServerNode> nodes = new ArrayList<>();
        private final List<MetadataRequest> requests = new ArrayList<>();

        private TestingMetadataRpcClient(
                MetadataResponse tabletResponse, MetadataResponse recoveryResponse) {
            this.tabletResponse = tabletResponse;
            this.recoveryResponse = recoveryResponse;
        }

        @Override
        public CompletableFuture<ApiMessage> sendRequest(
                ServerNode node, ApiKeys apiKey, ApiMessage request) {
            assertThat(apiKey).isEqualTo(ApiKeys.GET_METADATA);
            nodes.add(node);
            requests.add((MetadataRequest) request);
            assertThat(node.serverType()).isEqualTo(ServerType.TABLET_SERVER);
            Exception failure = requests.size() == 1 ? firstFailure : recoveryFailure;
            if (failure != null) {
                return completedExceptionally(failure);
            }
            return CompletableFuture.completedFuture(
                    requests.size() == 1 ? tabletResponse : recoveryResponse);
        }

        @Override
        public boolean connect(ServerNode node) {
            return true;
        }

        @Override
        public boolean isReady(ServerNode node) {
            return true;
        }

        @Override
        public CompletableFuture<Void> disconnect(ServerNode node) {
            return CompletableFuture.completedFuture(null);
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

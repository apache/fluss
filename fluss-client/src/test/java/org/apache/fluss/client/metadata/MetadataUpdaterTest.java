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
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableOrPartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.AdminReadOnlyGateway;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.PbPartitionMetadata;
import org.apache.fluss.rpc.messages.PbTableMetadata;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.server.coordinator.TestCoordinatorGateway;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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

    @Test
    void testFullUpdateResolvesPartitionUsingOriginCluster() throws Exception {
        TablePath tablePath = TablePath.of("db", "table");
        PhysicalTablePath partitionPath = PhysicalTablePath.of(tablePath, "p");
        Cluster originCluster =
                new Cluster(
                        Collections.singletonMap(TS_NODE.id(), TS_NODE),
                        CS_NODE,
                        Collections.emptyMap(),
                        Collections.singletonMap(tablePath, 1L),
                        Collections.emptyMap(),
                        Collections.singletonMap(TableOrPartition.ofTable(1L), 8));
        MetadataResponse response = new MetadataResponse();
        response.addTabletServer()
                .setNodeId(TS_NODE.id())
                .setHost(TS_NODE.host())
                .setPort(TS_NODE.port());
        response.addPartitionMetadata()
                .setTableId(1L)
                .setPartitionId(11L)
                .setPartitionName("p")
                .setBucketCount(4);
        AdminReadOnlyGateway gateway =
                new TestCoordinatorGateway() {
                    @Override
                    public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
                        return CompletableFuture.completedFuture(response);
                    }
                };

        Cluster rebuilt =
                MetadataUtils.sendMetadataRequestAndRebuildCluster(
                        gateway, false, originCluster, null, null, Collections.singleton(11L));

        assertThat(rebuilt.getPartitionId(partitionPath)).hasValue(11L);
        assertThat(rebuilt.getBucketCount(TableOrPartition.ofTable(1L))).hasValue(8);
        assertThat(rebuilt.getTableId(tablePath)).isEmpty();
    }

    @Test
    void testRebuildFailureIsWrappedInExecutionException() {
        AdminReadOnlyGateway gateway =
                new TestCoordinatorGateway() {
                    @Override
                    public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
                        return CompletableFuture.completedFuture(new MetadataResponse());
                    }
                };

        assertThatThrownBy(
                        () ->
                                MetadataUtils.sendMetadataRequestAndRebuildCluster(
                                        gateway, Collections.emptySet()))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(StaleMetadataException.class);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testNodeOnlyPartialUpdateReusesMetadata(boolean changedNodes) {
        TablePath tablePath = TablePath.of("db", "table");
        PhysicalTablePath partitionPath = PhysicalTablePath.of(tablePath, "p");
        TableBucket bucket = new TableBucket(1L, 11L, 0);
        ServerNode oldNode =
                new ServerNode(
                        1,
                        "localhost",
                        8080,
                        ServerType.TABLET_SERVER,
                        null,
                        ServerTag.TEMPORARY_OFFLINE);
        Cluster original =
                new Cluster(
                        Collections.singletonMap(1, oldNode),
                        CS_NODE,
                        Collections.singletonMap(
                                partitionPath,
                                Collections.singletonList(
                                        new BucketLocation(
                                                partitionPath, bucket, 1, new int[] {1}))),
                        Collections.singletonMap(tablePath, 1L),
                        Collections.singletonMap(partitionPath, 11L),
                        Collections.singletonMap(TableOrPartition.ofPartition(11L), 1));
        MetadataResponse response = new MetadataResponse();
        response.addTabletServer()
                .setNodeId(1)
                .setHost(changedNodes ? "new-host" : "localhost")
                .setPort(changedNodes ? 8081 : 8080);
        if (changedNodes) {
            response.getTabletServersList().get(0).setRack("new-rack");
        }
        response.setCoordinatorServer()
                .setNodeId(1)
                .setHost(changedNodes ? "new-coordinator" : "localhost")
                .setPort(8080);

        Cluster refreshed =
                MetadataUtils.rebuildCluster(
                        original, true, response, Collections.emptySet(), Collections.emptySet());

        assertThat(refreshed.getBucketLocationsByPath())
                .isSameAs(original.getBucketLocationsByPath());
        assertThat(refreshed.getTableIdByPath()).isSameAs(original.getTableIdByPath());
        assertThat(refreshed.getPartitionIdByPath()).isSameAs(original.getPartitionIdByPath());
        assertThat(refreshed.getBucketCountByTableOrPartition())
                .isSameAs(original.getBucketCountByTableOrPartition());
        assertThat(refreshed.getTabletServer(1).host())
                .isEqualTo(changedNodes ? "new-host" : "localhost");
        assertThat(refreshed.getTabletServer(1).port()).isEqualTo(changedNodes ? 8081 : 8080);
        assertThat(refreshed.getTabletServer(1).rack()).isEqualTo(changedNodes ? "new-rack" : null);
        assertThat(refreshed.getTabletServer(1).serverTag()).isNull();
        assertThat(refreshed.getCoordinatorServer().host())
                .isEqualTo(changedNodes ? "new-coordinator" : "localhost");

        Cluster invalidated =
                refreshed.invalidPhysicalTableBucketAndPartitionMeta(
                        Collections.singleton(partitionPath));
        assertThat(invalidated.leaderFor(bucket)).isNull();
        assertThat(invalidated.getPartitionId(partitionPath)).isEmpty();
        assertThat(refreshed.leaderFor(bucket)).isEqualTo(1);
        assertThat(original.getPartitionId(partitionPath)).hasValue(11L);
        assertThat(original.getTabletServer(1).serverTag()).isEqualTo(ServerTag.TEMPORARY_OFFLINE);

        MetadataResponse partitionResponse = new MetadataResponse();
        partitionResponse.addTabletServer().setNodeId(2).setHost("localhost").setPort(8081);
        partitionResponse
                .addPartitionMetadata()
                .setTableId(1L)
                .setPartitionId(11L)
                .setPartitionName("p")
                .setBucketCount(1)
                .addBucketMetadata()
                .setBucketId(0)
                .setLeaderId(2);
        Cluster updated =
                MetadataUtils.rebuildCluster(
                        refreshed,
                        true,
                        partitionResponse,
                        Collections.emptySet(),
                        Collections.emptySet());
        assertThat(updated.leaderFor(bucket)).isEqualTo(2);
        assertThat(updated.getCoordinatorServer()).isNull();
        assertThat(refreshed.leaderFor(bucket)).isEqualTo(1);

        Cluster full =
                MetadataUtils.rebuildCluster(
                        original, false, response, Collections.emptySet(), Collections.emptySet());
        assertThat(full.getBucketLocationsByPath()).isEmpty();
        assertThat(full.getTableIdByPath()).isEmpty();
        assertThat(full.getPartitionIdByPath()).isEmpty();
        assertThatThrownBy(
                        () ->
                                MetadataUtils.rebuildCluster(
                                        original,
                                        true,
                                        new MetadataResponse(),
                                        Collections.emptySet(),
                                        Collections.emptySet()))
                .isInstanceOf(StaleMetadataException.class);
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

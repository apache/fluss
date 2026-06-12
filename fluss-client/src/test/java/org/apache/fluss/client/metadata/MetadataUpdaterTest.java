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

import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.AdminReadOnlyGateway;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.server.coordinator.TestCoordinatorGateway;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.PartitionMetadata;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.fluss.client.utils.MetadataUtils.sendMetadataRequestAndRebuildCluster;
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
    void testPartialUpdatePartitionMetadataUsesRequestedPathForStaleTableMetadata()
            throws Exception {
        TablePath tablePath = TablePath.of("fluss", "partitioned_table");
        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath, "dt=2026-01-01");
        long staleTableId = 99L;
        long tableId = 100L;
        long partitionId = 200L;
        BucketMetadata bucketMetadata =
                new BucketMetadata(0, TS_NODE.id(), 0, Collections.singletonList(TS_NODE.id()));
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("dt", DataTypes.STRING())
                                        .build())
                        .partitionedBy("dt")
                        .distributedBy(1)
                        .build();
        TableInfo tableInfo =
                TableInfo.of(tablePath, staleTableId, 0, tableDescriptor, "/tmp/table", 1L, 1L);
        MetadataResponse metadataResponse =
                buildMetadataResponse(
                        CS_NODE,
                        Collections.singleton(TS_NODE),
                        Collections.singletonList(
                                new TableMetadata(tableInfo, Collections.emptyList())),
                        Collections.singletonList(
                                new PartitionMetadata(
                                        tableId,
                                        "dt=2026-01-01",
                                        partitionId,
                                        Collections.singletonList(bucketMetadata))));

        Cluster cluster =
                sendMetadataRequestAndRebuildCluster(
                        new StaticMetadataGateway(metadataResponse, 1, 1),
                        true,
                        Cluster.empty(),
                        Collections.singleton(tablePath),
                        Collections.singleton(physicalTablePath),
                        null);

        assertThat(cluster.getTableId(tablePath)).hasValue(tableId);
        assertThat(cluster.getPartitionId(physicalTablePath)).hasValue(partitionId);
        assertThat(cluster.getBucketLocation(new TableBucket(tableId, partitionId, 0)))
                .map(BucketLocation::getLeader)
                .hasValue(TS_NODE.id());
        assertThat(cluster.getAvailableBucketsForPhysicalTablePath(physicalTablePath)).hasSize(1);
    }

    @Test
    void testPartialUpdatePartitionMetadataFallsBackToClusterForAmbiguousRequestedPartitionName()
            throws Exception {
        TablePath tablePath = TablePath.of("fluss", "partitioned_table");
        TablePath otherTablePath = TablePath.of("fluss", "other_partitioned_table");
        String partitionName = "dt=2026-01-01";
        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath, partitionName);
        PhysicalTablePath otherPhysicalTablePath =
                PhysicalTablePath.of(otherTablePath, partitionName);
        long tableId = 100L;
        long partitionId = 200L;
        BucketMetadata bucketMetadata =
                new BucketMetadata(0, TS_NODE.id(), 0, Collections.singletonList(TS_NODE.id()));
        MetadataResponse metadataResponse =
                buildMetadataResponse(
                        CS_NODE,
                        Collections.singleton(TS_NODE),
                        Collections.emptyList(),
                        Collections.singletonList(
                                new PartitionMetadata(
                                        tableId,
                                        partitionName,
                                        partitionId,
                                        Collections.singletonList(bucketMetadata))));
        Map<TablePath, Long> tableIds = new HashMap<>();
        tableIds.put(tablePath, tableId);
        Cluster originCluster =
                new Cluster(
                        Collections.singletonMap(TS_NODE.id(), TS_NODE),
                        CS_NODE,
                        Collections.emptyMap(),
                        tableIds,
                        Collections.emptyMap());

        Cluster cluster =
                sendMetadataRequestAndRebuildCluster(
                        new StaticMetadataGateway(metadataResponse, 2, 2),
                        true,
                        originCluster,
                        new HashSet<>(Arrays.asList(tablePath, otherTablePath)),
                        Arrays.asList(physicalTablePath, otherPhysicalTablePath),
                        null);

        assertThat(cluster.getTableId(tablePath)).hasValue(tableId);
        assertThat(cluster.getPartitionId(physicalTablePath)).hasValue(partitionId);
        assertThat(cluster.getPartitionId(otherPhysicalTablePath)).isEmpty();
    }

    @Test
    void testPartialUpdatePartitionMetadataRejectsAmbiguousPartitionNameWithoutTableIdMapping() {
        TablePath tablePath = TablePath.of("fluss", "partitioned_table");
        TablePath otherTablePath = TablePath.of("fluss", "other_partitioned_table");
        String partitionName = "dt=2026-01-01";
        MetadataResponse metadataResponse =
                buildMetadataResponse(
                        CS_NODE,
                        Collections.singleton(TS_NODE),
                        Collections.emptyList(),
                        Collections.singletonList(
                                new PartitionMetadata(
                                        100L,
                                        partitionName,
                                        200L,
                                        Collections.singletonList(
                                                new BucketMetadata(
                                                        0,
                                                        TS_NODE.id(),
                                                        0,
                                                        Collections.singletonList(
                                                                TS_NODE.id()))))));

        assertThatThrownBy(
                        () ->
                                sendMetadataRequestAndRebuildCluster(
                                        new StaticMetadataGateway(metadataResponse, 2, 2),
                                        true,
                                        Cluster.empty(),
                                        new HashSet<>(Arrays.asList(tablePath, otherTablePath)),
                                        Arrays.asList(
                                                PhysicalTablePath.of(tablePath, partitionName),
                                                PhysicalTablePath.of(
                                                        otherTablePath, partitionName)),
                                        null))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("table path not found for tableId 100 in cluster");
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

    private static final class StaticMetadataGateway extends TestCoordinatorGateway {

        private final MetadataResponse metadataResponse;
        private final int expectedTablePathCount;
        private final int expectedPartitionPathCount;

        private StaticMetadataGateway(
                MetadataResponse metadataResponse,
                int expectedTablePathCount,
                int expectedPartitionPathCount) {
            this.metadataResponse = metadataResponse;
            this.expectedTablePathCount = expectedTablePathCount;
            this.expectedPartitionPathCount = expectedPartitionPathCount;
        }

        @Override
        public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
            assertThat(request.getTablePathsList()).hasSize(expectedTablePathCount);
            assertThat(request.getPartitionsPathsList()).hasSize(expectedPartitionPathCount);
            assertThat(request.getPartitionsIds()).isEmpty();
            return CompletableFuture.completedFuture(metadataResponse);
        }
    }
}

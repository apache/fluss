/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.admin;

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.utils.TestingRpcClient;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.DescribeClusterResponse;
import org.apache.fluss.rpc.messages.ListPartitionInfosRequest;
import org.apache.fluss.rpc.messages.ListPartitionInfosResponse;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.PartitionUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for cluster discovery and inspection through {@link FlussAdmin}. */
class FlussAdminTest {

    private static final ServerNode OLD_COORDINATOR =
            new ServerNode(0, "old-coordinator", 9123, ServerType.COORDINATOR);

    @Test
    void testListPartitionInfosByPredicateFiltersLegacyResponse() throws Exception {
        TableInfo tableInfo =
                TableInfo.of(
                        TablePath.of("test_db", "test_table"),
                        1L,
                        0,
                        TableDescriptor.builder()
                                .schema(Schema.newBuilder().column("pt", DataTypes.INT()).build())
                                .distributedBy(1)
                                .partitionedBy("pt")
                                .build(),
                        "file:///tmp",
                        0L,
                        0L);
        ListPartitionInfosResponse response = new ListPartitionInfosResponse();
        for (int value : new int[] {2, 10}) {
            response.addPartitionsInfo()
                    .setPartitionId(value)
                    .setBucketCount(1)
                    .setPartitionSpec()
                    .addPartitionKeyValue()
                    .setKey("pt")
                    .setValue(String.valueOf(value));
        }

        TestingRpcClient client =
                new TestingRpcClient(
                        (node, api) -> {
                            assertThat(api).isEqualTo(ApiKeys.LIST_PARTITION_INFOS);
                            return CompletableFuture.completedFuture(response);
                        }) {
                    @Override
                    public CompletableFuture<ApiMessage> sendRequest(
                            ServerNode node, ApiKeys api, ApiMessage request) {
                        assertThat(((ListPartitionInfosRequest) request).hasPartitionPredicate())
                                .isTrue();
                        // A legacy server ignores the predicate and returns all partitions.
                        return super.sendRequest(node, api, request);
                    }
                };
        MetadataUpdater updater =
                new MetadataUpdater(client, new Configuration(), Cluster.empty()) {
                    @Override
                    public ServerNode getRandomTabletServer() {
                        return new ServerNode(0, "tablet", 9123, ServerType.TABLET_SERVER);
                    }
                };
        try (FlussAdmin admin = new FlussAdmin(client, updater)) {
            PredicateBuilder builder =
                    new PredicateBuilder(PartitionUtils.partitionRowType(tableInfo));
            assertThat(
                            admin.listPartitionInfosByPredicate(
                                            tableInfo, builder.greaterThan(0, 2))
                                    .get(10, TimeUnit.SECONDS))
                    .extracting(PartitionInfo::getPartitionName)
                    .containsExactly("10");
        }
    }

    @ParameterizedTest
    @CsvSource({
        "NETWORK_EXCEPTION, true, new-coordinator, 9123",
        "NETWORK_EXCEPTION, false, new-coordinator, 9123",
        "NOT_COORDINATOR_LEADER_EXCEPTION, true, new-coordinator, 9123",
        "NOT_COORDINATOR_LEADER_EXCEPTION, false, new-coordinator, 9123",
        "NETWORK_EXCEPTION, true, old-coordinator, 9124",
        "NETWORK_EXCEPTION, false, old-coordinator, 9124",
        "NOT_COORDINATOR_LEADER_EXCEPTION, true, old-coordinator, 9124",
        "NOT_COORDINATOR_LEADER_EXCEPTION, false, old-coordinator, 9124"
    })
    void testDescribeClusterRefreshesAndRetriesOnce(
            Errors error, boolean retrySucceeds, String host, int port) throws Exception {
        ServerNode newCoordinator =
                new ServerNode(OLD_COORDINATOR.id(), host, port, ServerType.COORDINATOR);
        RuntimeException failure = error.exception("coordinator unavailable");
        AtomicInteger refreshes = new AtomicInteger();
        List<ServerNode> contactedNodes = new ArrayList<>();
        TestingRpcClient client =
                new TestingRpcClient(
                        (node, api) -> {
                            assertThat(api).isEqualTo(ApiKeys.DESCRIBE_CLUSTER);
                            contactedNodes.add(node);
                            if (node.equals(OLD_COORDINATOR) || !retrySucceeds) {
                                return failed(failure);
                            }
                            DescribeClusterResponse response = new DescribeClusterResponse();
                            response.setCoordinatorServer()
                                    .setNodeId(node.id())
                                    .setHost(node.host())
                                    .setPort(node.port());
                            return CompletableFuture.completedFuture(response);
                        });
        MetadataUpdater updater =
                new MetadataUpdater(client, new Configuration(), Cluster.empty()) {
                    @Override
                    public ServerNode getCoordinatorServer() {
                        return refreshes.get() == 0 ? OLD_COORDINATOR : newCoordinator;
                    }

                    @Override
                    public void refreshClusterUntilAvailable() {
                        refreshes.incrementAndGet();
                    }
                };
        try (FlussAdmin admin = new FlussAdmin(client, updater)) {
            if (retrySucceeds) {
                assertThat(admin.describeCluster().get(10, TimeUnit.SECONDS).getCoordinatorServer())
                        .contains(newCoordinator);
            } else {
                assertThatThrownBy(() -> admin.describeCluster().get(10, TimeUnit.SECONDS))
                        .rootCause()
                        .isSameAs(failure);
            }
            assertThat(contactedNodes).containsExactly(OLD_COORDINATOR, newCoordinator);
            assertThat(refreshes).hasValue(1);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDescribeClusterDoesNotHideNonRetriableFailures(boolean unsupported) throws Exception {
        RuntimeException error =
                unsupported
                        ? new UnsupportedVersionException("DescribeCluster is unavailable")
                        : new AuthorizationException("cluster DESCRIBE denied");
        AtomicInteger requests = new AtomicInteger();
        TestingRpcClient client =
                new TestingRpcClient(
                        (node, api) -> {
                            assertThat(api).isEqualTo(ApiKeys.DESCRIBE_CLUSTER);
                            requests.incrementAndGet();
                            return failed(error);
                        });
        MetadataUpdater updater =
                new MetadataUpdater(client, new Configuration(), Cluster.empty()) {
                    @Override
                    public ServerNode getCoordinatorServer() {
                        return OLD_COORDINATOR;
                    }

                    @Override
                    public void refreshClusterUntilAvailable() {
                        throw new AssertionError(
                                "Non-retriable failures must not refresh metadata");
                    }
                };
        try (FlussAdmin admin = new FlussAdmin(client, updater)) {
            assertThatThrownBy(() -> admin.describeCluster().get(10, TimeUnit.SECONDS))
                    .rootCause()
                    .isSameAs(error);
            assertThat(requests).hasValue(1);
        }
    }

    @Test
    void testDiscoveryUsesMetadataWithoutDescribeCluster() throws Exception {
        TestingRpcClient client =
                new TestingRpcClient(
                        (node, api) -> {
                            assertThat(api).isEqualTo(ApiKeys.GET_METADATA);
                            MetadataResponse response = new MetadataResponse();
                            response.setCoordinatorServer()
                                    .setNodeId(0)
                                    .setHost("coordinator")
                                    .setPort(9123);
                            response.addTabletServer().setNodeId(1).setHost("tablet").setPort(9124);
                            return CompletableFuture.completedFuture(response);
                        });
        MetadataUpdater updater =
                new MetadataUpdater(client, new Configuration(), Cluster.empty()) {
                    @Override
                    public ServerNode getRandomTabletServer() {
                        return new ServerNode(1, "tablet", 9124, ServerType.TABLET_SERVER);
                    }
                };
        try (FlussAdmin admin = new FlussAdmin(client, updater)) {
            assertThat(admin.getServerNodes().get(10, TimeUnit.SECONDS))
                    .containsExactly(
                            new ServerNode(0, "coordinator", 9123, ServerType.COORDINATOR),
                            new ServerNode(1, "tablet", 9124, ServerType.TABLET_SERVER));
        }
    }

    private static CompletableFuture<ApiMessage> failed(Throwable error) {
        CompletableFuture<ApiMessage> result = new CompletableFuture<>();
        result.completeExceptionally(error);
        return result;
    }
}

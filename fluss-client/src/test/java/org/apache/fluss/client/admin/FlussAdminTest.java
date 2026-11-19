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
import org.apache.fluss.exception.ConsumerOffsetMetadataTooLargeException;
import org.apache.fluss.exception.InvalidRequestException;
import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsResponse;
import org.apache.fluss.rpc.messages.DeleteConsumerGroupsResponse;
import org.apache.fluss.rpc.messages.DeleteConsumerOffsetsResponse;
import org.apache.fluss.rpc.messages.DescribeClusterResponse;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerResponse;
import org.apache.fluss.rpc.messages.ListConsumerGroupsResponse;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.Errors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for cluster discovery and inspection through {@link FlussAdmin}. */
class FlussAdminTest {

    private static final ServerNode OLD_COORDINATOR =
            new ServerNode(0, "old-coordinator", 9123, ServerType.COORDINATOR);

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

    @Test
    void testListConsumerGroupsUsesDiscoveredTabletServerEndpoints() throws Exception {
        ServerNode discovery = new ServerNode(1, "discovery", 9124, ServerType.TABLET_SERVER);
        ServerNode oldEndpoint = new ServerNode(2, "old-tablet", 9125, ServerType.TABLET_SERVER);
        ServerNode newEndpoint = new ServerNode(2, "new-tablet", 9126, ServerType.TABLET_SERVER);
        ServerNode added = new ServerNode(3, "added-tablet", 9127, ServerType.TABLET_SERVER);
        List<ServerNode> queried = new ArrayList<>();
        AtomicInteger metadataRequests = new AtomicInteger();
        TestingRpcClient client =
                new TestingRpcClient(
                        (node, api) -> {
                            if (api == ApiKeys.GET_METADATA) {
                                assertThat(node).isEqualTo(discovery);
                                metadataRequests.incrementAndGet();
                                MetadataResponse response = new MetadataResponse();
                                response.setCoordinatorServer()
                                        .setNodeId(0)
                                        .setHost("coordinator")
                                        .setPort(9123);
                                response.addTabletServer()
                                        .setNodeId(discovery.id())
                                        .setHost(discovery.host())
                                        .setPort(discovery.port());
                                response.addTabletServer()
                                        .setNodeId(newEndpoint.id())
                                        .setHost(newEndpoint.host())
                                        .setPort(newEndpoint.port());
                                response.addTabletServer()
                                        .setNodeId(added.id())
                                        .setHost(added.host())
                                        .setPort(added.port());
                                return CompletableFuture.completedFuture(response);
                            }
                            assertThat(api).isEqualTo(ApiKeys.LIST_CONSUMER_GROUPS);
                            assertThat(node).isIn(discovery, newEndpoint, added);
                            queried.add(node);
                            ListConsumerGroupsResponse response = new ListConsumerGroupsResponse();
                            response.addGroup().setGroupId("shared");
                            response.addGroup().setGroupId("group-" + node.id());
                            return CompletableFuture.completedFuture(response);
                        });
        Map<Integer, ServerNode> cached = new HashMap<>();
        cached.put(discovery.id(), discovery);
        cached.put(oldEndpoint.id(), oldEndpoint);
        MetadataUpdater updater =
                new MetadataUpdater(
                        client,
                        new Configuration(),
                        new Cluster(
                                cached,
                                null,
                                Collections.emptyMap(),
                                Collections.emptyMap(),
                                Collections.emptyMap(),
                                Collections.emptyMap(),
                                Collections.emptyMap()));

        try (FlussAdmin admin = new FlussAdmin(client, updater)) {
            assertThat(admin.listConsumerGroups().get(10, TimeUnit.SECONDS))
                    .containsExactlyInAnyOrder("shared", "group-1", "group-2", "group-3");
        }
        assertThat(metadataRequests).hasValue(1);
        assertThat(queried).containsExactlyInAnyOrder(discovery, newEndpoint, added);
        assertThat(updater.getCluster().getTabletServer(added.id())).isNull();
    }

    @Test
    void testListConsumerGroupsFailsWhenDiscoveryFails() throws Exception {
        ServerNode discovery = new ServerNode(1, "discovery", 9124, ServerType.TABLET_SERVER);
        AuthorizationException failure = new AuthorizationException("metadata unavailable");
        TestingRpcClient client =
                new TestingRpcClient(
                        (node, api) -> {
                            assertThat(node).isEqualTo(discovery);
                            if (api == ApiKeys.GET_METADATA) {
                                return failed(failure);
                            }
                            throw new AssertionError(
                                    "Must not list groups without server discovery");
                        });
        MetadataUpdater updater =
                new MetadataUpdater(
                        client,
                        new Configuration(),
                        new Cluster(
                                Collections.singletonMap(discovery.id(), discovery),
                                null,
                                Collections.emptyMap(),
                                Collections.emptyMap(),
                                Collections.emptyMap(),
                                Collections.emptyMap(),
                                Collections.emptyMap()));

        try (FlussAdmin admin = new FlussAdmin(client, updater)) {
            assertThatThrownBy(() -> admin.listConsumerGroups().get(10, TimeUnit.SECONDS))
                    .rootCause()
                    .isSameAs(failure);
        }
    }

    @Test
    void testListConsumerGroupsDoesNotTreatEmptyMetadataAsNoGroups() throws Exception {
        ServerNode discovery = new ServerNode(1, "discovery", 9124, ServerType.TABLET_SERVER);
        TestingRpcClient client =
                new TestingRpcClient(
                        (node, api) -> {
                            assertThat(node).isEqualTo(discovery);
                            assertThat(api).isEqualTo(ApiKeys.GET_METADATA);
                            return CompletableFuture.completedFuture(new MetadataResponse());
                        });
        MetadataUpdater updater =
                new MetadataUpdater(client, new Configuration(), Cluster.empty()) {
                    @Override
                    public ServerNode getRandomTabletServer() {
                        return discovery;
                    }
                };

        try (FlussAdmin admin = new FlussAdmin(client, updater)) {
            assertThatThrownBy(() -> admin.listConsumerGroups().get(10, TimeUnit.SECONDS))
                    .rootCause()
                    .isInstanceOf(StaleMetadataException.class);
        }
    }

    @Test
    void testDiscoveredGroupOwnerMissingFromMetadataCache() throws Exception {
        ServerNode discoveryNode = new ServerNode(1, "discovery", 9124, ServerType.TABLET_SERVER);
        ServerNode owner = new ServerNode(2, "owner", 9125, ServerType.TABLET_SERVER);
        List<ServerNode> contactedNodes = new ArrayList<>();
        TestingRpcClient client =
                new TestingRpcClient(
                        (node, api) -> {
                            contactedNodes.add(node);
                            if (api == ApiKeys.FIND_CONSUMER_GROUP_OWNER) {
                                assertThat(node).isEqualTo(discoveryNode);
                                FindConsumerGroupOwnerResponse response =
                                        new FindConsumerGroupOwnerResponse();
                                response.setOwner()
                                        .setNodeId(owner.id())
                                        .setHost(owner.host())
                                        .setPort(owner.port());
                                return CompletableFuture.completedFuture(response);
                            }
                            assertThat(api).isEqualTo(ApiKeys.DELETE_CONSUMER_GROUPS);
                            assertThat(node).isEqualTo(owner);
                            DeleteConsumerGroupsResponse response =
                                    new DeleteConsumerGroupsResponse();
                            response.addGroup().setGroupId("group-1");
                            return CompletableFuture.completedFuture(response);
                        });
        MetadataUpdater updater =
                new MetadataUpdater(
                        client,
                        new Configuration(),
                        new Cluster(
                                Collections.singletonMap(discoveryNode.id(), discoveryNode),
                                null,
                                Collections.emptyMap(),
                                Collections.emptyMap(),
                                Collections.emptyMap(),
                                Collections.emptyMap(),
                                Collections.emptyMap()));
        assertThat(updater.getCluster().getTabletServer(owner.id())).isNull();

        try (FlussAdmin admin = new FlussAdmin(client, updater)) {
            admin.deleteConsumerGroups(Collections.singleton("group-1"))
                    .all()
                    .get(10, TimeUnit.SECONDS);
        }
        assertThat(contactedNodes).containsExactly(discoveryNode, owner);
    }

    @Test
    void testAlterConsumerGroupOffsetsFailsForRejectedBucket() throws Exception {
        ServerNode tabletServer = new ServerNode(1, "tablet", 9124, ServerType.TABLET_SERVER);
        TableBucket accepted = new TableBucket(1L, null, 0);
        TableBucket rejected = new TableBucket(1L, null, 1);
        TestingRpcClient client =
                new TestingRpcClient(
                        (node, api) -> {
                            assertThat(node).isEqualTo(tabletServer);
                            if (api == ApiKeys.FIND_CONSUMER_GROUP_OWNER) {
                                FindConsumerGroupOwnerResponse response =
                                        new FindConsumerGroupOwnerResponse();
                                response.setOwner()
                                        .setNodeId(tabletServer.id())
                                        .setHost(tabletServer.host())
                                        .setPort(tabletServer.port());
                                return CompletableFuture.completedFuture(response);
                            }
                            assertThat(api).isEqualTo(ApiKeys.COMMIT_CONSUMER_OFFSETS);
                            CommitConsumerOffsetsResponse response =
                                    new CommitConsumerOffsetsResponse();
                            response.addTable().setTableId(1L).addBucket().setBucketId(0);
                            response.addTable()
                                    .setTableId(1L)
                                    .addBucket()
                                    .setBucketId(1)
                                    .setErrorCode(Errors.CONSUMER_OFFSET_METADATA_TOO_LARGE.code())
                                    .setErrorMessage("metadata rejected");
                            return CompletableFuture.completedFuture(response);
                        });
        MetadataUpdater updater =
                new MetadataUpdater(
                        client,
                        new Configuration(),
                        new Cluster(
                                Collections.singletonMap(tabletServer.id(), tabletServer),
                                null,
                                Collections.emptyMap(),
                                Collections.emptyMap(),
                                Collections.emptyMap(),
                                Collections.emptyMap(),
                                Collections.emptyMap()));
        Map<TableBucket, OffsetMetadata> offsets = new HashMap<>();
        offsets.put(accepted, new OffsetMetadata(10L));
        offsets.put(rejected, new OffsetMetadata(20L));

        try (FlussAdmin admin = new FlussAdmin(client, updater)) {
            AlterConsumerGroupOffsetsResult result =
                    admin.alterConsumerGroupOffsets("group-1", offsets);

            assertThat(result.bucketResult(accepted).get(10, TimeUnit.SECONDS)).isNull();
            assertThatThrownBy(() -> result.bucketResult(rejected).get(10, TimeUnit.SECONDS))
                    .rootCause()
                    .isInstanceOf(ConsumerOffsetMetadataTooLargeException.class)
                    .hasMessage("metadata rejected");
            assertThatThrownBy(() -> result.all().get(10, TimeUnit.SECONDS))
                    .rootCause()
                    .isInstanceOf(ConsumerOffsetMetadataTooLargeException.class)
                    .hasMessage("metadata rejected");
        }
    }

    @Test
    void testListConsumerGroupOffsetsWithEmptySelectionSkipsOwnerDiscovery() throws Exception {
        TestingRpcClient client =
                new TestingRpcClient(
                        (node, api) -> {
                            throw new AssertionError("Empty selection must not send an RPC");
                        });
        MetadataUpdater updater =
                new MetadataUpdater(client, new Configuration(), Cluster.empty()) {
                    @Override
                    public ServerNode getRandomTabletServer() {
                        throw new AssertionError("Empty selection must not discover an owner");
                    }
                };
        try (FlussAdmin admin = new FlussAdmin(client, updater)) {
            assertThat(
                            admin.listConsumerGroupOffsets("group-1", Collections.emptySet())
                                    .get(10, TimeUnit.SECONDS))
                    .isEmpty();
        }
    }

    @Test
    void testDeleteConsumerResultsPreserveServerErrorMessages() throws Exception {
        ServerNode tabletServer = new ServerNode(1, "tablet", 9124, ServerType.TABLET_SERVER);
        TableBucket bucket = new TableBucket(1L, null, 0);
        TestingRpcClient client =
                new TestingRpcClient(
                        (node, api) -> {
                            assertThat(node).isEqualTo(tabletServer);
                            if (api == ApiKeys.FIND_CONSUMER_GROUP_OWNER) {
                                FindConsumerGroupOwnerResponse response =
                                        new FindConsumerGroupOwnerResponse();
                                response.setOwner()
                                        .setNodeId(tabletServer.id())
                                        .setHost(tabletServer.host())
                                        .setPort(tabletServer.port());
                                return CompletableFuture.completedFuture(response);
                            }
                            if (api == ApiKeys.DELETE_CONSUMER_GROUPS) {
                                DeleteConsumerGroupsResponse response =
                                        new DeleteConsumerGroupsResponse();
                                response.addGroup()
                                        .setGroupId("group-1")
                                        .setErrorCode(Errors.INVALID_REQUEST.code())
                                        .setErrorMessage("group deletion blocked by server");
                                return CompletableFuture.completedFuture(response);
                            }
                            assertThat(api).isEqualTo(ApiKeys.DELETE_CONSUMER_OFFSETS);
                            DeleteConsumerOffsetsResponse response =
                                    new DeleteConsumerOffsetsResponse();
                            response.addTable()
                                    .setTableId(1L)
                                    .addBucket()
                                    .setBucketId(0)
                                    .setErrorCode(Errors.INVALID_REQUEST.code())
                                    .setErrorMessage("bucket deletion blocked by server");
                            return CompletableFuture.completedFuture(response);
                        });
        MetadataUpdater updater =
                new MetadataUpdater(
                        client,
                        new Configuration(),
                        new Cluster(
                                Collections.singletonMap(tabletServer.id(), tabletServer),
                                null,
                                Collections.emptyMap(),
                                Collections.emptyMap(),
                                Collections.emptyMap(),
                                Collections.emptyMap(),
                                Collections.emptyMap()));

        try (FlussAdmin admin = new FlussAdmin(client, updater)) {
            DeleteConsumerGroupsResult groups =
                    admin.deleteConsumerGroups(Collections.singleton("group-1"));
            assertThatThrownBy(() -> groups.deletedGroup("group-1").get(10, TimeUnit.SECONDS))
                    .rootCause()
                    .isInstanceOf(InvalidRequestException.class)
                    .hasMessage("group deletion blocked by server");

            DeleteConsumerGroupOffsetsResult offsets =
                    admin.deleteConsumerGroupOffsets("group-1", Collections.singleton(bucket));
            assertThatThrownBy(() -> offsets.bucketResult(bucket).get(10, TimeUnit.SECONDS))
                    .rootCause()
                    .isInstanceOf(InvalidRequestException.class)
                    .hasMessage("bucket deletion blocked by server");
        }
    }

    @Test
    void testDeleteConsumerGroupsSkipsInvalidIdsAndDeduplicatesBeforeDiscovery() throws Exception {
        ServerNode tabletServer = new ServerNode(1, "tablet", 9124, ServerType.TABLET_SERVER);
        AtomicInteger discoveries = new AtomicInteger();
        AtomicInteger deletions = new AtomicInteger();
        CompletableFuture<ApiMessage> pendingDelete = new CompletableFuture<>();
        TestingRpcClient client =
                new TestingRpcClient(
                        (node, api) -> {
                            assertThat(node).isEqualTo(tabletServer);
                            if (api == ApiKeys.FIND_CONSUMER_GROUP_OWNER) {
                                discoveries.incrementAndGet();
                                FindConsumerGroupOwnerResponse response =
                                        new FindConsumerGroupOwnerResponse();
                                response.setOwner()
                                        .setNodeId(tabletServer.id())
                                        .setHost(tabletServer.host())
                                        .setPort(tabletServer.port());
                                return CompletableFuture.completedFuture(response);
                            }
                            assertThat(api).isEqualTo(ApiKeys.DELETE_CONSUMER_GROUPS);
                            DeleteConsumerGroupsResponse response =
                                    new DeleteConsumerGroupsResponse();
                            response.addGroup().setGroupId("g1");
                            return deletions.incrementAndGet() == 1
                                    ? pendingDelete
                                    : CompletableFuture.completedFuture(response);
                        });
        MetadataUpdater updater =
                new MetadataUpdater(
                        client,
                        new Configuration(),
                        new Cluster(
                                Collections.singletonMap(tabletServer.id(), tabletServer),
                                null,
                                Collections.emptyMap(),
                                Collections.emptyMap(),
                                Collections.emptyMap(),
                                Collections.emptyMap(),
                                Collections.emptyMap()));

        String overlong = String.join("", Collections.nCopies(201, "g"));
        try (FlussAdmin admin = new FlussAdmin(client, updater)) {
            assertThatThrownBy(() -> admin.deleteConsumerGroups(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("groupIds must not be null");
            DeleteConsumerGroupsResult invalid =
                    admin.deleteConsumerGroups(
                            Arrays.asList(null, "", "__internal", "bad group", overlong));
            assertThat(invalid.perGroupFutures()).isEmpty();
            assertThat(invalid.all()).isCompleted();
            assertThat(discoveries).hasValue(0);
            assertThat(deletions).hasValue(0);

            DeleteConsumerGroupsResult result =
                    admin.deleteConsumerGroups(
                            Arrays.asList(
                                    null, "", "__internal", "bad group", overlong, "g1", "g1"));
            assertThat(discoveries).hasValue(1);
            assertThat(deletions).hasValue(1);
            assertThat(result.perGroupFutures()).containsOnlyKeys("g1");
            assertThat(result.all()).isNotDone();

            DeleteConsumerGroupsResponse response = new DeleteConsumerGroupsResponse();
            response.addGroup().setGroupId("g1");
            pendingDelete.complete(response);
            assertThat(result.deletedGroup("g1")).isCompleted();
            assertThat(result.all()).isCompleted();
        }
    }

    private static CompletableFuture<ApiMessage> failed(Throwable error) {
        CompletableFuture<ApiMessage> result = new CompletableFuture<>();
        result.completeExceptionally(error);
        return result;
    }
}

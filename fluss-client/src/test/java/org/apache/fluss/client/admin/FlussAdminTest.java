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
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.DescribeClusterResponse;
import org.apache.fluss.rpc.messages.ListDatabasesResponse;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.Errors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for cluster discovery and admin retries through {@link FlussAdmin}. */
class FlussAdminTest {

    private static final ServerNode OLD_COORDINATOR =
            new ServerNode(0, "old-coordinator", 9123, ServerType.COORDINATOR);

    private static final ServerNode FIRST = node(1, 1001);
    private static final ServerNode SECOND = node(2, 1002);

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

    @ParameterizedTest
    @MethodSource("retryCandidates")
    void testRetrySelectsCurrentAlternativeAddress(
            List<ServerNode> current, List<ServerNode> backingOff, List<ServerNode> eligible)
            throws Exception {
        List<ServerNode> attempts = new ArrayList<>();
        AtomicInteger refreshes = new AtomicInteger();
        TestingRpcClient client =
                new TestingRpcClient(
                        (node, api) -> {
                            assertThat(api).isEqualTo(ApiKeys.LIST_DATABASES);
                            attempts.add(node);
                            return attempts.size() == 1
                                    ? failed(new NetworkException("disconnected"))
                                    : CompletableFuture.completedFuture(
                                            new ListDatabasesResponse());
                        }) {
                    @Override
                    public boolean isBackingOff(ServerNode node) {
                        return backingOff.contains(node);
                    }
                };
        MetadataUpdater metadata =
                readOnlyMetadata(client, current, () -> FIRST, refreshes::incrementAndGet);
        try (FlussAdmin admin = new FlussAdmin(client, metadata)) {
            assertThat(admin.listDatabases().get(10, TimeUnit.SECONDS)).isEmpty();
        }
        assertThat(attempts).hasSize(2);
        assertThat(attempts.get(0)).isEqualTo(FIRST);
        assertThat(attempts.get(1)).isIn(eligible);
        assertThat(refreshes).hasValue(1);
    }

    private static Stream<Arguments> retryCandidates() {
        ServerNode changed = node(FIRST.id(), 1003);
        ServerNode alias = node(3, FIRST.port());
        ServerNode third = node(3, 1003);
        List<ServerNode> none = Collections.emptyList();
        return Stream.of(
                Arguments.of(Arrays.asList(FIRST, SECOND), none, Collections.singletonList(SECOND)),
                Arguments.of(
                        Collections.singletonList(changed),
                        none,
                        Collections.singletonList(changed)),
                Arguments.of(
                        Arrays.asList(FIRST, alias, SECOND),
                        none,
                        Collections.singletonList(SECOND)),
                Arguments.of(
                        Collections.singletonList(FIRST), none, Collections.singletonList(FIRST)),
                Arguments.of(Arrays.asList(FIRST, alias), none, Arrays.asList(FIRST, alias)),
                Arguments.of(
                        Arrays.asList(FIRST, SECOND, changed),
                        none,
                        Arrays.asList(SECOND, changed)),
                Arguments.of(
                        Arrays.asList(FIRST, SECOND, third),
                        Collections.singletonList(SECOND),
                        Collections.singletonList(third)),
                Arguments.of(
                        Arrays.asList(FIRST, SECOND),
                        Collections.singletonList(SECOND),
                        Collections.singletonList(SECOND)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testEmptyCandidatesFailWithStaleMetadata(boolean hasInitialNode) throws Exception {
        List<ServerNode> attempts = new ArrayList<>();
        AtomicInteger refreshes = new AtomicInteger();
        TestingRpcClient client =
                new TestingRpcClient(
                        (node, api) -> {
                            attempts.add(node);
                            return failed(new NetworkException("disconnected"));
                        });
        MetadataUpdater metadata =
                readOnlyMetadata(
                        client,
                        Collections.emptyList(),
                        () -> hasInitialNode ? FIRST : null,
                        refreshes::incrementAndGet);
        try (FlussAdmin admin = new FlussAdmin(client, metadata)) {
            assertThatThrownBy(() -> admin.listDatabases().get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(StaleMetadataException.class);
        }
        assertThat(attempts)
                .isEqualTo(
                        hasInitialNode
                                ? Collections.singletonList(FIRST)
                                : Collections.emptyList());
        assertThat(refreshes).hasValue(hasInitialNode ? 1 : 0);
    }

    @Test
    void testFailedAlternativeExhaustsExistingSingleRetry() {
        List<ServerNode> attempts = new ArrayList<>();
        AtomicInteger refreshes = new AtomicInteger();
        TestingRpcClient client =
                new TestingRpcClient(
                        (node, api) -> {
                            assertThat(api).isEqualTo(ApiKeys.LIST_DATABASES);
                            attempts.add(node);
                            return failed(new NetworkException("disconnected"));
                        });
        MetadataUpdater metadata =
                readOnlyMetadata(
                        client,
                        Arrays.asList(FIRST, SECOND),
                        () -> FIRST,
                        refreshes::incrementAndGet);
        try (FlussAdmin admin = new FlussAdmin(client, metadata)) {
            assertThatThrownBy(() -> admin.listDatabases().get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(NetworkException.class);
        }
        assertThat(attempts).containsExactly(FIRST, SECOND);
        assertThat(refreshes).hasValue(1);
    }

    @Test
    void testConcurrentCallsKeepSeparateFailedEndpointsAndShareRefresh() throws Exception {
        List<CompletableFuture<ApiMessage>> initial =
                Arrays.asList(new CompletableFuture<>(), new CompletableFuture<>());
        AtomicInteger requests = new AtomicInteger();
        TestingRpcClient client =
                new TestingRpcClient(
                        (node, api) -> {
                            assertThat(api).isEqualTo(ApiKeys.LIST_DATABASES);
                            int attempt = requests.getAndIncrement();
                            if (attempt < initial.size()) {
                                assertThat(node).isEqualTo(attempt == 0 ? FIRST : SECOND);
                                return initial.get(attempt);
                            }
                            ListDatabasesResponse response = new ListDatabasesResponse();
                            response.addDatabaseName(node.uid());
                            return CompletableFuture.completedFuture(response);
                        });
        AtomicInteger selections = new AtomicInteger();
        AtomicInteger refreshes = new AtomicInteger();
        CountDownLatch refreshStarted = new CountDownLatch(1);
        CompletableFuture<Void> releaseRefresh = new CompletableFuture<>();
        MetadataUpdater metadata =
                readOnlyMetadata(
                        client,
                        Arrays.asList(FIRST, SECOND),
                        () -> selections.getAndIncrement() == 0 ? FIRST : SECOND,
                        () -> {
                            refreshes.incrementAndGet();
                            refreshStarted.countDown();
                            releaseRefresh.join();
                        });
        try (FlussAdmin admin = new FlussAdmin(client, metadata)) {
            CompletableFuture<List<String>> first = admin.listDatabases();
            CompletableFuture<List<String>> second = admin.listDatabases();
            try {
                initial.get(0).completeExceptionally(new NetworkException("first disconnected"));
                assertThat(refreshStarted.await(10, TimeUnit.SECONDS)).isTrue();
                initial.get(1).completeExceptionally(new NetworkException("second disconnected"));
            } finally {
                releaseRefresh.complete(null);
            }
            assertThat(first.get(10, TimeUnit.SECONDS)).containsExactly(SECOND.uid());
            assertThat(second.get(10, TimeUnit.SECONDS)).containsExactly(FIRST.uid());
        }
        assertThat(selections).hasValue(2);
        assertThat(refreshes).hasValue(1);
        assertThat(requests).hasValue(4);
    }

    private static MetadataUpdater readOnlyMetadata(
            TestingRpcClient client,
            List<ServerNode> current,
            Supplier<ServerNode> first,
            Runnable refresh) {
        Map<Integer, ServerNode> nodes = new HashMap<>();
        current.forEach(node -> nodes.put(node.id(), node));
        Cluster cluster =
                new Cluster(
                        nodes,
                        null,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        return new MetadataUpdater(client, new Configuration(), cluster) {
            @Override
            public ServerNode getRandomTabletServer() {
                return first.get();
            }

            @Override
            public void refreshClusterUntilAvailable() {
                refresh.run();
            }
        };
    }

    private static ServerNode node(int id, int port) {
        return new ServerNode(id, "localhost", port, ServerType.TABLET_SERVER);
    }

    private static CompletableFuture<ApiMessage> failed(Throwable error) {
        CompletableFuture<ApiMessage> result = new CompletableFuture<>();
        result.completeExceptionally(error);
        return result;
    }
}

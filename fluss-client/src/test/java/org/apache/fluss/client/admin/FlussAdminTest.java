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

package org.apache.fluss.client.admin;

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.ListDatabasesResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for read-only retries in {@link FlussAdmin}. */
class FlussAdminTest {
    private static final ServerNode FIRST = node(1, 1001);
    private static final ServerNode SECOND = node(2, 1002);

    @Test
    void testReadOnlyRetryAvoidsFailedEndpointAdvertisedByRefresh() throws Exception {
        ServerNode failed = new ServerNode(1, "localhost", 1001, ServerType.TABLET_SERVER);
        ServerNode healthy = new ServerNode(2, "localhost", 1002, ServerType.TABLET_SERVER);
        Map<Integer, ServerNode> nodes = new HashMap<>();
        nodes.put(failed.id(), failed);
        nodes.put(healthy.id(), healthy);
        Cluster stale =
                new Cluster(
                        nodes,
                        null,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        MetadataUpdater metadata = mock(MetadataUpdater.class);
        when(metadata.getRandomTabletServer()).thenReturn(failed);
        when(metadata.getCluster()).thenReturn(stale);
        RpcClient client = mock(RpcClient.class);
        List<ServerNode> attempts = Collections.synchronizedList(new ArrayList<>());
        when(client.sendRequest(any(), eq(ApiKeys.LIST_DATABASES), any()))
                .thenAnswer(
                        invocation -> {
                            ServerNode selected = invocation.getArgument(0);
                            attempts.add(selected);
                            return selected.equals(failed)
                                    ? FutureUtils.completedExceptionally(
                                            new NetworkException("disconnected"))
                                    : CompletableFuture.completedFuture(
                                            new ListDatabasesResponse());
                        });
        try (FlussAdmin admin = new FlussAdmin(client, metadata)) {
            assertThat(admin.listDatabases().get(10, TimeUnit.SECONDS)).isEmpty();
        }
        assertThat(attempts).containsExactly(failed, healthy);
        verify(metadata).refreshClusterUntilAvailable();
    }

    @ParameterizedTest
    @MethodSource("retryCandidates")
    void testRetrySelectsCurrentAlternativeAddress(
            List<ServerNode> current, List<ServerNode> eligible) throws Exception {
        MetadataUpdater metadata = metadata(FIRST, current);
        RpcClient client = mock(RpcClient.class);
        List<ServerNode> attempts = Collections.synchronizedList(new ArrayList<>());
        when(client.sendRequest(any(), eq(ApiKeys.LIST_DATABASES), any()))
                .thenAnswer(
                        invocation -> {
                            attempts.add(invocation.getArgument(0));
                            return attempts.size() == 1
                                    ? FutureUtils.completedExceptionally(
                                            new NetworkException("disconnected"))
                                    : CompletableFuture.completedFuture(
                                            new ListDatabasesResponse());
                        });
        try (FlussAdmin admin = new FlussAdmin(client, metadata)) {
            assertThat(admin.listDatabases().get(10, TimeUnit.SECONDS)).isEmpty();
        }
        assertThat(attempts).hasSize(2);
        assertThat(attempts.get(0)).isEqualTo(FIRST);
        assertThat(attempts.get(1)).isIn(eligible);
        verify(metadata).getRandomTabletServer();
        verify(metadata).refreshClusterUntilAvailable();
    }

    private static Stream<Arguments> retryCandidates() {
        ServerNode changedAddress = node(FIRST.id(), 1003);
        ServerNode alias = node(3, FIRST.port());
        return Stream.of(
                Arguments.of(
                        Collections.singletonList(changedAddress),
                        Collections.singletonList(changedAddress)),
                Arguments.of(
                        Arrays.asList(FIRST, alias, SECOND), Collections.singletonList(SECOND)),
                Arguments.of(Collections.singletonList(FIRST), Collections.singletonList(FIRST)),
                Arguments.of(Arrays.asList(FIRST, alias), Arrays.asList(FIRST, alias)),
                Arguments.of(
                        Arrays.asList(FIRST, SECOND, changedAddress),
                        Arrays.asList(SECOND, changedAddress)));
    }

    @Test
    void testRetryPrefersNonBackingOffAlternative() throws Exception {
        ServerNode third = node(3, 1003);
        MetadataUpdater metadata = metadata(FIRST, Arrays.asList(FIRST, SECOND, third));
        RpcClient client = mock(RpcClient.class);
        when(client.isBackingOff(SECOND)).thenReturn(true);
        List<ServerNode> attempts = Collections.synchronizedList(new ArrayList<>());
        when(client.sendRequest(any(), eq(ApiKeys.LIST_DATABASES), any()))
                .thenAnswer(
                        invocation -> {
                            attempts.add(invocation.getArgument(0));
                            return attempts.size() == 1
                                    ? FutureUtils.completedExceptionally(
                                            new NetworkException("disconnected"))
                                    : CompletableFuture.completedFuture(
                                            new ListDatabasesResponse());
                        });
        try (FlussAdmin admin = new FlussAdmin(client, metadata)) {
            admin.listDatabases().get(10, TimeUnit.SECONDS);
        }
        assertThat(attempts).containsExactly(FIRST, third);
    }

    @Test
    void testAllAlternativesBackingOffStillExcludeFirstEndpoint() throws Exception {
        MetadataUpdater metadata = metadata(FIRST, Arrays.asList(FIRST, SECOND));
        RpcClient client = mock(RpcClient.class);
        when(client.isBackingOff(SECOND)).thenReturn(true);
        List<ServerNode> attempts = Collections.synchronizedList(new ArrayList<>());
        when(client.sendRequest(any(), eq(ApiKeys.LIST_DATABASES), any()))
                .thenAnswer(
                        invocation -> {
                            attempts.add(invocation.getArgument(0));
                            return attempts.size() == 1
                                    ? FutureUtils.completedExceptionally(
                                            new NetworkException("disconnected"))
                                    : CompletableFuture.completedFuture(
                                            new ListDatabasesResponse());
                        });
        try (FlussAdmin admin = new FlussAdmin(client, metadata)) {
            admin.listDatabases().get(10, TimeUnit.SECONDS);
        }
        assertThat(attempts).containsExactly(FIRST, SECOND);
    }

    @Test
    void testEmptyRetryCandidatesFailWithStaleMetadata() throws Exception {
        MetadataUpdater metadata = metadata(FIRST, Collections.emptyList());
        RpcClient client = mock(RpcClient.class);
        when(client.sendRequest(any(), any(), any()))
                .thenReturn(
                        FutureUtils.completedExceptionally(new NetworkException("disconnected")));
        try (FlussAdmin admin = new FlussAdmin(client, metadata)) {
            CompletableFuture<List<String>> result = admin.listDatabases();
            assertThatThrownBy(() -> result.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(StaleMetadataException.class);
        }
        verify(client).sendRequest(eq(FIRST), eq(ApiKeys.LIST_DATABASES), any());
        verify(metadata).refreshClusterUntilAvailable();
    }

    @Test
    void testEmptyInitialCandidatesFailWithoutSending() throws Exception {
        MetadataUpdater metadata = metadata(null, Collections.emptyList());
        RpcClient client = mock(RpcClient.class);
        try (FlussAdmin admin = new FlussAdmin(client, metadata)) {
            CompletableFuture<List<String>> result = admin.listDatabases();
            assertThatThrownBy(() -> result.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(StaleMetadataException.class);
        }
        verify(client, never()).sendRequest(any(), any(), any());
        verify(metadata, never()).refreshClusterUntilAvailable();
    }

    @Test
    void testFailedAlternativeExhaustsExistingSingleRetry() throws Exception {
        MetadataUpdater metadata = metadata(FIRST, Arrays.asList(FIRST, SECOND));
        RpcClient client = mock(RpcClient.class);
        when(client.sendRequest(any(), any(), any()))
                .thenReturn(
                        FutureUtils.completedExceptionally(new NetworkException("disconnected")));
        try (FlussAdmin admin = new FlussAdmin(client, metadata)) {
            assertThatThrownBy(() -> admin.listDatabases().get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(NetworkException.class);
        }
        verify(client).sendRequest(eq(FIRST), eq(ApiKeys.LIST_DATABASES), any());
        verify(client).sendRequest(eq(SECOND), eq(ApiKeys.LIST_DATABASES), any());
        verify(client, times(2)).sendRequest(any(), any(), any());
        verify(metadata).refreshClusterUntilAvailable();
    }

    @Test
    void testConcurrentCallsKeepSeparateFailedEndpointsAndShareRefresh() throws Exception {
        MetadataUpdater metadata = metadata(FIRST, Arrays.asList(FIRST, SECOND));
        when(metadata.getRandomTabletServer()).thenReturn(FIRST, SECOND);
        CountDownLatch refreshStarted = new CountDownLatch(1);
        CountDownLatch releaseRefresh = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            refreshStarted.countDown();
                            assertThat(releaseRefresh.await(10, TimeUnit.SECONDS)).isTrue();
                            return null;
                        })
                .when(metadata)
                .refreshClusterUntilAvailable();
        RpcClient client = mock(RpcClient.class);
        Map<ApiMessage, List<ServerNode>> attempts =
                Collections.synchronizedMap(new IdentityHashMap<>());
        List<CompletableFuture<ApiMessage>> initialResponses = new ArrayList<>();
        when(client.sendRequest(any(), eq(ApiKeys.LIST_DATABASES), any()))
                .thenAnswer(
                        invocation -> {
                            ApiMessage request = invocation.getArgument(2);
                            List<ServerNode> selected =
                                    attempts.computeIfAbsent(request, ignored -> new ArrayList<>());
                            selected.add(invocation.getArgument(0));
                            if (selected.size() == 1) {
                                CompletableFuture<ApiMessage> initial = new CompletableFuture<>();
                                initialResponses.add(initial);
                                return initial;
                            }
                            return CompletableFuture.completedFuture(new ListDatabasesResponse());
                        });
        try (FlussAdmin admin = new FlussAdmin(client, metadata)) {
            CompletableFuture<List<String>> first = admin.listDatabases();
            CompletableFuture<List<String>> second = admin.listDatabases();
            try {
                initialResponses
                        .get(0)
                        .completeExceptionally(new NetworkException("first disconnected"));
                assertThat(refreshStarted.await(10, TimeUnit.SECONDS)).isTrue();
                initialResponses
                        .get(1)
                        .completeExceptionally(new NetworkException("second disconnected"));
                releaseRefresh.countDown();
                first.get(10, TimeUnit.SECONDS);
                second.get(10, TimeUnit.SECONDS);
            } finally {
                releaseRefresh.countDown();
            }
        }
        assertThat(attempts.values())
                .containsExactlyInAnyOrder(
                        Arrays.asList(FIRST, SECOND), Arrays.asList(SECOND, FIRST));
        verify(metadata, times(2)).getRandomTabletServer();
        verify(metadata).refreshClusterUntilAvailable();
        verify(client, times(4)).sendRequest(any(), any(), any());
    }

    private static MetadataUpdater metadata(ServerNode first, List<ServerNode> current) {
        Map<Integer, ServerNode> nodes = new HashMap<>();
        current.forEach(node -> nodes.put(node.id(), node));
        Cluster cluster =
                new Cluster(
                        nodes,
                        null,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        MetadataUpdater metadata = mock(MetadataUpdater.class);
        when(metadata.getRandomTabletServer()).thenReturn(first);
        when(metadata.getCluster()).thenReturn(cluster);
        return metadata;
    }

    private static ServerNode node(int id, int port) {
        return new ServerNode(id, "localhost", port, ServerType.TABLET_SERVER);
    }
}

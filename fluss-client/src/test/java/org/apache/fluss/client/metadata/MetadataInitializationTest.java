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

import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.messages.MetadataResponse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Regression tests for table initialization without relying on a running Fluss service. */
class MetadataInitializationTest {

    private static final TablePath TABLE_PATH = TablePath.of("db", "docs");
    private static final long TABLE_ID = 1622L;

    @ParameterizedTest
    @MethodSource("retryableFailures")
    void testTransientFailureOnlyReturnsAfterTableMetadataIsReady(Exception failure) {
        AtomicInteger requests = new AtomicInteger();
        TestingMetadataRpcClient rpcClient =
                new TestingMetadataRpcClient(
                        request ->
                                requests.incrementAndGet() < 3
                                        ? failedFuture(failure)
                                        : CompletableFuture.completedFuture(
                                                metadataResponse(true)));

        MetadataUpdater updater = updater(rpcClient, false);
        updater.initializeTableMetadata(TABLE_PATH);

        assertThat(requests.get()).isEqualTo(3);
        assertThat(updater.getCluster().getTableId(TABLE_PATH)).hasValue(TABLE_ID);
        assertThat(updater.getCluster().getTablePath(TABLE_ID)).hasValue(TABLE_PATH);
    }

    @ParameterizedTest
    @MethodSource("retryableFailures")
    void testExhaustedRetriesPreserveUnderlyingCause(Exception failure) {
        TestingMetadataRpcClient rpcClient =
                new TestingMetadataRpcClient(request -> failedFuture(failure));

        MetadataUpdater updater = updater(rpcClient, false);
        assertThatThrownBy(() -> updater.initializeTableMetadata(TABLE_PATH)).hasRootCause(failure);

        assertThat(rpcClient.getRequests()).hasSize(3);
        assertThat(updater.getCluster().getTableId(TABLE_PATH)).isEmpty();
    }

    @Test
    void testSuccessfulRpcWithoutRequiredMappingCannotCompleteInitialization() {
        TestingMetadataRpcClient rpcClient =
                new TestingMetadataRpcClient(
                        request -> CompletableFuture.completedFuture(metadataResponse(false)));

        MetadataUpdater updater = updater(rpcClient, false);
        assertThatThrownBy(() -> updater.initializeTableMetadata(TABLE_PATH))
                .isInstanceOf(RuntimeException.class);

        assertThat(rpcClient.getRequests()).hasSize(3);
        assertThat(updater.getCluster().getTableId(TABLE_PATH)).isEmpty();
        assertThat(updater.getCluster().getTablePath(TABLE_ID)).isEmpty();
    }

    @Test
    void testBootstrapNodeDiscoveryDoesNotCompleteTableInitialization() {
        TestingMetadataRpcClient rpcClient =
                new TestingMetadataRpcClient(
                        request -> CompletableFuture.completedFuture(metadataResponse(false)));
        Configuration configuration = new Configuration();
        configuration.set(
                ConfigOptions.BOOTSTRAP_SERVERS, Collections.singletonList("localhost:9080"));
        MetadataUpdater updater = new MetadataUpdater(rpcClient, configuration, Cluster.empty());

        assertThatThrownBy(() -> updater.initializeTableMetadata(TABLE_PATH))
                .isInstanceOf(RuntimeException.class);

        assertThat(rpcClient.getRequests()).hasSize(3);
        assertThat(updater.getCluster().getAliveTabletServers()).hasSize(3);
        assertThat(updater.getCluster().getTableId(TABLE_PATH)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testExistingMappingRequiresSuccessfulRefresh(boolean failRequest) {
        TimeoutException failure = new TimeoutException("table metadata request timed out");
        TestingMetadataRpcClient rpcClient =
                new TestingMetadataRpcClient(
                        request ->
                                failRequest
                                        ? failedFuture(failure)
                                        : CompletableFuture.completedFuture(
                                                metadataResponse(false)));

        MetadataUpdater updater = updater(rpcClient, true);
        if (failRequest) {
            assertThatThrownBy(() -> updater.initializeTableMetadata(TABLE_PATH))
                    .hasRootCause(failure);
        } else {
            updater.initializeTableMetadata(TABLE_PATH);
        }

        assertThat(rpcClient.getRequests()).hasSize(failRequest ? 3 : 1);
        assertThat(updater.getCluster().getTableId(TABLE_PATH)).hasValue(TABLE_ID);
        assertThat(updater.getCluster().getTablePath(TABLE_ID)).hasValue(TABLE_PATH);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void testBootstrapRecoveryRespectsInitializationAttemptBudget(int serverCount) {
        AtomicInteger tableRequests = new AtomicInteger();
        TestingMetadataRpcClient rpcClient =
                new TestingMetadataRpcClient(
                        request ->
                                !request.getTablePathsList().isEmpty()
                                                && tableRequests.incrementAndGet() <= serverCount
                                        ? failedFuture(
                                                new TimeoutException("table metadata unavailable"))
                                        : CompletableFuture.completedFuture(
                                                metadataResponse(
                                                        !request.getTablePathsList().isEmpty(),
                                                        serverCount)));
        MetadataUpdater updater = updater(rpcClient, false, serverCount);
        if (serverCount == 1) {
            updater.initializeTableMetadata(TABLE_PATH);
            assertThat(updater.getCluster().getTablePath(TABLE_ID)).hasValue(TABLE_PATH);
        } else {
            assertThatThrownBy(() -> updater.initializeTableMetadata(TABLE_PATH))
                    .hasRootCauseInstanceOf(StaleMetadataException.class);
            assertThat(updater.getCluster().getTableId(TABLE_PATH)).isEmpty();
        }
        assertThat(rpcClient.getRequests()).hasSize(3);
        assertThat(rpcClient.getRequests().get(serverCount).getTablePathsList()).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("permanentFailures")
    void testPermanentFailureIsNotRetried(Exception failure) {
        TestingMetadataRpcClient rpcClient =
                new TestingMetadataRpcClient(request -> failedFuture(failure));

        MetadataUpdater updater = updater(rpcClient, false);
        assertThatThrownBy(() -> updater.initializeTableMetadata(TABLE_PATH)).hasRootCause(failure);

        assertThat(rpcClient.getRequests()).hasSize(1);
    }

    @Test
    void testInterruptedRequestRestoresInterruptAndStopsRetry() {
        InterruptedException failure = new InterruptedException("interrupted metadata request");
        TestingMetadataRpcClient rpcClient =
                new TestingMetadataRpcClient(request -> failedFuture(failure));

        try {
            MetadataUpdater updater = updater(rpcClient, false);
            assertThatThrownBy(() -> updater.initializeTableMetadata(TABLE_PATH))
                    .hasRootCause(failure);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(rpcClient.getRequests()).hasSize(1);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void testInterruptedBackoffRestoresInterruptAndStopsRetry() {
        TestingMetadataRpcClient rpcClient =
                new TestingMetadataRpcClient(
                        request -> {
                            Thread.currentThread().interrupt();
                            return failedFuture(new StaleMetadataException("metadata unavailable"));
                        });
        MetadataUpdater updater = updater(rpcClient, false);
        try {
            assertThatThrownBy(() -> updater.initializeTableMetadata(TABLE_PATH))
                    .hasRootCauseInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(rpcClient.getRequests()).hasSize(1);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void testWrappedBootstrapInterruptRestoresInterruptAndStopsRetry() {
        InterruptedException failure = new InterruptedException("interrupted bootstrap request");
        TestingMetadataRpcClient rpcClient =
                new TestingMetadataRpcClient(request -> failedFuture(failure));
        Configuration configuration = new Configuration();
        configuration.set(
                ConfigOptions.BOOTSTRAP_SERVERS, Collections.singletonList("localhost:9080"));

        try {
            MetadataUpdater updater =
                    new MetadataUpdater(rpcClient, configuration, Cluster.empty());
            assertThatThrownBy(() -> updater.initializeTableMetadata(TABLE_PATH))
                    .hasRootCause(failure);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(rpcClient.getRequests()).hasSize(1);
        } finally {
            Thread.interrupted();
        }
    }

    private static Stream<Exception> retryableFailures() {
        return Stream.of(
                new TimeoutException("table metadata request timed out"),
                new StaleMetadataException("tablet server metadata is not ready"));
    }

    private static Stream<Exception> permanentFailures() {
        return Stream.of(
                new AuthenticationException("authentication failed"),
                new AuthorizationException("access denied"),
                new TableNotExistException("table does not exist"));
    }

    private static MetadataUpdater updater(RpcClient rpcClient, boolean tableMapped) {
        return updater(rpcClient, tableMapped, 3);
    }

    private static MetadataUpdater updater(
            RpcClient rpcClient, boolean tableMapped, int serverCount) {
        Configuration configuration = new Configuration();
        configuration.set(
                ConfigOptions.BOOTSTRAP_SERVERS, Collections.singletonList("localhost:9080"));
        Cluster cluster =
                new Cluster(
                        tabletServers(serverCount),
                        null,
                        Collections.emptyMap(),
                        tableMapped
                                ? Collections.singletonMap(TABLE_PATH, TABLE_ID)
                                : Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        return new MetadataUpdater(rpcClient, configuration, cluster);
    }

    private static Map<Integer, ServerNode> tabletServers(int serverCount) {
        Map<Integer, ServerNode> tabletServers = new HashMap<>();
        for (int id = 1; id <= serverCount; id++) {
            tabletServers.put(
                    id, new ServerNode(id, "localhost", 8080 + id, ServerType.TABLET_SERVER));
        }
        return tabletServers;
    }

    private static MetadataResponse metadataResponse(boolean tableMapped) {
        return metadataResponse(tableMapped, 3);
    }

    private static MetadataResponse metadataResponse(boolean tableMapped, int serverCount) {
        MetadataResponse response = new MetadataResponse();
        tabletServers(serverCount)
                .values()
                .forEach(
                        server ->
                                response.addTabletServer()
                                        .setNodeId(server.id())
                                        .setHost(server.host())
                                        .setPort(server.port()));
        if (tableMapped) {
            response.addTableMetadata()
                    .setTableId(TABLE_ID)
                    .setTablePath()
                    .setDatabaseName(TABLE_PATH.getDatabaseName())
                    .setTableName(TABLE_PATH.getTableName());
        }
        return response;
    }

    private static CompletableFuture<MetadataResponse> failedFuture(Exception failure) {
        CompletableFuture<MetadataResponse> future = new CompletableFuture<>();
        future.completeExceptionally(failure);
        return future;
    }
}

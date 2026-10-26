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
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.exception.NotCoordinatorLeaderException;
import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests authoritative metadata refresh after coordinator failover. */
class CoordinatorMetadataRefreshTest {
    private static final ServerNode OLD = new ServerNode(0, "old", 90, ServerType.COORDINATOR);
    private static final ServerNode NEW = new ServerNode(1, "new", 90, ServerType.COORDINATOR);
    private static final ServerNode TABLET =
            new ServerNode(2, "tablet", 91, ServerType.TABLET_SERVER);

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testAuthoritativeRefreshDiscoversNewCoordinator(boolean unknownCoordinator)
            throws Exception {
        RecordingClient client = new RecordingClient();
        Cluster initial =
                new Cluster(
                        Collections.singletonMap(TABLET.id(), TABLET),
                        unknownCoordinator ? null : OLD,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        MetadataUpdater updater = new MetadataUpdater(client, new Configuration(), initial);
        updater.updatePhysicalTableMetadataFromCoordinator(
                Collections.singleton(PhysicalTablePath.of(TablePath.of("db", "table"))));
        if (unknownCoordinator) {
            assertThat(client.nodes).containsExactly(TABLET, NEW);
        } else {
            assertThat(client.nodes).containsExactly(OLD, TABLET, NEW);
        }
        assertThat(updater.getCoordinatorServer()).isEqualTo(NEW);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPartitionRefreshUsesRemainingBudget(boolean bootstrap) throws Exception {
        CountDownLatch requested = new CountDownLatch(1);
        RecordingClient client =
                new RecordingClient() {
                    @Override
                    public CompletableFuture<ApiMessage> sendRequest(
                            ServerNode node, ApiKeys key, ApiMessage request) {
                        requested.countDown();
                        return new CompletableFuture<>();
                    }
                };
        Cluster initial =
                new Cluster(
                        Collections.singletonMap(TABLET.id(), TABLET),
                        OLD,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.BOOTSTRAP_SERVERS, Collections.singletonList("127.0.0.1:92"));
        MetadataUpdater updater =
                new MetadataUpdater(client, conf, bootstrap ? Cluster.empty() : initial);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> refresh =
                    executor.submit(
                            () ->
                                    assertThatThrownBy(
                                                    () ->
                                                            updater.checkAndUpdatePartitionMetadata(
                                                                    PhysicalTablePath.of(
                                                                            TablePath.of(
                                                                                    "db", "table"),
                                                                            "p"),
                                                                    System.nanoTime()
                                                                            + TimeUnit.MILLISECONDS
                                                                                    .toNanos(200)))
                                            .isInstanceOf(StaleMetadataException.class)
                                            .hasRootCauseInstanceOf(TimeoutException.class));
            assertThat(requested.await(5, TimeUnit.SECONDS)).isTrue();
            refresh.get(5, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void testFailedNodeDiscoveryFallsBackToBootstrapOnNextRefresh() throws Exception {
        RecordingClient client = new RecordingClient();
        client.failTablet = true;
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.BOOTSTRAP_SERVERS, Collections.singletonList("127.0.0.1:92"));
        Cluster initial =
                new Cluster(
                        Collections.singletonMap(TABLET.id(), TABLET),
                        OLD,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        MetadataUpdater updater = new MetadataUpdater(client, conf, initial);
        PhysicalTablePath path = PhysicalTablePath.of(TablePath.of("db", "table"));
        assertThatThrownBy(
                        () ->
                                updater.updatePhysicalTableMetadataFromCoordinator(
                                        Collections.singleton(path)))
                .isInstanceOf(StaleMetadataException.class);
        updater.updatePhysicalTableMetadataFromCoordinator(Collections.singleton(path));
        assertThat(updater.getCoordinatorServer()).isEqualTo(NEW);
        assertThat(client.nodes)
                .containsExactly(
                        OLD,
                        TABLET,
                        OLD,
                        new ServerNode(-1, "127.0.0.1", 92, ServerType.UNKNOWN),
                        NEW);
    }

    private static class RecordingClient implements RpcClient {
        private final List<ServerNode> nodes = new ArrayList<>();
        private boolean failTablet;

        @Override
        public CompletableFuture<ApiMessage> sendRequest(
                ServerNode node, ApiKeys key, ApiMessage request) {
            nodes.add(node);
            if (failTablet && node.equals(TABLET)) {
                CompletableFuture<ApiMessage> failed = new CompletableFuture<>();
                failed.completeExceptionally(new NetworkException("Tablet unavailable"));
                return failed;
            }
            if (node.equals(OLD)) {
                CompletableFuture<ApiMessage> failed = new CompletableFuture<>();
                failed.completeExceptionally(new NotCoordinatorLeaderException("No longer leader"));
                return failed;
            }
            MetadataResponse response = new MetadataResponse();
            response.addTabletServer()
                    .setNodeId(TABLET.id())
                    .setHost(TABLET.host())
                    .setPort(TABLET.port());
            response.setCoordinatorServer()
                    .setNodeId(NEW.id())
                    .setHost(NEW.host())
                    .setPort(NEW.port());
            return CompletableFuture.completedFuture(response);
        }

        @Override
        public boolean connect(ServerNode node) {
            return true;
        }

        @Override
        public boolean isReady(String uid) {
            return true;
        }

        @Override
        public CompletableFuture<Void> disconnect(String uid) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {}
    }
}

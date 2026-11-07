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

package org.apache.fluss.server.tablet;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.server.kv.KvCloseMode;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for the shutdown ordering of {@link TabletServer}. */
final class TabletServerShutdownTest {

    @Test
    void testQuiescesTabletUsersBeforeClosingTabletManagers() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.TABLET_SERVER_ID, 0);
        conf.setString(ConfigOptions.REMOTE_DATA_DIR, DEFAULT_REMOTE_DATA_DIR);
        TestingTabletServer server = new TestingTabletServer(conf);
        ExecutorService shutdownExecutor = Executors.newSingleThreadExecutor();
        Future<CompletableFuture<Void>> stopServicesInvocation =
                shutdownExecutor.submit(server::stopServices);

        try {
            assertThat(server.awaitRpcShutdownStarted()).isTrue();
            assertThat(server.getShutdownEvents()).containsExactly("rpc-started");

            server.completeRpcShutdown();
            stopServicesInvocation.get(10, TimeUnit.SECONDS).get(10, TimeUnit.SECONDS);

            assertThat(server.getShutdownEvents())
                    .containsExactly(
                            "rpc-started", "rpc-completed", "replica-manager", "tablet-managers");
            assertThat(server.getKvCloseMode()).isEqualTo(KvCloseMode.DISCARD_UNPERSISTED_STATE);
        } finally {
            server.completeRpcShutdown();
            shutdownExecutor.shutdownNow();
        }
    }

    @Test
    void testClosesTabletManagersWhenReplicaManagerShutdownFails() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.TABLET_SERVER_ID, 0);
        conf.setString(ConfigOptions.REMOTE_DATA_DIR, DEFAULT_REMOTE_DATA_DIR);
        TestingTabletServer server = new TestingTabletServer(conf);
        server.completeRpcShutdown();
        server.failReplicaManagerShutdown();

        assertThatThrownBy(() -> server.stopServices().get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(FlussRuntimeException.class);
        assertThat(server.getShutdownEvents())
                .containsExactly(
                        "rpc-started", "rpc-completed", "replica-manager", "tablet-managers");
        assertThat(server.getKvCloseMode()).isEqualTo(KvCloseMode.DISCARD_UNPERSISTED_STATE);
    }

    @Test
    void testInterruptedShutdownDrainsCoordinatorAndWorkerBeforeClosingDependencies()
            throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.TABLET_SERVER_ID, 0);
        conf.setString(ConfigOptions.REMOTE_DATA_DIR, DEFAULT_REMOTE_DATA_DIR);
        TestingTabletServer server = new TestingTabletServer(conf);
        server.completeRpcShutdown();
        ExecutorService coordinator = Executors.newSingleThreadExecutor();
        ExecutorService workers = Executors.newSingleThreadExecutor();
        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        CountDownLatch coordinatorAfterWorker = new CountDownLatch(1);
        CountDownLatch releaseCoordinator = new CountDownLatch(1);
        AtomicBoolean workerExited = new AtomicBoolean();
        AtomicBoolean shutdownInterrupted = new AtomicBoolean();
        AtomicBoolean rpcClosed = new AtomicBoolean();
        setField(server, "replicaStateChangeExecutor", coordinator);
        setField(
                server,
                "rpcClient",
                new RpcClient() {
                    @Override
                    public boolean connect(ServerNode node) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public CompletableFuture<Void> disconnect(String uid) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public boolean isReady(String uid) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public CompletableFuture<ApiMessage> sendRequest(
                            ServerNode node, ApiKeys key, ApiMessage request) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void close() {
                        assertThat(workerExited).isTrue();
                        assertThat(coordinator.isTerminated()).isTrue();
                        rpcClosed.set(true);
                    }
                });
        Future<?> recovery =
                coordinator.submit(
                        () -> {
                            try {
                                workers.submit(
                                                () -> {
                                                    workerStarted.countDown();
                                                    await(releaseWorker);
                                                    workerExited.set(true);
                                                })
                                        .get();
                                coordinatorAfterWorker.countDown();
                                await(releaseCoordinator);
                            } catch (Exception e) {
                                throw new AssertionError(e);
                            }
                        });
        CompletableFuture<Void> stopped = new CompletableFuture<>();
        Thread closer =
                new Thread(
                        () -> {
                            try {
                                server.stopServices().join();
                                shutdownInterrupted.set(Thread.currentThread().isInterrupted());
                                stopped.complete(null);
                            } catch (Throwable t) {
                                stopped.completeExceptionally(t);
                            }
                        });
        try {
            assertThat(workerStarted.await(10, TimeUnit.SECONDS)).isTrue();
            closer.start();
            waitUntil(
                    () ->
                            coordinator.isShutdown()
                                    && closer.getState() == Thread.State.TIMED_WAITING,
                    Duration.ofSeconds(10),
                    "Shutdown did not await coordinator");
            closer.interrupt();
            waitUntil(
                    () ->
                            !closer.isInterrupted()
                                    && closer.getState() == Thread.State.TIMED_WAITING,
                    Duration.ofSeconds(10),
                    "Interrupted shutdown did not keep draining");
            assertThat(stopped).isNotDone();
            assertThat(rpcClosed).isFalse();
            assertThat(server.getShutdownEvents()).containsExactly("rpc-started", "rpc-completed");
            releaseWorker.countDown();
            assertThat(coordinatorAfterWorker.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(workerExited).isTrue();
            assertThat(stopped).isNotDone();
            assertThat(rpcClosed).isFalse();
            releaseCoordinator.countDown();
            stopped.get(10, TimeUnit.SECONDS);
            recovery.get(10, TimeUnit.SECONDS);
            assertThat(rpcClosed).isTrue();
            assertThat(shutdownInterrupted).isTrue();
            assertThat(coordinator.isTerminated()).isTrue();
            assertThat(server.getShutdownEvents())
                    .containsExactly(
                            "rpc-started", "rpc-completed", "replica-manager", "tablet-managers");
        } finally {
            releaseWorker.countDown();
            releaseCoordinator.countDown();
            closer.join(10000);
            coordinator.shutdownNow();
            workers.shutdownNow();
        }
    }

    private static void setField(TabletServer server, String name, Object value) throws Exception {
        Field field = TabletServer.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(server, value);
    }

    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static final class TestingTabletServer extends TabletServer {
        private final CompletableFuture<Void> rpcShutdownFuture = new CompletableFuture<>();
        private final CountDownLatch rpcShutdownStarted = new CountDownLatch(1);
        private final List<String> shutdownEvents = new CopyOnWriteArrayList<>();
        private boolean failReplicaManagerShutdown;
        private KvCloseMode kvCloseMode;

        private TestingTabletServer(Configuration conf) {
            super(conf);
        }

        @Override
        CompletableFuture<Void> shutdownRpcServer() {
            shutdownEvents.add("rpc-started");
            rpcShutdownStarted.countDown();
            return rpcShutdownFuture.whenComplete(
                    (ignored, throwable) -> shutdownEvents.add("rpc-completed"));
        }

        @Override
        void shutdownReplicaManager() {
            shutdownEvents.add("replica-manager");
            if (failReplicaManagerShutdown) {
                throw new FlussRuntimeException("Expected replica manager shutdown failure.");
            }
        }

        @Override
        void shutdownKvManager(KvCloseMode closeMode) {
            shutdownEvents.add("tablet-managers");
            kvCloseMode = closeMode;
        }

        private boolean awaitRpcShutdownStarted() throws InterruptedException {
            return rpcShutdownStarted.await(10, TimeUnit.SECONDS);
        }

        private void completeRpcShutdown() {
            rpcShutdownFuture.complete(null);
        }

        private void failReplicaManagerShutdown() {
            failReplicaManagerShutdown = true;
        }

        private List<String> getShutdownEvents() {
            return shutdownEvents;
        }

        private KvCloseMode getKvCloseMode() {
            return kvCloseMode;
        }
    }
}

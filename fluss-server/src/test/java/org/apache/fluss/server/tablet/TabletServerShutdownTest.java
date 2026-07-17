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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
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
    }

    private static final class TestingTabletServer extends TabletServer {
        private final CompletableFuture<Void> rpcShutdownFuture = new CompletableFuture<>();
        private final CountDownLatch rpcShutdownStarted = new CountDownLatch(1);
        private final List<String> shutdownEvents = new CopyOnWriteArrayList<>();
        private boolean failReplicaManagerShutdown;

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
        void shutdownTabletManagers() {
            shutdownEvents.add("tablet-managers");
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
    }
}

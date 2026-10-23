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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.rpc.messages.ListCompactionsRequest;
import org.apache.fluss.rpc.messages.ListCompactionsResponse;
import org.apache.fluss.rpc.netty.server.Session;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.server.ServerBase;
import org.apache.fluss.server.ServerTestBase;
import org.apache.fluss.server.coordinator.event.CompactionEvent;
import org.apache.fluss.server.zk.data.CoordinatorAddress;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link CoordinatorServer} . */
class CoordinatorServerTest extends ServerTestBase {

    private CoordinatorServer coordinatorServer;

    @BeforeEach
    void beforeEach() throws Exception {
        coordinatorServer = startCoordinatorServer(createConfiguration());
        waitUntilCoordinatorServerElected();
    }

    @AfterEach
    void after() throws Exception {
        if (coordinatorServer != null) {
            coordinatorServer.close();
        }
    }

    @Override
    protected ServerBase getServer() {
        return coordinatorServer;
    }

    @Override
    protected ServerBase getStartFailServer() {
        Configuration configuration = createConfiguration();
        // CoordinatorServer starts leader services asynchronously in a separate election
        // thread. An invalid port wouldn't cause start() to throw because the port binding
        // happens asynchronously. Instead, use an empty ZK address to cause a synchronous
        // failure in startZookeeperClient() during start().
        configuration.setString(ConfigOptions.ZOOKEEPER_ADDRESS, "");
        return new CoordinatorServer(configuration);
    }

    @Override
    protected void checkAfterStartServer() throws Exception {
        assertThat(coordinatorServer.getRpcServer()).isNotNull();
        // check the data put in zk after coordinator server start
        Optional<CoordinatorAddress> optCoordinatorAddr =
                zookeeperClient.getCoordinatorLeaderAddress();
        assertThat(optCoordinatorAddr).isNotEmpty();
        verifyEndpoint(
                optCoordinatorAddr.get().getEndpoints(),
                coordinatorServer.getRpcServer().getBindEndpoints());
    }

    public void waitUntilCoordinatorServerElected() {
        waitUntil(
                () -> zookeeperClient.getCoordinatorLeaderAddress().isPresent(),
                Duration.ofSeconds(5),
                "Fail to wait coordinator server elected");
    }

    @Test
    void testCompactionCloseDrainsCleanupBeforeEventQueueStops() throws Exception {
        coordinatorServer
                .getCoordinatorService()
                .setCurrentSession(
                        new Session(
                                (short) 0,
                                "FLUSS",
                                true,
                                InetAddress.getLoopbackAddress(),
                                FlussPrincipal.ANONYMOUS));
        retry(
                Duration.ofSeconds(10),
                () ->
                        assertThatCode(
                                        () ->
                                                coordinatorServer
                                                        .getCoordinatorService()
                                                        .listCompactions(
                                                                new ListCompactionsRequest())
                                                        .get())
                                .doesNotThrowAnyException());
        CoordinatorEventProcessor processor = coordinatorServer.getCoordinatorEventProcessor();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        processor
                .getCoordinatorEventManager()
                .put(
                        new CompactionEvent(
                                () -> {
                                    entered.countDown();
                                    try {
                                        release.await();
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                    }
                                }));
        assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
        CompletableFuture<ListCompactionsResponse> pending =
                coordinatorServer
                        .getCoordinatorService()
                        .listCompactions(new ListCompactionsRequest());
        CompletableFuture<Void> closing =
                CompletableFuture.runAsync(
                        () -> {
                            Thread.currentThread().interrupt();
                            try {
                                processor.closeCompactionManager();
                                assertThat(Thread.currentThread().isInterrupted()).isTrue();
                            } finally {
                                Thread.interrupted();
                            }
                        });
        try {
            assertThatThrownBy(() -> pending.get(10, TimeUnit.SECONDS))
                    .rootCause()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("not active");
            assertThatThrownBy(() -> closing.get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(java.util.concurrent.TimeoutException.class);
        } finally {
            release.countDown();
        }
        closing.get(10, TimeUnit.SECONDS);
        AtomicBoolean lateCallback = new AtomicBoolean();
        assertThat(processor.putCompactionEvent(new CompactionEvent(() -> lateCallback.set(true))))
                .isFalse();
        assertThat(lateCallback).isFalse();
        assertThatThrownBy(
                        () ->
                                coordinatorServer
                                        .getCoordinatorService()
                                        .listCompactions(new ListCompactionsRequest())
                                        .get(10, TimeUnit.SECONDS))
                .rootCause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not active");
        coordinatorServer.close();
    }
}

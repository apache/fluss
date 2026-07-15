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

import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CoordinatorLeaderElection}. */
class CoordinatorLeaderElectionTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zooKeeperClient;

    @BeforeAll
    static void beforeAll() {
        zooKeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @Test
    void testCloseWaitsForLeaderCleanup() throws Exception {
        CoordinatorLeaderElection election =
                new CoordinatorLeaderElection(zooKeeperClient, "coordinator-1");
        CountDownLatch cleanupStarted = new CountDownLatch(1);
        CountDownLatch allowCleanup = new CountDownLatch(1);
        CountDownLatch cleanupFinished = new CountDownLatch(1);
        AtomicInteger cleanupCount = new AtomicInteger();

        election.startElectLeaderAsync(
                () -> {},
                ignored -> {
                    cleanupCount.incrementAndGet();
                    cleanupStarted.countDown();
                    try {
                        allowCleanup.await();
                        cleanupFinished.countDown();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
        waitUntil(
                election::isLeader,
                Duration.ofSeconds(30),
                "Coordinator was not elected as leader");

        CompletableFuture<Void> closeFuture = CompletableFuture.runAsync(election::close);
        assertThat(cleanupStarted.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(closeFuture).isNotDone();

        allowCleanup.countDown();
        closeFuture.get(30, TimeUnit.SECONDS);
        assertThat(cleanupFinished.getCount()).isEqualTo(0);
        assertThat(cleanupCount).hasValue(1);
    }

    @Test
    void testCloseDuringLeaderInitializationCleansUp() throws Exception {
        CoordinatorLeaderElection election =
                new CoordinatorLeaderElection(zooKeeperClient, "coordinator-2");
        CountDownLatch initializationStarted = new CountDownLatch(1);
        CountDownLatch allowInitialization = new CountDownLatch(1);
        CountDownLatch closeStarted = new CountDownLatch(1);
        AtomicInteger cleanupCount = new AtomicInteger();

        election.startElectLeaderAsync(
                () -> {
                    initializationStarted.countDown();
                    try {
                        allowInitialization.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                },
                ignored -> cleanupCount.incrementAndGet());
        assertThat(initializationStarted.await(30, TimeUnit.SECONDS)).isTrue();

        CompletableFuture<Void> closeFuture =
                CompletableFuture.runAsync(
                        () -> {
                            closeStarted.countDown();
                            election.close();
                        });
        assertThat(closeStarted.await(30, TimeUnit.SECONDS)).isTrue();
        try {
            assertThat(closeFuture).isNotDone();
            assertThat(cleanupCount).hasValue(0);
        } finally {
            // allow to continue to initialize as leader after the election is close.
            allowInitialization.countDown();
        }

        closeFuture.get(30, TimeUnit.SECONDS);
        assertThat(cleanupCount).hasValue(1);
        assertThat(election.isLeader()).isFalse();
    }
}

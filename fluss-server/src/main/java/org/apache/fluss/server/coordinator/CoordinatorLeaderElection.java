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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.exception.CoordinatorEpochFencedException;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.leader.LeaderLatchListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Using by coordinator server. Coordinator servers listen ZK node and elect leadership.
 *
 * <p>This class manages the leader election lifecycle:
 *
 * <ul>
 *   <li>Start election and participate as a candidate
 *   <li>When elected as leader, invoke the initialization callback
 *   <li>When losing leadership, clean up leader resources but continue participating in election
 *   <li>Can be re-elected as leader multiple times
 * </ul>
 */
public class CoordinatorLeaderElection implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorLeaderElection.class);

    private final String serverId;
    private final ZooKeeperClient zkClient;
    private final CoordinatorContext coordinatorContext;
    private final LeaderLatch leaderLatch;
    private final AtomicBoolean isLeader = new AtomicBoolean(false);
    // Cached thread pool to run leader init/cleanup callbacks outside Curator's EventThread.
    // Curator's LeaderLatchListener callbacks run on its internal EventThread; performing
    // synchronous ZK operations there causes deadlock because ZK response dispatch also
    // needs that same thread. A cached pool is used because these callbacks are transient
    // (only run during leadership transitions), so idle threads can be reclaimed.
    private final ExecutorService leaderCallbackExecutor;
    // Tracks the pending cleanup task so that init can wait for it to complete.
    private final AtomicReference<CompletableFuture<Void>> pendingCleanup =
            new AtomicReference<>(CompletableFuture.completedFuture(null));

    public CoordinatorLeaderElection(
            ZooKeeperClient zkClient, String serverId, CoordinatorContext coordinatorContext) {
        this.serverId = serverId;
        this.zkClient = zkClient;
        this.coordinatorContext = coordinatorContext;
        this.leaderLatch =
                new LeaderLatch(
                        zkClient.getCuratorClient(),
                        ZkData.CoordinatorElectionZNode.path(),
                        String.valueOf(serverId));
        this.leaderCallbackExecutor =
                Executors.newCachedThreadPool(
                        r -> {
                            Thread t = new Thread(r, "coordinator-leader-callback-" + serverId);
                            // Daemon threads ensure the JVM can exit even if close() is not
                            // called. Orderly shutdown is handled by close() -> shutdownNow().
                            t.setDaemon(true);
                            return t;
                        });
    }

    /**
     * Starts the leader election process asynchronously.
     *
     * <p>After the first election, the server will continue to participate in future elections.
     * When re-elected as leader, the initLeaderServices callback will be invoked again.
     *
     * @param initLeaderServices the callback to initialize leader services once elected
     * @param cleanupLeaderServices the callback to clean up leader services when losing leadership
     */
    public void startElectLeaderAsync(
            Runnable initLeaderServices, Consumer<Throwable> cleanupLeaderServices) {
        leaderLatch.addListener(
                new LeaderLatchListener() {
                    @Override
                    public void isLeader() {
                        // return if already marked as leader to avoid duplicate init calls
                        if (isLeader.get()) {
                            return;
                        }
                        LOG.info("Coordinator server {} has become the leader.", serverId);
                        // Capture the pending cleanup future at this point so that
                        // init waits for it before proceeding.
                        CompletableFuture<Void> cleanup = pendingCleanup.get();
                        // Run init on a separate thread to avoid deadlock with
                        // Curator's EventThread when performing ZK operations.
                        leaderCallbackExecutor.execute(
                                () -> {
                                    // Wait for any pending cleanup to finish first.
                                    try {
                                        cleanup.get(60, TimeUnit.SECONDS);
                                    } catch (TimeoutException e) {
                                        LOG.warn(
                                                "Pending cleanup for server {} did not complete within 60s, proceeding with init.",
                                                serverId);
                                    } catch (Exception e) {
                                        LOG.warn(
                                                "Error waiting for pending cleanup for server {}",
                                                serverId,
                                                e);
                                    }
                                    try {
                                        // to avoid split-brain
                                        Optional<ZkEpoch> optionalEpoch =
                                                zkClient.fenceBecomeCoordinatorLeader(serverId);
                                        optionalEpoch.ifPresent(
                                                integer ->
                                                        coordinatorContext
                                                                .setCoordinatorEpochAndZkVersion(
                                                                        optionalEpoch
                                                                                .get()
                                                                                .getCoordinatorEpoch(),
                                                                        optionalEpoch
                                                                                .get()
                                                                                .getCoordinatorEpochZkVersion()));
                                        initLeaderServices.run();
                                    } catch (CoordinatorEpochFencedException e) {
                                        LOG.warn(
                                                "Coordinator server {} has been fenced and not become leader successfully.",
                                                serverId);
                                        throw e;
                                    } catch (Exception e) {
                                        LOG.error(
                                                "Failed to initialize leader services for server {}",
                                                serverId,
                                                e);
                                    }
                                });
                        // Set leader flag before init completes, so when zk found this leader, the
                        // leader can accept requests
                        isLeader.set(true);
                    }

                    @Override
                    public void notLeader() {
                        if (isLeader.compareAndSet(true, false)) {
                            LOG.warn(
                                    "Coordinator server {} has lost the leadership, cleaning up leader services.",
                                    serverId);
                            // Submit cleanup to leaderCallbackExecutor. The cached thread
                            // pool can spawn a new thread even if init is still running.
                            // The cleanup completion is tracked via pendingCleanup so
                            // that subsequent init waits for it.
                            CompletableFuture<Void> cleanupFuture = new CompletableFuture<>();
                            pendingCleanup.set(cleanupFuture);
                            leaderCallbackExecutor.execute(
                                    () -> {
                                        try {
                                            if (cleanupLeaderServices != null) {
                                                cleanupLeaderServices.accept(null);
                                            }
                                        } catch (Exception e) {
                                            LOG.error(
                                                    "Failed to cleanup leader services for server {}",
                                                    serverId,
                                                    e);
                                        } finally {
                                            cleanupFuture.complete(null);
                                        }
                                    });
                        }
                    }
                });

        try {
            leaderLatch.start();
            LOG.info("Coordinator server {} started leader election.", serverId);
        } catch (Exception e) {
            LOG.error("Failed to start LeaderLatch for server {}", serverId, e);
        }
    }

    @Override
    public void close() {
        LOG.info("Closing LeaderLatch for server {}.", serverId);

        if (leaderLatch != null) {
            try {
                leaderLatch.close();
            } catch (Exception e) {
                LOG.error("Failed to close LeaderLatch for server {}.", serverId, e);
            }
        }

        leaderCallbackExecutor.shutdownNow();
    }

    public boolean isLeader() {
        return this.isLeader.get();
    }
}

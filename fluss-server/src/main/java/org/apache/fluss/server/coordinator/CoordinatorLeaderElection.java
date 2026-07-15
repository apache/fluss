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
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.leader.LeaderLatchListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
 *
 * <p>Leadership callbacks and state transitions are serialized by {@code leaderCallbackExecutor}.
 * The state machine is:
 *
 * <pre>
 *                         leadership granted + initialization succeeds
 *        +----------------------------------------------------------------+
 *        |                                                                v
 * +-----------+  leadership revoked  +-----------+  grant + init  +----------+
 * |  INITIAL  | -------------------> |  STANDBY  | -------------> |  LEADER  |
 * +-----------+                      +-----------+                +----------+
 *                                         ^                           |
 *                                         +---- revoke + cleanup -----+
 *
 *        INITIAL / STANDBY / LEADER -- close + optional cleanup --> CLOSED
 *        initialization failure -------- cleanup -----------------> STANDBY
 * </pre>
 *
 * <p>If leader initialization fails, any partially initialized leader resources are cleaned up and
 * the state becomes {@code STANDBY}. The {@code CLOSED} state is terminal.
 */
public class CoordinatorLeaderElection implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorLeaderElection.class);

    private final String serverId;
    private final LeaderLatch leaderLatch;
    // Single-threaded executor to run leader init/cleanup callbacks outside Curator's EventThread.
    // Curator's LeaderLatchListener callbacks run on its internal EventThread; performing
    // synchronous ZK operations there causes deadlock because ZK response dispatch also
    // needs that same thread. Serial execution also guarantees that leader initialization,
    // cleanup, and state transitions never overlap.
    private final ExecutorService leaderCallbackExecutor;
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

    private final AtomicBoolean closing = new AtomicBoolean(false);
    private volatile State state = State.INITIAL;

    private volatile Consumer<Throwable> cleanupLeaderServices;

    public CoordinatorLeaderElection(ZooKeeperClient zkClient, String serverId) {
        this.serverId = serverId;
        this.leaderLatch =
                new LeaderLatch(
                        zkClient.getCuratorClient(),
                        ZkData.CoordinatorElectionZNode.path(),
                        String.valueOf(serverId));
        this.leaderCallbackExecutor =
                Executors.newSingleThreadExecutor(
                        r -> {
                            Thread t = new Thread(r, "coordinator-leader-callback-" + serverId);
                            // Daemon threads ensure the JVM can exit even if close() is not
                            // called. Orderly shutdown is handled by close().
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
        this.cleanupLeaderServices = cleanupLeaderServices;
        leaderLatch.addListener(
                new LeaderLatchListener() {
                    @Override
                    public void isLeader() {
                        submitLeadershipEvent(true, initLeaderServices);
                    }

                    @Override
                    public void notLeader() {
                        submitLeadershipEvent(false, initLeaderServices);
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

        if (closing.compareAndSet(false, true)) {
            try {
                leaderLatch.close();
            } catch (Exception e) {
                LOG.error("Failed to close LeaderLatch for server {}.", serverId, e);
            }

            // Events submitted after closing starts are ignored by their executor-side check.
            // Since the executor is single-threaded, this task runs after all leadership work
            // already queued before close and completes only after leader cleanup has finished.
            leaderCallbackExecutor.execute(
                    () -> {
                        try {
                            boolean cleanupRequired = state == State.LEADER;
                            state = State.CLOSED;
                            if (cleanupRequired) {
                                cleanupLeaderServices(null);
                            }
                        } finally {
                            closeFuture.complete(null);
                        }
                    });
        }

        closeFuture.join();
        leaderCallbackExecutor.shutdown();
        awaitCallbackExecutorTermination();
    }

    public boolean isLeader() {
        return !closing.get() && state == State.LEADER;
    }

    private void submitLeadershipEvent(boolean leader, Runnable initLeaderServices) {
        if (closing.get()) {
            return;
        }
        try {
            leaderCallbackExecutor.execute(
                    () -> {
                        if (closing.get() || state == State.CLOSED) {
                            return;
                        }
                        if (leader) {
                            becomeLeader(initLeaderServices);
                        } else {
                            becomeStandby();
                        }
                    });
        } catch (RejectedExecutionException e) {
            if (!closing.get()) {
                throw e;
            }
        }
    }

    private void becomeLeader(Runnable initLeaderServices) {
        if (closing.get() || state == State.LEADER || state == State.CLOSED) {
            return;
        }

        LOG.info("Coordinator server {} has become the leader.", serverId);
        Throwable initializationFailure = null;
        try {
            initLeaderServices.run();
        } catch (CoordinatorEpochFencedException e) {
            LOG.warn(
                    "Coordinator server {} has been fenced and not become leader successfully.",
                    serverId);
            initializationFailure = e;
        } catch (Exception e) {
            LOG.error("Failed to initialize leader services for server {}", serverId, e);
            initializationFailure = e;
        }

        if (initializationFailure == null && !closing.get()) {
            state = State.LEADER;
        } else {
            cleanupLeaderServices(initializationFailure);
            state = State.STANDBY;
        }
    }

    private void becomeStandby() {
        if (state == State.CLOSED) {
            return;
        }

        boolean cleanupRequired = state == State.LEADER;
        state = State.STANDBY;
        if (cleanupRequired) {
            cleanupLeaderServices(null);
        }
    }

    private void cleanupLeaderServices(Throwable cause) {
        LOG.warn(
                "Coordinator server {} has lost the leadership, cleaning up leader services.",
                serverId);
        try {
            if (cleanupLeaderServices != null) {
                cleanupLeaderServices.accept(cause);
            }
        } catch (Exception e) {
            LOG.error("Failed to cleanup leader services for server {}", serverId, e);
        }
    }

    private void awaitCallbackExecutorTermination() {
        boolean interrupted = false;
        while (true) {
            try {
                if (leaderCallbackExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                    break;
                }
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Internal lifecycle states. All transitions are performed by {@code leaderCallbackExecutor}.
     */
    private enum State {
        INITIAL,
        LEADER,
        STANDBY,
        CLOSED
    }
}

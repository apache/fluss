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
import java.util.concurrent.atomic.AtomicBoolean;

/** Using by coordinator server. Coordinator servers listen ZK node and elect leadership. */
public class CoordinatorLeaderElection implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorLeaderElection.class);

    private final int serverId;
    private final ZooKeeperClient zkClient;
    private final CoordinatorContext coordinatorContext;
    private final LeaderLatch leaderLatch;
    private final AtomicBoolean isLeader = new AtomicBoolean(false);
    private final CompletableFuture<Void> leaderReadyFuture = new CompletableFuture<>();
    private volatile Thread electionThread;

    public CoordinatorLeaderElection(
            ZooKeeperClient zkClient, int serverId, CoordinatorContext coordinatorContext) {
        this.serverId = serverId;
        this.zkClient = zkClient;
        this.coordinatorContext = coordinatorContext;
        this.leaderLatch =
                new LeaderLatch(
                        zkClient.getCuratorClient(),
                        ZkData.CoordinatorElectionZNode.path(),
                        String.valueOf(serverId));
    }

    /**
     * Starts the leader election process asynchronously. The returned future completes when this
     * server becomes the leader and initializes the leader services.
     *
     * @param initLeaderServices the runnable to initialize leader services once elected
     * @return a CompletableFuture that completes when this server becomes leader
     */
    public CompletableFuture<Void> startElectLeaderAsync(Runnable initLeaderServices) {
        leaderLatch.addListener(
                new LeaderLatchListener() {
                    @Override
                    public void isLeader() {
                        LOG.info("Coordinator server {} has become the leader.", serverId);
                        isLeader.set(true);
                    }

                    @Override
                    public void notLeader() {
                        relinquishLeadership();
                        LOG.warn("Coordinator server {} has lost the leadership.", serverId);
                        isLeader.set(false);
                    }
                });

        try {
            leaderLatch.start();
            LOG.info("Coordinator server {} started leader election.", serverId);
        } catch (Exception e) {
            LOG.error("Failed to start LeaderLatch for server {}", serverId, e);
            leaderReadyFuture.completeExceptionally(
                    new RuntimeException("Leader election start failed", e));
            return leaderReadyFuture;
        }

        // Run the await and initialization in a separate thread to avoid blocking
        electionThread =
                new Thread(
                        () -> {
                            try {
                                // todo: Currently, we await the leader latch and do nothing until
                                // it becomes leader.
                                // Later we can make it as a hot backup server to continuously
                                // synchronize metadata from
                                // Zookeeper, which save time from recovering context
                                leaderLatch.await();
                                doInitLeaderServices(initLeaderServices);
                                leaderReadyFuture.complete(null);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                LOG.info(
                                        "Leader election for server {} was interrupted.", serverId);
                                leaderReadyFuture.completeExceptionally(e);
                            } catch (Exception e) {
                                LOG.error(
                                        "Failed during leader election for server {}", serverId, e);
                                leaderReadyFuture.completeExceptionally(e);
                            }
                        },
                        "coordinator-leader-election-" + serverId);
        electionThread.start();

        return leaderReadyFuture;
    }

    public void doInitLeaderServices(Runnable initLeaderServices) {
        try {
            // to avoid split-brain
            Optional<ZkEpoch> optionalEpoch = zkClient.fenceBecomeCoordinatorLeader(serverId);
            optionalEpoch.ifPresent(
                    integer ->
                            coordinatorContext.setCoordinatorEpochAndZkVersion(
                                    optionalEpoch.get().getCoordinatorEpoch(),
                                    optionalEpoch.get().getCoordinatorEpochZkVersion()));
        } catch (CoordinatorEpochFencedException e) {
            relinquishLeadership();
            LOG.warn(
                    "Coordinator server {} has been fence and not become leader successfully.",
                    serverId);
            throw e;
        } catch (Exception e) {
            LOG.warn("Coordinator server {} failed to become leader successfully.", serverId, e);
            relinquishLeadership();
            throw new RuntimeException("Failed to become leader", e);
        }
        initLeaderServices.run();
    }

    @Override
    public void close() {
        LOG.info("Closing LeaderLatch for server {}.", serverId);

        // Interrupt the election thread if it's waiting
        if (electionThread != null && electionThread.isAlive()) {
            electionThread.interrupt();
        }

        if (leaderLatch != null) {
            try {
                leaderLatch.close();
            } catch (Exception e) {
                LOG.error("Failed to close LeaderLatch for server {}.", serverId, e);
            }
        }

        // Complete the future exceptionally if it hasn't been completed yet
        leaderReadyFuture.completeExceptionally(
                new RuntimeException("Leader election closed for server " + serverId));
    }

    public boolean isLeader() {
        return this.isLeader.get();
    }

    private void relinquishLeadership() {
        isLeader.set(false);
        LOG.info("Coordinator server {} has been fenced.", serverId);

        this.close();
    }
}

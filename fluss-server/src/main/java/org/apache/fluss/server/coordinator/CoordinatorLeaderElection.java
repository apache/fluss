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

import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.leader.LeaderLatchListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Using by coordinator server. Coordinator servers listen ZK node and elect leadership. */
public class CoordinatorLeaderElection implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorLeaderElection.class);

    private final int serverId;
    private final LeaderLatch leaderLatch;
    private final AtomicBoolean isLeader = new AtomicBoolean(false);

    public CoordinatorLeaderElection(CuratorFramework zkClient, int serverId) {
        this.serverId = serverId;
        this.leaderLatch =
                new LeaderLatch(
                        zkClient, ZkData.CoordinatorElectionZNode.path(), String.valueOf(serverId));
    }

    public void startElectLeader(Runnable initLeaderServices) {
        leaderLatch.addListener(
                new LeaderLatchListener() {
                    @Override
                    public void isLeader() {
                        LOG.info("Coordinator server {} has become the leader.", serverId);
                        isLeader.set(true);
                    }

                    @Override
                    public void notLeader() {
                        LOG.warn("Coordinator server {} has lost the leadership.", serverId);
                        isLeader.set(false);
                    }
                });

        try {
            leaderLatch.start();
            LOG.info("Coordinator server {} started leader election.", serverId);

            // todo: Currently, we await the leader latch and do nothing until it becomes leader.
            // Later we can make it as a hot backup server to continuously synchronize metadata from
            // Zookeeper, which save time from initializing context
            leaderLatch.await();
            initLeaderServices.run();

        } catch (Exception e) {
            LOG.error("Failed to start LeaderLatch for server {}", serverId, e);
            throw new RuntimeException("Leader election start failed", e);
        }
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing LeaderLatch for server {}.", serverId);
        if (leaderLatch != null) {
            leaderLatch.close();
        }
    }

    public boolean isLeader() {
        return this.isLeader.get();
    }
}

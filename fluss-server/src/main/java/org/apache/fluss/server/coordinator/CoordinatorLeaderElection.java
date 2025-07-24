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

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.server.zk.data.ZkData;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.framework.recipes.leader.LeaderSelector;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.framework.state.ConnectionState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Using by coordinator server. Coordinator servers listen ZK node and elect leadership. */
public class CoordinatorLeaderElection {
    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorLeaderElection.class);

    private final CuratorFramework zkClient;
    private final int serverId;

    public CoordinatorLeaderElection(CuratorFramework zkClient, int serverId) {
        this.zkClient = zkClient;
        this.serverId = serverId;
    }

    public void startElectLeader(Runnable startLeaderServices) {
        LeaderSelector leaderSelector =
                new LeaderSelector(
                        zkClient,
                        ZkData.CoordinatorLeaderZNode.path(),
                        new LeaderSelectorListener() {
                            @Override
                            public void takeLeadership(CuratorFramework client) {
                                LOG.info(
                                        "Coordinator server {} win the leader in election now.",
                                        serverId);
                                startLeaderServices.run();
                            }

                            @Override
                            public void stateChanged(
                                    CuratorFramework client, ConnectionState newState) {
                                if (newState == ConnectionState.LOST) {
                                    LOG.info("Coordinator leader {} lost connection", serverId);
                                }
                            }
                        });

        // allow reelection
        leaderSelector.autoRequeue();
        leaderSelector.start();
    }
}

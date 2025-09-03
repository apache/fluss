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

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Utils related to coordinator for test purpose. */
public class CoordinatorTestUtils {

    public static void makeSendLeaderAndStopRequestAlwaysSuccess(
            CoordinatorContext coordinatorContext,
            TestCoordinatorChannelManager testCoordinatorChannelManager) {
        Map<Integer, TabletServerGateway> gateways =
                makeTabletServerGateways(
                        coordinatorContext.getLiveTabletServers().keySet(), Collections.emptySet());
        testCoordinatorChannelManager.setGateways(gateways);
    }

    public static void makeSendLeaderAndStopRequestFailContext(
            CoordinatorContext coordinatorContext,
            TestCoordinatorChannelManager testCoordinatorChannelManager,
            Set<Integer> failServers) {
        Map<Integer, TabletServerGateway> gateways =
                makeTabletServerGateways(
                        coordinatorContext.getLiveTabletServers().keySet(), failServers);
        testCoordinatorChannelManager.setGateways(gateways);
    }

    public static void makeSendLeaderAndStopRequestAlwaysSuccess(
            TestCoordinatorChannelManager testCoordinatorChannelManager, Set<Integer> servers) {
        Map<Integer, TabletServerGateway> gateways =
                makeTabletServerGateways(servers, Collections.emptySet());
        testCoordinatorChannelManager.setGateways(gateways);
    }

    public static void makeSendLeaderAndStopRequestFailContext(
            TestCoordinatorChannelManager testCoordinatorChannelManager,
            Set<Integer> servers,
            Set<Integer> failServers) {
        Map<Integer, TabletServerGateway> gateways = makeTabletServerGateways(servers, failServers);
        testCoordinatorChannelManager.setGateways(gateways);
    }

    private static Map<Integer, TabletServerGateway> makeTabletServerGateways(
            Set<Integer> servers, Set<Integer> failedServers) {
        Map<Integer, TabletServerGateway> gateways = new HashMap<>();
        for (Integer server : servers) {
            TabletServerGateway tabletServerGateway =
                    new TestTabletServerGateway(failedServers.contains(server));
            gateways.put(server, tabletServerGateway);
        }
        return gateways;
    }

    public static List<ServerInfo> createServers(List<Integer> servers) {
        List<ServerInfo> tabletServes = new ArrayList<>();
        for (int server : servers) {
            tabletServes.add(
                    new ServerInfo(
                            server,
                            "RACK" + server,
                            Endpoint.fromListenersString("CLIENT://host:100"),
                            ServerType.TABLET_SERVER));
        }
        return tabletServes;
    }

    public static void checkLeaderAndIsr(
            ZooKeeperClient zooKeeperClient,
            TableBucket tableBucket,
            int expectLeaderEpoch,
            int expectLeader)
            throws Exception {
        LeaderAndIsr leaderAndIsr = zooKeeperClient.getLeaderAndIsr(tableBucket).get();
        assertThat(leaderAndIsr.leaderEpoch()).isEqualTo(expectLeaderEpoch);
        assertThat(leaderAndIsr.leader()).isEqualTo(expectLeader);
    }
}

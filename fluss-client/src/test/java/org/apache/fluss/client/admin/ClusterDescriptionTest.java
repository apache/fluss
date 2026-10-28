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

package org.apache.fluss.client.admin;

import org.apache.fluss.client.utils.ClientRpcMessageUtils;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.rpc.messages.DescribeClusterResponse;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ClusterDescription} and its RPC conversion. */
class ClusterDescriptionTest {

    @Test
    void testLiveNodesAndOfflineLoads() {
        DescribeClusterResponse response = new DescribeClusterResponse();
        response.setCoordinatorServer().setNodeId(0).setHost("coordinator").setPort(9123);
        response.addTabletServer()
                .setNodeId(2)
                .setHost("ts-2")
                .setPort(9125)
                .setRack("rack-2")
                .setServerTag(ServerTag.PERMANENT_OFFLINE.value);
        response.addTabletServer().setNodeId(0).setHost("ts-0").setPort(9124);
        response.addTabletServer().setNodeId(3).setHost("ts-3").setPort(9126).setServerTag(42);
        response.addTabletServerLoad()
                .setServerId(5)
                .setNumReplicas(3)
                .setInSyncReplicas(1)
                .setNumLeaderReplicas(2)
                .setActiveLeaderReplicas(1)
                .setLeaderlessReplicas(1);
        response.addTabletServerLoad()
                .setServerId(2)
                .setNumReplicas(0)
                .setInSyncReplicas(0)
                .setNumLeaderReplicas(0)
                .setActiveLeaderReplicas(0)
                .setLeaderlessReplicas(0);

        ClusterDescription description = ClientRpcMessageUtils.toClusterDescription(response);

        assertThat(description.getCoordinatorServer())
                .contains(new ServerNode(0, "coordinator", 9123, ServerType.COORDINATOR));
        assertThat(description.getTabletServers().keySet()).containsExactly(0, 2, 3);
        ServerNode taggedNode = description.getTabletServers().get(2);
        assertThat(taggedNode.host()).isEqualTo("ts-2");
        assertThat(taggedNode.port()).isEqualTo(9125);
        assertThat(taggedNode.rack()).isEqualTo("rack-2");
        assertThat(taggedNode.serverTag()).isEqualTo(ServerTag.PERMANENT_OFFLINE);
        assertThat(description.getTabletServers().get(0).serverTag()).isNull();
        assertThat(description.getTabletServers().get(3).serverTag()).isNull();
        assertThat(description.getTabletServerLoads().keySet()).containsExactly(2, 5);
        assertThat(description.getTabletServerLoads().get(2))
                .isEqualTo(new TabletServerDescription(2, 0, 0, 0, 0, 0));
        assertThat(description.getTabletServerLoads().get(5))
                .isEqualTo(new TabletServerDescription(5, 3, 1, 2, 1, 1));
        assertThat(description.getTabletServerLoads()).doesNotContainKey(0);
    }

    @Test
    void testMissingStatisticsAreNotZeroLoads() {
        DescribeClusterResponse response = new DescribeClusterResponse();
        response.addTabletServer().setNodeId(1).setHost("ts-1").setPort(9124);

        ClusterDescription description = ClientRpcMessageUtils.toClusterDescription(response);

        assertThat(description.getCoordinatorServer()).isEmpty();
        assertThat(description.getTabletServers()).containsOnlyKeys(1);
        assertThat(description.getTabletServerLoads()).isEmpty();
        assertThat(description.getTabletServerLoads().get(1)).isNull();
    }

    @Test
    void testImmutableSnapshot() {
        Map<Integer, ServerNode> nodes = new HashMap<>();
        nodes.put(1, new ServerNode(1, "ts-1", 9124, ServerType.TABLET_SERVER));
        Map<Integer, TabletServerDescription> loads = new HashMap<>();
        loads.put(1, new TabletServerDescription(1, 0, 0, 0, 0, 0));
        ClusterDescription description = new ClusterDescription(null, nodes, loads);
        ClusterDescription sameDescription = new ClusterDescription(null, nodes, loads);

        nodes.clear();
        loads.clear();

        assertThat(description.getTabletServers()).hasSize(1);
        assertThat(description.getTabletServerLoads()).hasSize(1);
        assertThatThrownBy(() -> description.getTabletServers().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> description.getTabletServerLoads().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(description).isEqualTo(sameDescription).hasSameHashCodeAs(sameDescription);
    }

    @Test
    void testEqualityIncludesServerTags() {
        ServerNode node = new ServerNode(1, "ts-1", 9124, ServerType.TABLET_SERVER);
        ServerNode taggedNode =
                new ServerNode(
                        1,
                        "ts-1",
                        9124,
                        ServerType.TABLET_SERVER,
                        null,
                        ServerTag.PERMANENT_OFFLINE);
        ClusterDescription description =
                new ClusterDescription(
                        null, Collections.singletonMap(1, node), Collections.emptyMap());
        ClusterDescription taggedDescription =
                new ClusterDescription(
                        null, Collections.singletonMap(1, taggedNode), Collections.emptyMap());

        assertThat(description).isNotEqualTo(taggedDescription);
    }
}

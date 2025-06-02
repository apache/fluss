/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.utils;

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.metrics.TestingClientMetricGroup;
import com.alibaba.fluss.rpc.netty.client.NettyClient;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link com.alibaba.fluss.server.utils.RpcGatewayManager}. */
class RpcGatewayManagerTest {

    @Test
    void testRpcGatewayManage() throws Exception {
        RpcGatewayManager<TabletServerGateway> gatewayRpcGatewayManager =
                new RpcGatewayManager<>(
                        new NettyClient(
                                new Configuration(), TestingClientMetricGroup.newInstance()),
                        TabletServerGateway.class);

        ServerNode serverNode1 =
                new ServerNode(1, "localhost", 1234, ServerType.TABLET_SERVER, "rack1");
        // should be empty at the beginning
        assertThat(gatewayRpcGatewayManager.getRpcGateway(serverNode1.id())).isEmpty();
        gatewayRpcGatewayManager.addServer(serverNode1);
        // shouldn't be empty then
        assertThat(gatewayRpcGatewayManager.getRpcGateway(serverNode1.id())).isPresent();

        // add the server again
        gatewayRpcGatewayManager.addServer(serverNode1);
        assertThat(gatewayRpcGatewayManager.getRpcGateway(serverNode1.id())).isPresent();

        // add another server2
        ServerNode serverNode2 =
                new ServerNode(2, "localhost", 1234, ServerType.TABLET_SERVER, "rack2");
        gatewayRpcGatewayManager.addServer(serverNode2);
        assertThat(gatewayRpcGatewayManager.getRpcGateway(serverNode2.id())).isPresent();

        // test remove
        gatewayRpcGatewayManager.removeServer(serverNode1.id());
        assertThat(gatewayRpcGatewayManager.getRpcGateway(serverNode1.id())).isEmpty();
        assertThat(gatewayRpcGatewayManager.getRpcGateway(serverNode2.id())).isPresent();
        // remove server2
        gatewayRpcGatewayManager.removeServer(serverNode2.id());
        assertThat(gatewayRpcGatewayManager.getRpcGateway(serverNode2.id())).isEmpty();

        gatewayRpcGatewayManager.close();
    }
}

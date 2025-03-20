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

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.messages.UpdateMetadataRequest;
import com.alibaba.fluss.rpc.metrics.TestingClientMetricGroup;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.server.zk.ZooKeeperExtension;
import com.alibaba.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.fluss.server.utils.RpcMessageUtils.makeUpdateMetadataRequest;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Test for {@link CoordinatorChannelManager} . */
class CoordinatorChannelManagerTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(2).build();

    @Test
    void testCoordinatorChannelManager() throws Exception {
        Configuration configuration = new Configuration();
        CoordinatorChannelManager coordinatorChannelManager =
                new CoordinatorChannelManager(
                        RpcClient.create(configuration, TestingClientMetricGroup.newInstance()));
        List<ServerNode> tabletServersNode = FLUSS_CLUSTER_EXTENSION.getTabletServerNodes();

        // test start up using server 0
        ServerNode server0 = tabletServersNode.get(0);
        coordinatorChannelManager.startup(Collections.singletonList(server0));
        // try to send message, should send
        checkSendRequest(coordinatorChannelManager, server0.id(), true);

        // test remove tablet server
        coordinatorChannelManager.removeTabletServer(server0.id());
        // now, shouldn't send as we already remove the tablet server
        checkSendRequest(coordinatorChannelManager, server0.id(), false);

        // test add tablet server
        // before add, shouldn't send
        ServerNode server1 = tabletServersNode.get(1);
        checkSendRequest(coordinatorChannelManager, server1.id(), false);

        coordinatorChannelManager.addTabletServer(server1);

        // after add the tablet server, should send
        // try to send message
        checkSendRequest(coordinatorChannelManager, server1.id(), true);

        coordinatorChannelManager.close();
    }

    private void checkSendRequest(
            CoordinatorChannelManager coordinatorChannelManager,
            int targetServerId,
            boolean expectCanSend) {
        // 0 represents not send, 1 represents prepare to send, 2 represents success(received the
        // success response)
        AtomicInteger sendFlag = new AtomicInteger(0);
        // we use update metadata request to test for simplicity
        UpdateMetadataRequest updateMetadataRequest =
                makeUpdateMetadataRequest(Optional.empty(), Collections.emptySet());
        coordinatorChannelManager.sendRequest(
                targetServerId,
                updateMetadataRequest,
                // when
                (gateway, request) -> {
                    // sending... set to 1
                    sendFlag.set(1);
                    return gateway.updateMetadata(request);
                },
                (response, throwable) -> {
                    // receive response, set to 2
                    sendFlag.set(2);
                });

        // if expect can send, flag is 2;
        // otherwise, flag is 0
        int expectedFlag = expectCanSend ? 2 : 0;
        retry(Duration.ofMinutes(1), () -> assertThat(sendFlag.get()).isEqualTo(expectedFlag));
    }
}

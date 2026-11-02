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

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeUpdateMetadataRequest;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
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
                        RpcClient.create(configuration, TestingClientMetricGroup.newInstance()),
                        () -> 0,
                        configuration,
                        TestingMetricGroups.COORDINATOR_METRICS);
        List<ServerNode> tabletServersNode = FLUSS_CLUSTER_EXTENSION.getTabletServerNodes();

        // test start up using server 0
        ServerNode server0 = tabletServersNode.get(0);
        coordinatorChannelManager.startup(Collections.singletonList(server0));
        // try to send message, should send
        checkSendRequest(coordinatorChannelManager, server0.id(), true);
        checkEnqueueRequest(coordinatorChannelManager, server0.id(), true);

        // test remove tablet server
        coordinatorChannelManager.removeTabletServer(server0.id());
        // now, shouldn't send as we already remove the tablet server
        checkSendRequest(coordinatorChannelManager, server0.id(), false);
        checkEnqueueRequest(coordinatorChannelManager, server0.id(), false);

        // test add tablet server
        // before add, shouldn't send
        ServerNode server1 = tabletServersNode.get(1);
        checkSendRequest(coordinatorChannelManager, server1.id(), false);

        coordinatorChannelManager.addTabletServer(server1);

        // after add the tablet server, should send
        // try to send message
        checkSendRequest(coordinatorChannelManager, server1.id(), true);
        checkEnqueueRequest(coordinatorChannelManager, server1.id(), true);

        checkSynchronousSendFailure(coordinatorChannelManager, server1.id());

        coordinatorChannelManager.close();
    }

    private void checkEnqueueRequest(
            CoordinatorChannelManager coordinatorChannelManager,
            int targetServerId,
            boolean expectCanEnqueue) {
        AtomicInteger sendFlag = new AtomicInteger(0);
        boolean enqueued =
                coordinatorChannelManager.enqueueRequest(
                        targetServerId,
                        ApiKeys.API_VERSIONS,
                        0,
                        ignored -> {
                            sendFlag.set(1);
                            return CompletableFuture.completedFuture(new ApiVersionsResponse());
                        },
                        (response, throwable) -> sendFlag.set(2));

        assertThat(enqueued).isEqualTo(expectCanEnqueue);
        int expectedFlag = expectCanEnqueue ? 2 : 0;
        retry(Duration.ofMinutes(1), () -> assertThat(sendFlag.get()).isEqualTo(expectedFlag));
    }

    private void checkSendRequest(
            CoordinatorChannelManager coordinatorChannelManager,
            int targetServerId,
            boolean expectCanSend) {
        AtomicInteger requestInvocations = new AtomicInteger();
        AtomicInteger callbackInvocations = new AtomicInteger();
        AtomicReference<Object> responseRef = new AtomicReference<>();
        AtomicReference<Throwable> failureRef = new AtomicReference<>();
        // we use update metadata request to test for simplicity
        UpdateMetadataRequest updateMetadataRequest =
                makeUpdateMetadataRequest(
                        null,
                        null,
                        Collections.emptySet(),
                        Collections.emptyList(),
                        Collections.emptyList());
        coordinatorChannelManager.sendRequest(
                targetServerId,
                updateMetadataRequest,
                // when
                (gateway, request) -> {
                    requestInvocations.incrementAndGet();
                    return gateway.updateMetadata(request);
                },
                (response, throwable) -> {
                    callbackInvocations.incrementAndGet();
                    responseRef.set(response);
                    failureRef.set(throwable);
                });

        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(requestInvocations.get()).isEqualTo(expectCanSend ? 1 : 0);
                    assertThat(callbackInvocations.get()).isEqualTo(1);
                    if (expectCanSend) {
                        assertThat(responseRef.get()).isNotNull();
                        assertThat(failureRef.get()).isNull();
                    } else {
                        assertThat(responseRef.get()).isNull();
                        assertThat(failureRef.get()).isInstanceOf(IllegalStateException.class);
                    }
                });
    }

    private void checkSynchronousSendFailure(
            CoordinatorChannelManager coordinatorChannelManager, int targetServerId) {
        AtomicInteger requestInvocations = new AtomicInteger();
        AtomicInteger callbackInvocations = new AtomicInteger();
        AtomicReference<Object> responseRef = new AtomicReference<>();
        AtomicReference<Throwable> failureRef = new AtomicReference<>();
        IllegalStateException expectedFailure = new IllegalStateException("test send failure");
        UpdateMetadataRequest updateMetadataRequest =
                makeUpdateMetadataRequest(
                        null,
                        null,
                        Collections.emptySet(),
                        Collections.emptyList(),
                        Collections.emptyList());

        coordinatorChannelManager.sendRequest(
                targetServerId,
                updateMetadataRequest,
                (gateway, request) -> {
                    requestInvocations.incrementAndGet();
                    throw expectedFailure;
                },
                (response, throwable) -> {
                    callbackInvocations.incrementAndGet();
                    responseRef.set(response);
                    failureRef.set(throwable);
                });

        assertThat(requestInvocations.get()).isEqualTo(1);
        assertThat(callbackInvocations.get()).isEqualTo(1);
        assertThat(responseRef.get()).isNull();
        assertThat(failureRef.get()).isSameAs(expectedFailure);
    }
}

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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.util.TestMetricGroup;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.messages.NotifyKvSnapshotOffsetRequest;
import org.apache.fluss.rpc.messages.NotifyLakeTableOffsetRequest;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.NotifyRemoteLogOffsetsRequest;
import org.apache.fluss.rpc.messages.StopReplicaRequest;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongConsumer;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeUpdateMetadataRequest;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

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

        coordinatorChannelManager.close();
    }

    @Test
    void testControlPlaneRequestsAreEnqueuedWithTheirCoordinatorEpoch() throws Exception {
        Configuration configuration = new Configuration();
        try (RpcClient rpcClient =
                RpcClient.create(configuration, TestingClientMetricGroup.newInstance())) {
            RecordingCoordinatorChannelManager channelManager =
                    new RecordingCoordinatorChannelManager(rpcClient, configuration);
            int serverId = 1;
            int coordinatorEpoch = 42;
            long tabletServerEpoch = 84L;
            channelManager.setTabletServerEpoch(tabletServerEpoch);
            NotifyLeaderAndIsrRequest notifyLeaderAndIsrRequest =
                    new NotifyLeaderAndIsrRequest().setCoordinatorEpoch(coordinatorEpoch);
            StopReplicaRequest stopReplicaRequest =
                    new StopReplicaRequest().setCoordinatorEpoch(coordinatorEpoch);
            UpdateMetadataRequest updateMetadataRequest =
                    new UpdateMetadataRequest().setCoordinatorEpoch(coordinatorEpoch);

            channelManager.sendBucketLeaderAndIsrRequest(
                    serverId, notifyLeaderAndIsrRequest, (response, throwable) -> {});
            channelManager.sendStopBucketReplicaRequest(
                    serverId, stopReplicaRequest, (response, throwable) -> {});
            channelManager.sendUpdateMetadataRequest(
                    serverId, updateMetadataRequest, (response, throwable) -> {});
            channelManager.sendNotifyRemoteLogOffsetsRequest(
                    serverId,
                    new NotifyRemoteLogOffsetsRequest().setCoordinatorEpoch(coordinatorEpoch),
                    (response, throwable) -> {});
            channelManager.sendNotifyKvSnapshotOffsetRequest(
                    serverId,
                    new NotifyKvSnapshotOffsetRequest().setCoordinatorEpoch(coordinatorEpoch),
                    (response, throwable) -> {});
            channelManager.sendNotifyLakeTableOffsetRequest(
                    serverId,
                    new NotifyLakeTableOffsetRequest().setCoordinatorEpoch(coordinatorEpoch),
                    (response, throwable) -> {});

            assertThat(channelManager.getTargetServerIds())
                    .containsExactly(serverId, serverId, serverId, serverId, serverId, serverId);
            assertThat(channelManager.getApiKeys())
                    .containsExactly(
                            ApiKeys.NOTIFY_LEADER_AND_ISR,
                            ApiKeys.STOP_REPLICA,
                            ApiKeys.UPDATE_METADATA,
                            ApiKeys.NOTIFY_REMOTE_LOG_OFFSETS,
                            ApiKeys.NOTIFY_KV_SNAPSHOT_OFFSET,
                            ApiKeys.NOTIFY_LAKE_TABLE_OFFSET);
            assertThat(channelManager.getCoordinatorEpochs())
                    .containsExactlyElementsOf(
                            Arrays.asList(
                                    coordinatorEpoch,
                                    coordinatorEpoch,
                                    coordinatorEpoch,
                                    coordinatorEpoch,
                                    coordinatorEpoch,
                                    coordinatorEpoch));
            assertThat(channelManager.getTabletServerEpochs())
                    .containsExactly(tabletServerEpoch, tabletServerEpoch, tabletServerEpoch);
            assertThat(notifyLeaderAndIsrRequest.getTabletServerEpoch())
                    .isEqualTo(tabletServerEpoch);
            assertThat(stopReplicaRequest.getTabletServerEpoch()).isEqualTo(tabletServerEpoch);
            assertThat(updateMetadataRequest.getTabletServerEpoch()).isEqualTo(tabletServerEpoch);
        }
    }

    @Test
    void testQueuedRequestsResumeInOrderAfterTransientTabletServerOutage() throws Exception {
        int serverId = 1;
        ServerNode initialServerNode = getTabletServerNode(serverId);
        Configuration configuration = controlRequestTestConfiguration();
        TestMetricGroup metricGroup = TestMetricGroup.createTestMetricGroup();
        RpcClient rpcClient =
                RpcClient.create(configuration, TestingClientMetricGroup.newInstance());
        CoordinatorChannelManager channelManager =
                new CoordinatorChannelManager(rpcClient, () -> 0, configuration, metricGroup);
        boolean tabletServerRunning = true;
        try {
            channelManager.startup(Collections.singletonList(initialServerNode));
            FLUSS_CLUSTER_EXTENSION.stopTabletServer(serverId);
            tabletServerRunning = false;

            CountDownLatch callbackLatch = new CountDownLatch(2);
            AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
            List<Integer> callbackOrder = new ArrayList<>();
            channelManager.sendUpdateMetadataRequest(
                    serverId,
                    makeUpdateMetadataRequest(
                            null,
                            0,
                            Collections.emptySet(),
                            Collections.emptyList(),
                            Collections.emptyList()),
                    (response, failure) -> {
                        callbackOrder.add(1);
                        callbackFailure.compareAndSet(null, failure);
                        callbackLatch.countDown();
                    });
            channelManager.sendUpdateMetadataRequest(
                    serverId,
                    makeUpdateMetadataRequest(
                            null,
                            0,
                            Collections.emptySet(),
                            Collections.emptyList(),
                            Collections.emptyList()),
                    (response, failure) -> {
                        callbackOrder.add(2);
                        callbackFailure.compareAndSet(null, failure);
                        callbackLatch.countDown();
                    });

            waitUntil(
                    () -> getRetryCount(metricGroup) > 0,
                    Duration.ofSeconds(10),
                    "The queue head was not retried while the tablet server was down");
            assertThat(callbackLatch.getCount()).isEqualTo(2);

            FLUSS_CLUSTER_EXTENSION.startTabletServer(serverId);
            tabletServerRunning = true;
            channelManager.addTabletServer(getTabletServerNode(serverId));

            assertThat(callbackLatch.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(callbackFailure.get()).isNull();
            assertThat(callbackOrder).containsExactly(1, 2);
        } finally {
            if (!tabletServerRunning) {
                FLUSS_CLUSTER_EXTENSION.startTabletServer(serverId);
            }
            channelManager.close();
            rpcClient.close();
        }
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
        // 0 represents not sent, 2 represents success (received the success response).
        AtomicInteger sendFlag = new AtomicInteger(0);
        // we use update metadata request to test for simplicity
        UpdateMetadataRequest updateMetadataRequest =
                makeUpdateMetadataRequest(
                        null,
                        0,
                        Collections.emptySet(),
                        Collections.emptyList(),
                        Collections.emptyList());
        coordinatorChannelManager.sendUpdateMetadataRequest(
                targetServerId,
                updateMetadataRequest,
                (response, throwable) -> {
                    // receive response, set to 2
                    sendFlag.set(2);
                });

        // if expect can send, flag is 2;
        // otherwise, flag is 0
        int expectedFlag = expectCanSend ? 2 : 0;
        retry(Duration.ofMinutes(1), () -> assertThat(sendFlag.get()).isEqualTo(expectedFlag));
    }

    private static Configuration controlRequestTestConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(
                ConfigOptions.COORDINATOR_CONTROL_REQUEST_RETRY_BACKOFF, Duration.ofMillis(10));
        configuration.set(
                ConfigOptions.COORDINATOR_CONTROL_REQUEST_TIMEOUT, Duration.ofMillis(100));
        return configuration;
    }

    private static ServerNode getTabletServerNode(int serverId) {
        return FLUSS_CLUSTER_EXTENSION.getTabletServerNodes().stream()
                .filter(serverNode -> serverNode.id() == serverId)
                .findFirst()
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "Tablet server " + serverId + " is not running"));
    }

    private static long getRetryCount(TestMetricGroup metricGroup) {
        return ((Counter) metricGroup.getMetric(MetricNames.SENDER_RETRY_COUNT)).getCount();
    }

    private static final class RecordingCoordinatorChannelManager
            extends CoordinatorChannelManager {
        private final List<Integer> targetServerIds = new ArrayList<>();
        private final List<ApiKeys> apiKeys = new ArrayList<>();
        private final List<Integer> coordinatorEpochs = new ArrayList<>();
        private final List<Long> tabletServerEpochs = new ArrayList<>();
        private long tabletServerEpoch;

        private RecordingCoordinatorChannelManager(
                RpcClient rpcClient, Configuration configuration) {
            super(rpcClient, () -> 0, configuration, TestingMetricGroups.COORDINATOR_METRICS);
        }

        @Override
        protected <ResponseT extends ApiMessage> boolean enqueueRequest(
                int targetServerId,
                ApiKeys apiKey,
                int coordinatorEpoch,
                Function<TabletServerGateway, CompletableFuture<ResponseT>> requestSender,
                BiConsumer<ResponseT, ? super Throwable> responseConsumer) {
            recordRequest(targetServerId, apiKey, coordinatorEpoch);
            return true;
        }

        @Override
        protected <ResponseT extends ApiMessage> boolean enqueueRequest(
                int targetServerId,
                ApiKeys apiKey,
                int coordinatorEpoch,
                LongConsumer tabletServerEpochSetter,
                Function<TabletServerGateway, CompletableFuture<ResponseT>> requestSender,
                BiConsumer<ResponseT, ? super Throwable> responseConsumer) {
            recordRequest(targetServerId, apiKey, coordinatorEpoch);
            tabletServerEpochSetter.accept(tabletServerEpoch);
            tabletServerEpochs.add(tabletServerEpoch);
            return true;
        }

        private void recordRequest(int targetServerId, ApiKeys apiKey, int coordinatorEpoch) {
            targetServerIds.add(targetServerId);
            apiKeys.add(apiKey);
            coordinatorEpochs.add(coordinatorEpoch);
        }

        private void setTabletServerEpoch(long tabletServerEpoch) {
            this.tabletServerEpoch = tabletServerEpoch;
        }

        private List<Integer> getTargetServerIds() {
            return targetServerIds;
        }

        private List<ApiKeys> getApiKeys() {
            return apiKeys;
        }

        private List<Integer> getCoordinatorEpochs() {
            return coordinatorEpochs;
        }

        private List<Long> getTabletServerEpochs() {
            return tabletServerEpochs;
        }
    }
}

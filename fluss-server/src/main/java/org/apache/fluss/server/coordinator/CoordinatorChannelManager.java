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
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.NotifyKvSnapshotOffsetRequest;
import org.apache.fluss.rpc.messages.NotifyKvSnapshotOffsetResponse;
import org.apache.fluss.rpc.messages.NotifyLakeTableOffsetRequest;
import org.apache.fluss.rpc.messages.NotifyLakeTableOffsetResponse;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrResponse;
import org.apache.fluss.rpc.messages.NotifyRemoteLogOffsetsRequest;
import org.apache.fluss.rpc.messages.NotifyRemoteLogOffsetsResponse;
import org.apache.fluss.rpc.messages.StopReplicaRequest;
import org.apache.fluss.rpc.messages.StopReplicaResponse;
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.rpc.messages.UpdateMetadataResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.server.coordinator.channel.ControlRequestSendThread;
import org.apache.fluss.server.coordinator.channel.QueueItem;
import org.apache.fluss.server.coordinator.channel.TabletServerChannelState;
import org.apache.fluss.server.utils.RpcGatewayManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongConsumer;

import static org.apache.fluss.server.metadata.ServerInfo.UNKNOWN_TABLET_SERVER_EPOCH;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Used by the coordinator server to manage RPC channels to tablet servers and send requests.
 * Mutations are guarded by {@code channelLock} so the metric reporter thread can safely read queue
 * sizes. Mirrors Kafka's {@code ControllerChannelManager} which uses a {@code brokerLock} for the
 * same purpose.
 */
public class CoordinatorChannelManager {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorChannelManager.class);

    /** A manager for the rpc gateways to tablet servers. */
    private final RpcGatewayManager<TabletServerGateway> rpcGatewayManager;

    private final IntSupplier epochSupplier;
    private final Configuration conf;
    private final MetricGroup coordinatorMetricGroup;

    private final Object channelLock = new Object();
    private final Map<Integer, TabletServerChannelState> channelStates = new HashMap<>();

    public CoordinatorChannelManager(
            RpcClient rpcClient,
            IntSupplier epochSupplier,
            Configuration conf,
            MetricGroup coordinatorMetricGroup) {
        this.rpcGatewayManager = new RpcGatewayManager<>(rpcClient, TabletServerGateway.class);
        this.epochSupplier = epochSupplier;
        this.conf = conf;
        this.coordinatorMetricGroup = coordinatorMetricGroup;
    }

    public void startup(Collection<ServerNode> serverNodes) {
        startup(serverNodes, Collections.emptyMap());
    }

    /** Starts channels for the currently registered tablet server incarnations. */
    public void startup(Collection<ServerNode> serverNodes, Map<Integer, Long> tabletServerEpochs) {
        for (ServerNode serverNode : serverNodes) {
            addNewTabletServer(
                    serverNode,
                    tabletServerEpochs.getOrDefault(serverNode.id(), UNKNOWN_TABLET_SERVER_EPOCH));
        }
        synchronized (channelLock) {
            for (TabletServerChannelState state : channelStates.values()) {
                startSendThread(state);
            }
        }
    }

    public void close() throws Exception {
        shutdown();
        rpcGatewayManager.close();
    }

    /** Adds a tablet server and immediately starts its sender thread (runtime addition). */
    public void addTabletServer(ServerNode serverNode) {
        addTabletServer(serverNode, UNKNOWN_TABLET_SERVER_EPOCH);
    }

    /** Adds one tablet server incarnation and immediately starts its sender thread. */
    public void addTabletServer(ServerNode serverNode, long tabletServerEpoch) {
        addNewTabletServer(serverNode, tabletServerEpoch);
        synchronized (channelLock) {
            TabletServerChannelState state = channelStates.get(serverNode.id());
            if (state != null) {
                startSendThread(state);
            }
        }
    }

    private void addNewTabletServer(ServerNode serverNode, long tabletServerEpoch) {
        checkState(
                serverNode.serverType().equals(ServerType.TABLET_SERVER),
                "The server type should be TABLET_SERVER, but was " + serverNode.serverType());

        rpcGatewayManager.addServer(serverNode);

        int id = serverNode.id();
        synchronized (channelLock) {
            if (channelStates.containsKey(id)) {
                return;
            }
            BlockingQueue<QueueItem<?>> queue = new LinkedBlockingQueue<>();

            MetricGroup tsGroup =
                    coordinatorMetricGroup.addGroup("tablet_server_id", String.valueOf(id));
            tsGroup.gauge(MetricNames.SENDER_QUEUE_SIZE, queue::size);

            ControlRequestSendThread thread =
                    new ControlRequestSendThread(
                            id,
                            queue,
                            () -> rpcGatewayManager.getRpcGateway(id),
                            () -> rpcGatewayManager.disconnectServer(id),
                            epochSupplier,
                            conf,
                            tsGroup);
            channelStates.put(
                    id, new TabletServerChannelState(queue, thread, tsGroup, tabletServerEpoch));
        }
    }

    private void startSendThread(TabletServerChannelState state) {
        ControlRequestSendThread thread = state.getSendThread();
        if (thread.getState() == Thread.State.NEW) {
            thread.start();
        }
    }

    public void removeTabletServer(Integer serverId) {
        TabletServerChannelState state;
        synchronized (channelLock) {
            state = channelStates.remove(serverId);
        }
        teardownChannelState(serverId, state);

        rpcGatewayManager
                .removeServer(serverId)
                .exceptionally(
                        throwable -> {
                            LOG.debug(
                                    "Failed to remove the server {} from server gateway manager.",
                                    serverId,
                                    throwable);
                            return null;
                        });
    }

    /** Shuts down all per-tablet-server sender threads and deregisters their metrics. */
    public void shutdown() {
        Map<Integer, TabletServerChannelState> statesToTeardown;
        synchronized (channelLock) {
            statesToTeardown = new HashMap<>(channelStates);
            channelStates.clear();
        }
        for (Map.Entry<Integer, TabletServerChannelState> entry : statesToTeardown.entrySet()) {
            teardownChannelState(entry.getKey(), entry.getValue());
        }
    }

    private void teardownChannelState(int serverId, @Nullable TabletServerChannelState state) {
        if (state == null) {
            return;
        }
        try {
            try {
                state.getSendThread().shutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn(
                        "Interrupted while shutting down sender thread for tabletServer {}",
                        serverId,
                        e);
            }
            state.getQueue().clear();
            state.getMetricGroup().close();
        } catch (Throwable t) {
            LOG.error("Error tearing down channel state for tabletServer {}", serverId, t);
        }
    }

    /** Enqueue a NotifyLeaderAndIsr request and handle its response. */
    public void sendBucketLeaderAndIsrRequest(
            int receiveServerId,
            NotifyLeaderAndIsrRequest notifyLeaderAndIsrRequest,
            BiConsumer<NotifyLeaderAndIsrResponse, ? super Throwable> responseConsumer) {
        enqueueRequest(
                receiveServerId,
                ApiKeys.NOTIFY_LEADER_AND_ISR,
                notifyLeaderAndIsrRequest.getCoordinatorEpoch(),
                notifyLeaderAndIsrRequest::setTabletServerEpoch,
                gateway -> gateway.notifyLeaderAndIsr(notifyLeaderAndIsrRequest),
                responseConsumer);
    }

    /** Enqueue a StopReplica request and handle its response. */
    public void sendStopBucketReplicaRequest(
            int receiveServerId,
            StopReplicaRequest stopReplicaRequest,
            BiConsumer<StopReplicaResponse, ? super Throwable> responseConsumer) {
        enqueueRequest(
                receiveServerId,
                ApiKeys.STOP_REPLICA,
                stopReplicaRequest.getCoordinatorEpoch(),
                stopReplicaRequest::setTabletServerEpoch,
                gateway -> gateway.stopReplica(stopReplicaRequest),
                responseConsumer);
    }

    /**
     * Enqueues a control-plane request for the target tablet server. The per-tablet-server sender
     * retries transport failures and timeouts until the request succeeds or the channel is removed.
     * Other failures are completed through the response consumer without retrying.
     *
     * @return whether the request was enqueued
     */
    protected <ResponseT extends ApiMessage> boolean enqueueRequest(
            int targetServerId,
            ApiKeys apiKey,
            int coordinatorEpoch,
            Function<TabletServerGateway, CompletableFuture<ResponseT>> requestSender,
            @Nullable BiConsumer<ResponseT, ? super Throwable> responseConsumer) {
        return enqueueRequest(
                targetServerId, apiKey, coordinatorEpoch, null, requestSender, responseConsumer);
    }

    /** Enqueues a control request and binds it to the current tablet server incarnation. */
    protected <ResponseT extends ApiMessage> boolean enqueueRequest(
            int targetServerId,
            ApiKeys apiKey,
            int coordinatorEpoch,
            @Nullable LongConsumer tabletServerEpochSetter,
            Function<TabletServerGateway, CompletableFuture<ResponseT>> requestSender,
            @Nullable BiConsumer<ResponseT, ? super Throwable> responseConsumer) {
        synchronized (channelLock) {
            TabletServerChannelState state = channelStates.get(targetServerId);
            if (state == null) {
                LOG.warn(
                        "Cannot enqueue {} for tablet server {} because its channel does not exist.",
                        apiKey,
                        targetServerId);
                return false;
            }

            long tabletServerEpoch = state.getTabletServerEpoch();
            if (tabletServerEpochSetter != null
                    && tabletServerEpoch != UNKNOWN_TABLET_SERVER_EPOCH) {
                tabletServerEpochSetter.accept(tabletServerEpoch);
            }

            try {
                state.getQueue()
                        .put(
                                new QueueItem<>(
                                        apiKey,
                                        requestSender,
                                        responseConsumer,
                                        coordinatorEpoch,
                                        System.currentTimeMillis()));
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn(
                        "Interrupted while enqueueing {} for tablet server {}",
                        apiKey,
                        targetServerId);
                return false;
            }
        }
    }

    /** Enqueue an UpdateMetadata request and handle its response. */
    public void sendUpdateMetadataRequest(
            int receiveServerId,
            UpdateMetadataRequest updateMetadataRequest,
            BiConsumer<UpdateMetadataResponse, ? super Throwable> responseConsumer) {
        enqueueRequest(
                receiveServerId,
                ApiKeys.UPDATE_METADATA,
                updateMetadataRequest.getCoordinatorEpoch(),
                updateMetadataRequest::setTabletServerEpoch,
                gateway -> gateway.updateMetadata(updateMetadataRequest),
                responseConsumer);
    }

    /** Enqueue a NotifyRemoteLogOffsets request and handle its response. */
    public void sendNotifyRemoteLogOffsetsRequest(
            int receiveServerId,
            NotifyRemoteLogOffsetsRequest notifyRemoteLogOffsetsRequest,
            BiConsumer<NotifyRemoteLogOffsetsResponse, ? super Throwable> responseConsumer) {
        enqueueRequest(
                receiveServerId,
                ApiKeys.NOTIFY_REMOTE_LOG_OFFSETS,
                notifyRemoteLogOffsetsRequest.getCoordinatorEpoch(),
                gateway -> gateway.notifyRemoteLogOffsets(notifyRemoteLogOffsetsRequest),
                responseConsumer);
    }

    /** Enqueue a NotifyKvSnapshotOffset request and handle its response. */
    public void sendNotifyKvSnapshotOffsetRequest(
            int receiveServerId,
            NotifyKvSnapshotOffsetRequest notifySnapshotOffsetRequest,
            BiConsumer<NotifyKvSnapshotOffsetResponse, ? super Throwable> responseConsumer) {
        enqueueRequest(
                receiveServerId,
                ApiKeys.NOTIFY_KV_SNAPSHOT_OFFSET,
                notifySnapshotOffsetRequest.getCoordinatorEpoch(),
                gateway -> gateway.notifyKvSnapshotOffset(notifySnapshotOffsetRequest),
                responseConsumer);
    }

    /** Enqueue a NotifyLakeTableOffset request and handle its response. */
    public void sendNotifyLakeTableOffsetRequest(
            int receiveServerId,
            NotifyLakeTableOffsetRequest notifyLakeTableOffsetRequest,
            BiConsumer<NotifyLakeTableOffsetResponse, ? super Throwable> responseConsumer) {
        enqueueRequest(
                receiveServerId,
                ApiKeys.NOTIFY_LAKE_TABLE_OFFSET,
                notifyLakeTableOffsetRequest.getCoordinatorEpoch(),
                gateway -> gateway.notifyLakeTableOffset(notifyLakeTableOffsetRequest),
                responseConsumer);
    }
}

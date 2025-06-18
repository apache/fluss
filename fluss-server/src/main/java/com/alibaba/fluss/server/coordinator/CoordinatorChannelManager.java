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

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.ApiMessage;
import com.alibaba.fluss.rpc.messages.NotifyKvSnapshotOffsetRequest;
import com.alibaba.fluss.rpc.messages.NotifyKvSnapshotOffsetResponse;
import com.alibaba.fluss.rpc.messages.NotifyLakeTableOffsetRequest;
import com.alibaba.fluss.rpc.messages.NotifyLakeTableOffsetResponse;
import com.alibaba.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import com.alibaba.fluss.rpc.messages.NotifyLeaderAndIsrResponse;
import com.alibaba.fluss.rpc.messages.NotifyRemoteLogOffsetsRequest;
import com.alibaba.fluss.rpc.messages.NotifyRemoteLogOffsetsResponse;
import com.alibaba.fluss.rpc.messages.StopReplicaRequest;
import com.alibaba.fluss.rpc.messages.StopReplicaResponse;
import com.alibaba.fluss.rpc.messages.UpdateMetadataRequest;
import com.alibaba.fluss.rpc.messages.UpdateMetadataResponse;
import com.alibaba.fluss.server.utils.RpcGatewayManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import static com.alibaba.fluss.utils.Preconditions.checkState;

/**
 * Using by coordinator server. It's a manager to manage the rpc channels to tablet servers and send
 * request to the servers.
 */
@NotThreadSafe
public class CoordinatorChannelManager {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorChannelManager.class);

    /** A manager for the rpc gateways to tablet servers. */
    private final RpcGatewayManager<TabletServerGateway> rpcGatewayManager;

    public CoordinatorChannelManager(RpcClient rpcClient) {
        this.rpcGatewayManager = new RpcGatewayManager<>(rpcClient, TabletServerGateway.class);
    }

    public void startup(Collection<ServerNode> serverNodes) {
        for (ServerNode serverNode : serverNodes) {
            addTabletServer(serverNode);
        }
    }

    public void close() throws Exception {
        rpcGatewayManager.close();
    }

    public void addTabletServer(ServerNode serverNode) {
        // add new tablet server to the channel manager
        checkState(
                serverNode.serverType().equals(ServerType.TABLET_SERVER),
                "The server type should be TABLET_SERVER, but was " + serverNode.serverType());

        rpcGatewayManager.addServer(serverNode);
    }

    public void removeTabletServer(Integer serverId) {
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

    /** Send NotifyLeaderAndIsr request to the server and handle the response. */
    public void sendBucketLeaderAndIsrRequest(
            int receiveServerId,
            NotifyLeaderAndIsrRequest notifyLeaderAndIsrRequest,
            BiConsumer<NotifyLeaderAndIsrResponse, ? super Throwable> responseConsumer) {
        sendRequest(
                receiveServerId,
                notifyLeaderAndIsrRequest,
                TabletServerGateway::notifyLeaderAndIsr,
                responseConsumer);
    }

    /** Send StopBucketReplicaRequest to the server and handle the response. */
    public void sendStopBucketReplicaRequest(
            int receiveServerId,
            StopReplicaRequest stopReplicaRequest,
            BiConsumer<StopReplicaResponse, ? super Throwable> responseConsumer) {
        sendRequest(
                receiveServerId,
                stopReplicaRequest,
                TabletServerGateway::stopReplica,
                responseConsumer);
    }

    /** Send UpdateMetadataRequest to the server and handle the response. */
    public void sendUpdateMetadataRequest(
            int receiveServerId,
            UpdateMetadataRequest updateMetadataRequest,
            BiConsumer<UpdateMetadataResponse, ? super Throwable> responseConsumer) {
        sendRequest(
                receiveServerId,
                updateMetadataRequest,
                TabletServerGateway::updateMetadata,
                responseConsumer);
    }

    /** Send NotifyRemoteLogOffsetsRequest to the server and handle the response. */
    public void sendNotifyRemoteLogOffsetsRequest(
            int receiveServerId,
            NotifyRemoteLogOffsetsRequest notifyRemoteLogOffsetsRequest,
            BiConsumer<NotifyRemoteLogOffsetsResponse, ? super Throwable> responseConsumer) {
        sendRequest(
                receiveServerId,
                notifyRemoteLogOffsetsRequest,
                TabletServerGateway::notifyRemoteLogOffsets,
                responseConsumer);
    }

    /** Send NotifyKvSnapshotOffsetRequest to the server and handle the response. */
    public void sendNotifyKvSnapshotOffsetRequest(
            int receiveServerId,
            NotifyKvSnapshotOffsetRequest notifySnapshotOffsetRequest,
            BiConsumer<NotifyKvSnapshotOffsetResponse, ? super Throwable> responseConsumer) {
        sendRequest(
                receiveServerId,
                notifySnapshotOffsetRequest,
                TabletServerGateway::notifyKvSnapshotOffset,
                responseConsumer);
    }

    public void sendNotifyLakeTableOffsetRequest(
            int receiveServerId,
            NotifyLakeTableOffsetRequest notifyLakeTableOffsetRequest,
            BiConsumer<NotifyLakeTableOffsetResponse, ? super Throwable> responseConsumer) {
        sendRequest(
                receiveServerId,
                notifyLakeTableOffsetRequest,
                TabletServerGateway::notifyLakeTableOffset,
                responseConsumer);
    }

    @VisibleForTesting
    protected <Request extends ApiMessage, Response extends ApiMessage> void sendRequest(
            int targetServerId,
            Request request,
            RequestSendFunction<Request, Response> requestFunction,
            BiConsumer<Response, ? super Throwable> responseConsumer) {
        Optional<TabletServerGateway> optionalTabletServerGateway =
                getTabletServerGateway(targetServerId);
        if (!optionalTabletServerGateway.isPresent()) {
            LOG.warn(
                    "Can't not send {} to the tablet server {} as the server is offline.",
                    request.getClass().getSimpleName(),
                    targetServerId);
        } else {
            TabletServerGateway tabletServerGateway = optionalTabletServerGateway.get();
            requestFunction.apply(tabletServerGateway, request).whenComplete(responseConsumer);
        }
    }

    protected Optional<TabletServerGateway> getTabletServerGateway(int targetServerId) {
        return rpcGatewayManager.getRpcGateway(targetServerId);
    }

    /** A functional interface to send request via TabletServerGateway. */
    @VisibleForTesting
    @FunctionalInterface
    interface RequestSendFunction<RequestT extends ApiMessage, ResponseT extends ApiMessage> {
        CompletableFuture<ResponseT> apply(TabletServerGateway gateway, RequestT request);
    }
}

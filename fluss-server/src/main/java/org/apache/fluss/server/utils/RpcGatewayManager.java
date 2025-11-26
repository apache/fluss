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

package org.apache.fluss.server.utils;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.RpcGateway;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** A manager to manage the rpc gateways to the servers. */
@NotThreadSafe
public class RpcGatewayManager<T extends RpcGateway> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RpcGatewayManager.class);

    private final RpcClient rpcClient;
    private final Class<T> gatewayClass;

    /** A mapping from the server id to the Server Rpc gateway. */
    private final Map<Integer, ServerRpcGateway> serverRpcGateways;

    public RpcGatewayManager(RpcClient rpcClient, Class<T> gatewayClass) {
        this.rpcClient = rpcClient;
        this.gatewayClass = gatewayClass;
        this.serverRpcGateways = new HashMap<>();
    }

    /**
     * Get the rpc gateway of the server with the given id.
     *
     * @param serverUid the id of the server
     * @return the rpc gateway of the server, empty if the server doesn't exist
     */
    public Optional<T> getRpcGateway(int serverUid) {
        if (!serverRpcGateways.containsKey(serverUid)) {
            return Optional.empty();
        }
        return Optional.of(serverRpcGateways.get(serverUid).rpcGateway);
    }

    /**
     * Add a server to the manager. It'll create a new gateway for the server andadd it to the
     * manager. If the server has already existed, it'll remove the already existing server before
     * adding the new one.
     */
    public void addServer(ServerNode serverNode) {
        int serverId = serverNode.id();
        if (serverRpcGateways.containsKey(serverId)) {
            // close the already existing server
            removeServer(serverId)
                    .exceptionally(
                            throwable -> {
                                LOG.warn("Failed to close the server {}.", serverId, throwable);
                                return null;
                            });
        }

        // create a new gateway for the server
        T gateway =
                GatewayClientProxy.createGatewayProxy(() -> serverNode, rpcClient, gatewayClass);
        // put the gateway for the server
        serverRpcGateways.put(serverNode.id(), new ServerRpcGateway(serverNode.uid(), gateway));
    }

    /**
     * Remove the server with the given id from the manager. It'll disconnect from the server to be
     * removed.
     *
     * @param serverId the id of the server to be removed
     * @return a future to be completed when the disconnection is complete
     */
    public CompletableFuture<Void> removeServer(int serverId) {
        ServerRpcGateway serverRpcGateway = serverRpcGateways.remove(serverId);
        if (serverRpcGateway != null) {
            return rpcClient.disconnect(serverRpcGateway.serverUid);
        }
        return FutureUtils.completedVoidFuture();
    }

    @Override
    public void close() throws Exception {
        // do nothing.
    }

    private class ServerRpcGateway {
        private final String serverUid;
        private final T rpcGateway;

        public ServerRpcGateway(String serverUid, T rpcGateway) {
            this.serverUid = serverUid;
            this.rpcGateway = rpcGateway;
        }
    }
}

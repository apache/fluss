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

package org.apache.fluss.server.kv.snapshot;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.server.metadata.ServerMetadataCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeCommitKvSnapshotRequest;

/**
 * A default implementation of {@link CompletedKvSnapshotCommitter} which will send the completed
 * snapshot to coordinator server to have the coordinator server stored the completed snapshot.
 */
public class DefaultCompletedKvSnapshotCommitter implements CompletedKvSnapshotCommitter {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultCompletedKvSnapshotCommitter.class);

    private final CoordinatorGateway coordinatorGateway;

    public DefaultCompletedKvSnapshotCommitter(CoordinatorGateway coordinatorGateway) {
        this.coordinatorGateway = coordinatorGateway;
    }

    public static DefaultCompletedKvSnapshotCommitter create(
            RpcClient rpcClient, ServerMetadataCache metadataCache, String interListenerName) {
        CoordinatorServerSupplier coordinatorServerSupplier =
                new CoordinatorServerSupplier(metadataCache, interListenerName);
        return new DefaultCompletedKvSnapshotCommitter(
                GatewayClientProxy.createGatewayProxy(
                        coordinatorServerSupplier, rpcClient, CoordinatorGateway.class));
    }

    @Override
    public void commitKvSnapshot(
            CompletedSnapshot snapshot, int coordinatorEpoch, int bucketLeaderEpoch)
            throws Exception {
        coordinatorGateway
                .commitKvSnapshot(
                        makeCommitKvSnapshotRequest(snapshot, coordinatorEpoch, bucketLeaderEpoch))
                .get();
    }

    private static class CoordinatorServerSupplier implements Supplier<ServerNode> {

        private static final int BACK_OFF_MILLS = 500;

        private final ServerMetadataCache metadataCache;
        private final String interListenerName;

        public CoordinatorServerSupplier(
                ServerMetadataCache metadataCache, String interListenerName) {
            this.metadataCache = metadataCache;
            this.interListenerName = interListenerName;
        }

        @Override
        public ServerNode get() {
            ServerNode serverNode = metadataCache.getCoordinatorServer(interListenerName);
            if (serverNode == null) {
                LOG.info("No coordinator provided, retrying after backoff.");
                // backoff some times
                try {
                    Thread.sleep(BACK_OFF_MILLS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new FlussRuntimeException(
                            "The thread was interrupted while waiting coordinator providing.");
                }
                return get();
            }
            return serverNode;
        }
    }
}

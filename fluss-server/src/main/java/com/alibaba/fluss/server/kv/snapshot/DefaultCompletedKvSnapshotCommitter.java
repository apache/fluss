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

package com.alibaba.fluss.server.kv.snapshot;

import com.alibaba.fluss.cluster.MetadataCache;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.rpc.GatewayClientProxy;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.gateway.CoordinatorGateway;
import com.alibaba.fluss.server.utils.RpcMessageUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

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
            RpcClient rpcClient, MetadataCache metadataCache) {
        CoordinatorServerSupplier coordinatorServerSupplier =
                new CoordinatorServerSupplier(metadataCache);
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
                        RpcMessageUtils.makeCommitKvSnapshotRequest(
                                snapshot, coordinatorEpoch, bucketLeaderEpoch))
                .get();
    }

    private static class CoordinatorServerSupplier implements Supplier<ServerNode> {

        private static final int BACK_OFF_MILLS = 500;

        private final MetadataCache metadataCache;

        public CoordinatorServerSupplier(MetadataCache metadataCache) {
            this.metadataCache = metadataCache;
        }

        @Override
        public ServerNode get() {
            ServerNode serverNode = metadataCache.getCoordinatorServer();
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

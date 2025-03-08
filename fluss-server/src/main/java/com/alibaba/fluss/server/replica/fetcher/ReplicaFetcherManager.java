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

package com.alibaba.fluss.server.replica.fetcher;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.GatewayClientProxy;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.server.replica.ReplicaManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.fluss.utils.function.ThrowingConsumer.unchecked;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This manager will construct a remote {@link RemoteLeaderEndpoint} when creating a fetch thread,
 * which is used to communicate with the remote tablet server who contains leader replica.
 */
@ThreadSafe
public class ReplicaFetcherManager {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicaFetcherManager.class);

    // map of (source tablet_server_id, fetcher_id per source tablet server) => fetcher.
    @GuardedBy("lock")
    private final Map<ServerIdAndFetcherId, ReplicaFetcherThread> fetcherThreadMap =
            new HashMap<>();

    private final Configuration conf;
    private final RpcClient rpcClient;
    private final int serverId;
    private final ReplicaManager replicaManager;
    private final int numFetchersPerServer;
    private final Object lock = new Object();

    public ReplicaFetcherManager(
            Configuration conf, RpcClient rpcClient, int serverId, ReplicaManager replicaManager) {
        this.conf = conf;
        this.rpcClient = rpcClient;
        this.serverId = serverId;
        this.replicaManager = replicaManager;
        this.numFetchersPerServer = conf.getInt(ConfigOptions.LOG_REPLICA_FETCHER_NUMBER);
    }

    public void addFetcherForBuckets(Map<TableBucket, InitialFetchStatus> bucketAndStatus) {
        synchronized (lock) {
            // Group the table bucket and their initial fetch status by server id and fetcher id,
            // this can reuse the fetcher thread.
            Map<ServerAndFetcherId, Map<TableBucket, InitialFetchStatus>> replicasPerFetcher =
                    bucketAndStatus.entrySet().stream()
                            .collect(
                                    Collectors.groupingBy(
                                            entry ->
                                                    new ServerAndFetcherId(
                                                            entry.getValue().leader(),
                                                            getFetcherId(entry.getKey())),
                                            Collectors.toMap(
                                                    Map.Entry::getKey, Map.Entry::getValue)));

            replicasPerFetcher.forEach(
                    (serverAndFetcherId, initialFetchStatusMap) -> {
                        ServerIdAndFetcherId serverIdAndFetcherId =
                                new ServerIdAndFetcherId(
                                        serverAndFetcherId.serverNode.id(),
                                        serverAndFetcherId.fetcherId);
                        // default reuse the exists thread.
                        ReplicaFetcherThread fetcherThread =
                                fetcherThreadMap.get(serverIdAndFetcherId);
                        if (fetcherThread == null) {
                            fetcherThread =
                                    addAndStartFetcherThread(
                                            serverAndFetcherId, serverIdAndFetcherId);
                        } else if (!fetcherThread
                                .getLeader()
                                .leaderNode()
                                .equals(serverAndFetcherId.serverNode)) {
                            try {
                                fetcherThread.shutdown();
                            } catch (InterruptedException e) {
                                LOG.error("Interrupted while shutting down fetcher threads.", e);
                            }
                            fetcherThread =
                                    addAndStartFetcherThread(
                                            serverAndFetcherId, serverIdAndFetcherId);
                        }

                        // failed buckets are removed when added buckets to thread
                        addBucketsToFetcherThread(fetcherThread, initialFetchStatusMap);
                    });
        }
    }

    public void removeFetcherForBuckets(Set<TableBucket> tableBuckets) {
        synchronized (lock) {
            fetcherThreadMap
                    .values()
                    .forEach(
                            fetcher -> {
                                try {
                                    fetcher.removeBuckets(tableBuckets);
                                } catch (InterruptedException e) {
                                    LOG.error(
                                            "Interrupted while shutting down fetcher threads.", e);
                                }
                            });
        }

        if (!tableBuckets.isEmpty()) {
            LOG.info("Remove fetcher for buckets: {}", tableBuckets);
        }
    }

    public void shutdownIdleFetcherThreads() {
        synchronized (lock) {
            Set<ServerIdAndFetcherId> keysToBeRemoved = new HashSet<>();
            fetcherThreadMap.forEach(
                    (serverIdAndFetcherId, fetcher) -> {
                        if (fetcher.getBucketCount() <= 0) {
                            try {
                                fetcher.shutdown();
                            } catch (InterruptedException e) {
                                LOG.error("Interrupted while shutting down fetcher threads.", e);
                            }
                            keysToBeRemoved.add(serverIdAndFetcherId);
                        }
                    });

            keysToBeRemoved.forEach(fetcherThreadMap::remove);
        }
    }

    private int getFetcherId(TableBucket tableBucket) {
        return tableBucket.hashCode() % numFetchersPerServer;
    }

    private ReplicaFetcherThread addAndStartFetcherThread(
            ServerAndFetcherId serverAndFetcherId, ServerIdAndFetcherId serverIdAndFetcherId) {
        ReplicaFetcherThread fetcherThread =
                createFetcherThread(serverAndFetcherId.fetcherId, serverAndFetcherId.serverNode);
        fetcherThreadMap.put(serverIdAndFetcherId, fetcherThread);
        fetcherThread.start();
        return fetcherThread;
    }

    ReplicaFetcherThread createFetcherThread(int fetcherId, ServerNode remoteNode) {
        String threadName = "ReplicaFetcherThread-" + fetcherId + "-" + remoteNode.id();
        LeaderEndpoint leaderEndpoint =
                new RemoteLeaderEndpoint(
                        conf,
                        serverId,
                        remoteNode,
                        GatewayClientProxy.createGatewayProxy(
                                () -> remoteNode, rpcClient, TabletServerGateway.class));
        return new ReplicaFetcherThread(
                threadName,
                replicaManager,
                leaderEndpoint,
                (int) conf.get(ConfigOptions.LOG_REPLICA_FETCH_BACKOFF_INTERVAL).toMillis());
    }

    private void addBucketsToFetcherThread(
            ReplicaFetcherThread fetcherThread,
            Map<TableBucket, InitialFetchStatus> initialFetchStatusMap) {
        try {
            fetcherThread.addBuckets(initialFetchStatusMap);
            LOG.info(
                    "Added fetcher to server {} with initial fetch status {}",
                    fetcherThread.getLeader().leaderNode().id(),
                    initialFetchStatusMap);
        } catch (InterruptedException e) {
            LOG.error("Interrupted while add buckets to fetcher threads.", e);
        }
    }

    public void shutdown() throws InterruptedException {
        LOG.info("Shutting down fetcher manager.");
        closeAllFetchers();
        LOG.info("Shut down fetcher manager Completed.");
    }

    private void closeAllFetchers() {
        synchronized (lock) {
            fetcherThreadMap.values().forEach(ReplicaFetcherThread::initiateShutdown);
            fetcherThreadMap.values().forEach(unchecked(ReplicaFetcherThread::shutdown));
            fetcherThreadMap.clear();
        }
    }

    @GuardedBy("lock")
    @VisibleForTesting
    Map<ServerIdAndFetcherId, ReplicaFetcherThread> getFetcherThreadMap() {
        return fetcherThreadMap;
    }

    /** Class to represent server node and fetcher id. */
    private static class ServerAndFetcherId {
        private final ServerNode serverNode;
        private final int fetcherId;

        ServerAndFetcherId(ServerNode serverNode, int fetcherId) {
            this.serverNode = serverNode;
            this.fetcherId = fetcherId;
        }
    }

    /** Class to represent server id and fetcher id. */
    public static class ServerIdAndFetcherId {
        private final int serverId;
        private final int fetcherId;

        ServerIdAndFetcherId(int serverId, int fetcherId) {
            this.serverId = serverId;
            this.fetcherId = fetcherId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ServerIdAndFetcherId that = (ServerIdAndFetcherId) o;
            return serverId == that.serverId && fetcherId == that.fetcherId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(serverId, fetcherId);
        }
    }
}

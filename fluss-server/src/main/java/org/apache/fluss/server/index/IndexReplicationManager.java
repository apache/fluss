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

package org.apache.fluss.server.index;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.server.index.IndexReplicationSupervisor.IndexProgressListener;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.remote.RemoteLogManager;
import org.apache.fluss.server.metadata.MetadataProvider;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.utils.ExponentialBackoff;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.ShutdownableThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/** Owns the TabletServer's index replication workers, shared payload budget and source assembly. */
@Internal
public final class IndexReplicationManager implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(IndexReplicationManager.class);

    private static final long WORKER_BACKOFF_MS = 50L;
    private static final long INITIAL_RETRY_BACKOFF_MS = 100L;

    private final Configuration conf;
    private final TabletServerMetadataCache metadataCache;
    @Nullable private final MetadataProvider metadataProvider;
    private final RpcClient rpcClient;
    private final RemoteLogManager remoteLogManager;
    private final Executor remoteReadExecutor;
    private final TabletServerMetricGroup metrics;
    private final int sourceReadTargetBytes;
    private final long maxTransportRequestBytes;
    private final ExponentialBackoff retryBackoff;
    private final IndexSendBuffer sendBuffer;
    private final IndexReplicationScheduler scheduler;
    private final ReplicatorWorker[] workers;
    private final IndexSender sender;

    /** Starts node-owned replication workers using the supplied shared infrastructure. */
    public IndexReplicationManager(
            Configuration conf,
            TabletServerMetadataCache metadataCache,
            @Nullable MetadataProvider metadataProvider,
            RpcClient rpcClient,
            RemoteLogManager remoteLogManager,
            Executor remoteReadExecutor,
            TabletServerMetricGroup metrics) {
        this.conf = conf;
        this.metadataCache = metadataCache;
        this.metadataProvider = metadataProvider;
        this.rpcClient = rpcClient;
        this.remoteLogManager = remoteLogManager;
        this.remoteReadExecutor = remoteReadExecutor;
        this.metrics = metrics;
        long sourceMaxBytes = conf.get(ConfigOptions.INDEX_REPLICATION_SOURCE_MAX_BYTES).getBytes();
        checkArgument(
                sourceMaxBytes > 0 && sourceMaxBytes <= Integer.MAX_VALUE,
                "%s must be between 1 and %s bytes, but was %s",
                ConfigOptions.INDEX_REPLICATION_SOURCE_MAX_BYTES.key(),
                Integer.MAX_VALUE,
                sourceMaxBytes);
        this.sourceReadTargetBytes = (int) sourceMaxBytes;
        this.maxTransportRequestBytes =
                conf.get(ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE).getBytes();
        this.retryBackoff =
                createRetryBackoff(
                        conf.get(ConfigOptions.INDEX_REPLICATION_RETRY_BACKOFF_MAX).toMillis());
        this.sendBuffer =
                new IndexSendBuffer(
                        conf.get(ConfigOptions.INDEX_REPLICATION_BUFFER_MAX_BYTES).getBytes());
        int numWorkers = conf.getInt(ConfigOptions.INDEX_REPLICATION_READER_THREADS);
        checkArgument(numWorkers > 0, "numWorkers must be positive");
        this.scheduler = new IndexReplicationScheduler(numWorkers);
        this.workers = new ReplicatorWorker[numWorkers];
        for (int i = 0; i < numWorkers; i++) {
            this.workers[i] =
                    new ReplicatorWorker(
                            "index-replicator-pool-" + i, i, WORKER_BACKOFF_MS, scheduler);
            this.workers[i].start();
        }
        LOG.info("Index replication started with {} workers", numWorkers);
        sendBuffer.setCapacityListener(this::wakeupWorkers);
        try {
            this.sender =
                    new IndexSender(
                            sendBuffer,
                            metadataCache::getBucketLeader,
                            this::gatewayFor,
                            metrics,
                            conf.getInt(ConfigOptions.INDEX_REPLICATION_SENDER_THREADS),
                            WORKER_BACKOFF_MS,
                            conf.get(ConfigOptions.INDEX_REPLICATION_REQUEST_TARGET_BYTES)
                                    .getBytes(),
                            maxTransportRequestBytes,
                            IndexRequestFactory.DEFAULT_REQUEST_TIMEOUT_MS);
        } catch (RuntimeException | Error failure) {
            closeWorkers();
            throw failure;
        }
    }

    /** Creates the per-replica owner of metadata readiness and leader transitions. */
    public IndexReplicationSupervisor createSupervisor(
            TableInfo tableInfo, TableBucket tableBucket) {
        return new IndexReplicationSupervisor(
                tableInfo,
                tableBucket,
                metadataCache,
                metadataProvider,
                this,
                metrics.indexReplicationRetries());
    }

    /**
     * Creates an unregistered source owning the supplied read context. Its supervisor installs the
     * source reference before registering it with the workers.
     */
    IndexReplicator createReplicator(
            LogTablet log,
            List<IndexReplicationEncoder> indexes,
            LogRecordReadContext readContext,
            long replayStartOffset,
            IndexProgressListener onProgress) {
        return new IndexReplicator(
                new IndexSourceReader(log, remoteLogManager, remoteReadExecutor),
                indexes,
                sendBuffer,
                readContext,
                replayStartOffset,
                sourceReadTargetBytes,
                onProgress,
                scheduler,
                workerFor(log.getTableBucket())::wakeup,
                retryBackoff,
                SystemClock.getInstance(),
                metrics,
                maxTransportRequestBytes);
    }

    void register(IndexReplicator replicator) {
        ReplicatorWorker worker = workerFor(replicator.sourceBucket());
        replicator.checkScheduler(scheduler);
        scheduler.register(worker.workerId, replicator);
        worker.wakeup();
    }

    void unregister(IndexReplicator replicator) {
        scheduler.unregister(workerFor(replicator.sourceBucket()).workerId, replicator);
    }

    /** Charged payload page bytes across construction, queued and in-flight windows / capacity. */
    public double bufferUsageRatio() {
        return (double) sendBuffer.pendingBytes() / sendBuffer.capacityBytes();
    }

    @VisibleForTesting
    IndexSendBuffer sendBuffer() {
        return sendBuffer;
    }

    static ExponentialBackoff createRetryBackoff(long maxRetryBackoffMs) {
        checkArgument(
                maxRetryBackoffMs >= INITIAL_RETRY_BACKOFF_MS
                        && maxRetryBackoffMs <= Integer.MAX_VALUE,
                "maxRetryBackoffMs must be between %s and %s milliseconds, but was %s",
                INITIAL_RETRY_BACKOFF_MS,
                Integer.MAX_VALUE,
                maxRetryBackoffMs);
        return new ExponentialBackoff(INITIAL_RETRY_BACKOFF_MS, 2, maxRetryBackoffMs, 0.2);
    }

    private TabletServerGateway gatewayFor(int serverId) {
        String listenerName = conf.get(ConfigOptions.INTERNAL_LISTENER_NAME);
        ServerNode node =
                metadataCache
                        .getTabletServer(serverId, listenerName)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "TabletServer "
                                                        + serverId
                                                        + " not found in cache"));
        return GatewayClientProxy.createGatewayProxy(
                () -> node, rpcClient, TabletServerGateway.class);
    }

    /**
     * Stops owned workers; shared RPC, remote-log and executor resources remain with their owners.
     */
    @Override
    public void close() {
        sender.close();
        closeWorkers();
    }

    private void wakeupWorkers() {
        for (ReplicatorWorker worker : workers) {
            worker.wakeup();
        }
    }

    private ReplicatorWorker workerFor(TableBucket tableBucket) {
        return workers[Math.floorMod(tableBucket.hashCode(), workers.length)];
    }

    private void closeWorkers() {
        LOG.info("Closing index replication workers");
        for (ReplicatorWorker worker : workers) {
            worker.initiateShutdown();
        }
        for (ReplicatorWorker worker : workers) {
            worker.wakeup();
            try {
                worker.awaitShutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Drives the scheduling algorithm for the sources assigned to one worker. */
    static final class ReplicatorWorker extends ShutdownableThread {

        private final IndexReplicationScheduler scheduler;
        private final long backoffMs;
        private final int workerId;
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition condition = lock.newCondition();
        private volatile boolean wakeupRequested;

        ReplicatorWorker(
                String name, int workerId, long backoffMs, IndexReplicationScheduler scheduler) {
            super(name, false);
            this.scheduler = scheduler;
            this.backoffMs = backoffMs;
            this.workerId = workerId;
        }

        void wakeup() {
            // A pending request guarantees a new poll before the worker waits again.
            if (wakeupRequested) {
                return;
            }
            inLock(
                    lock,
                    () -> {
                        wakeupRequested = true;
                        condition.signalAll();
                    });
        }

        @Override
        public void doWork() {
            boolean didWork = scheduler.pollWorker(workerId);
            if (!didWork) {
                inLock(
                        lock,
                        () -> {
                            try {
                                if (!wakeupRequested) {
                                    condition.await(backoffMs, TimeUnit.MILLISECONDS);
                                }
                                wakeupRequested = false;
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });
            }
        }
    }
}

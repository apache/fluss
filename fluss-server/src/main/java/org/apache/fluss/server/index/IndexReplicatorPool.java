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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.concurrent.ShutdownableThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/**
 * TabletServer-global read layer for index replication. A fixed pool of worker threads drives all
 * leader-side {@link IndexReplicator}s on this server, mirroring the {@code ReplicaFetcherManager}
 * fixed-worker model.
 *
 * <p>Each replicator is assigned to a worker by {@code tableBucket.hashCode() % N}; the worker
 * fairly polls every replicator it owns. {@link IndexReplicator#poll()} reads at most one window
 * per call and is a no-op while a window is in flight, so the loop naturally idles once every
 * replicator is caught up or blocked, at which point the worker awaits a back-off until signalled
 * (HW advance, registration, or window completion).
 */
@Internal
@ThreadSafe
public final class IndexReplicatorPool implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexReplicatorPool.class);

    private final ReplicatorWorker[] workers;
    private final int maxWindowBytes;
    private final long preferredMaxRequestBytes;
    private final Executor remoteReadExecutor;

    public IndexReplicatorPool(
            int numWorkers,
            int maxWindowBytes,
            long preferredMaxRequestBytes,
            long backoffMs,
            Executor remoteReadExecutor) {
        checkArgument(numWorkers > 0, "numWorkers must be positive");
        checkArgument(maxWindowBytes > 0, "maxWindowBytes must be positive");
        checkArgument(preferredMaxRequestBytes > 0, "preferredMaxRequestBytes must be positive");
        checkArgument(backoffMs > 0, "backoffMs must be positive");
        this.maxWindowBytes = maxWindowBytes;
        this.preferredMaxRequestBytes = preferredMaxRequestBytes;
        this.remoteReadExecutor = remoteReadExecutor;
        this.workers = new ReplicatorWorker[numWorkers];
        for (int i = 0; i < numWorkers; i++) {
            this.workers[i] = new ReplicatorWorker("index-replicator-pool-" + i, backoffMs);
            this.workers[i].start();
        }
        LOG.info("IndexReplicatorPool started with {} workers", numWorkers);
    }

    /** Maximum number of WAL bytes a replicator reads per window. */
    public int maxWindowBytes() {
        return maxWindowBytes;
    }

    /** Preferred aggregate encoded output bound for one source window. */
    public long preferredMaxRequestBytes() {
        return preferredMaxRequestBytes;
    }

    /** Executor for blocking remote-WAL reads initiated by this replication pool. */
    Executor remoteReadExecutor() {
        return remoteReadExecutor;
    }

    /** Registers a replicator and wires its wake-up signal to the owning worker. */
    public void register(TableBucket tableBucket, IndexReplicator replicator) {
        ReplicatorWorker worker = workerFor(tableBucket);
        replicator.setWakeupSignal(worker::wakeup);
        worker.add(tableBucket, replicator);
    }

    /** Unregisters the replicator for the given bucket, if any. */
    public void unregister(TableBucket tableBucket) {
        workerFor(tableBucket).remove(tableBucket);
    }

    /** Wakes the worker owning the given bucket so it polls promptly (e.g. on HW advance). */
    public void signal(TableBucket tableBucket) {
        workerFor(tableBucket).wakeup();
    }

    private ReplicatorWorker workerFor(TableBucket tableBucket) {
        return workers[Math.floorMod(tableBucket.hashCode(), workers.length)];
    }

    @Override
    public void close() {
        LOG.info("IndexReplicatorPool closing");
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

    /** A worker that fairly polls all replicators assigned to it. */
    private static final class ReplicatorWorker extends ShutdownableThread {

        private final long backoffMs;
        private final Map<TableBucket, IndexReplicator> replicators = new ConcurrentHashMap<>();
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition condition = lock.newCondition();
        private boolean wakeupRequested;

        ReplicatorWorker(String name, long backoffMs) {
            super(name, false);
            this.backoffMs = backoffMs;
        }

        void add(TableBucket tableBucket, IndexReplicator replicator) {
            replicators.put(tableBucket, replicator);
            wakeup();
        }

        void remove(TableBucket tableBucket) {
            replicators.remove(tableBucket);
        }

        void wakeup() {
            inLock(
                    lock,
                    () -> {
                        wakeupRequested = true;
                        condition.signalAll();
                    });
        }

        @Override
        public void doWork() {
            boolean didWork = false;
            for (IndexReplicator replicator : replicators.values()) {
                try {
                    if (replicator.poll()) {
                        didWork = true;
                    }
                } catch (Throwable t) {
                    LOG.error("Error polling index replicator", t);
                }
            }
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

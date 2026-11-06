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
import org.apache.fluss.utils.ExponentialBackoff;
import org.apache.fluss.utils.concurrent.ShutdownableThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

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
 * <p>Each source bucket has a fixed owning worker. The scheduling strategy runs a bounded round of
 * that worker's sources; completed reads are decoded and staged on the same worker. Remote reads
 * and acknowledgements wake the worker without running construction on their completion threads.
 * Idle workers await a signal or the existing backoff interval.
 */
@Internal
@ThreadSafe
public final class IndexReplicatorPool implements AutoCloseable {

    private static final long INITIAL_RETRY_BACKOFF_MS = 100L;

    private static final Logger LOG = LoggerFactory.getLogger(IndexReplicatorPool.class);

    private final ReplicatorWorker[] workers;
    private final IndexReplicationScheduler scheduler;
    private final int maxWindowBytes;
    private final long maxTransportRequestBytes;
    private final ExponentialBackoff retryBackoff;
    private final Executor remoteReadExecutor;

    public IndexReplicatorPool(
            int numWorkers,
            int maxWindowBytes,
            long maxRetryBackoffMs,
            long backoffMs,
            Executor remoteReadExecutor,
            long maxTransportRequestBytes) {
        checkArgument(maxTransportRequestBytes > 0, "maxTransportRequestBytes must be positive");
        checkArgument(numWorkers > 0, "numWorkers must be positive");
        checkArgument(maxWindowBytes > 0, "maxWindowBytes must be positive");
        checkArgument(backoffMs > 0, "backoffMs must be positive");
        checkArgument(
                maxRetryBackoffMs >= INITIAL_RETRY_BACKOFF_MS
                        && maxRetryBackoffMs <= Integer.MAX_VALUE,
                "maxRetryBackoffMs must be between %s and %s milliseconds, but was %s",
                INITIAL_RETRY_BACKOFF_MS,
                Integer.MAX_VALUE,
                maxRetryBackoffMs);
        this.maxWindowBytes = maxWindowBytes;
        this.maxTransportRequestBytes = maxTransportRequestBytes;
        this.retryBackoff =
                new ExponentialBackoff(INITIAL_RETRY_BACKOFF_MS, 2, maxRetryBackoffMs, 0.2);
        this.remoteReadExecutor = remoteReadExecutor;
        this.scheduler = new FairIndexReplicationScheduler(numWorkers);
        this.workers = new ReplicatorWorker[numWorkers];
        for (int i = 0; i < numWorkers; i++) {
            this.workers[i] = new ReplicatorWorker("index-replicator-pool-" + i, i, backoffMs);
            this.workers[i].start();
        }
        LOG.info("IndexReplicatorPool started with {} workers", numWorkers);
    }

    /** Maximum number of WAL bytes a replicator reads per window. */
    public int maxWindowBytes() {
        return maxWindowBytes;
    }

    long maxTransportRequestBytes() {
        return maxTransportRequestBytes;
    }

    /** Retry policy shared by source reads and index-window construction. */
    ExponentialBackoff retryBackoff() {
        return retryBackoff;
    }

    /** Wakes readers after shared memory becomes available. */
    public void wakeup() {
        for (ReplicatorWorker worker : workers) {
            worker.wakeup();
        }
    }

    /** Executor for blocking remote-WAL reads initiated by this replication pool. */
    Executor remoteReadExecutor() {
        return remoteReadExecutor;
    }

    /** Registers a replicator and wires its wake-up signal to the owning worker. */
    public void register(TableBucket tableBucket, IndexReplicator replicator) {
        ReplicatorWorker worker = workerFor(tableBucket);
        replicator.setWakeupSignal(worker::wakeup);
        replicator.setScheduler(scheduler);
        scheduler.register(worker.workerId, replicator);
        worker.wakeup();
    }

    /** Unregisters the bucket only if it still belongs to the expected replicator. */
    public void unregister(TableBucket tableBucket, IndexReplicator expectedSource) {
        scheduler.unregister(workerFor(tableBucket).workerId, expectedSource);
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

    /** Runs the scheduling strategy for the sources assigned to one worker. */
    private final class ReplicatorWorker extends ShutdownableThread {

        private final long backoffMs;
        private final int workerId;
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition condition = lock.newCondition();
        private boolean wakeupRequested;

        ReplicatorWorker(String name, int workerId, long backoffMs) {
            super(name, false);
            this.backoffMs = backoffMs;
            this.workerId = workerId;
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

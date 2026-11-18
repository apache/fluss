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

package org.apache.fluss.server.coordinator.bulkload;

import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Supplier;

/** Runs restartable metadata maintenance on workers, with one pass in flight per Coordinator. */
public final class BulkLoadMaintenance implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(BulkLoadMaintenance.class);
    private static final long CLEANUP_INTERVAL_MS = 60_000L;
    private final BulkLoadMetadataStore store;
    private final Scheduler scheduler;
    private final Executor executor;
    private final Supplier<CompletableFuture<ZkEpoch>> epochSupplier;
    private final BulkLoadRecoveryManager recovery;
    private final Clock clock;
    private final long retentionMillis;
    private ScheduledFuture<?> scheduled;
    private CompletableFuture<?> current;
    private boolean closed = true;
    private boolean running;
    private boolean cleanupRequested;
    private long nextCleanup;

    public BulkLoadMaintenance(
            BulkLoadMetadataStore store,
            Scheduler scheduler,
            Executor executor,
            Supplier<CompletableFuture<ZkEpoch>> epochSupplier,
            Clock clock,
            long retentionMillis,
            BulkLoadRecoveryManager recovery) {
        this.store = store;
        this.scheduler = scheduler;
        this.executor = executor;
        this.epochSupplier = epochSupplier;
        this.clock = clock;
        this.retentionMillis = retentionMillis;
        this.recovery = recovery;
    }

    public synchronized void start() {
        closed = false;
        triggerCleanup();
    }

    /** Schedules cleanup promptly after a metadata deletion, without blocking the event thread. */
    public synchronized void triggerCleanup() {
        cleanupRequested = true;
        trigger();
    }

    public synchronized void trigger() {
        if (closed || running) {
            return;
        }
        if (scheduled != null) {
            scheduled.cancel(false);
        }
        scheduled = scheduler.scheduleOnce("bulk-load-maintenance", this::run);
    }

    private synchronized void run() {
        scheduled = null;
        if (closed || running) {
            return;
        }
        running = true;
        try {
            current =
                    epochSupplier
                            .get()
                            .thenAcceptAsync(
                                    epoch -> {
                                        synchronized (this) {
                                            if (closed) {
                                                return;
                                            }
                                        }
                                        if (epoch.getCoordinatorEpochZkVersion() < 0) {
                                            return;
                                        }
                                        try {
                                            recovery.recoverImports(epoch);
                                        } catch (Exception e) {
                                            LOG.warn(
                                                    "BulkLoad recovery scan failed; the next pass will retry.",
                                                    e);
                                        }
                                        long now = clock.milliseconds();
                                        if (shouldCleanUp(now)) {
                                            try {
                                                store.maintain(
                                                        now,
                                                        retentionMillis,
                                                        epoch.getCoordinatorEpochZkVersion());
                                            } catch (Exception e) {
                                                LOG.warn(
                                                        "BulkLoad result cleanup failed; the next pass will retry.",
                                                        e);
                                            }
                                        }
                                    },
                                    executor)
                            .whenComplete((ignored, error) -> finish(error));
        } catch (Exception e) {
            finish(e);
        }
    }

    private synchronized boolean shouldCleanUp(long now) {
        if (closed || (!cleanupRequested && now < nextCleanup)) {
            return false;
        }
        cleanupRequested = false;
        nextCleanup = now + CLEANUP_INTERVAL_MS;
        return true;
    }

    private synchronized void finish(Throwable error) {
        running = false;
        current = null;
        if (closed) {
            return;
        }
        if (error != null) {
            LOG.warn("BulkLoad metadata maintenance failed; the next pass will retry.", error);
        }
        scheduled =
                scheduler.scheduleOnce(
                        "bulk-load-maintenance",
                        this::run,
                        cleanupRequested
                                ? 0L
                                : recovery.hasPendingWork() ? 1_000L : CLEANUP_INTERVAL_MS);
    }

    @Override
    public synchronized void close() {
        closed = true;
        if (scheduled != null) {
            scheduled.cancel(false);
            scheduled = null;
        }
        if (current != null) {
            current.cancel(false);
            current = null;
        }
    }
}

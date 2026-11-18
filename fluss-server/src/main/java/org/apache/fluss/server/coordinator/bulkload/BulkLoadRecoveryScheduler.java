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
import org.apache.fluss.utils.concurrent.FutureUtils;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Schedules one BulkLoad recovery scan at a time on Coordinator workers. */
public final class BulkLoadRecoveryScheduler implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(BulkLoadRecoveryScheduler.class);
    private static final long IDLE_SCAN_INTERVAL_MS = 60_000L;
    private final Scheduler scheduler;
    private final Executor executor;
    private final Supplier<CompletableFuture<ZkEpoch>> epochSupplier;
    private final BulkLoadRecoveryManager recovery;
    private ScheduledFuture<?> scheduled;
    private boolean closed = true;
    private boolean running;

    public BulkLoadRecoveryScheduler(
            Scheduler scheduler,
            Executor executor,
            Supplier<CompletableFuture<ZkEpoch>> epochSupplier,
            BulkLoadRecoveryManager recovery) {
        this.scheduler = scheduler;
        this.executor = executor;
        this.epochSupplier = epochSupplier;
        this.recovery = recovery;
    }

    public synchronized void start() {
        closed = false;
        trigger();
    }

    public synchronized void trigger() {
        if (closed || running) {
            return;
        }
        if (scheduled != null) {
            scheduled.cancel(false);
        }
        scheduled = scheduler.scheduleOnce("bulk-load-recovery", this::run);
    }

    private synchronized void run() {
        scheduled = null;
        if (closed || running) {
            return;
        }
        running = true;
        try {
            FutureUtils.orTimeout(
                            epochSupplier.get(),
                            30,
                            TimeUnit.SECONDS,
                            "BulkLoad coordinator epoch lookup timed out.")
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
                            },
                            executor)
                    .whenComplete((ignored, error) -> finish(error));
        } catch (Exception e) {
            finish(e);
        }
    }

    private synchronized void finish(Throwable error) {
        running = false;
        if (closed) {
            return;
        }
        if (error != null) {
            LOG.warn("BulkLoad recovery scheduling failed; the next pass will retry.", error);
        }
        scheduled =
                scheduler.scheduleOnce(
                        "bulk-load-recovery",
                        this::run,
                        recovery.hasPendingWork() ? 1_000L : IDLE_SCAN_INTERVAL_MS);
    }

    @Override
    public synchronized void close() {
        closed = true;
        if (scheduled != null) {
            scheduled.cancel(false);
            scheduled = null;
        }
    }
}

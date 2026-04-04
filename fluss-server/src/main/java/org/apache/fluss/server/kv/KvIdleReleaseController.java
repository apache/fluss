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

package org.apache.fluss.server.kv;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.utils.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Periodically checks OPEN KvTablets and releases idle ones back to LAZY state. Tablets are sorted
 * by last access time (LRU). Operates directly on {@link KvTablet} — no dependency on Replica
 * layer.
 */
public class KvIdleReleaseController implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(KvIdleReleaseController.class);

    private final ScheduledExecutorService scheduler;
    private final Supplier<Collection<KvTablet>> tabletSupplier;
    private final Clock clock;

    private final long checkIntervalMs;
    private final long idleIntervalMs;

    private volatile ScheduledFuture<?> scheduledTask;

    public KvIdleReleaseController(
            ScheduledExecutorService scheduler,
            Supplier<Collection<KvTablet>> tabletSupplier,
            Clock clock,
            long checkIntervalMs,
            long idleIntervalMs) {
        this.scheduler = scheduler;
        this.tabletSupplier = tabletSupplier;
        this.clock = clock;
        this.checkIntervalMs = checkIntervalMs;
        this.idleIntervalMs = idleIntervalMs;
    }

    public void start() {
        scheduledTask =
                scheduler.scheduleWithFixedDelay(
                        this::checkAndRelease,
                        checkIntervalMs,
                        checkIntervalMs,
                        TimeUnit.MILLISECONDS);
        LOG.info(
                "KvIdleReleaseController started: checkInterval={}ms, idleInterval={}ms",
                checkIntervalMs,
                idleIntervalMs);
    }

    /**
     * Scans all open tablets, selects those idle beyond the configured threshold, and releases them
     * in LRU order (coldest first).
     */
    @VisibleForTesting
    void checkAndRelease() {
        try {
            Collection<KvTablet> tablets = tabletSupplier.get();
            long now = clock.milliseconds();

            // Snapshot timestamp once per tablet to avoid double-read of volatile field
            List<ReleaseCandidate> idleCandidates =
                    tablets.stream()
                            .map(t -> new ReleaseCandidate(t, t.getLastAccessTimestamp()))
                            .filter(c -> now - c.lastAccessTimestamp > idleIntervalMs)
                            .sorted(Comparator.comparingLong(c -> c.lastAccessTimestamp))
                            .collect(Collectors.toList());

            if (idleCandidates.isEmpty()) {
                LOG.debug("Idle release round: no idle tablets found");
                return;
            }

            LOG.info("Idle release round: open={}, idle={}", tablets.size(), idleCandidates.size());

            int released = 0;
            for (ReleaseCandidate candidate : idleCandidates) {
                KvTablet tablet = candidate.tablet;

                if (tablet.canRelease(idleIntervalMs, now)) {
                    try {
                        boolean success = tablet.releaseKv();
                        if (success) {
                            released++;
                            LOG.debug(
                                    "Released KvTablet for {} (idle {}ms)",
                                    tablet.getTableBucket(),
                                    now - candidate.lastAccessTimestamp);
                        }
                    } catch (Exception e) {
                        LOG.warn("Failed to release KvTablet for {}", tablet.getTableBucket(), e);
                    }
                }
            }

            if (released > 0) {
                LOG.info(
                        "Idle release round complete: released={}/{}",
                        released,
                        idleCandidates.size());
            }
        } catch (Exception e) {
            LOG.error("Error during idle release check", e);
        }
    }

    @Override
    public void close() {
        if (scheduledTask != null) {
            scheduledTask.cancel(false);
        }
    }

    private static class ReleaseCandidate {
        final KvTablet tablet;
        final long lastAccessTimestamp;

        ReleaseCandidate(KvTablet tablet, long lastAccessTimestamp) {
            this.tablet = tablet;
            this.lastAccessTimestamp = lastAccessTimestamp;
        }
    }
}

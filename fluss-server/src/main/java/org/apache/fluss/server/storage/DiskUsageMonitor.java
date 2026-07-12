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

package org.apache.fluss.server.storage;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Periodically samples the local data disk usage ratio and toggles the tablet server write-lock
 * state with a configurable hysteresis: writes are locked when the usage reaches the configured
 * write-limit ratio and resume only after the usage drops below {@code (limit - recoverGap)}. The
 * monitor is single-state and intended to be driven by a scheduler thread; it never blocks.
 */
@Internal
public final class DiskUsageMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(DiskUsageMonitor.class);

    private final int serverId;
    private final DiskUsageCollector collector;
    private volatile double writeLimitRatio;
    private volatile double recoverGap;
    private volatile double recoverThreshold;
    private final Listener listener;

    private volatile boolean locked;
    private volatile double lastUsageRatio;

    public DiskUsageMonitor(
            int serverId,
            DiskUsageCollector collector,
            double writeLimitRatio,
            double recoverGap,
            Listener listener) {
        checkValidWriteLimitConfig(writeLimitRatio, recoverGap);
        this.serverId = serverId;
        this.collector = checkNotNull(collector, "collector");
        this.writeLimitRatio = writeLimitRatio;
        this.recoverGap = recoverGap;
        this.recoverThreshold = writeLimitRatio - recoverGap;
        this.listener = checkNotNull(listener, "listener");
    }

    /** Samples the disk usage once and updates the lock state. Never throws. */
    public void runOnce() {
        double usage;
        try {
            usage = collector.collect();
        } catch (IOException e) {
            LOG.warn(
                    "Failed to collect disk usage for tablet server {}; "
                            + "keep the previous write-lock state {} (last usage {}%).",
                    serverId, locked, String.format("%.2f", lastUsageRatio * 100), e);
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "[DISK-MONITOR-DEBUG] TabletServer {} disk usage: {}% | limit: {}% | "
                            + "recover: {}% | locked: {}",
                    serverId,
                    String.format("%.4f", usage * 100),
                    String.format("%.2f", writeLimitRatio * 100),
                    String.format("%.2f", recoverThreshold * 100),
                    locked);
        }
        update(usage);
    }

    @VisibleForTesting
    public void update(double usage) {
        lastUsageRatio = usage;
        // writeLimitRatio == 1.0 means the protection is disabled; skip state transitions
        // but still keep sampling for metrics.
        if (Double.compare(writeLimitRatio, 1.0) >= 0) {
            if (locked) {
                locked = false;
            }
            listener.onSample(lastUsageRatio, false);
            return;
        }
        boolean wasLocked = locked;
        if (!wasLocked && usage >= writeLimitRatio) {
            locked = true;
            LOG.warn(
                    "TabletServer {} disk usage reached {}% (limit {}%); rejecting writes "
                            + "until usage drops below {}%.",
                    serverId,
                    String.format("%.2f", usage * 100),
                    String.format("%.2f", writeLimitRatio * 100),
                    String.format("%.2f", recoverThreshold * 100));
        } else if (wasLocked && usage <= recoverThreshold) {
            locked = false;
            LOG.info(
                    "TabletServer {} disk usage dropped to {}% (recover threshold {}%); "
                            + "resuming writes.",
                    serverId,
                    String.format("%.2f", usage * 100),
                    String.format("%.2f", recoverThreshold * 100));
        }
        listener.onSample(lastUsageRatio, locked);
    }

    public boolean isLocked() {
        return locked;
    }

    public double getLastUsageRatio() {
        return lastUsageRatio;
    }

    public double getWriteLimitRatio() {
        return writeLimitRatio;
    }

    public double getRecoverGap() {
        return recoverGap;
    }

    public double getRecoverThreshold() {
        return recoverThreshold;
    }

    /**
     * Dynamically updates the write-limit ratio, recover gap and the derived recover threshold. The
     * new values take effect on the next {@link #runOnce()} invocation or {@link #update(double)}
     * call.
     *
     * @param newRatio the new write-limit ratio, must be within (recoverGap, 1.0]
     * @param newRecoverGap the new recover gap, must be within (0.0, newRatio)
     */
    public void updateWriteLimitConfig(double newRatio, double newRecoverGap) {
        checkValidWriteLimitConfig(newRatio, newRecoverGap);
        this.writeLimitRatio = newRatio;
        this.recoverGap = newRecoverGap;
        this.recoverThreshold = newRatio - newRecoverGap;
    }

    /**
     * Dynamically updates the write-limit ratio while keeping the current recover gap.
     *
     * @param newRatio the new write-limit ratio, must be greater than the current recover gap and
     *     no greater than 1.0
     */
    public void updateWriteLimitRatio(double newRatio) {
        updateWriteLimitConfig(newRatio, recoverGap);
    }

    private static void checkValidWriteLimitConfig(double writeLimitRatio, double recoverGap) {
        String validationError =
                DiskWriteLimitConfigValidator.getValidationError(writeLimitRatio, recoverGap);
        checkArgument(validationError == null, validationError);
    }

    /** Receives every sample for downstream state synchronization (e.g. metrics gauges). */
    @FunctionalInterface
    public interface Listener {
        void onSample(double usageRatio, boolean locked);
    }
}

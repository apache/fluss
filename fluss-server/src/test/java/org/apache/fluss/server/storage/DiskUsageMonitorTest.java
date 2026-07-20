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

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link DiskUsageMonitor}. */
class DiskUsageMonitorTest {

    private static final int SERVER_ID = 7;
    private static final double WRITE_RECOVER_RATIO = 0.80;

    @Test
    void testInvalidLimitConfigRejected() {
        DiskUsageCollector collector = new DiskUsageCollector(Collections.emptyList());
        assertThatThrownBy(
                        () ->
                                new DiskUsageMonitor(
                                        SERVER_ID,
                                        collector,
                                        0.0,
                                        WRITE_RECOVER_RATIO,
                                        (usage, locked) -> {}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                new DiskUsageMonitor(
                                        SERVER_ID,
                                        collector,
                                        1.1,
                                        WRITE_RECOVER_RATIO,
                                        (usage, locked) -> {}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                new DiskUsageMonitor(
                                        SERVER_ID, collector, 0.85, 0.0, (usage, locked) -> {}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                new DiskUsageMonitor(
                                        SERVER_ID, collector, 0.85, 0.85, (usage, locked) -> {}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testStaysUnlockedBelowLimit() {
        Recorder recorder = new Recorder();
        DiskUsageMonitor monitor = newMonitor(0.85, recorder);

        monitor.update(0.50);
        assertThat(monitor.isLocked()).isFalse();
        assertThat(monitor.getLastUsageRatio()).isEqualTo(0.50);
        assertThat(recorder.lastLocked.get()).isFalse();
        assertThat(recorder.lastUsage.get()).isEqualTo(0.50);
    }

    @Test
    void testLockedWhenReachingLimit() {
        Recorder recorder = new Recorder();
        DiskUsageMonitor monitor = newMonitor(0.85, recorder);

        monitor.update(0.85);
        assertThat(monitor.isLocked()).isTrue();
        assertThat(recorder.lastLocked.get()).isTrue();

        monitor.update(0.90);
        assertThat(monitor.isLocked()).isTrue();
    }

    @Test
    void testStaysLockedAboveRecoverThreshold() {
        Recorder recorder = new Recorder();
        DiskUsageMonitor monitor = newMonitor(0.85, recorder);

        monitor.update(0.86);
        assertThat(monitor.isLocked()).isTrue();

        // recover threshold is 0.80, 0.81 still above it -> stay locked
        monitor.update(0.81);
        assertThat(monitor.isLocked()).isTrue();
    }

    @Test
    void testUnlockedAtRecoverThreshold() {
        Recorder recorder = new Recorder();
        DiskUsageMonitor monitor = newMonitor(0.85, recorder);

        monitor.update(0.90);
        assertThat(monitor.isLocked()).isTrue();

        monitor.update(monitor.getWriteRecoverRatio());
        assertThat(monitor.isLocked()).isFalse();
        assertThat(recorder.lastLocked.get()).isFalse();
    }

    @Test
    void testRecoverThresholdUsesConfiguredRecoverRatio() {
        DiskUsageMonitor monitor = newMonitor(0.85, 0.65, new Recorder());
        assertThat(monitor.getWriteLimitRatio()).isEqualTo(0.85);
        assertThat(monitor.getWriteRecoverRatio()).isEqualTo(0.65);
    }

    @Test
    void testRunOncePreservesStateWhenCollectorThrows() {
        // Pointing the collector at a non-existent path makes Files.getFileStore raise
        // NoSuchFileException -> IOException, which runOnce must swallow without flipping state.
        DiskUsageCollector failing =
                new DiskUsageCollector(
                        Collections.singletonList(
                                new File("/__fluss_disk_monitor_does_not_exist__/x")));
        Recorder recorder = new Recorder();
        DiskUsageMonitor monitor =
                new DiskUsageMonitor(SERVER_ID, failing, 0.85, WRITE_RECOVER_RATIO, recorder);

        // First put the monitor into the locked state via update().
        monitor.update(0.95);
        assertThat(monitor.isLocked()).isTrue();
        double usageBefore = monitor.getLastUsageRatio();
        recorder.lastUsage.set(Double.NaN);
        recorder.lastLocked.set(false);

        // runOnce should swallow the IOException, keep locked=true and not invoke the listener.
        monitor.runOnce();
        assertThat(monitor.isLocked()).isTrue();
        assertThat(monitor.getLastUsageRatio()).isEqualTo(usageBefore);
        assertThat(Double.isNaN(recorder.lastUsage.get())).isTrue();
        assertThat(recorder.lastLocked.get()).isFalse();
    }

    @Test
    void testRunOnceUpdatesUsageWhenCollectorReturnsValue() {
        // Reuse a real collector backed by an empty data dirs list -> always returns 0.0.
        DiskUsageCollector collector = new DiskUsageCollector(Collections.emptyList());
        Recorder recorder = new Recorder();
        DiskUsageMonitor monitor =
                new DiskUsageMonitor(SERVER_ID, collector, 0.85, WRITE_RECOVER_RATIO, recorder);

        monitor.runOnce();
        assertThat(monitor.isLocked()).isFalse();
        assertThat(monitor.getLastUsageRatio()).isEqualTo(0.0);
        assertThat(recorder.lastUsage.get()).isEqualTo(0.0);
        assertThat(recorder.lastLocked.get()).isFalse();
    }

    private DiskUsageMonitor newMonitor(double limit, DiskUsageMonitor.Listener listener) {
        return newMonitor(limit, WRITE_RECOVER_RATIO, listener);
    }

    private DiskUsageMonitor newMonitor(
            double limit, double writeRecoverRatio, DiskUsageMonitor.Listener listener) {
        return new DiskUsageMonitor(
                SERVER_ID,
                new DiskUsageCollector(Collections.emptyList()),
                limit,
                writeRecoverRatio,
                listener);
    }

    @Test
    void testUpdateWriteLimitRatioKeepsRecoverRatio() {
        Recorder recorder = new Recorder();
        DiskUsageMonitor monitor = newMonitor(0.85, recorder);

        // Initially ratio=0.85, recover=0.80
        assertThat(monitor.getWriteLimitRatio()).isEqualTo(0.85);
        assertThat(monitor.getWriteRecoverRatio()).isEqualTo(WRITE_RECOVER_RATIO);

        // Simulate usage at 0.82 — should NOT lock (below 0.85)
        monitor.update(0.82);
        assertThat(monitor.isLocked()).isFalse();

        // Lower the limit to 0.81; the absolute recover ratio remains 0.80.
        monitor.updateWriteLimitRatio(0.81);
        assertThat(monitor.getWriteLimitRatio()).isEqualTo(0.81);
        assertThat(monitor.getWriteRecoverRatio()).isEqualTo(0.80);

        // Re-evaluate with same usage — should lock
        monitor.update(0.82);
        assertThat(monitor.isLocked()).isTrue();
        assertThat(recorder.lastLocked.get()).isTrue();

        // Raise the limit to 0.90; the recover ratio remains 0.80.
        monitor.updateWriteLimitRatio(0.90);
        monitor.update(0.86);
        assertThat(monitor.isLocked()).isTrue();

        // Drop usage to 0.80; at the recover ratio, writes should resume.
        monitor.update(0.80);
        assertThat(monitor.isLocked()).isFalse();
    }

    @Test
    void testUpdateWriteLimitConfigChangesRecoverRatio() {
        Recorder recorder = new Recorder();
        DiskUsageMonitor monitor = newMonitor(0.85, recorder);

        monitor.updateWriteLimitConfig(0.85, 0.75);
        assertThat(monitor.getWriteLimitRatio()).isEqualTo(0.85);
        assertThat(monitor.getWriteRecoverRatio()).isEqualTo(0.75);

        monitor.update(0.90);
        assertThat(monitor.isLocked()).isTrue();
        monitor.update(0.76);
        assertThat(monitor.isLocked()).isTrue();
        monitor.update(0.75);
        assertThat(monitor.isLocked()).isFalse();
        assertThat(recorder.lastLocked.get()).isFalse();
    }

    @Test
    void testDisabledWhenRatioIsOne() {
        Recorder recorder = new Recorder();
        DiskUsageMonitor monitor = newMonitor(1.0, recorder);

        monitor.update(1.0);
        assertThat(monitor.isLocked()).isFalse();
        assertThat(recorder.lastLocked.get()).isFalse();
        assertThat(recorder.lastUsage.get()).isEqualTo(1.0);

        DiskUsageMonitor monitor2 = newMonitor(0.85, recorder);
        monitor2.update(0.85);
        assertThat(monitor2.isLocked()).isTrue();

        monitor2.updateWriteLimitRatio(1.0);
        monitor2.update(0.85);
        assertThat(monitor2.isLocked()).isFalse();
        assertThat(recorder.lastLocked.get()).isFalse();
    }

    @Test
    void testUpdateWriteLimitRatioRejectsInvalidValues() {
        Recorder recorder = new Recorder();
        DiskUsageMonitor monitor = newMonitor(0.85, recorder);

        assertThatThrownBy(() -> monitor.updateWriteLimitRatio(0.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> monitor.updateWriteLimitRatio(WRITE_RECOVER_RATIO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> monitor.updateWriteLimitRatio(1.1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> monitor.updateWriteLimitRatio(-0.5))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> monitor.updateWriteLimitConfig(0.85, 0.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> monitor.updateWriteLimitConfig(0.85, 0.85))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** Captures the latest sample observed by the listener. */
    private static final class Recorder implements DiskUsageMonitor.Listener {
        final AtomicReference<Double> lastUsage = new AtomicReference<>(Double.NaN);
        final AtomicBoolean lastLocked = new AtomicBoolean(false);

        @Override
        public void onSample(double usageRatio, boolean locked) {
            lastUsage.set(usageRatio);
            lastLocked.set(locked);
        }
    }
}

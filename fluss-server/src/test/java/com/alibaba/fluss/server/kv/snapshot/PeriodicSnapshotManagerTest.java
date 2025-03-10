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

import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.server.metrics.group.TestingMetricGroups;
import com.alibaba.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

import static com.alibaba.fluss.shaded.guava32.com.google.common.collect.Iterators.getOnlyElement;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PeriodicSnapshotManager} . */
class PeriodicSnapshotManagerTest {

    private static final long periodicMaterializeDelay = 10_000L;
    private final TableBucket tableBucket = new TableBucket(1, 1);
    private ManuallyTriggeredScheduledExecutorService scheduledExecutorService;
    private ManuallyTriggeredScheduledExecutorService asyncSnapshotExecutorService;
    private PeriodicSnapshotManager periodicSnapshotManager;

    @BeforeEach
    void before() {
        scheduledExecutorService = new ManuallyTriggeredScheduledExecutorService();
        asyncSnapshotExecutorService = new ManuallyTriggeredScheduledExecutorService();
    }

    @AfterEach
    void close() {
        if (periodicSnapshotManager != null) {
            periodicSnapshotManager.close();
        }
    }

    @Test
    void testInitialDelay() {
        periodicSnapshotManager = createSnapshotManager(NopSnapshotTarget.INSTANCE);
        periodicSnapshotManager.start();
        checkOnlyOneScheduledTasks();
    }

    @Test
    void testInitWithNonPositiveSnapshotInterval() {
        periodicSnapshotManager = createSnapshotManager(0, NopSnapshotTarget.INSTANCE);
        periodicSnapshotManager.start();
        // periodic snapshot is disabled when periodicMaterializeDelay is not positive
        Assertions.assertEquals(0, scheduledExecutorService.getAllScheduledTasks().size());
    }

    @Test
    void testPeriodicSnapshot() {
        periodicSnapshotManager = createSnapshotManager(NopSnapshotTarget.INSTANCE);
        periodicSnapshotManager.start();
        // check only one schedule task
        checkOnlyOneScheduledTasks();
        scheduledExecutorService.triggerNonPeriodicScheduledTasks();
        // after trigger, should still remain one task
        checkOnlyOneScheduledTasks();
    }

    @Test
    void testSnapshot() {
        // use local filesystem to make the FileSystem plugin happy
        String snapshotDir = "file:/test/snapshot1";
        TestSnapshotTarget target = new TestSnapshotTarget(new FsPath(snapshotDir));
        periodicSnapshotManager = createSnapshotManager(target);
        periodicSnapshotManager.start();
        // trigger schedule
        scheduledExecutorService.triggerNonPeriodicScheduledTasks();
        // trigger async snapshot
        asyncSnapshotExecutorService.trigger();

        // now, check the result
        assertThat(target.getCollectedRemoteDirs())
                .isEqualTo(Collections.singletonList(snapshotDir));
    }

    @Test
    void testSnapshotWithException() {
        // use local filesystem to make the FileSystem plugin happy
        String remoteDir = "file:/test/snapshot1";
        String exceptionMessage = "Exception while initializing Materialization";
        TestSnapshotTarget target = new TestSnapshotTarget(new FsPath(remoteDir), exceptionMessage);
        periodicSnapshotManager = createSnapshotManager(target);
        periodicSnapshotManager.start();
        // trigger schedule
        scheduledExecutorService.triggerNonPeriodicScheduledTasks();

        // trigger async snapshot
        asyncSnapshotExecutorService.trigger();

        assertThat(target.getCause())
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessage(exceptionMessage);
    }

    private void checkOnlyOneScheduledTasks() {
        assertThat(
                        getOnlyElement(scheduledExecutorService.getAllScheduledTasks().iterator())
                                .getDelay(MILLISECONDS))
                .as(
                        String.format(
                                "task for initial materialization should be scheduled with a 0..%d delay",
                                periodicMaterializeDelay))
                .isLessThanOrEqualTo(periodicMaterializeDelay);
    }

    private PeriodicSnapshotManager createSnapshotManager(
            PeriodicSnapshotManager.SnapshotTarget target) {
        return createSnapshotManager(periodicMaterializeDelay, target);
    }

    private PeriodicSnapshotManager createSnapshotManager(
            long periodicMaterializeDelay, PeriodicSnapshotManager.SnapshotTarget target) {
        return new PeriodicSnapshotManager(
                tableBucket,
                target,
                periodicMaterializeDelay,
                asyncSnapshotExecutorService,
                scheduledExecutorService,
                TestingMetricGroups.BUCKET_METRICS);
    }

    private static class NopSnapshotTarget implements PeriodicSnapshotManager.SnapshotTarget {
        private static final NopSnapshotTarget INSTANCE = new NopSnapshotTarget();

        @Override
        public Optional<PeriodicSnapshotManager.SnapshotRunnable> initSnapshot() {
            return Optional.empty();
        }

        @Override
        public void handleSnapshotResult(
                long snapshotId,
                int coordinatorEpoch,
                int bucketLeaderEpoch,
                SnapshotLocation snapshotLocation,
                SnapshotResult snapshotResult) {}

        @Override
        public void handleSnapshotFailure(
                long snapshotId, SnapshotLocation snapshotLocation, Throwable cause) {}

        @Override
        public long getSnapshotSize() {
            return 0L;
        }
    }

    private static class TestSnapshotTarget implements PeriodicSnapshotManager.SnapshotTarget {

        private final FsPath snapshotPath;
        private final SnapshotLocation snapshotLocation;
        private final List<String> collectedRemoteDirs;
        private final String exceptionMessage;
        private Throwable cause;

        public TestSnapshotTarget(FsPath snapshotPath) {
            this(snapshotPath, null);
        }

        public TestSnapshotTarget(FsPath snapshotPath, String exceptionMessage) {
            this.snapshotPath = snapshotPath;
            this.collectedRemoteDirs = new ArrayList<>();
            this.exceptionMessage = exceptionMessage;
            try {
                this.snapshotLocation =
                        new SnapshotLocation(
                                snapshotPath.getFileSystem(), snapshotPath, snapshotPath, 1024);
            } catch (IOException e) {
                throw new FlussRuntimeException(e);
            }
        }

        @Override
        public Optional<PeriodicSnapshotManager.SnapshotRunnable> initSnapshot() {
            RunnableFuture<SnapshotResult> runnableFuture =
                    new FutureTask<>(
                            () -> {
                                if (exceptionMessage != null) {
                                    throw new FlussRuntimeException(exceptionMessage);
                                } else {
                                    final long logOffset = 0;
                                    return new SnapshotResult(null, snapshotPath, logOffset);
                                }
                            });
            int snapshotId = 1;
            int coordinatorEpoch = 0;
            int leaderEpoch = 0;
            return Optional.of(
                    new PeriodicSnapshotManager.SnapshotRunnable(
                            runnableFuture,
                            snapshotId,
                            coordinatorEpoch,
                            leaderEpoch,
                            snapshotLocation));
        }

        @Override
        public void handleSnapshotResult(
                long snapshotId,
                int coordinatorEpoch,
                int bucketLeaderEpoch,
                SnapshotLocation snapshotLocation,
                SnapshotResult snapshotResult) {
            collectedRemoteDirs.add(snapshotResult.getSnapshotPath().toString());
        }

        @Override
        public void handleSnapshotFailure(
                long snapshotId, SnapshotLocation snapshotLocation, Throwable cause) {
            this.cause = cause;
        }

        @Override
        public long getSnapshotSize() {
            return 0L;
        }

        private List<String> getCollectedRemoteDirs() {
            return collectedRemoteDirs;
        }

        private Throwable getCause() {
            return this.cause;
        }
    }
}

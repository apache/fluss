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

package org.apache.fluss.server.kv.snapshot;

import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.utils.CloseableRegistry;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.fluss.shaded.guava32.com.google.common.collect.Iterators.getOnlyElement;
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
        asyncSnapshotExecutorService.trigger();
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

    @Test
    void testStartWhenInitialDelayIsZero() {
        // When periodicSnapshotDelay = 1, murmurHash(...) % 1 == 0 always,
        // so initialDelay = 0. With a positive snapshot interval, start()
        // should still schedule snapshots — but the current "initialDelay > 0"
        // guard silently skips scheduling.
        periodicSnapshotManager = createSnapshotManager(1L, NopSnapshotTarget.INSTANCE);
        periodicSnapshotManager.start();

        // A positive interval means snapshots should be scheduled.
        // With the bug, this assertion fails: no task is scheduled.
        assertThat(scheduledExecutorService.getAllScheduledTasks())
                .as("snapshot should be scheduled even when initialDelay is 0")
                .hasSize(1);
    }

    @Test
    void testDynamicSnapshotInterval() {
        long initialInterval = 10_000L;
        long updatedInterval = 5_000L;
        AtomicLong currentInterval = new AtomicLong(initialInterval);

        periodicSnapshotManager =
                new PeriodicSnapshotManager(
                        tableBucket,
                        NopSnapshotTarget.INSTANCE,
                        currentInterval::get,
                        asyncSnapshotExecutorService,
                        scheduledExecutorService);
        periodicSnapshotManager.start();

        // Trigger the initial task; NopTarget returns empty, so scheduleNextSnapshot() is
        // called with the current supplier value (initialInterval = 10000ms).
        scheduledExecutorService.triggerNonPeriodicScheduledTasks();
        asyncSnapshotExecutorService.trigger();
        assertThat(
                        getOnlyElement(scheduledExecutorService.getAllScheduledTasks().iterator())
                                .getDelay(MILLISECONDS))
                .as("next snapshot should be scheduled with the initial interval")
                .isGreaterThan(updatedInterval)
                .isLessThanOrEqualTo(initialInterval);

        // Update snapshot interval to 5000ms via the supplier.
        currentInterval.set(updatedInterval);

        // Trigger again; scheduleNextSnapshot() should now use updatedInterval (5000ms).
        scheduledExecutorService.triggerNonPeriodicScheduledTasks();
        asyncSnapshotExecutorService.trigger();
        assertThat(
                        getOnlyElement(scheduledExecutorService.getAllScheduledTasks().iterator())
                                .getDelay(MILLISECONDS))
                .as("next snapshot should be scheduled with the updated interval")
                .isLessThanOrEqualTo(updatedInterval);
    }

    @Test
    void testManualSnapshotWithPeriodicSnapshotsDisabled() throws Exception {
        TestSnapshotTarget target = new TestSnapshotTarget(new FsPath("file:/test/manual"));
        periodicSnapshotManager = createSnapshotManager(0, target);
        periodicSnapshotManager.start();
        CompletableFuture<CompletedSnapshotHandle> result =
                periodicSnapshotManager.triggerSnapshotAfterCompaction();
        asyncSnapshotExecutorService.trigger();
        assertThat(result.get(10, TimeUnit.SECONDS).getSnapshotId()).isEqualTo(1);
        assertThat(scheduledExecutorService.getAllScheduledTasks()).isEmpty();
    }

    @Test
    void testManualSnapshotWaitsForPeriodicSnapshot() throws Exception {
        CountDownLatch committing = new CountDownLatch(1);
        CountDownLatch allowCommit = new CountDownLatch(1);
        TestSnapshotTarget target =
                new TestSnapshotTarget(new FsPath("file:/test/manual")) {
                    @Override
                    public void handleSnapshotResult(
                            long id,
                            int coordinator,
                            int leader,
                            SnapshotLocation location,
                            SnapshotResult result) {
                        if (id == 1) {
                            committing.countDown();
                            try {
                                assertThat(allowCommit.await(10, TimeUnit.SECONDS)).isTrue();
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        super.handleSnapshotResult(id, coordinator, leader, location, result);
                    }
                };
        periodicSnapshotManager = createSnapshotManager(target);
        periodicSnapshotManager.start();
        periodicSnapshotManager.triggerSnapshot();
        CompletableFuture<Void> worker =
                CompletableFuture.runAsync(asyncSnapshotExecutorService::trigger);
        assertThat(committing.await(10, TimeUnit.SECONDS)).isTrue();
        CompletableFuture<CompletedSnapshotHandle> manual =
                periodicSnapshotManager.triggerSnapshotAfterCompaction();
        try {
            assertThat(manual).isNotDone();
            assertThat(target.nextId.get()).isEqualTo(2);
        } finally {
            allowCommit.countDown();
        }
        worker.get(10, TimeUnit.SECONDS);
        asyncSnapshotExecutorService.trigger();
        assertThat(manual.get(10, TimeUnit.SECONDS).getSnapshotId()).isEqualTo(2);
    }

    @Test
    void testCommitFailureFailsManualFuture() {
        TestSnapshotTarget target =
                new TestSnapshotTarget(new FsPath("file:/test/manual")) {
                    @Override
                    public void handleSnapshotResult(
                            long id,
                            int coordinator,
                            int leader,
                            SnapshotLocation location,
                            SnapshotResult result) {
                        throw new FlussRuntimeException("Commit failed");
                    }
                };
        periodicSnapshotManager = createSnapshotManager(target);
        CompletableFuture<CompletedSnapshotHandle> manual =
                periodicSnapshotManager.triggerSnapshotAfterCompaction();
        asyncSnapshotExecutorService.trigger();
        assertThat(manual).isCompletedExceptionally();
        assertThat(org.assertj.core.api.Assertions.catchThrowable(manual::join))
                .hasCauseInstanceOf(FlussRuntimeException.class);
    }

    @Test
    void testCancelManualWaitingForPeriodicAndResumePeriodic() throws Exception {
        TestSnapshotTarget target = new TestSnapshotTarget(new FsPath("file:/test/manual"));
        periodicSnapshotManager = createSnapshotManager(target);
        periodicSnapshotManager.start();
        periodicSnapshotManager.triggerSnapshot();
        CompletableFuture<CompletedSnapshotHandle> manual =
                periodicSnapshotManager.triggerSnapshotAfterCompaction();
        assertThat(periodicSnapshotManager.triggerSnapshotAfterCompaction())
                .isCompletedExceptionally();
        assertThat(periodicSnapshotManager.cancelSnapshotAfterCompaction()).isCompleted();
        assertThat(manual).isCancelled();
        asyncSnapshotExecutorService.trigger();
        assertThat(target.getCollectedRemoteDirs()).hasSize(1);
        scheduledExecutorService.triggerNonPeriodicScheduledTasks();
        asyncSnapshotExecutorService.trigger();
        assertThat(target.getCollectedRemoteDirs()).hasSize(2);
    }

    @Test
    void testCancelBeforeManualRuns() throws Exception {
        TestSnapshotTarget target = new TestSnapshotTarget(new FsPath("file:/test/manual"));
        periodicSnapshotManager = createSnapshotManager(target);
        CompletableFuture<CompletedSnapshotHandle> manual =
                periodicSnapshotManager.triggerSnapshotAfterCompaction();
        assertThat(periodicSnapshotManager.cancelSnapshotAfterCompaction()).isCompleted();
        assertThat(manual).isCancelled();
        asyncSnapshotExecutorService.trigger();
        assertThat(target.nextId.get()).isEqualTo(1);
        CompletableFuture<CompletedSnapshotHandle> next =
                periodicSnapshotManager.triggerSnapshotAfterCompaction();
        asyncSnapshotExecutorService.trigger();
        assertThat(next.get(10, TimeUnit.SECONDS).getSnapshotId()).isEqualTo(1);
    }

    @Test
    void testExecutorRejectionReleasesManualSlot() {
        ExecutorService rejected = java.util.concurrent.Executors.newSingleThreadExecutor();
        rejected.shutdown();
        TestSnapshotTarget target = new TestSnapshotTarget(new FsPath("file:/test/manual"));
        periodicSnapshotManager =
                new PeriodicSnapshotManager(
                        tableBucket, target, 0, rejected, scheduledExecutorService);
        assertThat(periodicSnapshotManager.triggerSnapshotAfterCompaction())
                .isCompletedExceptionally();
        assertThat(periodicSnapshotManager.cancelSnapshotAfterCompaction()).isCompleted();
        assertThat(periodicSnapshotManager.closeAsync()).isCompleted();
        assertThat(target.nextId.get()).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCancellationAndCloseWaitForResourceOwnerExit(boolean runCancellationCallback)
            throws Exception {
        CountDownLatch uploading = new CountDownLatch(1);
        CountDownLatch allowExit = new CountDownLatch(1);
        AtomicBoolean released = new AtomicBoolean();
        TestSnapshotTarget target =
                new TestSnapshotTarget(new FsPath("file:/test/manual")) {
                    @Override
                    public Optional<PeriodicSnapshotManager.SnapshotRunnable> initSnapshot() {
                        AsyncSnapshotCallable<SnapshotResult> callable =
                                new AsyncSnapshotCallable<SnapshotResult>() {
                                    @Override
                                    protected SnapshotResult callInternal() throws Exception {
                                        uploading.countDown();
                                        boolean done = false;
                                        while (!done) {
                                            try {
                                                done = allowExit.await(10, TimeUnit.SECONDS);
                                            } catch (InterruptedException ignored) {
                                                // Model an I/O call that cannot exit until its
                                                // provider unblocks it.
                                            }
                                        }
                                        return new SnapshotResult(
                                                KvSnapshotHandle.create(
                                                        Collections.emptyList(),
                                                        Collections.emptyList(),
                                                        0),
                                                new FsPath("file:/test/manual"),
                                                new TabletState(7, null, null));
                                    }

                                    @Override
                                    protected void cleanupProvidedResources() {
                                        released.set(true);
                                    }
                                };
                        try {
                            return Optional.of(
                                    new PeriodicSnapshotManager.SnapshotRunnable(
                                            callable.toAsyncSnapshotFutureTask(
                                                    new CloseableRegistry()),
                                            1,
                                            0,
                                            0,
                                            snapshotLocation));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
        periodicSnapshotManager = createSnapshotManager(target);
        CompletableFuture<CompletedSnapshotHandle> manual =
                periodicSnapshotManager.triggerSnapshotAfterCompaction();
        CompletableFuture<Void> worker =
                CompletableFuture.runAsync(asyncSnapshotExecutorService::trigger);
        assertThat(uploading.await(10, TimeUnit.SECONDS)).isTrue();
        CompletableFuture<Void> exited = periodicSnapshotManager.cancelSnapshotAfterCompaction();
        if (runCancellationCallback) {
            scheduledExecutorService.trigger();
        }
        CompletableFuture<Void> closed = periodicSnapshotManager.closeAsync();
        try {
            assertThat(manual).isCancelled();
            assertThat(exited).isNotDone();
            assertThat(closed).isNotDone();
            assertThat(released).isFalse();
        } finally {
            allowExit.countDown();
        }
        worker.get(10, TimeUnit.SECONDS);
        exited.get(10, TimeUnit.SECONDS);
        closed.get(10, TimeUnit.SECONDS);
        assertThat(released).isTrue();
        assertThat(target.getCollectedRemoteDirs()).isEmpty();
    }

    @Test
    void testCancelDuringPreparationReleasesCheckpointBeforeExit() throws Exception {
        CountDownLatch prepared = new CountDownLatch(1);
        CountDownLatch finishPreparation = new CountDownLatch(1);
        AtomicBoolean released = new AtomicBoolean();
        AtomicBoolean uploaded = new AtomicBoolean();
        TestSnapshotTarget target =
                new TestSnapshotTarget(new FsPath("file:/test/manual")) {
                    @Override
                    public Optional<PeriodicSnapshotManager.SnapshotRunnable> initSnapshot() {
                        try {
                            AsyncSnapshotCallable<SnapshotResult> callable =
                                    new AsyncSnapshotCallable<SnapshotResult>() {
                                        @Override
                                        protected SnapshotResult callInternal() {
                                            uploaded.set(true);
                                            return null;
                                        }

                                        @Override
                                        protected void cleanupProvidedResources() {
                                            released.set(true);
                                        }
                                    };
                            RunnableFuture<SnapshotResult> future =
                                    callable.toAsyncSnapshotFutureTask(new CloseableRegistry());
                            prepared.countDown();
                            assertThat(finishPreparation.await(10, TimeUnit.SECONDS)).isTrue();
                            return Optional.of(
                                    new PeriodicSnapshotManager.SnapshotRunnable(
                                            future, 1, 0, 0, snapshotLocation));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
        periodicSnapshotManager = createSnapshotManager(target);
        periodicSnapshotManager.triggerSnapshotAfterCompaction();
        CompletableFuture<Void> worker =
                CompletableFuture.runAsync(asyncSnapshotExecutorService::trigger);
        assertThat(prepared.await(10, TimeUnit.SECONDS)).isTrue();
        CompletableFuture<Void> exited = periodicSnapshotManager.cancelSnapshotAfterCompaction();
        try {
            assertThat(exited).isNotDone();
            assertThat(released).isFalse();
        } finally {
            finishPreparation.countDown();
        }
        worker.get(10, TimeUnit.SECONDS);
        exited.get(10, TimeUnit.SECONDS);
        assertThat(released).isTrue();
        assertThat(uploaded).isFalse();
    }

    @Test
    void testCancellationWaitsForCommitExit() throws Exception {
        CountDownLatch committing = new CountDownLatch(1);
        CountDownLatch allowCommit = new CountDownLatch(1);
        TestSnapshotTarget target =
                new TestSnapshotTarget(new FsPath("file:/test/manual")) {
                    @Override
                    public void handleSnapshotResult(
                            long id,
                            int coordinator,
                            int leader,
                            SnapshotLocation location,
                            SnapshotResult result) {
                        committing.countDown();
                        try {
                            assertThat(allowCommit.await(10, TimeUnit.SECONDS)).isTrue();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        super.handleSnapshotResult(id, coordinator, leader, location, result);
                    }
                };
        periodicSnapshotManager = createSnapshotManager(target);
        CompletableFuture<CompletedSnapshotHandle> manual =
                periodicSnapshotManager.triggerSnapshotAfterCompaction();
        CompletableFuture<Void> worker =
                CompletableFuture.runAsync(asyncSnapshotExecutorService::trigger);
        assertThat(committing.await(10, TimeUnit.SECONDS)).isTrue();
        CompletableFuture<Void> exited = periodicSnapshotManager.cancelSnapshotAfterCompaction();
        try {
            assertThat(manual).isCancelled();
            assertThat(exited).isNotDone();
        } finally {
            allowCommit.countDown();
        }
        worker.get(10, TimeUnit.SECONDS);
        exited.get(10, TimeUnit.SECONDS);
        assertThat(target.getCollectedRemoteDirs()).hasSize(1);
        assertThat(target.getCause()).isNull();
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
                scheduledExecutorService);
    }

    private static class NopSnapshotTarget implements PeriodicSnapshotManager.SnapshotTarget {
        private static final NopSnapshotTarget INSTANCE = new NopSnapshotTarget();

        @Override
        public long currentSnapshotId() {
            return 0;
        }

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
        protected final SnapshotLocation snapshotLocation;
        private final List<String> collectedRemoteDirs;
        private final String exceptionMessage;
        private Throwable cause;
        private final AtomicLong nextId = new AtomicLong(1);

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
        public long currentSnapshotId() {
            return 0;
        }

        @Override
        public Optional<PeriodicSnapshotManager.SnapshotRunnable> initSnapshot() {
            RunnableFuture<SnapshotResult> runnableFuture =
                    new FutureTask<>(
                            () -> {
                                if (exceptionMessage != null) {
                                    throw new FlussRuntimeException(exceptionMessage);
                                } else {
                                    return new SnapshotResult(
                                            null, snapshotPath, new TabletState(0, null, null));
                                }
                            });
            long snapshotId = nextId.getAndIncrement();
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

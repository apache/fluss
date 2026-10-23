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

package org.apache.fluss.server.tablet.compaction;

import org.apache.fluss.cluster.compaction.CompactionInfo;
import org.apache.fluss.cluster.compaction.CompactionProgress;
import org.apache.fluss.cluster.compaction.CompactionStatus;
import org.apache.fluss.cluster.compaction.CompactionType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.CompactionInProgressException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.PbBucketCompactionStatus;
import org.apache.fluss.rpc.messages.StartBucketCompactionRequest;
import org.apache.fluss.rpc.util.CommonRpcMessageUtils;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.kv.snapshot.CompletedKvSnapshotCommitter;
import org.apache.fluss.server.kv.snapshot.KvSnapshotDataUploader;
import org.apache.fluss.server.kv.snapshot.TestingCompletedKvSnapshotCommitter;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaTestBase;
import org.apache.fluss.server.utils.CancellationToken;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.utils.clock.ManualClock;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Exercises admission and true exit with real replica, native options and snapshot resources. */
class BucketCompactionRunnerTest extends ReplicaTestBase {
    private final TableBucket bucket = new TableBucket(DATA1_TABLE_ID_PK, 1);
    private final CountDownLatch nativeEntered = new CountDownLatch(1);
    private final CountDownLatch nativeRelease = new CountDownLatch(1);
    private CountDownLatch snapshotEntered = new CountDownLatch(1);
    private CountDownLatch snapshotRelease = new CountDownLatch(1);
    private final CountDownLatch snapshotPublished = new CountDownLatch(1);
    private final CountDownLatch cleanupRelease = new CountDownLatch(1);
    private final AtomicInteger nativeCalls = new AtomicInteger();
    private final List<CancellationToken> tokens = new ArrayList<>();
    private final TestingCompletedKvSnapshotCommitter store =
            new TestingCompletedKvSnapshotCommitter();
    private CompletableFuture<CompactionProgress> lookup;
    private final FlussScheduler timer = new FlussScheduler(1);
    private ExecutorService worker;
    private ExecutorService snapshotWorker;
    private ExecutorService uploader;
    private BucketCompactionRunner runner;
    private final AtomicInteger admissions = new AtomicInteger();
    private final AtomicBoolean admissionBlocked = new AtomicBoolean();
    private Replica replica;
    private long deadline;
    private String operationForToken = "op";
    private int leaderEpochForToken;

    @Override
    protected Configuration getServerConf() {
        Configuration result = super.getServerConf();
        result.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofSeconds(1));
        return result;
    }

    @BeforeEach
    void createRunner() throws Exception {
        deadline = System.currentTimeMillis() + 60000;
        timer.startup();
        worker = Executors.newSingleThreadExecutor();
        snapshotWorker = Executors.newSingleThreadExecutor();
        uploader = Executors.newSingleThreadExecutor();
        TestSnapshotContext context =
                new TestSnapshotContext(remoteDataDir) {
                    @Override
                    public long getSnapshotIntervalMs() {
                        return 0;
                    }

                    @Override
                    public ExecutorService getAsyncOperationsThreadPool() {
                        return snapshotWorker;
                    }

                    @Override
                    public KvSnapshotDataUploader getSnapshotDataUploader() {
                        return new KvSnapshotDataUploader(uploader);
                    }

                    @Override
                    public CompletedKvSnapshotCommitter getCompletedSnapshotReporter() {
                        return (snapshot, coordinator, leader) -> {
                            snapshotEntered.countDown();
                            awaitUninterruptibly(snapshotRelease);
                            store.commitKvSnapshot(snapshot, coordinator, leader);
                            snapshotPublished.countDown();
                            awaitUninterruptibly(cleanupRelease);
                        };
                    }
                };
        replica = makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, bucket, context);
        replica.makeLeader(
                new NotifyLeaderAndIsrData(
                        DATA1_PHYSICAL_TABLE_PATH_PK,
                        bucket,
                        Collections.singletonList(TABLET_SERVER_ID),
                        new LeaderAndIsr(
                                TABLET_SERVER_ID,
                                0,
                                Collections.singletonList(TABLET_SERVER_ID),
                                Collections.emptyList(),
                                INITIAL_COORDINATOR_EPOCH,
                                0)));
        lookup = CompletableFuture.completedFuture(progress("op", TABLET_SERVER_ID, 0));
        runner = newRunner(worker, timer);
    }

    private BucketCompactionRunner newRunner(ExecutorService executor, Scheduler scheduler) {
        return new BucketCompactionRunner(
                TABLET_SERVER_ID,
                ignored -> replica,
                ignored -> lookup,
                scheduler,
                SystemClock.getInstance(),
                () -> {
                    admissions.incrementAndGet();
                    if (admissionBlocked.get()) {
                        throw new IllegalStateException("Disk protection active");
                    }
                },
                executor,
                this::newToken);
    }

    private CancellationToken newToken() {
        StartBucketCompactionRequest observation =
                command(operationForToken, replica.getCoordinatorEpoch(), leaderEpochForToken);
        CancellationToken token =
                new CancellationToken() {
                    @Override
                    public AutoCloseable register(Runnable callback) {
                        AutoCloseable registration = super.register(callback);
                        if (runner.startBucketCompaction(observation)
                                .join()
                                .getPhase()
                                .equals("COMPACTION")) {
                            nativeCalls.incrementAndGet();
                            nativeEntered.countDown();
                            awaitUninterruptibly(nativeRelease);
                        }
                        return registration;
                    }
                };
        tokens.add(token);
        return token;
    }

    @AfterEach
    void closeRunner() throws Exception {
        nativeRelease.countDown();
        snapshotRelease.countDown();
        cleanupRelease.countDown();
        if (runner != null) {
            runner.closeAsync().get(10, TimeUnit.SECONDS);
        }
        if (replica != null) {
            replica.delete().get(10, TimeUnit.SECONDS);
        }
        if (worker != null) {
            worker.shutdownNow();
        }
        if (snapshotWorker != null) {
            snapshotWorker.shutdownNow();
        }
        if (uploader != null) {
            uploader.shutdownNow();
        }
        timer.shutdown();
    }

    @Test
    void testAcceptReturnsBeforeNativeCompletion() throws Exception {
        CompletableFuture<PbBucketCompactionStatus> startFuture =
                runner.startBucketCompaction(command("op"));
        assertThat(startFuture).isCompleted();
        assertThat(nativeEntered.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(status().getStatus()).isEqualTo(CompactionStatus.RUNNING.name());
        assertThat(status().getCoveredOffset()).isEqualTo(replica.getLogHighWatermark());
    }

    @Test
    void testBusySlotFailsWithoutQueue() throws Exception {
        runner.startBucketCompaction(command("op")).get();
        assertThat(nativeEntered.await(10, TimeUnit.SECONDS)).isTrue();
        CompletableFuture<PbBucketCompactionStatus> secondStartFuture =
                runner.startBucketCompaction(command("second"));
        assertThatThrownBy(secondStartFuture::join)
                .hasCauseInstanceOf(CompactionInProgressException.class);
        assertThat(nativeCalls).hasValue(1);
    }

    @Test
    void testCancelDoesNotReleaseRunningNativeSlot() throws Exception {
        runner.startBucketCompaction(command("op")).get();
        assertThat(nativeEntered.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(runner.cancelBucketCompaction("op", bucket)).isCompleted();
        CompletableFuture<Void> usersExitedFuture = CompletableFuture.runAsync(() -> {}, worker);
        assertThat(usersExitedFuture).isNotDone();
        assertThat(status().getStatus()).isEqualTo(CompactionStatus.CANCELLED.name());
        assertThatThrownBy(() -> runner.startBucketCompaction(command("second")).join())
                .hasCauseInstanceOf(CompactionInProgressException.class);
        CompletableFuture<Void> closeExit = runner.closeAsync();
        assertThat(closeExit).isNotDone();
        nativeRelease.countDown();
        usersExitedFuture.get(10, TimeUnit.SECONDS);
        closeExit.get(10, TimeUnit.SECONDS);
    }

    @Test
    void testSuccessRequiresSnapshotCommitAndCleanup() throws Exception {
        nativeRelease.countDown();
        assertThat(runner.startBucketCompaction(command("op"))).isCompleted();
        assertThat(snapshotEntered.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(status().getStatus()).isEqualTo(CompactionStatus.RUNNING.name());
        assertThat(status().getPhase()).isEqualTo("SNAPSHOT");
        assertThat(status().hasSnapshotId()).isFalse();
        assertThat(store.getLatestCompletedSnapshot(bucket)).isNull();
        snapshotRelease.countDown();
        assertThat(snapshotPublished.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(status().getStatus()).isEqualTo(CompactionStatus.RUNNING.name());
        assertThat(runner.startBucketCompaction(command("second"))).isCompletedExceptionally();
        cleanupRelease.countDown();
        drainWorker();
        assertThat(status().getStatus()).isEqualTo(CompactionStatus.SUCCEEDED.name());
        assertThat(status().getSnapshotId())
                .isEqualTo(store.getLatestCompletedSnapshot(bucket).getSnapshotID());
    }

    @Test
    void testCancellationBeforeWorkerStarts() throws Exception {
        CountDownLatch releaseWorker = new CountDownLatch(1);
        worker.execute(() -> awaitUninterruptibly(releaseWorker));
        try {
            assertThat(runner.startBucketCompaction(command("op"))).isCompleted();
            assertThat(runner.cancelBucketCompaction("op", bucket)).isCompleted();
            CompletableFuture<Void> exited = CompletableFuture.runAsync(() -> {}, worker);
            assertThat(exited).isNotDone();
            releaseWorker.countDown();
            exited.get(10, TimeUnit.SECONDS);
            assertThat(nativeCalls).hasValue(0);
        } finally {
            releaseWorker.countDown();
        }
    }

    @Test
    void testLateCommandFailsWithoutPhysicalWork() {
        deadline = System.currentTimeMillis() - 1;
        assertThat(runner.startBucketCompaction(command("op"))).isCompletedExceptionally();
        assertThat(nativeCalls).hasValue(0);
    }

    @Test
    void testLookupCancellationWakesWorker() throws Exception {
        lookup = new CompletableFuture<>();
        CountDownLatch querying = new CountDownLatch(1);
        runner =
                new BucketCompactionRunner(
                        TABLET_SERVER_ID,
                        ignored -> replica,
                        ignored -> {
                            querying.countDown();
                            return lookup;
                        },
                        timer,
                        SystemClock.getInstance(),
                        () -> {},
                        worker,
                        this::newToken);
        runner.startBucketCompaction(command("op")).get();
        assertThat(querying.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(runner.cancelBucketCompaction("op", bucket)).isCompleted();
        drainWorker();
        assertThat(lookup).isNotDone();
        assertThat(nativeCalls).hasValue(0);
    }

    @Test
    void testOriginalReplicaInvalidationCancelsOperation() throws Exception {
        runner.startBucketCompaction(command("op")).get();
        assertThat(nativeEntered.await(10, TimeUnit.SECONDS)).isTrue();
        CompletableFuture<Void> deleted = replica.delete();
        assertThat(deleted).isNotDone();
        assertThat(status().getStatus()).isEqualTo(CompactionStatus.CANCELLED.name());
        assertThat(runner.startBucketCompaction(command("second"))).isCompletedExceptionally();
        nativeRelease.countDown();
        drainWorker();
        deleted.get(10, TimeUnit.SECONDS);
        replica = null;
    }

    @Test
    void testCancellationAcknowledgesBeforeSnapshotCleanup() throws Exception {
        nativeRelease.countDown();
        runner.startBucketCompaction(command("op")).get();
        assertThat(snapshotEntered.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(runner.cancelBucketCompaction("op", bucket)).isCompleted();
        CompletableFuture<Void> exited = CompletableFuture.runAsync(() -> {}, worker);
        assertThat(exited).isNotDone();
        assertThat(runner.startBucketCompaction(command("second"))).isCompletedExceptionally();
        snapshotRelease.countDown();
        cleanupRelease.countDown();
        exited.get(10, TimeUnit.SECONDS);
        assertThat(status().getStatus()).isEqualTo(CompactionStatus.CANCELLED.name());
    }

    @Test
    void testBoundLayoutMismatchFailsBeforePhysicalWork() throws Exception {
        StartBucketCompactionRequest wrong = command("op");
        wrong.getConfigBaselineAt(0).setValue("true");
        runner.startBucketCompaction(wrong).get();
        drainWorker();
        assertThat(status().getStatus()).isEqualTo(CompactionStatus.FAILED.name());
        assertThat(status().getReason()).contains("configuration baseline");
        assertThat(nativeCalls).hasValue(0);
    }

    @Test
    void testExistingWritePressureRejectsBeforeFlushAndPhysicalWork() throws Exception {
        replica.getKvTablet().getRocksDBKv().recordWriteRejected();
        long flushedOffset = replica.getKvTablet().getFlushedLogOffset();
        runner.startBucketCompaction(command("op")).get();
        drainWorker();
        assertThat(status().getStatus()).isEqualTo(CompactionStatus.FAILED.name());
        assertThat(status().getReason()).contains("flush budget exceeded");
        assertThat(nativeCalls).hasValue(0);
        assertThat(snapshotEntered.getCount()).isEqualTo(1);
        assertThat(store.getLatestCompletedSnapshot(bucket)).isNull();
        assertThat(replica.getKvTablet().getFlushedLogOffset()).isEqualTo(flushedOffset);
        assertThat(replica.getKvTablet().getRocksDBKv().wouldExceedFlushBudget(0)).isTrue();
    }

    @Test
    void testStaleCoordinatorDispatchFailsBeforePhysicalWork() throws Exception {
        lookup = CompletableFuture.completedFuture(progress("op", 99, 0));
        runner.startBucketCompaction(command("op")).get();
        drainWorker();
        assertThat(status().getStatus()).isEqualTo(CompactionStatus.FAILED.name());
        assertThat(nativeCalls).hasValue(0);
    }

    @Test
    void testRejectionReleasesReservation() throws Exception {
        ExecutorService rejectsOnce = mock(ExecutorService.class);
        AtomicBoolean rejected = new AtomicBoolean();
        doAnswer(
                        invocation -> {
                            if (rejected.compareAndSet(false, true)) {
                                throw new RejectedExecutionException("test rejection");
                            }
                            worker.execute(invocation.getArgument(0));
                            return null;
                        })
                .when(rejectsOnce)
                .execute(any());
        runner = newRunner(rejectsOnce, timer);
        assertThat(runner.startBucketCompaction(command("op"))).isCompletedExceptionally();
        assertThat(runner.startBucketCompaction(command("op"))).isCompleted();
        assertThat(nativeEntered.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void testRetainedResultDoesNotExecuteAgain() throws Exception {
        nativeRelease.countDown();
        snapshotRelease.countDown();
        cleanupRelease.countDown();
        runner.startBucketCompaction(command("op")).get();
        drainWorker();
        assertThat(runner.startBucketCompaction(command("op")).get().getStatus())
                .isEqualTo(CompactionStatus.SUCCEEDED.name());
        assertThat(nativeCalls).hasValue(1);
    }

    @Test
    void testFailedResultIsRetainedWithinCoordinatorEpoch() throws Exception {
        lookup = CompletableFuture.completedFuture(progress("op", 99, 0));
        runner.startBucketCompaction(command("op")).get();
        drainWorker();
        assertThat(status().getStatus()).isEqualTo(CompactionStatus.FAILED.name());
        lookup = CompletableFuture.completedFuture(progress("op", TABLET_SERVER_ID, 0));
        assertThat(status().getStatus()).isEqualTo(CompactionStatus.FAILED.name());
        assertThat(admissions).hasValue(1);
        assertThat(nativeCalls).hasValue(0);
    }

    @Test
    void testRequestAndObservationAreIndependentSnapshots() throws Exception {
        lookup = new CompletableFuture<>();
        StartBucketCompactionRequest request = command("op");
        PbBucketCompactionStatus accepted = runner.startBucketCompaction(request).get();
        admissionBlocked.set(true);
        request.setOperationId("changed");
        request.getTableBucket().setBucketId(99);
        request.getConfigBaselineAt(0).setValue("true");
        accepted.setOperationId("changed").setStatus("FAILED");
        accepted.getTableBucket().setBucketId(99);
        lookup.complete(progress("op", TABLET_SERVER_ID, 0));
        assertThat(nativeEntered.await(10, TimeUnit.SECONDS)).isTrue();
        PbBucketCompactionStatus observation = status();
        observation.setStatus("FAILED");
        observation.getTableBucket().setBucketId(99);
        assertThat(status().getStatus()).isEqualTo(CompactionStatus.RUNNING.name());
        assertThat(status().getTableBucket().getBucketId()).isEqualTo(bucket.getBucket());
        assertThat(admissions).hasValue(1);
        nativeRelease.countDown();
        snapshotRelease.countDown();
        cleanupRelease.countDown();
        drainWorker();
        PbBucketCompactionStatus result = status();
        result.setStatus("FAILED").setReason("changed");
        result.getTableBucket().setBucketId(99);
        assertThat(status().getStatus()).isEqualTo(CompactionStatus.SUCCEEDED.name());
        assertThat(status().hasReason()).isFalse();
        assertThat(admissions).hasValue(1);
        assertThatThrownBy(() -> runner.startBucketCompaction(command("second")).join())
                .hasRootCauseMessage("Disk protection active");
    }

    @Test
    void testRepeatedRunningCommandDoesNotExecuteAgain() throws Exception {
        runner.startBucketCompaction(command("op")).get();
        assertThat(nativeEntered.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(runner.startBucketCompaction(command("op")).get().getPhase())
                .isEqualTo("COMPACTION");
        assertThat(nativeCalls).hasValue(1);
        assertThatThrownBy(
                        () ->
                                runner.startBucketCompaction(
                                                command("op", replica.getCoordinatorEpoch() + 1, 0))
                                        .join())
                .hasCauseInstanceOf(CompactionInProgressException.class);
        assertThatThrownBy(
                        () ->
                                runner.startBucketCompaction(
                                                command("op", replica.getCoordinatorEpoch(), 1))
                                        .join())
                .hasCauseInstanceOf(CompactionInProgressException.class);
    }

    @Test
    void testFailedResultDoesNotPreventRetryAcrossCoordinatorEpoch() throws Exception {
        lookup = CompletableFuture.completedFuture(progress("op", 99, 0));
        runner.startBucketCompaction(command("op")).get();
        drainWorker();
        assertThat(status().getStatus()).isEqualTo(CompactionStatus.FAILED.name());
        lookup = CompletableFuture.completedFuture(progress("op", TABLET_SERVER_ID, 0));
        advanceCoordinatorEpoch();
        runner.startBucketCompaction(command("op")).get();
        assertThat(nativeEntered.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(status().getStatus()).isEqualTo(CompactionStatus.RUNNING.name());
        assertThat(nativeCalls).hasValue(1);
    }

    @Test
    void testCancelledResultDoesNotPreventRetryAcrossCoordinatorEpoch() throws Exception {
        runner.startBucketCompaction(command("op")).get();
        assertThat(nativeEntered.await(10, TimeUnit.SECONDS)).isTrue();
        runner.cancelBucketCompaction("op", bucket).get();
        advanceCoordinatorEpoch();
        assertThatThrownBy(() -> runner.startBucketCompaction(command("op")).join())
                .hasCauseInstanceOf(CompactionInProgressException.class);
        nativeRelease.countDown();
        drainWorker();
        assertThat(
                        runner.startBucketCompaction(command("op", INITIAL_COORDINATOR_EPOCH, 0))
                                .get()
                                .getStatus())
                .isEqualTo(CompactionStatus.CANCELLED.name());
        runner.startBucketCompaction(command("op")).get();
        assertThat(snapshotEntered.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(nativeCalls).hasValue(2);
    }

    @Test
    void testSuccessfulResultReuseRequiresSameLeaderEpoch() throws Exception {
        nativeRelease.countDown();
        snapshotRelease.countDown();
        cleanupRelease.countDown();
        runner.startBucketCompaction(command("op")).get();
        drainWorker();
        advanceCoordinatorEpoch();
        assertThat(runner.startBucketCompaction(command("op")).get().getStatus())
                .isEqualTo(CompactionStatus.SUCCEEDED.name());
        assertThat(nativeCalls).hasValue(1);
        lookup = CompletableFuture.completedFuture(progress("op", TABLET_SERVER_ID, 1));
        leaderEpochForToken = 1;
        assertThat(
                        runner.startBucketCompaction(
                                        command("op", replica.getCoordinatorEpoch(), 1))
                                .get()
                                .getStatus())
                .isEqualTo(CompactionStatus.RUNNING.name());
        drainWorker();
        PbBucketCompactionStatus failed =
                runner.startBucketCompaction(command("op", replica.getCoordinatorEpoch(), 1)).get();
        assertThat(failed.getStatus()).isEqualTo(CompactionStatus.FAILED.name());
        assertThat(failed.getLeaderEpoch()).isEqualTo(1);
        assertThat(nativeCalls).hasValue(1);
    }

    private void advanceCoordinatorEpoch() throws Exception {
        replica.makeLeader(
                new NotifyLeaderAndIsrData(
                        DATA1_PHYSICAL_TABLE_PATH_PK,
                        bucket,
                        Collections.singletonList(TABLET_SERVER_ID),
                        new LeaderAndIsr(
                                TABLET_SERVER_ID,
                                0,
                                Collections.singletonList(TABLET_SERVER_ID),
                                Collections.emptyList(),
                                replica.getCoordinatorEpoch() + 1,
                                0)));
    }

    @Test
    void testWorkerReuseRemovesOldWakeupAndTimer() throws Exception {
        Scheduler scheduler = mock(Scheduler.class);
        List<Runnable> callbacks = new ArrayList<>();
        when(scheduler.scheduleOnce(any(), any(), anyLong()))
                .thenAnswer(
                        invocation -> {
                            callbacks.add(invocation.getArgument(1));
                            return mock(ScheduledFuture.class);
                        });
        runner = newRunner(worker, scheduler);
        nativeRelease.countDown();
        snapshotRelease.countDown();
        cleanupRelease.countDown();
        runner.startBucketCompaction(command("op")).get();
        drainWorker();
        assertThat(status().getStatus()).isEqualTo(CompactionStatus.SUCCEEDED.name());
        lookup = CompletableFuture.completedFuture(progress("second", TABLET_SERVER_ID, 0));
        operationForToken = "second";
        snapshotRelease = new CountDownLatch(1);
        snapshotEntered = new CountDownLatch(1);
        runner.startBucketCompaction(command("second")).get();
        assertThat(snapshotEntered.await(10, TimeUnit.SECONDS)).isTrue();
        callbacks.get(0).run();
        tokens.get(0).cancel();
        snapshotRelease.countDown();
        drainWorker();
        assertThat(runner.startBucketCompaction(command("second")).get().getStatus())
                .isEqualTo(CompactionStatus.SUCCEEDED.name());
    }

    @Test
    void testOriginalDeadlineRetainsNativeSlotAndExpiresResult() throws Exception {
        Scheduler scheduler = mock(Scheduler.class);
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        List<Runnable> callbacks = new ArrayList<>();
        when(scheduler.scheduleOnce(any(), any(), anyLong()))
                .thenAnswer(
                        invocation -> {
                            callbacks.add(invocation.getArgument(1));
                            return mock(ScheduledFuture.class);
                        });
        runner =
                new BucketCompactionRunner(
                        TABLET_SERVER_ID,
                        ignored -> replica,
                        ignored -> lookup,
                        scheduler,
                        clock,
                        () -> {},
                        worker,
                        this::newToken);
        runner.startBucketCompaction(command("op")).get();
        assertThat(nativeEntered.await(10, TimeUnit.SECONDS)).isTrue();
        clock.advanceTime(2, TimeUnit.MINUTES);
        callbacks.get(0).run();
        assertThat(status().getStatus()).isEqualTo(CompactionStatus.CANCELLED.name());
        assertThat(runner.startBucketCompaction(command("second"))).isCompletedExceptionally();
        nativeRelease.countDown();
        drainWorker();
        assertThat(runner.startBucketCompaction(command("op"))).isCompletedExceptionally();
    }

    private void drainWorker() throws Exception {
        worker.submit(() -> {}).get(10, TimeUnit.SECONDS);
    }

    private PbBucketCompactionStatus status() throws Exception {
        return runner.startBucketCompaction(command("op")).get(10, TimeUnit.SECONDS);
    }

    private StartBucketCompactionRequest command(String operation) {
        return command(operation, replica.getCoordinatorEpoch(), 0);
    }

    private StartBucketCompactionRequest command(
            String operation, int coordinatorEpoch, int leaderEpoch) {
        StartBucketCompactionRequest request =
                new StartBucketCompactionRequest()
                        .setOperationId(operation)
                        .setType(CompactionType.RECLAIM.name())
                        .setCoordinatorEpoch(coordinatorEpoch)
                        .setLeaderEpoch(leaderEpoch)
                        .setDeadline(deadline);
        CommonRpcMessageUtils.toCompactionBucket(bucket, request.setTableBucket());
        request.addConfigBaseline()
                .setKey(ConfigOptions.KV_PARTITIONED_INDEX_FILTERS.key())
                .setValue("false");
        return request;
    }

    private CompactionProgress progress(String operation, int server, int epoch) {
        return new CompactionProgress(
                new CompactionInfo(
                        operation,
                        CompactionType.RECLAIM,
                        CompactionStatus.RUNNING,
                        0,
                        null,
                        null,
                        null),
                1,
                0,
                1,
                0,
                0,
                0,
                Collections.singletonList(
                        new CompactionProgress.Execution(
                                server, bucket, epoch, "DISPATCHED", 0, null, null)),
                null,
                0,
                0,
                0);
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}

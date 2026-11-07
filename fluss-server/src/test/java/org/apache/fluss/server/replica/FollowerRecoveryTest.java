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

package org.apache.fluss.server.replica;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.LogStorageException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.log.LogManager;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.checkpoint.OffsetCheckpointFile;
import org.apache.fluss.server.replica.fetcher.ReplicaFetcherManager;
import org.apache.fluss.server.testutils.ServerTestTags;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_BUCKET_EPOCH;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_LEADER_EPOCH;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsWithWriterId;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests directory grouping and real log recovery through the follower transition. */
class FollowerRecoveryTest extends ReplicaTestBase {
    private volatile CountDownLatch recoveryStarted;
    private volatile CountDownLatch releaseRecovery;
    private volatile File blockedDirectory;
    private volatile boolean failRecovery;
    private volatile Consumer<Replica> beforeRecovery;
    private volatile Consumer<Replica> afterRecovery;

    @Override
    protected void beforeFollowerTruncation(Replica replica) {
        if (beforeRecovery != null) {
            beforeRecovery.accept(replica);
        }
        if (recoveryStarted == null) {
            return;
        }
        assertThat(fetcherBucketCount()).isZero();
        recoveryStarted.countDown();
        if (replica.getLogTablet().getDataDir().equals(blockedDirectory)) {
            await(releaseRecovery);
        } else if (failRecovery) {
            throw new LogStorageException("injected recovery failure");
        }
    }

    @Override
    protected void afterFollowerTruncation(Replica replica) {
        if (afterRecovery != null) {
            afterRecovery.accept(replica);
        }
    }

    @Override
    protected Configuration getServerConf() {
        Configuration conf = super.getServerConf();
        conf.set(ConfigOptions.LOG_REPLICA_FOLLOWER_RECOVERY_ENABLED, true);
        return conf;
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    @Tag(ServerTestTags.JBOD_MULTI_DIR_TAG)
    void testDirectoryThreadsControlOverlappingRealTruncation(int threadsPerDirectory)
            throws Exception {
        replicaManager.shutdown();
        conf.set(
                ConfigOptions.LOG_REPLICA_FOLLOWER_RECOVERY_THREADS_PER_DATA_DIR,
                threadsPerDirectory);
        replicaManager = buildReplicaManager(testCoordinatorGateway);
        replicaManager.startup();
        List<Replica> replicas = createReplicas(3);
        prepareLogs(replicas);
        File firstDirectory = replicas.get(0).getLogTablet().getDataDir();
        File secondDirectory = replicas.get(1).getLogTablet().getDataDir();
        assertThat(replicas.get(2).getLogTablet().getDataDir()).isEqualTo(firstDirectory);
        assertThat(firstDirectory).isNotEqualTo(secondDirectory);
        Object firstWriteLock = checkpointWriteLock(firstDirectory);
        Object secondWriteLock = checkpointWriteLock(secondDirectory);
        List<Thread> workers = new CopyOnWriteArrayList<>();
        AtomicInteger completed = new AtomicInteger();
        beforeRecovery = replica -> workers.add(Thread.currentThread());
        afterRecovery = replica -> completed.incrementAndGet();
        List<NotifyLeaderAndIsrData> notifications = new ArrayList<>();
        for (Replica replica : replicas) {
            notifications.add(followerNotification(replica.getTableBucket()));
        }
        CompletableFuture<Void> result = new CompletableFuture<>();
        Thread coordinator =
                new Thread(() -> transition(INITIAL_COORDINATOR_EPOCH, notifications, result));
        try {
            // Block publication only: independent truncation and writer recovery still execute.
            synchronized (firstWriteLock) {
                synchronized (secondWriteLock) {
                    coordinator.start();
                    int inFlight = threadsPerDirectory + 1;
                    waitUntil(
                            () ->
                                    replicas.stream()
                                                    .filter(
                                                            replica ->
                                                                    replica.getLogTablet()
                                                                                    .localLogEndOffset()
                                                                            == 10)
                                                    .count()
                                            == inFlight,
                            Duration.ofSeconds(10),
                            "Configured directory workers did not overlap real truncation");
                    assertThat(result).isNotDone();
                    assertThat(completed.get()).isZero();
                    assertThat(workers.stream().distinct().count()).isEqualTo(inFlight);
                    assertThat(replicas.get(1).getLogTablet().localLogEndOffset()).isEqualTo(10);
                    assertThat(replicas.get(2).getLogTablet().localLogEndOffset())
                            .isEqualTo(threadsPerDirectory == 1 ? 20 : 10);
                    assertThat(fetcherBucketCount()).isZero();
                }
            }
            result.get(10, TimeUnit.SECONDS);
            assertThat(completed.get()).isEqualTo(3);
            for (Thread worker : workers) {
                worker.join(10000);
                assertThat(worker.isAlive()).isFalse();
            }
            assertRecoveredLogs(replicas);
        } finally {
            coordinator.join(10000);
        }
    }

    private Object checkpointWriteLock(File directory) throws Exception {
        Map<?, ?> checkpoints =
                (Map<?, ?>) field(logManager, LogManager.class, "recoveryPointCheckpoints");
        Object checkpoint =
                field(checkpoints.get(directory), OffsetCheckpointFile.class, "checkpoint");
        return field(checkpoint, checkpoint.getClass(), "lock");
    }

    @Test
    @Tag(ServerTestTags.JBOD_MULTI_DIR_TAG)
    void testFailureDrainsOtherAcceptedReplicas() throws Exception {
        List<Replica> replicas = createReplicas(3);
        FollowerRecoveryExecutor recovery = new FollowerRecoveryExecutor(true, 1);
        List<Replica> visited = new CopyOnWriteArrayList<>();
        LogStorageException failure = new LogStorageException("injected failure");
        assertThatThrownBy(
                        () ->
                                recovery.recover(
                                        replicas,
                                        replica -> {
                                            visited.add(replica);
                                            if (replica == replicas.get(0)) {
                                                throw failure;
                                            }
                                        }))
                .isSameAs(failure);
        assertThat(visited).containsExactlyInAnyOrderElementsOf(replicas);
    }

    @Test
    void testDefaultSerialRecoveryKeepsCallerAndInputOrder() throws Exception {
        List<Replica> replicas = createReplicas(3);
        Collections.reverse(replicas);
        Configuration defaults = new Configuration();
        FollowerRecoveryExecutor recovery =
                new FollowerRecoveryExecutor(
                        defaults.get(ConfigOptions.LOG_REPLICA_FOLLOWER_RECOVERY_ENABLED),
                        defaults.get(
                                ConfigOptions.LOG_REPLICA_FOLLOWER_RECOVERY_THREADS_PER_DATA_DIR));
        Thread caller = Thread.currentThread();
        List<Replica> visited = new ArrayList<>();
        recovery.recover(
                replicas,
                replica -> {
                    assertThat(Thread.currentThread()).isSameAs(caller);
                    visited.add(replica);
                });
        assertThat(visited).containsExactlyElementsOf(replicas);
        LogStorageException failure = new LogStorageException("serial failure");
        assertThatThrownBy(
                        () ->
                                recovery.recover(
                                        replicas,
                                        replica -> {
                                            throw failure;
                                        }))
                .isSameAs(failure);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @Tag(ServerTestTags.JBOD_MULTI_DIR_TAG)
    void testRealLogWriterStateAndRecoveryCheckpoint(boolean enabled) throws Exception {
        List<Replica> replicas = createReplicas(3);
        prepareLogs(replicas);
        FollowerRecoveryExecutor recovery = new FollowerRecoveryExecutor(enabled, 1);
        recovery.recover(
                replicas, replica -> replica.truncateTo(replica.getLogTablet().getHighWatermark()));
        assertRecoveredLogs(replicas);
    }

    @Test
    @Tag(ServerTestTags.JBOD_MULTI_DIR_TAG)
    void testMakeFollowersRecoversRealLogsBeforeCompletion() throws Exception {
        List<Replica> replicas = createReplicas(3);
        prepareLogs(replicas);
        List<NotifyLeaderAndIsrData> notifications = new ArrayList<>();
        for (Replica replica : replicas) {
            notifications.add(followerNotification(replica.getTableBucket()));
        }
        AtomicInteger callbackCount = new AtomicInteger();
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                notifications,
                result -> {
                    assertThat(result)
                            .hasSize(3)
                            .allSatisfy(
                                    bucket -> assertThat(bucket.getError().isFailure()).isFalse());
                    for (Replica replica : replicas) {
                        assertThat(replica.getLogTablet().localLogEndOffset()).isEqualTo(10);
                    }
                    callbackCount.incrementAndGet();
                });
        assertThat(callbackCount.get()).isEqualTo(1);
        assertRecoveredLogs(replicas);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    @Tag(ServerTestTags.JBOD_MULTI_DIR_TAG)
    void testFetcherAdmissionWaitsForWholeRecoveryBatch(int mode) throws Exception {
        boolean fail = mode == 1;
        boolean interrupt = mode == 2;
        List<Replica> replicas = createReplicas(2);
        List<NotifyLeaderAndIsrData> initial = new ArrayList<>();
        List<NotifyLeaderAndIsrData> transition = new ArrayList<>();
        for (Replica replica : replicas) {
            initial.add(followerNotification(replica.getTableBucket(), 2, 1));
            transition.add(followerNotification(replica.getTableBucket(), 3, 2));
        }
        replicaManager.becomeLeaderOrFollower(INITIAL_COORDINATOR_EPOCH, initial, ignored -> {});
        assertThat(fetcherBucketCount()).isEqualTo(2);
        recoveryStarted = new CountDownLatch(2);
        releaseRecovery = new CountDownLatch(1);
        blockedDirectory = replicas.get(1).getLogTablet().getDataDir();
        failRecovery = fail;
        AtomicBoolean callback = new AtomicBoolean();
        AtomicBoolean restoredInterrupt = new AtomicBoolean();
        CompletableFuture<Throwable> result = new CompletableFuture<>();
        Thread coordinator =
                new Thread(
                        () -> {
                            Throwable failure = null;
                            try {
                                replicaManager.becomeLeaderOrFollower(
                                        INITIAL_COORDINATOR_EPOCH,
                                        transition,
                                        ignored -> callback.set(true));
                            } catch (Throwable t) {
                                failure = t;
                            }
                            restoredInterrupt.set(Thread.currentThread().isInterrupted());
                            result.complete(failure);
                        });
        try {
            coordinator.start();
            assertThat(recoveryStarted.await(10, TimeUnit.SECONDS)).isTrue();
            waitUntil(
                    () -> coordinator.getState() == Thread.State.WAITING,
                    Duration.ofSeconds(10),
                    "Coordinator did not await recovery");
            if (interrupt) {
                coordinator.interrupt();
                waitUntil(
                        () ->
                                !coordinator.isInterrupted()
                                        && coordinator.getState() == Thread.State.WAITING,
                        Duration.ofSeconds(10),
                        "Interrupted coordinator did not keep draining");
            }
            assertThat(result).isNotDone();
            assertThat(callback).isFalse();
            assertThat(fetcherBucketCount()).isZero();
            releaseRecovery.countDown();
            Throwable failure = result.get(10, TimeUnit.SECONDS);
            if (fail || interrupt) {
                if (interrupt) {
                    assertThat(failure).hasCauseInstanceOf(InterruptedException.class);
                    assertThat(restoredInterrupt).isTrue();
                } else {
                    assertThat(failure).isInstanceOf(LogStorageException.class);
                }
                assertThat(callback).isFalse();
                assertThat(fetcherBucketCount()).isZero();
            } else {
                assertThat(failure).isNull();
                assertThat(callback).isTrue();
                assertThat(fetcherBucketCount()).isEqualTo(2);
            }
        } finally {
            releaseRecovery.countDown();
            coordinator.join(10000);
        }
    }

    @Test
    @Tag(ServerTestTags.JBOD_MULTI_DIR_TAG)
    void testConcurrentRoleTransitionWaitsForRecoveryUnderManagerLock() throws Exception {
        List<Replica> replicas = createReplicas(2);
        prepareLogs(replicas);
        recoveryStarted = new CountDownLatch(2);
        releaseRecovery = new CountDownLatch(1);
        blockedDirectory = replicas.get(1).getLogTablet().getDataDir();
        List<NotifyLeaderAndIsrData> followers = new ArrayList<>();
        List<NotifyLeaderAndIsrData> leaders = new ArrayList<>();
        for (Replica replica : replicas) {
            followers.add(followerNotification(replica.getTableBucket(), 2, 1));
            leaders.add(
                    new NotifyLeaderAndIsrData(
                            DATA1_PHYSICAL_TABLE_PATH,
                            replica.getTableBucket(),
                            Arrays.asList(TABLET_SERVER_ID, 2, 3),
                            new LeaderAndIsr(
                                    TABLET_SERVER_ID,
                                    INITIAL_LEADER_EPOCH + 2,
                                    Collections.singletonList(TABLET_SERVER_ID),
                                    Collections.emptyList(),
                                    INITIAL_COORDINATOR_EPOCH + 1,
                                    INITIAL_BUCKET_EPOCH + 2),
                            TEST_ROUTING_BUCKET_COUNT,
                            0L));
        }
        CompletableFuture<Void> first = new CompletableFuture<>();
        CompletableFuture<Void> second = new CompletableFuture<>();
        Thread firstTransition =
                new Thread(() -> transition(INITIAL_COORDINATOR_EPOCH, followers, first));
        Thread secondTransition =
                new Thread(() -> transition(INITIAL_COORDINATOR_EPOCH + 1, leaders, second));
        ReentrantLock managerLock =
                (ReentrantLock)
                        field(replicaManager, ReplicaManager.class, "replicaStateChangeLock");
        try {
            firstTransition.start();
            assertThat(recoveryStarted.await(10, TimeUnit.SECONDS)).isTrue();
            waitUntil(
                    () -> replicas.get(0).getLogTablet().localLogEndOffset() == 10,
                    Duration.ofSeconds(10),
                    "First directory did not recover");
            secondTransition.start();
            waitUntil(
                    () -> managerLock.hasQueuedThread(secondTransition),
                    Duration.ofSeconds(10),
                    "Second transition did not wait on the manager lock");
            assertThat(first).isNotDone();
            assertThat(second).isNotDone();
            assertThat(replicaManager.getCoordinatorEpoch()).isEqualTo(INITIAL_COORDINATOR_EPOCH);
            for (Replica replica : replicas) {
                assertThat(replica.isLeader()).isFalse();
                assertThat(replica.getLeaderId()).isEqualTo(2);
                assertThat(replica.getLeaderEpoch()).isEqualTo(INITIAL_LEADER_EPOCH + 1);
                assertThat(replica.getBucketEpoch()).isEqualTo(INITIAL_BUCKET_EPOCH + 1);
            }
            assertThat(fetcherBucketCount()).isZero();
            releaseRecovery.countDown();
            first.get(10, TimeUnit.SECONDS);
            second.get(10, TimeUnit.SECONDS);
            assertThat(replicaManager.getCoordinatorEpoch())
                    .isEqualTo(INITIAL_COORDINATOR_EPOCH + 1);
            for (Replica replica : replicas) {
                assertThat(replica.isLeader()).isTrue();
                assertThat(replica.getLeaderId()).isEqualTo(TABLET_SERVER_ID);
                assertThat(replica.getLeaderEpoch()).isEqualTo(INITIAL_LEADER_EPOCH + 2);
                assertThat(replica.getBucketEpoch()).isEqualTo(INITIAL_BUCKET_EPOCH + 2);
                assertThat(replica.getCoordinatorEpoch()).isEqualTo(INITIAL_COORDINATOR_EPOCH + 1);
            }
            assertRecoveredLogs(replicas);
            assertThat(fetcherBucketCount()).isZero();
        } finally {
            releaseRecovery.countDown();
            firstTransition.join(10000);
            secondTransition.join(10000);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @Tag(ServerTestTags.JBOD_MULTI_DIR_TAG)
    void testFailedRecoveryNeedsHigherEpochToRetry(boolean enabled) throws Exception {
        replicaManager.shutdown();
        conf.set(ConfigOptions.LOG_REPLICA_FOLLOWER_RECOVERY_ENABLED, enabled);
        replicaManager = buildReplicaManager(testCoordinatorGateway);
        replicaManager.startup();
        List<Replica> replicas = createReplicas(2);
        prepareLogs(replicas);
        Replica earlier = replicas.get(0);
        Replica later = replicas.get(1);
        CountDownLatch earlierTruncated = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger completed = new AtomicInteger();
        LogStorageException failure = new LogStorageException("later bucket recovery failed");
        List<Thread> recoveryWorkers = new CopyOnWriteArrayList<>();
        beforeRecovery =
                replica -> {
                    attempts.incrementAndGet();
                    if (enabled) {
                        recoveryWorkers.add(Thread.currentThread());
                    }
                    if (replica == later) {
                        await(earlierTruncated);
                        throw failure;
                    }
                };
        afterRecovery =
                replica -> {
                    completed.incrementAndGet();
                    if (replica == earlier) {
                        earlierTruncated.countDown();
                    }
                };
        List<NotifyLeaderAndIsrData> initial = new ArrayList<>();
        List<NotifyLeaderAndIsrData> retry = new ArrayList<>();
        for (Replica replica : replicas) {
            initial.add(followerNotification(replica.getTableBucket(), 2, 1));
            retry.add(followerNotification(replica.getTableBucket(), 2, 2));
        }
        AtomicBoolean failedCallback = new AtomicBoolean();
        assertThatThrownBy(
                        () ->
                                replicaManager.becomeLeaderOrFollower(
                                        INITIAL_COORDINATOR_EPOCH,
                                        initial,
                                        ignored -> failedCallback.set(true)))
                .isSameAs(failure);
        assertThat(failedCallback).isFalse();
        assertThat(attempts.get()).isEqualTo(2);
        assertThat(completed.get()).isEqualTo(1);
        assertRecoveredLogs(Collections.singletonList(earlier));
        assertThat(later.getLogTablet().localLogEndOffset()).isEqualTo(20);
        assertThat(later.getLogTablet().getHighWatermark()).isEqualTo(10);
        assertThat(
                        later.getLogTablet()
                                .writerStateManager()
                                .lastEntry(101)
                                .get()
                                .lastBatchSequence())
                .isEqualTo(1);
        assertThat(fetcherBucketCount()).isZero();
        for (Thread worker : recoveryWorkers) {
            worker.join(10000);
            assertThat(worker.isAlive()).isFalse();
        }
        for (Replica replica : replicas) {
            assertThat(replica.getLeaderEpoch()).isEqualTo(INITIAL_LEADER_EPOCH + 1);
            assertThat(replica.getBucketEpoch()).isEqualTo(INITIAL_BUCKET_EPOCH + 1);
            assertThat(replica.getLeaderId()).isEqualTo(2);
        }
        beforeRecovery = ignored -> attempts.incrementAndGet();
        // makeFollower has already advanced the epoch, so a same-epoch notification skips recovery.
        AtomicBoolean sameEpochCallback = new AtomicBoolean();
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                initial,
                result -> {
                    assertThat(result)
                            .allSatisfy(bucket -> assertThat(bucket.succeeded()).isTrue());
                    sameEpochCallback.set(true);
                });
        assertThat(sameEpochCallback).isTrue();
        assertThat(attempts.get()).isEqualTo(2);
        assertThat(completed.get()).isEqualTo(1);
        assertThat(later.getLogTablet().localLogEndOffset()).isEqualTo(20);
        assertThat(
                        later.getLogTablet()
                                .writerStateManager()
                                .lastEntry(101)
                                .get()
                                .lastBatchSequence())
                .isEqualTo(1);
        assertThat(fetcherBucketCount()).isZero();
        AtomicBoolean higherEpochCallback = new AtomicBoolean();
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                retry,
                result -> {
                    assertThat(result)
                            .allSatisfy(bucket -> assertThat(bucket.succeeded()).isTrue());
                    higherEpochCallback.set(true);
                });
        assertThat(higherEpochCallback).isTrue();
        assertThat(attempts.get()).isEqualTo(4);
        assertThat(completed.get()).isEqualTo(3);
        assertRecoveredLogs(replicas);
        assertThat(fetcherBucketCount()).isEqualTo(2);
        for (Replica replica : replicas) {
            assertThat(replica.getLeaderEpoch()).isEqualTo(INITIAL_LEADER_EPOCH + 2);
            assertThat(replica.getBucketEpoch()).isEqualTo(INITIAL_BUCKET_EPOCH + 2);
            assertThat(replica.getLeaderId()).isEqualTo(2);
        }
    }

    private void transition(
            int coordinatorEpoch,
            List<NotifyLeaderAndIsrData> notifications,
            CompletableFuture<Void> result) {
        try {
            replicaManager.becomeLeaderOrFollower(coordinatorEpoch, notifications, ignored -> {});
            result.complete(null);
        } catch (Throwable failure) {
            result.completeExceptionally(failure);
        }
    }

    private static Object field(Object owner, Class<?> type, String name) throws Exception {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(owner);
    }

    private int fetcherBucketCount() {
        try {
            Field field = ReplicaFetcherManager.class.getDeclaredField("fetcherThreadMap");
            field.setAccessible(true);
            Map<?, ?> fetchers = (Map<?, ?>) field.get(replicaManager.getReplicaFetcherManager());
            int buckets = 0;
            for (Object fetcher : fetchers.values()) {
                Method getBucketCount = fetcher.getClass().getDeclaredMethod("getBucketCount");
                getBucketCount.setAccessible(true);
                buckets += (Integer) getBucketCount.invoke(fetcher);
            }
            return buckets;
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private List<Replica> createReplicas(int count) {
        List<Replica> replicas = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            makeLogTableAsLeader(i);
            replicas.add(replicaManager.getReplicaOrException(new TableBucket(DATA1_TABLE_ID, i)));
        }
        return replicas;
    }

    private void prepareLogs(List<Replica> replicas) throws Exception {
        for (Replica replica : replicas) {
            replica.appendRecordsToLeader(genMemoryLogRecordsWithWriterId(DATA1, 101, 0, 0), 0);
            replica.appendRecordsToLeader(genMemoryLogRecordsWithWriterId(DATA1, 101, 1, 10), 0);
            LogTablet log = replica.getLogTablet();
            log.flush(true);
            log.updateHighWatermark(10);
            assertThat(log.getRecoveryPoint()).isEqualTo(20);
            assertThat(log.writerStateManager().lastEntry(101).get().lastBatchSequence())
                    .isEqualTo(1);
        }
    }

    private void assertRecoveredLogs(List<Replica> replicas) throws Exception {
        for (Replica replica : replicas) {
            LogTablet log = replica.getLogTablet();
            assertThat(log.localLogEndOffset()).isEqualTo(10);
            assertThat(log.getHighWatermark()).isEqualTo(10);
            assertThat(log.getRecoveryPoint()).isEqualTo(10);
            assertThat(log.writerStateManager().mapEndOffset()).isEqualTo(10);
            assertThat(log.writerStateManager().lastEntry(101).get().lastBatchSequence())
                    .isEqualTo(0);
            assertThat(
                            new OffsetCheckpointFile(
                                            new File(
                                                    log.getDataDir(),
                                                    "recovery-point-offset-checkpoint"))
                                    .read())
                    .containsEntry(replica.getTableBucket(), 10L);
        }
    }

    private NotifyLeaderAndIsrData followerNotification(TableBucket bucket) {
        return followerNotification(bucket, LeaderAndIsr.NO_LEADER, 1);
    }

    private NotifyLeaderAndIsrData followerNotification(
            TableBucket bucket, int leader, int epochDelta) {
        return new NotifyLeaderAndIsrData(
                DATA1_PHYSICAL_TABLE_PATH,
                bucket,
                Arrays.asList(TABLET_SERVER_ID, 2, 3),
                new LeaderAndIsr(
                        leader,
                        INITIAL_LEADER_EPOCH + epochDelta,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        INITIAL_COORDINATOR_EPOCH,
                        INITIAL_BUCKET_EPOCH + epochDelta),
                TEST_ROUTING_BUCKET_COUNT,
                0L);
    }

    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }
}

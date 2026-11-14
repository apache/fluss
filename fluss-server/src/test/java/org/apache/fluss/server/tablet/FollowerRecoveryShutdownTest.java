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

package org.apache.fluss.server.tablet;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.kv.scan.ScannerManager;
import org.apache.fluss.server.log.LogManager;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.checkpoint.OffsetCheckpointFile;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.replica.ReplicaTestBase;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

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

/** Tests shutdown with real follower truncation and its storage handles still in flight. */
class FollowerRecoveryShutdownTest extends ReplicaTestBase {
    private volatile Thread recoveryWorker;

    @Override
    protected Configuration getServerConf() {
        Configuration conf = super.getServerConf();
        conf.set(ConfigOptions.LOG_REPLICA_FOLLOWER_RECOVERY_ENABLED, true);
        conf.set(ConfigOptions.LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL, Duration.ofDays(1));
        return conf;
    }

    @Override
    protected void beforeFollowerTruncation(Replica replica) {
        recoveryWorker = Thread.currentThread();
    }

    @Test
    void testStopServicesDrainsRealTruncationBeforeClosingItsDependencies() throws Exception {
        makeLogTableAsLeader(0);
        TableBucket bucket = new TableBucket(DATA1_TABLE_ID, 0);
        Replica replica = replicaManager.getReplicaOrException(bucket);
        replica.appendRecordsToLeader(genMemoryLogRecordsWithWriterId(DATA1, 101, 0, 0), 0);
        replica.appendRecordsToLeader(genMemoryLogRecordsWithWriterId(DATA1, 101, 1, 10), 0);
        LogTablet log = replica.getLogTablet();
        log.flush(true);
        log.updateHighWatermark(10);
        FileChannel logChannel = log.logSegments().get(0).getFileLogRecords().channel();
        Map<?, ?> checkpoints =
                (Map<?, ?>) field(logManager, LogManager.class, "recoveryPointCheckpoints");
        Object checkpoint =
                field(checkpoints.get(log.getDataDir()), OffsetCheckpointFile.class, "checkpoint");
        Object checkpointLock = field(checkpoint, checkpoint.getClass(), "lock");
        Object recovery = field(replicaManager, ReplicaManager.class, "followerRecoveryExecutor");
        FlussScheduler scheduler = (FlussScheduler) field(this, ReplicaTestBase.class, "scheduler");
        AtomicBoolean scannerClosed =
                (AtomicBoolean) field(scannerManager, ScannerManager.class, "closed");
        RpcClient ownedRpcClient = rpcClient;
        ExecutorService coordinator = Executors.newSingleThreadExecutor();
        TabletServer server = new TabletServer(conf);
        setServerField(server, "replicaManager", replicaManager);
        setServerField(server, "logManager", logManager);
        setServerField(server, "kvManager", kvManager);
        setServerField(server, "scannerManager", scannerManager);
        setServerField(server, "rpcClient", rpcClient);
        setServerField(server, "scheduler", scheduler);
        setServerField(server, "localDiskManager", localDiskManager);
        setServerField(server, "replicaStateChangeExecutor", coordinator);
        NotifyLeaderAndIsrData follower =
                new NotifyLeaderAndIsrData(
                        DATA1_PHYSICAL_TABLE_PATH,
                        bucket,
                        Arrays.asList(TABLET_SERVER_ID, 2),
                        new LeaderAndIsr(
                                LeaderAndIsr.NO_LEADER,
                                INITIAL_LEADER_EPOCH + 1,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                INITIAL_COORDINATOR_EPOCH,
                                INITIAL_BUCKET_EPOCH + 1),
                        TEST_ROUTING_BUCKET_COUNT,
                        0L);
        AtomicBoolean recovered = new AtomicBoolean();
        CompletableFuture<Void> stopped = new CompletableFuture<>();
        Thread closer =
                new Thread(
                        () -> {
                            try {
                                server.stopServices().get(10, TimeUnit.SECONDS);
                                stopped.complete(null);
                            } catch (Throwable failure) {
                                stopped.completeExceptionally(failure);
                            }
                        });
        Future<?> transition = null;
        try {
            // Hold only the existing checkpoint monitor; truncation and writer replay run normally.
            synchronized (checkpointLock) {
                transition =
                        coordinator.submit(
                                () ->
                                        replicaManager.becomeLeaderOrFollower(
                                                INITIAL_COORDINATOR_EPOCH,
                                                Collections.singletonList(follower),
                                                result -> {
                                                    assertThat(result)
                                                            .allSatisfy(
                                                                    value ->
                                                                            assertThat(
                                                                                            value
                                                                                                    .succeeded())
                                                                                    .isTrue());
                                                    recovered.set(true);
                                                }));
                waitUntil(
                        () -> blockedOn(recoveryWorker, checkpointLock),
                        Duration.ofSeconds(10),
                        "Real recovery did not reach the checkpoint write monitor");
                ThreadInfo workerInfo =
                        ManagementFactory.getThreadMXBean()
                                .getThreadInfo(recoveryWorker.getId(), Integer.MAX_VALUE);
                assertThat(
                                Arrays.stream(workerInfo.getStackTrace())
                                        .map(StackTraceElement::getClassName)
                                        .collect(Collectors.toList()))
                        .contains(
                                recovery.getClass().getName(),
                                Replica.class.getName(),
                                LogManager.class.getName());
                assertThat(log.localLogEndOffset()).isEqualTo(10);
                assertThat(log.writerStateManager().lastEntry(101).get().lastBatchSequence())
                        .isEqualTo(0);
                assertThat(transition.isDone()).isFalse();
                closer.start();
                waitUntil(
                        () ->
                                coordinator.isShutdown()
                                        && closer.getState() == Thread.State.TIMED_WAITING,
                        Duration.ofSeconds(10),
                        "Server shutdown did not wait for real recovery");
                assertThat(stopped).isNotDone();
                assertThat(recovered).isFalse();
                assertThat(recoveryWorker.isAlive()).isTrue();
                assertThat(logChannel.isOpen()).isTrue();
                assertThat(logChannel.read(ByteBuffer.allocate(1), 0)).isEqualTo(1);
                assertThat(ownedRpcClient.isReady("unused-shutdown-test-node")).isFalse();
                assertThat(scannerClosed).isFalse();
                assertThat(scheduler.isStarted()).isTrue();
            }
            transition.get(10, TimeUnit.SECONDS);
            stopped.get(10, TimeUnit.SECONDS);
            closer.join(10000);
            assertThat(recovered).isTrue();
            assertThat(coordinator.isTerminated()).isTrue();
            recoveryWorker.join(10000);
            assertThat(recoveryWorker.isAlive()).isFalse();
            assertThat(logChannel.isOpen()).isFalse();
            assertThat(scannerClosed).isTrue();
            assertThat(scheduler.isStarted()).isFalse();
            assertThatThrownBy(() -> ownedRpcClient.isReady("unused-shutdown-test-node"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Netty client is closed");
            assertThat(
                            new OffsetCheckpointFile(
                                            new File(
                                                    log.getDataDir(),
                                                    "recovery-point-offset-checkpoint"))
                                    .read())
                    .containsEntry(bucket, 10L);
        } finally {
            closer.join(10000);
            coordinator.shutdown();
            assertThat(coordinator.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            if (stopped.isDone()) {
                // These resources were transferred to TabletServer; the base fixture owns the rest.
                replicaManager = null;
                logManager = null;
                kvManager = null;
                scannerManager = null;
                rpcClient = null;
                localDiskManager = null;
            }
        }
    }

    private static boolean blockedOn(Thread worker, Object monitor) {
        if (worker == null) {
            return false;
        }
        ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(worker.getId());
        return info != null
                && info.getThreadState() == Thread.State.BLOCKED
                && info.getLockInfo() != null
                && info.getLockInfo().getIdentityHashCode() == System.identityHashCode(monitor);
    }

    private static Object field(Object owner, Class<?> type, String name) throws Exception {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(owner);
    }

    private static void setServerField(TabletServer server, String name, Object value)
            throws Exception {
        Field field = TabletServer.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(server, value);
    }
}

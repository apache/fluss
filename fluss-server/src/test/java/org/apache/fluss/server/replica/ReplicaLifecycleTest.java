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

import org.apache.fluss.cluster.compaction.CompactionType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.entity.StopReplicaData;
import org.apache.fluss.server.entity.StopReplicaResultForBucket;
import org.apache.fluss.server.kv.KvCloseMode;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.snapshot.KvFileHandleAndLocalPath;
import org.apache.fluss.server.kv.snapshot.KvSnapshotDataUploader;
import org.apache.fluss.server.kv.snapshot.SnapshotFileScope;
import org.apache.fluss.server.kv.snapshot.SnapshotLocation;
import org.apache.fluss.server.utils.CancellationToken;
import org.apache.fluss.server.utils.ResourceGuard;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.utils.CloseableRegistry;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.concurrent.FutureUtils;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.server.kv.KvTabletTestUtils.flushAndWait;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Resource lifetime regressions for asynchronous replica retirement. */
class ReplicaLifecycleTest extends ReplicaTestBase {
    private ExecutorService operations;
    private final AtomicBoolean holdUpload = new AtomicBoolean();
    private final CountDownLatch uploadEntered = new CountDownLatch(1);
    private final CountDownLatch releaseUpload = new CountDownLatch(1);

    @Override
    protected Configuration getServerConf() {
        Configuration conf = super.getServerConf();
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofSeconds(1));
        return conf;
    }

    @Override
    protected ExecutorService createIoExecutor() {
        return new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>()) {
            @Override
            protected void beforeExecute(Thread thread, Runnable task) {
                if (holdUpload.compareAndSet(true, false)) {
                    uploadEntered.countDown();
                    awaitUninterruptibly(releaseUpload);
                }
            }
        };
    }

    @BeforeEach
    void createOperations() {
        operations = Executors.newFixedThreadPool(3);
    }

    @AfterEach
    void closeOperations() {
        releaseUpload.countDown();
        operations.shutdownNow();
    }

    @Test
    void testCompactionLeaseAllowsLookupPutAndFlush() throws Exception {
        TableBucket bucket = new TableBucket(DATA1_TABLE_ID_PK, 1);
        Replica replica = makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, bucket);
        replica.makeLeader(leader(bucket, 0));
        KvTablet tablet = replica.getKvTablet();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CancellationToken cancellation =
                new CancellationToken() {
                    @Override
                    public AutoCloseable register(Runnable callback) {
                        AutoCloseable registration = super.register(callback);
                        entered.countDown();
                        try {
                            release.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                        return registration;
                    }
                };
        Future<?> compaction =
                operations.submit(
                        () -> {
                            tablet.compact(CompactionType.RECLAIM, cancellation);
                            return null;
                        });
        try {
            assertThat(entered.await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(tablet.getRocksDBKv().getResourceGuard().getLeaseCount()).isPositive();
            Future<?> foreground =
                    operations.submit(
                            () -> {
                                byte[] key = new byte[] {'k', '1'};
                                tablet.putAsLeader(
                                        genKvRecordBatch(
                                                Tuple2.of(
                                                        "k1",
                                                        new Object[] {1, "during-compaction"})),
                                        null);
                                flushAndWait(tablet, Long.MAX_VALUE);
                                assertThat(tablet.multiGet(Collections.singletonList(key)).get(0))
                                        .isNotNull();
                                assertThat(tablet.getRocksDBKv().get(key)).isNotNull();
                                return null;
                            });
            foreground.get(30, TimeUnit.SECONDS);
        } finally {
            release.countDown();
        }
        compaction.get(30, TimeUnit.SECONDS);
        assertThat(tablet.getRocksDBKv().getResourceGuard().getLeaseCount()).isZero();
        assertThat(tablet.inspectSstProperties().matches(tablet.isPartitionedIndexEnabled()))
                .isTrue();
    }

    @Test
    void testCompactionReleasesLeaseOnCancellationAndRejectsClosedTablet() throws Exception {
        TableBucket bucket = new TableBucket(DATA1_TABLE_ID_PK, 1);
        Replica replica = makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, bucket);
        replica.makeLeader(leader(bucket, 0));
        KvTablet tablet = replica.getKvTablet();
        CancellationToken cancelled = new CancellationToken();
        cancelled.cancel();
        assertThatThrownBy(() -> tablet.compact(CompactionType.RECLAIM, cancelled))
                .isInstanceOf(IOException.class);
        assertThat(tablet.getRocksDBKv().getResourceGuard().getLeaseCount()).isZero();
        tablet.close();
        assertThatThrownBy(() -> tablet.compact(CompactionType.RECLAIM, new CancellationToken()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("closed");
        assertThatThrownBy(tablet::inspectSstProperties).isInstanceOf(IOException.class);
    }

    @Test
    void testStopDeletesLocalAndRemoteLeaderData() throws Exception {
        TableBucket bucket = new TableBucket(DATA1_TABLE_ID_PK, 1);
        makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, 1);
        Replica replica = replicaManager.getReplicaOrException(bucket);
        File localKv = replica.getKvTablet().getKvTabletDir();
        File localLog = replica.getLogTablet().getLogDir();
        FsPath remoteKv =
                FlussPaths.remoteKvTabletDir(
                        FlussPaths.remoteKvDir(replica.getRemoteDataDir()),
                        replica.getPhysicalTablePath(),
                        bucket);
        FsPath remoteLog =
                FlussPaths.remoteLogTabletDir(
                        FlussPaths.remoteLogDir(replica.getRemoteDataDir()),
                        replica.getPhysicalTablePath(),
                        bucket);
        remoteKv.getFileSystem().mkdirs(remoteKv);
        remoteLog.getFileSystem().mkdirs(remoteLog);
        holdUpload.set(true);
        CompletableFuture<?> snapshot =
                replica.snapshotAfterCompaction(replica.getKvTablet(), replica.getLeaderEpoch());
        try {
            assertThat(uploadEntered.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(snapshot).isNotDone();
            CompletableFuture<List<StopReplicaResultForBucket>> response =
                    new CompletableFuture<>();
            replicaManager.stopReplicas(
                    INITIAL_COORDINATOR_EPOCH,
                    Collections.singletonList(
                            new StopReplicaData(
                                    bucket,
                                    true,
                                    true,
                                    INITIAL_COORDINATOR_EPOCH,
                                    replica.getLeaderEpoch())),
                    response::complete);
            assertThat(response.get()).containsExactly(new StopReplicaResultForBucket(bucket));
            assertThat(replicaManager.getReplicaCleanupFuture(bucket)).isNotDone();
            assertThat(remoteKv.getFileSystem().exists(remoteKv)).isTrue();
            assertThat(remoteLog.getFileSystem().exists(remoteLog)).isTrue();
        } finally {
            releaseUpload.countDown();
        }
        replicaManager.getReplicaCleanupFuture(bucket).get(10, TimeUnit.SECONDS);
        assertThat(snapshot).isCompletedExceptionally();
        assertThat(localKv).doesNotExist();
        assertThat(localLog).doesNotExist();
        assertThat(remoteKv.getFileSystem().exists(remoteKv)).isFalse();
        assertThat(remoteLog.getFileSystem().exists(remoteLog)).isFalse();
    }

    @Test
    void testShutdownWaitsForFollowerKvRetirement() throws Exception {
        TableBucket bucket = new TableBucket(DATA1_TABLE_ID_PK, 1);
        makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, 1);
        Replica replica = replicaManager.getReplicaOrException(bucket);
        KvTablet tablet = replica.getKvTablet();
        CompletableFuture<Void> shutdown;
        try (ResourceGuard.Lease ignored =
                tablet.getRocksDBKv().getResourceGuard().acquireResource()) {
            assertThat(
                            replica.makeFollower(
                                    new NotifyLeaderAndIsrData(
                                            DATA1_PHYSICAL_TABLE_PATH_PK,
                                            bucket,
                                            Collections.singletonList(TABLET_SERVER_ID),
                                            new LeaderAndIsr(
                                                    TABLET_SERVER_ID + 1,
                                                    1,
                                                    Collections.singletonList(TABLET_SERVER_ID + 1),
                                                    Collections.emptyList(),
                                                    INITIAL_COORDINATOR_EPOCH,
                                                    1),
                                            3,
                                            0L)))
                    .isTrue();
            assertThat(replica.isLeader()).isFalse();
            CompletableFuture<Void> retirement = replica.getKvCloseFuture();
            assertThat(retirement).isNotDone();
            assertThat(replica.closeKvAsync(KvCloseMode.DISCARD_UNPERSISTED_STATE))
                    .isSameAs(retirement);
            shutdown =
                    CompletableFuture.runAsync(
                            () -> {
                                try {
                                    replicaManager.shutdown();
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            },
                            operations);
            assertThatThrownBy(() -> shutdown.get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            assertThat(tablet.getRocksDBKv().getDb().isOwningHandle()).isTrue();
        }
        shutdown.get(10, TimeUnit.SECONDS);
        replicaManager = null;
        assertThat(tablet.getRocksDBKv().getDb().isOwningHandle()).isFalse();
        assertThat(tablet.getKvTabletDir()).doesNotExist();
    }

    @Test
    void testShutdownContinuesAfterRetirementFailure() throws Exception {
        TableBucket failedBucket = new TableBucket(DATA1_TABLE_ID_PK, 1);
        makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, 1);
        Replica failedReplica = replicaManager.getReplicaOrException(failedBucket);
        KvTablet failedTablet = failedReplica.getKvTablet();
        FsPath failedRemoteKv =
                FlussPaths.remoteKvTabletDir(
                        FlussPaths.remoteKvDir(failedReplica.getRemoteDataDir()),
                        failedReplica.getPhysicalTablePath(),
                        failedBucket);
        failedRemoteKv.getFileSystem().mkdirs(failedRemoteKv);
        CompletableFuture<Void> failed =
                kvManager.dropKvAsync(
                        failedBucket,
                        () -> {
                            try {
                                failedTablet.close(KvCloseMode.DISCARD_UNPERSISTED_STATE);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                            return FutureUtils.completedExceptionally(
                                    new KvStorageException("Expected cleanup failure after close"));
                        });
        assertThatThrownBy(() -> failed.get(10, TimeUnit.SECONDS))
                .hasRootCauseMessage("Expected cleanup failure after close");
        replicaManager.stopReplicas(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        new StopReplicaData(
                                failedBucket,
                                true,
                                true,
                                INITIAL_COORDINATOR_EPOCH,
                                failedReplica.getLeaderEpoch())),
                ignored -> {});
        assertThatThrownBy(
                        () ->
                                replicaManager
                                        .getReplicaCleanupFuture(failedBucket)
                                        .get(10, TimeUnit.SECONDS))
                .hasRootCauseMessage("Expected cleanup failure after close");
        assertThat(failedRemoteKv.getFileSystem().exists(failedRemoteKv)).isTrue();
        makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, 2);
        KvTablet healthyTablet =
                replicaManager
                        .getReplicaOrException(new TableBucket(DATA1_TABLE_ID_PK, 2))
                        .getKvTablet();
        Replica unmanaged =
                makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, new TableBucket(DATA1_TABLE_ID_PK, 0));
        unmanaged.makeLeader(leader(unmanaged.getTableBucket(), 0));
        KvTablet remainingTablet = unmanaged.getKvTablet();
        ReplicaManager retiringReplicas = replicaManager;
        try {
            assertThatThrownBy(retiringReplicas::shutdown)
                    .hasRootCauseMessage("Expected cleanup failure after close");
            assertThat(
                            retiringReplicas
                                    .getKvSnapshotContext()
                                    .getAsyncOperationsThreadPool()
                                    .isShutdown())
                    .isTrue();
            assertThat(retiringReplicas.getKvSnapshotContext().getSnapshotScheduler().isShutdown())
                    .isTrue();
            assertThat(healthyTablet.getRocksDBKv().getDb().isOwningHandle()).isFalse();

            CompletableFuture<Void> shutdown;
            try (ResourceGuard.Lease ignored =
                    remainingTablet.getRocksDBKv().getResourceGuard().acquireResource()) {
                shutdown = CompletableFuture.runAsync(kvManager::shutdown, operations);
                assertThatThrownBy(() -> shutdown.get(100, TimeUnit.MILLISECONDS))
                        .isInstanceOf(TimeoutException.class);
                assertThat(remainingTablet.getRocksDBKv().getDb().isOwningHandle()).isTrue();
            }
            assertThatThrownBy(() -> shutdown.get(10, TimeUnit.SECONDS))
                    .hasRootCauseMessage("Expected cleanup failure after close");
            assertThat(remainingTablet.getRocksDBKv().getDb().isOwningHandle()).isFalse();
        } finally {
            // Always stop fixture threads, including when the shutdown behavior under test fails.
            retiringReplicas.getKvSnapshotContext().getAsyncOperationsThreadPool().shutdownNow();
            retiringReplicas.getKvSnapshotContext().getSnapshotScheduler().shutdownNow();
            retiringReplicas.getReplicaFetcherManager().shutdown();
            retiringReplicas.getDelayedWriteManager().shutdown();
            retiringReplicas.getDelayedFetchLogManager().shutdown();
            replicaManager = null;
            try {
                kvManager.shutdown();
            } catch (RuntimeException ignored) {
                // The retained cleanup failure is already asserted above.
            }
            kvManager = null;
        }
    }

    @Test
    void testDropReturnsBeforeNativeExits() throws Exception {
        TableBucket bucket = new TableBucket(DATA1_TABLE_ID_PK, 1);
        Replica replica = makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, bucket);
        replica.makeLeader(leader(bucket, 0));
        KvTablet tablet = replica.getKvTablet();
        File directory = tablet.getKvTabletDir();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CancellationToken cancellation =
                new CancellationToken() {
                    @Override
                    public AutoCloseable register(Runnable callback) {
                        AutoCloseable registration = super.register(callback);
                        entered.countDown();
                        awaitUninterruptibly(release);
                        return registration;
                    }
                };
        CompletableFuture<Void> nativeCall =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                tablet.compact(CompactionType.RECLAIM, cancellation);
                            } catch (Exception e) {
                                if (!cancellation.isCancelled()) {
                                    throw new RuntimeException(e);
                                }
                            }
                        },
                        operations);
        CompletableFuture<CompletableFuture<Void>> transition = null;
        try {
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
            transition = CompletableFuture.supplyAsync(replica::delete, operations);
            CompletableFuture<Void> cleanup = transition.get(5, TimeUnit.SECONDS);
            assertThat(transition).isCompleted();
            assertThat(cleanup).isNotDone();
            assertThat(directory).exists();
            assertThat(directory.getParentFile()).exists();
            assertThatThrownBy(() -> tablet.multiGet(Collections.singletonList(new byte[] {1})))
                    .isInstanceOf(IllegalStateException.class);
            TableBucket other = new TableBucket(DATA1_TABLE_ID_PK, 2);
            Replica otherReplica = makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, other);
            otherReplica.makeLeader(leader(other, 0));
            assertThat(otherReplica.isLeader()).isTrue();
            otherReplica.delete().get(10, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            nativeCall.get(10, TimeUnit.SECONDS);
            if (transition != null) {
                transition.get(10, TimeUnit.SECONDS).get(10, TimeUnit.SECONDS);
            }
        }
        assertThat(directory).doesNotExist();
        assertThat(tablet.getRocksDBKv().getDb().isOwningHandle()).isFalse();
    }

    @Test
    void testCannotReopenBucketBeforeCleanup() throws Exception {
        TableBucket bucket = new TableBucket(DATA1_TABLE_ID_PK, 1);
        Replica replica = makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, bucket);
        replica.makeLeader(leader(bucket, 0));
        KvTablet old = replica.getKvTablet();
        try (ResourceGuard.Lease ignored =
                old.getRocksDBKv().getResourceGuard().acquireResource()) {
            CompletableFuture<Void> transition =
                    CompletableFuture.runAsync(
                            () -> {
                                assertThatThrownBy(() -> replica.makeLeader(leader(bucket, 1)))
                                        .isInstanceOf(KvStorageException.class);
                            },
                            operations);
            transition.get(5, TimeUnit.SECONDS);
            assertThat(replica.isLeader()).isFalse();
            assertThat(replica.getLeaderEpoch()).isEqualTo(0);
            assertThat(replica.getKvCloseFuture()).isNotDone();
            assertThat(old.getKvTabletDir()).exists();
            assertThatThrownBy(
                            () ->
                                    kvManager.createTabletDir(
                                            replica.getLogTablet().getDataDir(),
                                            DATA1_PHYSICAL_TABLE_PATH_PK,
                                            bucket))
                    .isInstanceOf(KvStorageException.class);
        }
        replica.getKvCloseFuture().get(10, TimeUnit.SECONDS);
        replica.makeLeader(leader(bucket, 1));
        assertThat(replica.isLeader()).isTrue();
        assertThat(replica.getLeaderEpoch()).isEqualTo(1);
        assertThat(replica.getKvTablet()).isNotSameAs(old);
        replica.delete().get(10, TimeUnit.SECONDS);
    }

    @Test
    void testStopRetainsDirectoriesAndRejectsOrphanSweepUntilCleanup() throws Exception {
        TableBucket bucket = new TableBucket(DATA1_TABLE_ID_PK, 1);
        makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, 1);
        Replica replica = replicaManager.getReplicaOrException(bucket);
        KvTablet tablet = replica.getKvTablet();
        File directory = tablet.getKvTabletDir();
        File parent = directory.getParentFile();
        StopReplicaData stop =
                new StopReplicaData(
                        bucket, true, false, INITIAL_COORDINATOR_EPOCH, replica.getLeaderEpoch());
        CompletableFuture<Void> cleanup;
        try (ResourceGuard.Lease ignored =
                tablet.getRocksDBKv().getResourceGuard().acquireResource()) {
            CompletableFuture<List<StopReplicaResultForBucket>> response =
                    new CompletableFuture<>();
            CompletableFuture.runAsync(
                            () ->
                                    replicaManager.stopReplicas(
                                            INITIAL_COORDINATOR_EPOCH,
                                            Collections.singletonList(stop),
                                            response::complete),
                            operations)
                    .get(5, TimeUnit.SECONDS);
            assertThat(response)
                    .isCompletedWithValue(
                            Collections.singletonList(new StopReplicaResultForBucket(bucket)));
            cleanup = replicaManager.getReplicaCleanupFuture(bucket);
            assertThat(cleanup).isNotDone();
            CompletableFuture<List<StopReplicaResultForBucket>> retry = new CompletableFuture<>();
            replicaManager.stopReplicas(
                    INITIAL_COORDINATOR_EPOCH, Collections.singletonList(stop), retry::complete);
            assertThat(retry.get()).doesNotContain(new StopReplicaResultForBucket(bucket));
            assertThat(directory).exists();
            assertThat(parent).exists();
        }
        cleanup.get(10, TimeUnit.SECONDS);
        assertThat(directory).doesNotExist();
        assertThat(parent).doesNotExist();
        makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, 1);
        assertThat(replicaManager.getReplicaOrException(bucket).isLeader()).isTrue();
    }

    @Test
    void testDropRejectsConflictingCloseOnlyRetirement() throws Exception {
        TableBucket bucket = new TableBucket(DATA1_TABLE_ID_PK, 1);
        Replica replica = makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, bucket);
        replica.makeLeader(leader(bucket, 0));
        KvTablet tablet = replica.getKvTablet();
        CompletableFuture<Void> closed;
        try (ResourceGuard.Lease ignored =
                tablet.getRocksDBKv().getResourceGuard().acquireResource()) {
            closed = replica.closeKvAsync(KvCloseMode.PRESERVE_LOCAL_STATE);
            assertThat(kvManager.closeKvAsync(bucket, KvCloseMode.PRESERVE_LOCAL_STATE))
                    .isSameAs(closed);
            assertThatThrownBy(() -> kvManager.dropKvAsync(bucket).join())
                    .hasCauseInstanceOf(KvStorageException.class);
            assertThatThrownBy(() -> replica.delete().join())
                    .hasCauseInstanceOf(KvStorageException.class);
            assertThat(closed).isNotDone();
            assertThat(tablet.getKvTabletDir()).exists();
            assertThat(replica.getLogTablet().getLogDir()).exists();
        }
        closed.get(10, TimeUnit.SECONDS);
        assertThat(tablet.getKvTabletDir()).exists();
    }

    @Test
    void testDropWaitsForSnapshotCleanup() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        TestSnapshotContext context =
                new TestSnapshotContext(remoteDataDir) {
                    @Override
                    public KvSnapshotDataUploader getSnapshotDataUploader() {
                        return new KvSnapshotDataUploader(executorService) {
                            @Override
                            public List<KvFileHandleAndLocalPath> uploadFilesToSnapshotLocation(
                                    List<Path> files,
                                    SnapshotLocation location,
                                    SnapshotFileScope scope,
                                    CloseableRegistry registry,
                                    CloseableRegistry temporary,
                                    Counter bytes)
                                    throws Exception {
                                entered.countDown();
                                awaitUninterruptibly(release);
                                return super.uploadFilesToSnapshotLocation(
                                        files, location, scope, registry, temporary, bytes);
                            }
                        };
                    }
                };
        TableBucket bucket = new TableBucket(DATA1_TABLE_ID_PK, 1);
        Replica replica = makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, bucket, context);
        replica.makeLeader(leader(bucket, 0));
        KvTablet tablet = replica.getKvTablet();
        CompletableFuture<?> snapshot = replica.snapshotAfterCompaction(tablet, 0);
        context.scheduledExecutorService.triggerAll();
        CompletableFuture<Void> cleanup = null;
        try {
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
            cleanup = replica.delete();
            assertThat(cleanup).isNotDone();
            assertThat(tablet.getKvTabletDir()).exists();
            assertThat(tablet.getKvTabletDir().getParentFile()).exists();
        } finally {
            release.countDown();
            if (cleanup != null) {
                cleanup.get(10, TimeUnit.SECONDS);
            }
        }
        assertThat(snapshot).isCompletedExceptionally();
        assertThat(tablet.getKvTabletDir()).doesNotExist();
    }

    private static NotifyLeaderAndIsrData leader(TableBucket bucket, int epoch) {
        return new NotifyLeaderAndIsrData(
                DATA1_PHYSICAL_TABLE_PATH_PK,
                bucket,
                Collections.singletonList(TABLET_SERVER_ID),
                new LeaderAndIsr(
                        TABLET_SERVER_ID,
                        epoch,
                        Collections.singletonList(TABLET_SERVER_ID),
                        Collections.emptyList(),
                        INITIAL_COORDINATOR_EPOCH,
                        epoch),
                3,
                0L);
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}

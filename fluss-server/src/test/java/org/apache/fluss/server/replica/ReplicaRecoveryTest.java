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

import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotJsonSerde;
import org.apache.fluss.server.kv.snapshot.KvFileHandle;
import org.apache.fluss.server.kv.snapshot.KvFileHandleAndLocalPath;
import org.apache.fluss.server.kv.snapshot.KvSnapshotHandle;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.ZkData;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests that bucket recovery does not block unrelated replica role changes. */
class ReplicaRecoveryTest extends ReplicaTestBase {

    @Test
    void testFailedRestoreReturnsRetriableReadAndWriteErrors(@TempDir Path directory)
            throws Exception {
        Replica replica = startLeader();
        TableBucket bucket = replica.getTableBucket();
        KvFileHandleAndLocalPath missingFile =
                KvFileHandleAndLocalPath.of(
                        new KvFileHandle(directory.resolve("missing.sst").toUri().toString(), 1),
                        "missing.sst");
        selectSnapshot(
                bucket,
                directory,
                KvSnapshotHandle.create(
                        Collections.singletonList(missingFile), Collections.emptyList(), 1));
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            assertThat(
                            replica.requestBulkLoadRestore(
                                            7L,
                                            replica.getLeaderEpoch(),
                                            replica.getCoordinatorEpoch(),
                                            worker)
                                    .get(5, TimeUnit.SECONDS))
                    .isNull();
            waitUntil(
                    () -> !replica.hasKvRecoveryInProgress(),
                    Duration.ofSeconds(10),
                    "failed recovery did not release its task");
            assertThat(replica.isLeader()).isTrue();
            assertThat(replica.getKvTablet()).isNull();
            assertThatThrownBy(() -> replica.lookups(Collections.singletonList(new byte[] {1})))
                    .isInstanceOf(NotLeaderOrFollowerException.class);
            assertThatThrownBy(
                            () ->
                                    replica.putRecordsToLeader(
                                            genKvRecordBatch(DATA1.toArray(new Object[0][])),
                                            null,
                                            MergeMode.DEFAULT,
                                            1))
                    .isInstanceOf(NotLeaderOrFollowerException.class);
        } finally {
            worker.shutdownNow();
        }
    }

    @Test
    void testDeleteBeforeQueuedRecoveryCannotRecreateKv() throws Exception {
        Replica replica = startLeader();
        TableBucket bucket = replica.getTableBucket();
        zkClient.getCuratorClient()
                .create()
                .creatingParentsIfNeeded()
                .forPath(
                        ZkData.RecoverySnapshotZNode.path(bucket),
                        ZkData.RecoverySnapshotZNode.encode(7));
        Queue<Runnable> pending = new ArrayDeque<>();
        CompletableFuture<Long> response =
                replica.requestBulkLoadRestore(
                        7L, replica.getLeaderEpoch(), replica.getCoordinatorEpoch(), pending::add);
        assertThat(pending).hasSize(1);
        replica.delete();
        pending.remove().run();
        assertThatThrownBy(response::join).hasCauseInstanceOf(NotLeaderOrFollowerException.class);
        assertThat(replica.getKvTablet()).isNull();
        assertThat(replica.hasKvRecoveryInProgress()).isFalse();
    }

    @Test
    void testRecoveryValidationReservesBucketBeforeWorkerStarts() throws Exception {
        Replica replica = startLeader();
        Queue<Runnable> pending = new ArrayDeque<>();
        CompletableFuture<Long> recovery =
                replica.requestBulkLoadRestore(
                        7L, replica.getLeaderEpoch(), replica.getCoordinatorEpoch(), pending::add);
        NotifyLeaderAndIsrData role =
                role(replica, replica.getLeaderEpoch(), replica.getBucketEpoch(), null);
        CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> response =
                new CompletableFuture<>();
        replicaManager.becomeLeaderOrFollower(
                replica.getCoordinatorEpoch(), Collections.singletonList(role), response::complete);
        assertThat(response.get(5, TimeUnit.SECONDS).get(0).getError().error())
                .isEqualTo(Errors.REQUEST_TIME_OUT);
        assertThat(recovery).isNotDone();
        pending.remove().run();
        assertThat(recovery.get(5, TimeUnit.SECONDS)).isNull();
        CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> retried =
                new CompletableFuture<>();
        replicaManager.becomeLeaderOrFollower(
                replica.getCoordinatorEpoch(), Collections.singletonList(role), retried::complete);
        assertThat(retried.get(5, TimeUnit.SECONDS).get(0).failed()).isFalse();
    }

    @Test
    void testRestoreNotificationDoesNotWaitForBusyBucket(@TempDir Path directory) throws Exception {
        Replica replica = startLeader();
        selectSnapshot(
                replica.getTableBucket(),
                directory,
                KvSnapshotHandle.create(Collections.emptyList(), Collections.emptyList(), 0));
        Lock bucketLock = replica.getLeaderIsrUpdateLock().writeLock();
        ExecutorService workers = Executors.newFixedThreadPool(2);
        bucketLock.lock();
        try {
            assertThat(notifyRestore(workers, replica, 6L).getRecoveredSnapshotId()).isNull();
            waitUntil(
                    () -> !replica.hasKvRecoveryInProgress(),
                    Duration.ofSeconds(5),
                    "stale recovery should finish without taking the bucket lock");
            assertThat(notifyRestore(workers, replica, 7L).getRecoveredSnapshotId()).isNull();
            assertThat(replica.hasKvRecoveryInProgress()).isTrue();
            awaitOtherBucket(workers);
        } finally {
            bucketLock.unlock();
            workers.shutdownNow();
            waitUntil(
                    () -> !replica.hasKvRecoveryInProgress(),
                    Duration.ofSeconds(10),
                    "recovery worker should finish before replica teardown");
        }
    }

    private Replica startLeader() throws Exception {
        TableBucket bucket = new TableBucket(DATA1_TABLE_ID_PK, 1);
        makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, bucket.getBucket());
        return replicaManager.getReplicaOrException(bucket);
    }

    private NotifyLeaderAndIsrResultForBucket notifyRestore(
            ExecutorService workers, Replica replica, long snapshotId) throws Exception {
        NotifyLeaderAndIsrData data =
                role(replica, replica.getLeaderEpoch(), replica.getBucketEpoch(), snapshotId);
        CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> response =
                new CompletableFuture<>();
        workers.submit(
                () ->
                        replicaManager.becomeLeaderOrFollower(
                                replica.getCoordinatorEpoch(),
                                Collections.singletonList(data),
                                response::complete));
        NotifyLeaderAndIsrResultForBucket result = response.get(5, TimeUnit.SECONDS).get(0);
        assertThat(result.failed()).isFalse();
        return result;
    }

    private void awaitOtherBucket(ExecutorService workers) throws Exception {
        workers.submit(
                        () -> {
                            makeLogTableAsLeader(2);
                            return null;
                        })
                .get(5, TimeUnit.SECONDS);
    }

    private void selectSnapshot(TableBucket bucket, Path directory, KvSnapshotHandle files)
            throws Exception {
        CompletedSnapshot snapshot =
                new CompletedSnapshot(
                        bucket, 7, new FsPath(directory.toUri()), files, 0, null, null);
        Path metadata = Paths.get(snapshot.getMetadataFilePath().toUri());
        Files.write(metadata, CompletedSnapshotJsonSerde.toJson(snapshot));
        zkClient.registerTableBucketSnapshot(
                bucket, new BucketSnapshot(7, 0, metadata.toUri().toString()));
        zkClient.getCuratorClient()
                .create()
                .forPath(
                        ZkData.RecoverySnapshotZNode.path(bucket),
                        ZkData.RecoverySnapshotZNode.encode(7));
    }

    private static NotifyLeaderAndIsrData role(
            Replica replica, int leaderEpoch, int bucketEpoch, Long recoverySnapshotId) {
        return new NotifyLeaderAndIsrData(
                replica.getPhysicalTablePath(),
                replica.getTableBucket(),
                Collections.singletonList(TABLET_SERVER_ID),
                new LeaderAndIsr(
                        TABLET_SERVER_ID,
                        leaderEpoch,
                        Collections.singletonList(TABLET_SERVER_ID),
                        Collections.emptyList(),
                        replica.getCoordinatorEpoch(),
                        bucketEpoch),
                replica.getRoutingBucketCount(),
                replica.getBucketCountEpoch(),
                recoverySnapshotId);
    }
}

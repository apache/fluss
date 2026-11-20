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

package org.apache.fluss.server.coordinator.bulkload;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.BulkLoadConflictException;
import org.apache.fluss.exception.LogStorageException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.AllocateBulkLoadBucketSnapshotRequest;
import org.apache.fluss.rpc.messages.AllocateBulkLoadBucketSnapshotResponse;
import org.apache.fluss.rpc.messages.BeginBulkLoadRequest;
import org.apache.fluss.rpc.messages.CommitBulkLoadBucketSnapshotRequest;
import org.apache.fluss.rpc.messages.CommitBulkLoadRequest;
import org.apache.fluss.rpc.messages.PbBulkLoadHandle;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotHandle;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.TestingBulkLoadSnapshot;
import org.apache.fluss.server.zk.ZkSequenceIDCounter;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.server.testutils.KvTestUtils.assertLookupResponse;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newLookupRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecords;
import static org.apache.fluss.testutils.DataTestUtils.getKeyValuePairs;
import static org.apache.fluss.testutils.DataTestUtils.toKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Initial import, transaction-independent recovery, replication and later snapshots. */
class BulkLoadITCase {
    @Test
    void testBucketCommitFailsIfWalAdvancedAfterAllocation() throws Exception {
        FlussClusterExtension cluster =
                FlussClusterExtension.builder().setNumOfTabletServers(1).build();
        try {
            cluster.start();
            TablePath path = TablePath.of("bulkload_db", "concurrent_writes");
            TableBucket bucket = createPrimaryKeyBucket(cluster, path, 1);
            CoordinatorGateway coordinator = cluster.newCoordinatorClient();
            PbBulkLoadHandle handle =
                    coordinator.beginBulkLoad(begin(path, "load")).get().getStatus().getHandle();
            AllocateBulkLoadBucketSnapshotResponse allocation =
                    allocateSnapshot(coordinator, handle, bucket);
            produceSnapshot(
                    cluster, path, bucket, genKvRecords(new Object[] {1, "imported"}), allocation);

            List<KvRecord> written = genKvRecords(new Object[] {2, "written"});
            putRecords(cluster, bucket, written);
            assertThatThrownBy(
                            () ->
                                    coordinator
                                            .commitBulkLoadBucketSnapshot(
                                                    new CommitBulkLoadBucketSnapshotRequest()
                                                            .setHandle(handle)
                                                            .setBucketId(bucket.getBucket()))
                                            .get())
                    .hasCauseInstanceOf(BulkLoadConflictException.class)
                    .hasStackTraceContaining("the leader WAL end offset changed");
            assertThat(
                            cluster.getZooKeeperClient()
                                    .getTableBucketSnapshot(bucket, allocation.getSnapshotId()))
                    .isEmpty();
            assertThat(cluster.getZooKeeperClient().getRecoverySnapshotSelection(bucket)).isEmpty();
            AllocateBulkLoadBucketSnapshotResponse retry =
                    allocateSnapshot(coordinator, handle, bucket);
            assertThat(retry.getSnapshotId()).isEqualTo(allocation.getSnapshotId());
            assertThat(retry.getLogOffset()).isEqualTo(allocation.getLogOffset());
            assertRows(cluster, bucket, written);
        } finally {
            cluster.close();
        }
    }

    @Test
    void testBulkLoadCheckpointFailureShutsDownTabletServer() throws Exception {
        Configuration conf =
                new Configuration()
                        .set(
                                ConfigOptions.LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL,
                                Duration.ofHours(1));
        FlussClusterExtension cluster =
                FlussClusterExtension.builder()
                        .setNumOfTabletServers(1)
                        .setClusterConf(conf)
                        .build();
        try {
            cluster.start();
            TablePath path = TablePath.of("bulkload_db", "checkpoint_failure");
            TableBucket bucket = createPrimaryKeyBucket(cluster, path, 1);
            CoordinatorGateway coordinator = cluster.newCoordinatorClient();
            PbBulkLoadHandle handle =
                    coordinator.beginBulkLoad(begin(path, "load")).get().getStatus().getHandle();
            AllocateBulkLoadBucketSnapshotResponse allocation =
                    allocateSnapshot(coordinator, handle, bucket);
            produceSnapshot(
                    cluster, path, bucket, genKvRecords(new Object[] {1, "imported"}), allocation);

            int leader = cluster.waitAndGetLeader(bucket);
            Replica replica =
                    cluster.getTabletServerById(leader)
                            .getReplicaManager()
                            .getReplicaOrException(bucket);
            Path checkpoint =
                    replica.getLogTablet()
                            .getDataDir()
                            .toPath()
                            .resolve("recovery-point-offset-checkpoint");
            Files.deleteIfExists(checkpoint);
            Files.createDirectory(checkpoint);
            // A nonempty directory cannot be replaced by the real checkpoint writer.
            Path occupied = Files.createFile(checkpoint.resolve("occupied"));
            try {
                coordinator
                        .commitBulkLoadBucketSnapshot(
                                new CommitBulkLoadBucketSnapshotRequest()
                                        .setHandle(handle)
                                        .setBucketId(bucket.getBucket()))
                        .get();
                retry(
                        Duration.ofMinutes(1),
                        () ->
                                assertThat(cluster.getZooKeeperClient().getTabletServer(leader))
                                        .isEmpty());
                assertThatThrownBy(() -> cluster.stopTabletServer(leader))
                        .hasCauseInstanceOf(LogStorageException.class);
            } finally {
                Files.deleteIfExists(occupied);
                Files.deleteIfExists(checkpoint);
            }
        } finally {
            cluster.close();
        }
    }

    @Test
    void testCoordinatorTakeoverRestoresCommittedImportOnSameLeader() throws Exception {
        Configuration conf =
                new Configuration()
                        .set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1))
                        .set(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, 1)
                        .set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ZERO);
        FlussClusterExtension cluster =
                FlussClusterExtension.builder()
                        .setNumOfTabletServers(2)
                        .setClusterConf(conf)
                        .build();
        try {
            cluster.start();
            TablePath path = TablePath.of("bulkload_db", "orders");
            TableBucket bucket = createPrimaryKeyBucket(cluster, path, 2);
            CoordinatorGateway coordinator = cluster.newCoordinatorClient();
            PbBulkLoadHandle handle =
                    coordinator.beginBulkLoad(begin(path, "load")).get().getStatus().getHandle();
            ZooKeeperClient zk = cluster.getZooKeeperClient();
            new ZkSequenceIDCounter(
                            zk.getCuratorClient(),
                            ZkData.BucketSnapshotSequenceIdZNode.path(bucket))
                    .getAndAdd(1L << 40);
            AllocateBulkLoadBucketSnapshotResponse allocation =
                    allocateSnapshot(coordinator, handle, bucket);
            List<KvRecord> imported = genKvRecords(new Object[] {1, "imported"});
            CompletedSnapshotHandle input =
                    produceSnapshot(cluster, path, bucket, imported, allocation);
            int leader = cluster.waitAndGetLeader(bucket);
            int leaderEpoch = cluster.waitAndGetLeaderReplica(bucket).getLeaderEpoch();
            int coordinatorVersion = zk.getCurrentEpoch().getCoordinatorEpochZkVersion();

            // Leave the durable commit behind without dispatching the recovery notification.
            // Keeping the TabletServer leader online rules out recovery on a new leader epoch.
            cluster.stopCoordinatorServer();
            BulkLoadMetadataStore store = new BulkLoadMetadataStore(zk);
            BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction =
                    store.transaction(handle.getTableId(), handle.getBulkLoadId()).get();
            store.commitBucket(
                    transaction,
                    store.target(transaction.value, null).get(),
                    bucket,
                    store.bucket(transaction.value, bucket).get(),
                    new BucketSnapshot(
                            input.getSnapshotId(),
                            input.getLogOffset(),
                            input.getMetadataFilePath().toString()),
                    Collections.emptyList(),
                    coordinatorVersion);
            cluster.startCoordinatorServer();
            retry(Duration.ofMinutes(1), () -> assertRows(cluster, bucket, imported));
            assertThat(cluster.waitAndGetLeader(bucket)).isEqualTo(leader);
            assertThat(cluster.waitAndGetLeaderReplica(bucket).getLeaderEpoch())
                    .isEqualTo(leaderEpoch);

            CoordinatorGateway current = cluster.newCoordinatorClient();
            assertThat(
                            current.commitBulkLoad(new CommitBulkLoadRequest().setHandle(handle))
                                    .get()
                                    .getStatus()
                                    .getState())
                    .isEqualTo(BulkLoadTransaction.COMMITTED);
            List<KvRecord> tail = genKvRecords(new Object[] {2, "after-import"});
            putRecords(cluster, bucket, tail);
            List<KvRecord> expected = new ArrayList<>(imported);
            expected.addAll(tail);
            CompletedSnapshot ordinary = cluster.triggerAndWaitSnapshot(bucket);
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(
                                            input.getMetadataFilePath()
                                                    .getFileSystem()
                                                    .exists(input.getMetadataFilePath()))
                                    .isFalse());
            ZooKeeperClient.VersionedData<Long> selected =
                    zk.getRecoverySnapshotSelection(bucket).get();
            assertThat(selected.data()).isEqualTo(ordinary.getSnapshotID());
            current.commitBulkLoadBucketSnapshot(
                            new CommitBulkLoadBucketSnapshotRequest()
                                    .setHandle(handle)
                                    .setBucketId(0))
                    .get();
            assertThat(zk.getRecoverySnapshotSelection(bucket).get().data())
                    .isEqualTo(selected.data());
            assertThat(zk.getRecoverySnapshotSelection(bucket).get().zkVersion())
                    .isEqualTo(selected.zkVersion());

            waitForFullIsr(cluster, bucket);
            cluster.stopTabletServer(leader);
            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat(cluster.waitAndGetLeader(bucket)).isNotEqualTo(leader));
            assertRows(cluster, bucket, expected);
        } finally {
            cluster.close();
        }
    }

    private static void waitForFullIsr(FlussClusterExtension cluster, TableBucket bucket)
            throws Exception {
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(cluster.getZooKeeperClient().getLeaderAndIsr(bucket).get().isr())
                                .hasSize(2));
    }

    private static TableBucket createPrimaryKeyBucket(
            FlussClusterExtension cluster, TablePath path, int replicationFactor) throws Exception {
        long tableId =
                createTable(
                        cluster,
                        path,
                        TableDescriptor.builder()
                                .schema(DATA1_SCHEMA_PK)
                                .distributedBy(1, "a")
                                .build()
                                .withReplicationFactor(replicationFactor));
        TableBucket bucket = new TableBucket(tableId, 0);
        cluster.waitUntilAllReplicaReady(bucket);
        return bucket;
    }

    private static AllocateBulkLoadBucketSnapshotResponse allocateSnapshot(
            CoordinatorGateway coordinator, PbBulkLoadHandle handle, TableBucket bucket)
            throws Exception {
        return coordinator
                .allocateBulkLoadBucketSnapshot(
                        new AllocateBulkLoadBucketSnapshotRequest()
                                .setHandle(handle)
                                .setBucketId(bucket.getBucket()))
                .get();
    }

    private static BeginBulkLoadRequest begin(TablePath path, String id) {
        BeginBulkLoadRequest request = new BeginBulkLoadRequest().setBulkLoadId(id);
        request.setTablePath()
                .setDatabaseName(path.getDatabaseName())
                .setTableName(path.getTableName());
        return request;
    }

    private static CompletedSnapshotHandle produceSnapshot(
            FlussClusterExtension cluster,
            TablePath path,
            TableBucket bucket,
            List<KvRecord> records,
            AllocateBulkLoadBucketSnapshotResponse allocation)
            throws Exception {
        return TestingBulkLoadSnapshot.writeSnapshot(
                new FsPath(cluster.getRemoteDataDir()),
                PhysicalTablePath.of(path),
                bucket,
                allocation.getSnapshotId(),
                allocation.getLogOffset(),
                (long) records.size(),
                getKeyValuePairs(records));
    }

    private static void putRecords(
            FlussClusterExtension cluster, TableBucket bucket, List<KvRecord> records)
            throws Exception {
        PutKvResponse response =
                cluster.newTabletServerClientForNode(cluster.waitAndGetLeader(bucket))
                        .putKv(
                                newPutKvRequest(
                                        bucket.getTableId(),
                                        bucket.getBucket(),
                                        -1,
                                        toKvRecordBatch(records)))
                        .get();
        assertThat(response.getBucketsRespAt(0).hasErrorCode()).isFalse();
    }

    private static void assertRows(
            FlussClusterExtension cluster, TableBucket bucket, List<KvRecord> records)
            throws Exception {
        TabletServerGateway gateway =
                cluster.newTabletServerClientForNode(cluster.waitAndGetLeader(bucket));
        for (Tuple2<byte[], byte[]> keyValue : getKeyValuePairs(records)) {
            assertLookupResponse(
                    gateway.lookup(
                                    newLookupRequest(
                                            bucket.getTableId(), bucket.getBucket(), keyValue.f0))
                            .get(),
                    keyValue.f1);
        }
    }
}

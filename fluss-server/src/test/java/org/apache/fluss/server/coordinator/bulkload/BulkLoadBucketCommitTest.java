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
import org.apache.fluss.exception.BulkLoadNotFoundException;
import org.apache.fluss.exception.InvalidBulkLoadSnapshotException;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.KvSnapshotFileMetadata;
import org.apache.fluss.metadata.KvSnapshotFileMetadataJsonSerde;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.AllocateBulkLoadBucketSnapshotRequest;
import org.apache.fluss.rpc.messages.AllocateBulkLoadBucketSnapshotResponse;
import org.apache.fluss.rpc.messages.CommitBulkLoadBucketSnapshotRequest;
import org.apache.fluss.rpc.messages.GetTableStatsRequest;
import org.apache.fluss.rpc.messages.PbBulkLoadHandle;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.coordinator.CompletedSnapshotStoreManager;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotHandle;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotStore;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperClient.VersionedData;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.ZooKeeperUtils;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadBucket;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.fluss.shaded.curator5.org.apache.curator.retry.RetryOneTime;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.json.TableDescriptorJsonSerde;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.server.testutils.KvTestUtils.assertLookupResponse;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newAlterTableRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newLookupRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecords;
import static org.apache.fluss.testutils.DataTestUtils.getKeyValuePairs;
import static org.apache.fluss.testutils.DataTestUtils.toKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises the Coordinator commit against a prepared allocation and real ZooKeeper state. */
class BulkLoadBucketCommitTest {
    @RegisterExtension
    static final AllCallbackWrapper<ZooKeeperExtension> ZK =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static final TablePath TABLE = TablePath.of("db", "orders");
    private static final String LOAD_ID = "load";
    private static final long SNAPSHOT_ID = 7;
    private static final long WAL_OFFSET = 100;

    @TempDir Path remote;

    private ZooKeeperClient client;
    private BulkLoadManager manager;
    private CompletedSnapshotStoreManager snapshots;
    private TableBucket bucket;
    private String remoteDataDir;

    private void prepareAllocatedBucket() throws Exception {
        ZooKeeperExtension zk = ZK.getCustomExtension();
        client = zk.getZooKeeperClient(NOPErrorHandler.INSTANCE);
        zk.cleanupRoot();
        Configuration conf = new Configuration();
        MetadataManager metadata =
                new MetadataManager(client, conf, new LakeCatalogDynamicLoader(conf, null, true));
        metadata.createDatabase(TABLE.getDatabaseName(), DatabaseDescriptor.EMPTY, false);
        long tableId =
                metadata.createTable(
                        TABLE,
                        remote.toUri().toString(),
                        TableDescriptor.builder()
                                .schema(DATA1_SCHEMA_PK)
                                .distributedBy(1, "a")
                                .build(),
                        null,
                        false);
        bucket = new TableBucket(tableId, 0);
        TableInfo table = metadata.getTable(TABLE);
        remoteDataDir = table.getRemoteDataDir();
        String path = seedRunningTarget(client, table);
        createNode(
                client,
                path + "/targets/table/buckets/0",
                new BulkLoadBucket(SNAPSHOT_ID, WAL_OFFSET, BulkLoadBucket.ALLOCATED).toJson());
        createNode(client, ZkData.BucketIdZNode.path(bucket), new byte[0]);
        int epoch =
                client.fenceBecomeCoordinatorLeader("bucket-commit").getCoordinatorEpochZkVersion();
        manager = new BulkLoadManager(client, metadata, epoch);
        snapshots =
                new CompletedSnapshotStoreManager(
                        1,
                        Runnable::run,
                        client,
                        TestingMetricGroups.COORDINATOR_METRICS,
                        ignored -> false);
    }

    @Test
    void testCommitReplacesExistingRecoverySelection() throws Exception {
        prepareAllocatedBucket();
        CompletedSnapshotHandle uploaded = uploadSnapshot(SNAPSHOT_ID, WAL_OFFSET);
        long previousId = SNAPSHOT_ID + 1;
        CompletedSnapshotHandle previous = uploadSnapshot(previousId, WAL_OFFSET);
        client.registerTableBucketSnapshot(
                bucket,
                new BucketSnapshot(
                        previousId, WAL_OFFSET, previous.getMetadataFilePath().toString()));
        client.updateRecoverySnapshot(bucket, previousId, null, -1);
        assertThat(commit(WAL_OFFSET)).isTrue();
        assertThat(
                        client.getBulkLoadBucket(
                                        client.getBulkLoadTransaction(bucket.getTableId(), LOAD_ID)
                                                .get()
                                                .data(),
                                        bucket)
                                .get()
                                .data()
                                .state)
                .isEqualTo(BulkLoadBucket.COMMITTED);
        assertThat(client.getRecoveryBucketSnapshot(bucket))
                .contains(
                        new BucketSnapshot(
                                SNAPSHOT_ID,
                                WAL_OFFSET,
                                uploaded.getMetadataFilePath().toString()));
        CompletedSnapshotStore snapshotStore =
                snapshots.getOrCreateCompletedSnapshotStore(TABLE, bucket);
        snapshotStore.retainSnapshotsAfterImport();
        assertThat(snapshotStore.getActiveSnapshotIds()).containsExactly(SNAPSHOT_ID);
        assertThat(
                        previous.getMetadataFilePath()
                                .getFileSystem()
                                .exists(previous.getMetadataFilePath()))
                .isFalse();
    }

    @Test
    void testSnapshotOffsetMustMatchAllocation() throws Exception {
        prepareAllocatedBucket();
        uploadSnapshot(SNAPSHOT_ID, WAL_OFFSET + 1);
        assertThatThrownBy(() -> commit(WAL_OFFSET))
                .isInstanceOf(InvalidBulkLoadSnapshotException.class)
                .hasMessageContaining("Snapshot recovery offset differs");
        assertThat(client.getTableBucketSnapshot(bucket, SNAPSHOT_ID)).isEmpty();
        assertBucketNotCommitted();
    }

    @Test
    void testExistingReferencePreventsPartialCommit() throws Exception {
        prepareAllocatedBucket();
        CompletedSnapshotHandle uploaded = uploadSnapshot(SNAPSHOT_ID, WAL_OFFSET);
        BucketSnapshot existing =
                new BucketSnapshot(
                        SNAPSHOT_ID, WAL_OFFSET, uploaded.getMetadataFilePath().toString());
        client.registerTableBucketSnapshot(bucket, existing);

        assertThatThrownBy(() -> commit(WAL_OFFSET))
                .isInstanceOf(KeeperException.NodeExistsException.class);
        assertThat(client.getTableBucketSnapshot(bucket, SNAPSHOT_ID)).contains(existing);
        assertBucketNotCommitted();
    }

    @Test
    void testCommitReportsTableDeletedAfterValidation() throws Exception {
        prepareAllocatedBucket();
        uploadSnapshot(SNAPSHOT_ID, WAL_OFFSET);

        assertThatThrownBy(
                        () ->
                                manager.commitBucketSnapshot(
                                        LOAD_ID,
                                        bucket,
                                        ignored -> {
                                            client.deleteTable(TABLE);
                                            return WAL_OFFSET;
                                        },
                                        snapshots,
                                        () -> {
                                            throw new AssertionError(
                                                    "No recovery should be requested.");
                                        }))
                .isInstanceOf(BulkLoadNotFoundException.class);

        assertBucketNotCommitted();
        assertThat(client.getTableBucketSnapshot(bucket, SNAPSHOT_ID)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRequestsRecoveryOnlyIfPublicationWasAttempted(boolean publicationAttempted)
            throws Exception {
        prepareAllocatedBucket();
        CompletedSnapshotHandle uploaded = uploadSnapshot(SNAPSHOT_ID, WAL_OFFSET);
        // Force the actual multi to return NodeExists and then require a selection readback.
        client.registerTableBucketSnapshot(
                bucket,
                new BucketSnapshot(
                        SNAPSHOT_ID, WAL_OFFSET, uploaded.getMetadataFilePath().toString()));
        Configuration conf = new Configuration().set(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir);
        CuratorFrameworkWithUnhandledErrorListener curator =
                ZooKeeperUtils.startZookeeperClient(
                        CuratorFrameworkFactory.builder()
                                .connectString(ZK.getCustomExtension().getConnectString())
                                .namespace(client.getCuratorClient().getNamespace())
                                .retryPolicy(new RetryOneTime(0)),
                        NOPErrorHandler.INSTANCE);
        try (ZooKeeperClient failingClient =
                new ZooKeeperClient(curator, conf) {
                    private int selectionReads;

                    @Override
                    public Optional<VersionedData<Long>> getRecoverySnapshotSelection(
                            TableBucket bucket) throws Exception {
                        if (++selectionReads == (publicationAttempted ? 2 : 1)) {
                            throw new KeeperException.ConnectionLossException();
                        }
                        return super.getRecoverySnapshotSelection(bucket);
                    }
                }) {
            assertThat(curator.asCuratorFramework().blockUntilConnected(10, TimeUnit.SECONDS))
                    .isTrue();
            BulkLoadManager failingManager =
                    new BulkLoadManager(
                            failingClient,
                            new MetadataManager(
                                    client, conf, new LakeCatalogDynamicLoader(conf, null, true)),
                            client.getCurrentEpoch().getCoordinatorEpochZkVersion());
            AtomicBoolean recoveryRequested = new AtomicBoolean();
            assertThatThrownBy(
                            () ->
                                    failingManager.commitBucketSnapshot(
                                            LOAD_ID,
                                            bucket,
                                            ignored -> WAL_OFFSET,
                                            snapshots,
                                            () -> recoveryRequested.set(true)))
                    .isInstanceOf(
                            publicationAttempted
                                    ? NetworkException.class
                                    : KeeperException.ConnectionLossException.class);
            assertThat(recoveryRequested.get()).isEqualTo(publicationAttempted);
        }
        assertBucketNotCommitted();
    }

    @Test
    void testCommitRejectsPrivateFileFromPreviousSnapshot() throws Exception {
        prepareAllocatedBucket();
        CompletedSnapshotHandle previous = uploadSnapshot(SNAPSHOT_ID - 1, WAL_OFFSET);
        Path previousMetadata = Paths.get(previous.getMetadataFilePath().toUri());
        KvSnapshotFileMetadata old =
                KvSnapshotFileMetadataJsonSerde.fromJson(Files.readAllBytes(previousMetadata));
        CompletedSnapshotHandle uploaded = uploadSnapshot(SNAPSHOT_ID, WAL_OFFSET);
        Path metadata = Paths.get(uploaded.getMetadataFilePath().toUri());
        Files.write(
                metadata,
                KvSnapshotFileMetadataJsonSerde.toJson(
                        new KvSnapshotFileMetadata(
                                bucket,
                                SNAPSHOT_ID,
                                uploaded.getMetadataFilePath().getParent().toString(),
                                old.getSharedFiles(),
                                old.getPrivateFiles(),
                                old.getIncrementalSize(),
                                WAL_OFFSET,
                                0L,
                                null)));
        assertThatThrownBy(() -> commit(WAL_OFFSET))
                .isInstanceOf(InvalidBulkLoadSnapshotException.class)
                .hasMessageContaining("Snapshot file escapes");
        assertBucketNotCommitted();
    }

    @ParameterizedTest
    @EnumSource(BucketCommitScenario.class)
    void testBucketRpcs(BucketCommitScenario scenario) throws Exception {
        Configuration clusterConf =
                new Configuration()
                        .set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1))
                        .set(ConfigOptions.SERVER_IO_POOL_SIZE, 1);
        FlussClusterExtension cluster =
                FlussClusterExtension.builder()
                        .setNumOfTabletServers(1)
                        .setClusterConf(clusterConf)
                        .build();
        try {
            cluster.start();
            TablePath path = TablePath.of("bucket_rpc", "orders");
            long tableId =
                    createTable(
                            cluster,
                            path,
                            TableDescriptor.builder()
                                    .schema(DATA1_SCHEMA_PK)
                                    .distributedBy(1, "a")
                                    .build()
                                    .withReplicationFactor(1));
            TableBucket target = new TableBucket(tableId, 0);
            cluster.waitUntilAllReplicaReady(target);
            ZooKeeperClient zk = cluster.getZooKeeperClient();
            Configuration conf = new Configuration();
            MetadataManager metadata =
                    new MetadataManager(zk, conf, new LakeCatalogDynamicLoader(conf, null, true));
            TableInfo info = metadata.getTable(path);
            seedRunningTarget(zk, info);

            CoordinatorGateway coordinator = cluster.newCoordinatorClient();
            PbBulkLoadHandle handle =
                    new PbBulkLoadHandle().setTableId(tableId).setBulkLoadId(LOAD_ID);
            AllocateBulkLoadBucketSnapshotRequest allocate =
                    new AllocateBulkLoadBucketSnapshotRequest().setHandle(handle).setBucketId(0);
            AllocateBulkLoadBucketSnapshotResponse allocated =
                    coordinator.allocateBulkLoadBucketSnapshot(allocate).get(30, TimeUnit.SECONDS);
            List<KvRecord> rows = genKvRecords(new Object[] {1, "imported"});
            CompletedSnapshotHandle uploaded =
                    TestingBulkLoadSnapshot.writeSnapshot(
                            new FsPath(info.getRemoteDataDir()),
                            PhysicalTablePath.of(path),
                            target,
                            allocated.getSnapshotId(),
                            allocated.getLogOffset(),
                            1L,
                            getKeyValuePairs(rows));
            CommitBulkLoadBucketSnapshotRequest commit =
                    new CommitBulkLoadBucketSnapshotRequest().setHandle(handle).setBucketId(0);
            TabletServerGateway leader =
                    cluster.newTabletServerClientForNode(cluster.waitAndGetLeader(target));
            if (scenario == BucketCommitScenario.WAL_ADVANCED) {
                assertThat(
                                leader.putKv(
                                                newPutKvRequest(
                                                        tableId,
                                                        0,
                                                        -1,
                                                        toKvRecordBatch(
                                                                genKvRecords(
                                                                        new Object[] {
                                                                            2, "written"
                                                                        }))))
                                        .get(30, TimeUnit.SECONDS)
                                        .getBucketsRespAt(0)
                                        .hasErrorCode())
                        .isFalse();
                assertThatThrownBy(
                                () ->
                                        coordinator
                                                .commitBulkLoadBucketSnapshot(commit)
                                                .get(30, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(BulkLoadConflictException.class);
                assertThat(zk.getSelectedRecoverySnapshotId(target)).isEmpty();
                assertThat(zk.getTableBucketSnapshot(target, allocated.getSnapshotId())).isEmpty();
                assertThat(
                                coordinator
                                        .allocateBulkLoadBucketSnapshot(allocate)
                                        .get(30, TimeUnit.SECONDS)
                                        .getState())
                        .isEqualTo(BulkLoadBucket.ALLOCATED);
            } else if (scenario == BucketCommitScenario.COORDINATOR_TAKEOVER) {
                // Persist the commit without sending its recovery notification. The TabletServer
                // keeps the same leader role, so only Coordinator takeover can dispatch recovery.
                int leaderEpoch = cluster.waitAndGetLeaderReplica(target).getLeaderEpoch();
                int coordinatorVersion = zk.getCurrentEpoch().getCoordinatorEpochZkVersion();
                cluster.stopCoordinatorServer();
                VersionedData<BulkLoadTransaction> transaction =
                        zk.getBulkLoadTransaction(tableId, LOAD_ID).get();
                zk.commitBulkLoadBucket(
                        transaction,
                        zk.getBulkLoadTarget(transaction.data(), null).get(),
                        target,
                        zk.getBulkLoadBucket(transaction.data(), target).get(),
                        new BucketSnapshot(
                                uploaded.getSnapshotId(),
                                uploaded.getLogOffset(),
                                uploaded.getMetadataFilePath().toString()),
                        zk.getTableWithVersion(path).get().zkVersion(),
                        null,
                        coordinatorVersion);
                cluster.startCoordinatorServer();

                Tuple2<byte[], byte[]> imported = getKeyValuePairs(rows).get(0);
                retry(Duration.ofSeconds(30), () -> assertLookupRow(leader, tableId, imported));
                assertThat(cluster.waitAndGetLeaderReplica(target).getLeaderEpoch())
                        .isEqualTo(leaderEpoch);
            } else if (scenario == BucketCommitScenario.RECOVERY_FAILS) {
                // A valid-size CURRENT file passes publication validation but RocksDB cannot open
                // it.
                Path current =
                        Paths.get(uploaded.getMetadataFilePath().getParent().toUri())
                                .resolve("CURRENT");
                Files.write(current, new byte[Files.readAllBytes(current).length]);
                coordinator.commitBulkLoadBucketSnapshot(commit).get(30, TimeUnit.SECONDS);
                GetTableStatsRequest stats = new GetTableStatsRequest().setTableId(tableId);
                stats.addBucketsReq().setBucketId(0);
                retry(
                        Duration.ofSeconds(30),
                        () ->
                                assertThat(
                                                leader.getTableStats(stats)
                                                        .get(5, TimeUnit.SECONDS)
                                                        .getBucketsRespAt(0)
                                                        .getErrorCode())
                                        .isEqualTo(Errors.NOT_LEADER_OR_FOLLOWER.code()));
            } else {
                coordinator
                        .alterTable(
                                newAlterTableRequest(
                                        path,
                                        Collections.singletonMap("owner", "orders-team"),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        false))
                        .get(30, TimeUnit.SECONDS);
                coordinator.commitBulkLoadBucketSnapshot(commit).get(30, TimeUnit.SECONDS);
                coordinator.commitBulkLoadBucketSnapshot(commit).get(30, TimeUnit.SECONDS);
                AllocateBulkLoadBucketSnapshotResponse replay =
                        coordinator
                                .allocateBulkLoadBucketSnapshot(allocate)
                                .get(30, TimeUnit.SECONDS);
                assertThat(replay.getState()).isEqualTo(BulkLoadBucket.COMMITTED);
                assertThat(replay.getSnapshotId()).isEqualTo(allocated.getSnapshotId());
                assertThat(replay.getLogOffset()).isEqualTo(allocated.getLogOffset());
                assertThat(zk.getSelectedRecoverySnapshotId(target))
                        .contains(allocated.getSnapshotId());

                Tuple2<byte[], byte[]> imported = getKeyValuePairs(rows).get(0);
                retry(Duration.ofSeconds(30), () -> assertLookupRow(leader, tableId, imported));
                List<KvRecord> tail = genKvRecords(new Object[] {2, "after-import"});
                assertThat(
                                leader.putKv(newPutKvRequest(tableId, 0, -1, toKvRecordBatch(tail)))
                                        .get(30, TimeUnit.SECONDS)
                                        .getBucketsRespAt(0)
                                        .hasErrorCode())
                        .isFalse();
                long ordinarySnapshot = cluster.triggerAndWaitSnapshot(target).getSnapshotID();
                assertThat(ordinarySnapshot).isGreaterThan(allocated.getSnapshotId());
                retry(
                        Duration.ofSeconds(30),
                        () ->
                                assertThat(zk.getSelectedRecoverySnapshotId(target).get())
                                        .isEqualTo(ordinarySnapshot));
                assertLookupRow(leader, tableId, imported);
                assertLookupRow(leader, tableId, getKeyValuePairs(tail).get(0));
            }
        } finally {
            cluster.close();
        }
    }

    private static void assertLookupRow(
            TabletServerGateway leader, long tableId, Tuple2<byte[], byte[]> row) throws Exception {
        assertLookupResponse(
                leader.lookup(newLookupRequest(tableId, 0, row.f0)).get(5, TimeUnit.SECONDS),
                row.f1);
    }

    private void assertBucketNotCommitted() throws Exception {
        assertThat(
                        client.getBulkLoadBucket(
                                        client.getBulkLoadTransaction(bucket.getTableId(), LOAD_ID)
                                                .get()
                                                .data(),
                                        bucket)
                                .get()
                                .data()
                                .state)
                .isEqualTo(BulkLoadBucket.ALLOCATED);
        assertThat(client.getSelectedRecoverySnapshotId(bucket)).isEmpty();
    }

    private boolean commit(long leaderOffset) throws Exception {
        AtomicBoolean recoveryRequested = new AtomicBoolean();
        manager.commitBucketSnapshot(
                LOAD_ID,
                bucket,
                ignored -> leaderOffset,
                snapshots,
                () -> recoveryRequested.set(true));
        return recoveryRequested.get();
    }

    private CompletedSnapshotHandle uploadSnapshot(long snapshotId, long offset) throws Exception {
        return TestingBulkLoadSnapshot.writeSnapshot(
                new FsPath(remoteDataDir),
                PhysicalTablePath.of(TABLE),
                bucket,
                snapshotId,
                offset,
                0L,
                Collections.emptyList());
    }

    private static String seedRunningTarget(ZooKeeperClient client, TableInfo table)
            throws Exception {
        String path = "/bulk_load/tables/" + table.getTableId() + "/transactions/" + LOAD_ID;
        createNode(client, path, runningTransaction(table));
        createNode(
                client,
                path + "/targets/table",
                JsonSerdeUtils.writeValueAsBytes(
                        table,
                        (target, out) -> {
                            out.writeStartObject();
                            out.writeNumberField("version", 1);
                            out.writeNumberField("bucket_count", target.getNumBuckets());
                            out.writeStringField("remote_data_dir", target.getRemoteDataDir());
                            out.writeBooleanField("withdrawn", false);
                            out.writeEndObject();
                        }));
        createNode(client, path + "/targets/table/buckets", new byte[0]);
        return path;
    }

    private static byte[] runningTransaction(TableInfo table) {
        return JsonSerdeUtils.writeValueAsBytes(
                table,
                (info, out) -> {
                    out.writeStartObject();
                    out.writeNumberField("version", 1);
                    out.writeNumberField("state", BulkLoadTransaction.RUNNING);
                    out.writeNumberField("created_time", 10);
                    out.writeObjectFieldStart("table_path");
                    out.writeStringField("database_name", info.getTablePath().getDatabaseName());
                    out.writeStringField("table_name", info.getTablePath().getTableName());
                    out.writeEndObject();
                    out.writeObjectFieldStart("table_info");
                    out.writeNumberField("schema_id", info.getSchemaId());
                    out.writeFieldName("table_json");
                    TableDescriptorJsonSerde.INSTANCE.serialize(info.toTableDescriptor(), out);
                    out.writeNumberField("created_time", info.getCreatedTime());
                    out.writeNumberField("modified_time", info.getModifiedTime());
                    out.writeStringField("remote_data_dir", info.getRemoteDataDir());
                    out.writeNumberField("bucket_count_epoch", info.getBucketCountEpoch());
                    out.writeEndObject();
                    out.writeEndObject();
                });
    }

    private static void createNode(ZooKeeperClient client, String path, byte[] data)
            throws Exception {
        client.getCuratorClient().create().creatingParentsIfNeeded().forPath(path, data);
    }

    private enum BucketCommitScenario {
        RECOVERY_SUCCEEDS,
        WAL_ADVANCED,
        COORDINATOR_TAKEOVER,
        RECOVERY_FAILS
    }
}

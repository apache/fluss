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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.BulkLoadConflictException;
import org.apache.fluss.exception.BulkLoadNotFoundException;
import org.apache.fluss.exception.InvalidBulkLoadSnapshotException;
import org.apache.fluss.exception.NotCoordinatorLeaderException;
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.KvSnapshotFileMetadata;
import org.apache.fluss.metadata.KvSnapshotFileMetadataJsonSerde;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
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
import org.apache.fluss.rpc.messages.LookupRequest;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.PbBulkLoadHandle;
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrRespForBucket;
import org.apache.fluss.rpc.messages.PbPutKvRespForBucket;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadManager;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotHandle;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotStore;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.ZkData.BulkLoadTargetZNode;
import org.apache.fluss.server.zk.data.ZkData.BulkLoadTransactionZNode;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadBucket;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTarget;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.fluss.shaded.curator5.org.apache.curator.retry.RetryNTimes;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.Op;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.OpResult;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.ZooKeeper;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.function.RunnableWithException;
import org.apache.fluss.utils.types.Tuple2;

import io.github.fluss_contrib.rocksdb.Checkpoint;
import io.github.fluss_contrib.rocksdb.FlushOptions;
import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.RocksDB;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.metadata.TableBucketSnapshot.NO_SNAPSHOT_ID;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.server.testutils.KvTestUtils.assertLookupResponse;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createPartition;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newLookupRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeNotifyBucketLeaderAndIsr;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeNotifyLeaderAndIsrRequest;
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

    @TempDir Path remote;

    private ZooKeeperClient client;
    private BulkLoadManager manager;
    private CompletedSnapshotStoreManager snapshots;
    private TableBucket bucket;
    private String remoteDataDir;
    private long snapshotId;
    private long leasedSnapshotId = -1;

    private FlussClusterExtension cluster;
    private TabletServerGateway leader;
    private CoordinatorGateway coordinator;
    private AllocateBulkLoadBucketSnapshotRequest allocateRequest;
    private CommitBulkLoadBucketSnapshotRequest commitRequest;
    private PhysicalTablePath physical;
    private long offset;
    private int expectedLeaderEpoch;

    private void prepareBucket() throws Exception {
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
        physical = PhysicalTablePath.of(TABLE);
        seedRunningTarget(table);
        int epoch =
                client.fenceBecomeCoordinatorLeader("bucket-commit").getCoordinatorEpochZkVersion();
        client.registerLeaderAndIsr(bucket, new LeaderAndIsr(0, 0), epoch);
        manager = new BulkLoadManager(client, epoch);
        snapshots =
                new CompletedSnapshotStoreManager(
                        1,
                        Runnable::run,
                        client,
                        TestingMetricGroups.COORDINATOR_METRICS,
                        snapshot -> snapshot.getSnapshotID() == leasedSnapshotId);
    }

    private void prepareAllocatedBucket() throws Exception {
        prepareBucket();
        BulkLoadBucket allocation = manager.allocateBucketSnapshot(LOAD_ID, bucket);
        snapshotId = allocation.snapshotId;
        offset = allocation.logOffset;
    }

    @Test
    void testImportRetainsLeasedSnapshot() throws Exception {
        prepareAllocatedBucket();
        long previousId = client.allocateBucketSnapshotId(bucket);
        FsPath previous = uploadSnapshot(previousId, offset);
        FsPath uploaded = uploadSnapshot(snapshotId, offset);
        // A legacy reference may predate the recovery selection znode.
        client.registerTableBucketSnapshot(
                bucket, new BucketSnapshot(previousId, offset, previous.toString()));
        leasedSnapshotId = previousId;
        commitAllocatedSnapshot();

        assertThat(client.getRecoveryBucketSnapshot(bucket))
                .contains(new BucketSnapshot(snapshotId, offset, uploaded.toString()));
        assertThat(Paths.get(previous.toUri())).exists();

        CompletedSnapshotStore store = snapshots.getOrCreateCompletedSnapshotStore(TABLE, bucket);
        assertThat(store.getNumSnapshots()).isOne();
        assertThat(store.getActiveSnapshotIds()).containsExactlyInAnyOrder(snapshotId, previousId);
    }

    @Test
    void testSnapshotOffsetMustMatchAllocation() throws Exception {
        prepareAllocatedBucket();
        uploadSnapshot(snapshotId, offset + 1);
        assertThatThrownBy(this::commitAllocatedSnapshot)
                .isInstanceOf(InvalidBulkLoadSnapshotException.class);
        assertBucketNotCommitted();
    }

    @Test
    void testCommitRejectsAStaleBucketRole() throws Exception {
        prepareAllocatedBucket();
        uploadSnapshot(snapshotId, offset);
        LeaderAndIsr role = client.getLeaderAndIsr(bucket).get();
        int epoch = client.getCurrentEpoch().getCoordinatorEpochZkVersion();
        assertThatThrownBy(
                        () ->
                                commitAfterRoleRead(
                                        () ->
                                                client.updateLeaderAndIsr(
                                                        bucket,
                                                        role.newLeaderAndIsr(
                                                                role.leader(),
                                                                role.isr(),
                                                                role.standbyReplicas()),
                                                        epoch)))
                .isInstanceOf(NotLeaderOrFollowerException.class);
        assertBucketNotCommitted();
        commitAllocatedSnapshot();
        assertThat(client.getLeaderAndIsr(bucket).get().leaderEpoch())
                .isEqualTo(role.leaderEpoch() + 2);
    }

    @Test
    void testCommitReportsTableDeletedAfterValidation() throws Exception {
        prepareAllocatedBucket();
        uploadSnapshot(snapshotId, offset);
        assertThatThrownBy(() -> commitAfterRoleRead(() -> client.deleteTable(TABLE)))
                .isInstanceOf(BulkLoadNotFoundException.class);
        assertBucketNotCommitted();
    }

    @Test
    void testAllocateAfterCoordinatorTakeover() throws Exception {
        prepareBucket();
        client.fenceBecomeCoordinatorLeader("replacement");
        assertThatThrownBy(() -> manager.allocateBucketSnapshot(LOAD_ID, bucket))
                .isInstanceOf(NotCoordinatorLeaderException.class);
        BulkLoadTransaction txn =
                client.getBulkLoadTransaction(bucket.getTableId(), LOAD_ID).get().data();
        assertThat(client.getBulkLoadBucket(txn, bucket)).isEmpty();
    }

    @Test
    void testCommitAfterCoordinatorTakeover() throws Exception {
        prepareAllocatedBucket();
        uploadSnapshot(snapshotId, offset);
        client.fenceBecomeCoordinatorLeader("replacement");
        assertThatThrownBy(this::commitAllocatedSnapshot)
                .isInstanceOf(NotCoordinatorLeaderException.class);
        assertBucketNotCommitted();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCommitRequiresPrivateFilesInTheAllocatedSnapshot(boolean shared) throws Exception {
        prepareAllocatedBucket();
        FsPath dir = snapshotDirectory(snapshotId);
        Path parent = shared ? Files.createDirectories(Paths.get(dir.toUri())) : remote;
        Path file = Files.write(parent.resolve("data.sst"), new byte[] {1});
        List<KvSnapshotFileMetadata.FileHandle> files =
                Collections.singletonList(
                        new KvSnapshotFileMetadata.FileHandle(
                                new FsPath(file.toUri()).toString(), 1, "data.sst"));
        writeManifest(
                dir,
                snapshotId,
                offset,
                0L,
                shared ? files : Collections.emptyList(),
                shared ? Collections.emptyList() : files);
        assertThatThrownBy(this::commitAllocatedSnapshot)
                .isInstanceOf(InvalidBulkLoadSnapshotException.class);
        assertBucketNotCommitted();
    }

    @AfterEach
    void closeCluster() throws Exception {
        if (cluster != null) {
            cluster.close();
        }
    }

    private void start(TableDescriptor descriptor, String partitionName) throws Exception {
        Configuration conf =
                new Configuration()
                        .set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1))
                        .set(ConfigOptions.SERVER_IO_POOL_SIZE, 1);
        cluster =
                FlussClusterExtension.builder()
                        .setNumOfTabletServers(1)
                        .setClusterConf(conf)
                        .build();
        cluster.start();
        long tableId = createTable(cluster, TABLE, descriptor.withReplicationFactor(1));
        Long partitionId =
                partitionName == null
                        ? null
                        : createPartition(
                                cluster,
                                TABLE,
                                new PartitionSpec(Collections.singletonMap("pt", partitionName)),
                                false);
        bucket = new TableBucket(tableId, partitionId, 0);
        physical = PhysicalTablePath.of(TABLE, partitionName);
        client = cluster.getZooKeeperClient();
        Replica replica = cluster.waitAndGetLeaderReplica(bucket);
        remoteDataDir = replica.getRemoteDataDir();
        seedRunningTarget(replica.getTableInfo());
        expectedLeaderEpoch = replica.getLeaderEpoch() + 1;
        leader = cluster.newTabletServerClientForNode(replica.getLeaderId());
        coordinator = cluster.newCoordinatorClient();
        manager =
                new BulkLoadManager(
                        client, client.getCurrentEpoch().getCoordinatorEpochZkVersion());
        snapshots =
                cluster.getCoordinatorServer()
                        .getCoordinatorEventProcessor()
                        .completedSnapshotStoreManager();
        PbBulkLoadHandle handle = new PbBulkLoadHandle().setTableId(tableId).setBulkLoadId(LOAD_ID);
        allocateRequest =
                new AllocateBulkLoadBucketSnapshotRequest().setHandle(handle).setBucketId(0);
        commitRequest = new CommitBulkLoadBucketSnapshotRequest().setHandle(handle).setBucketId(0);
        if (partitionId != null) {
            allocateRequest.setPartitionId(partitionId);
            commitRequest.setPartitionId(partitionId);
        }
    }

    private void start() throws Exception {
        start(
                TableDescriptor.builder().schema(DATA1_SCHEMA_PK).distributedBy(1, "a").build(),
                null);
    }

    private void allocate() throws Exception {
        AllocateBulkLoadBucketSnapshotResponse response =
                coordinator
                        .allocateBulkLoadBucketSnapshot(allocateRequest)
                        .get(30, TimeUnit.SECONDS);
        snapshotId = response.getSnapshotId();
        offset = response.getLogOffset();
    }

    private void commit() throws Exception {
        coordinator.commitBulkLoadBucketSnapshot(commitRequest).get(30, TimeUnit.SECONDS);
    }

    private Tuple2<byte[], byte[]> uploadCheckpoint(List<KvRecord> rows) throws Exception {
        List<Tuple2<byte[], byte[]>> values = getKeyValuePairs(rows);
        FsPath dir = snapshotDirectory(snapshotId);
        Path directory = Paths.get(dir.toUri());
        Files.createDirectories(directory.getParent());
        try (Options options = new Options().setCreateIfMissing(true);
                RocksDB db = RocksDB.open(options, remote.resolve("source-db").toString());
                FlushOptions flush = new FlushOptions().setWaitForFlush(true);
                Checkpoint checkpoint = Checkpoint.create(db)) {
            for (Tuple2<byte[], byte[]> entry : values) {
                db.put(entry.f0, entry.f1);
            }
            db.flush(flush);
            checkpoint.createCheckpoint(directory.toString());
        }
        List<KvSnapshotFileMetadata.FileHandle> files = new ArrayList<>();
        try (DirectoryStream<Path> paths =
                Files.newDirectoryStream(directory, Files::isRegularFile)) {
            for (Path file : paths) {
                String name = file.getFileName().toString();
                files.add(
                        new KvSnapshotFileMetadata.FileHandle(
                                new FsPath(dir, name).toString(), Files.size(file), name));
            }
        }
        writeManifest(dir, snapshotId, offset, rows.size(), Collections.emptyList(), files);
        return values.get(0);
    }

    private void assertRecovered(Tuple2<byte[], byte[]> imported) throws Exception {
        assertThat(client.getSelectedRecoverySnapshotId(bucket)).contains(snapshotId);
        retry(Duration.ofSeconds(30), () -> assertLookupRow(imported));
        assertThat(cluster.waitAndGetLeaderReplica(bucket).getLeaderEpoch())
                .isEqualTo(expectedLeaderEpoch);
    }

    @Test
    void testAdvancedRecoveryOffsetRejectsCommit() throws Exception {
        prepareAllocatedBucket();
        uploadSnapshot(snapshotId, offset);
        long selected = client.allocateBucketSnapshotId(bucket);
        CompletedSnapshotHandle successor =
                new CompletedSnapshotHandle(
                        selected, uploadSnapshot(selected, offset + 1), offset + 1);
        snapshots
                .getOrCreateCompletedSnapshotStore(TABLE, bucket)
                .add(
                        successor.retrieveCompleteSnapshot(),
                        NO_SNAPSHOT_ID,
                        client.getLeaderAndIsr(bucket).get().leaderEpoch());
        BulkLoadBucket replay = manager.allocateBucketSnapshot(LOAD_ID, bucket);
        assertThat(replay.state).isEqualTo(BulkLoadBucket.ALLOCATED);
        assertThat(replay.snapshotId).isEqualTo(snapshotId);
        assertThat(replay.logOffset).isEqualTo(offset);
        assertThatThrownBy(this::commitAllocatedSnapshot)
                .isInstanceOf(BulkLoadConflictException.class);
        assertThat(client.getSelectedRecoverySnapshotId(bucket)).contains(selected);
        assertThat(client.getTableBucketSnapshot(bucket, snapshotId)).isEmpty();
    }

    @Test
    void testFailedRecoveryStopsServingUntilRoleIsReapplied() throws Exception {
        start();
        allocate();
        Tuple2<byte[], byte[]> imported =
                uploadCheckpoint(genKvRecords(new Object[] {1, "imported"}));
        // Keep the declared size so the invalid CURRENT reaches RocksDB recovery.
        Path current = Paths.get(snapshotDirectory(snapshotId).toUri()).resolve("CURRENT");
        byte[] original = Files.readAllBytes(current);
        Files.write(current, new byte[original.length]);
        LeaderAndIsr published = manager.commitBucketSnapshot(LOAD_ID, bucket, snapshots).get();
        assertThat(notifyRole(published).hasErrorCode()).isTrue();

        LookupRequest lookup =
                newLookupRequest(bucket.getTableId(), bucket.getBucket(), imported.f0);
        lookup.getBucketsReqAt(0).setRoutingBucketCount(1);
        assertThat(
                        leader.lookup(lookup)
                                .get(5, TimeUnit.SECONDS)
                                .getBucketsRespAt(0)
                                .getErrorCode())
                .isEqualTo(Errors.NOT_LEADER_OR_FOLLOWER.code());

        Files.write(current, original);
        assertThat(notifyRole(published).hasErrorCode()).isFalse();
        assertRecovered(imported);
    }

    @Test
    void testCoordinatorTakeoverActivatesPersistedRole() throws Exception {
        start();
        allocate();
        Tuple2<byte[], byte[]> imported =
                uploadCheckpoint(genKvRecords(new Object[] {1, "imported"}));
        LeaderAndIsr role = client.getLeaderAndIsr(bucket).get();
        LeaderAndIsr futureRole =
                role.newLeaderAndIsr(role.leader(), role.isr(), role.standbyReplicas());
        assertThat(notifyRole(futureRole).getErrorCode())
                .isEqualTo(Errors.LEADER_NOT_AVAILABLE_EXCEPTION.code());
        assertThat(cluster.waitAndGetLeaderReplica(bucket).getLeaderEpoch())
                .isEqualTo(role.leaderEpoch());
        // Commit durably without delivering the notification, as on Coordinator failure.
        manager.commitBucketSnapshot(LOAD_ID, bucket, snapshots);
        cluster.stopCoordinatorServer();
        cluster.startCoordinatorServer();
        assertRecovered(imported);
    }

    private PbNotifyLeaderAndIsrRespForBucket notifyRole(LeaderAndIsr role) throws Exception {
        NotifyLeaderAndIsrRequest request =
                makeNotifyLeaderAndIsrRequest(
                        client.getCurrentEpoch().getCoordinatorEpoch(),
                        Collections.singletonList(
                                makeNotifyBucketLeaderAndIsr(
                                        new NotifyLeaderAndIsrData(
                                                physical, bucket, role.isr(), role, 1, null))));
        return leader.notifyLeaderAndIsr(request)
                .get(30, TimeUnit.SECONDS)
                .getNotifyBucketsLeaderRespAt(0);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testConnectionLossDuringPublication(boolean persisted) throws Exception {
        prepareAllocatedBucket();
        uploadSnapshot(snapshotId, offset);
        try (CuratorFramework curator = losePublicationResponse(client, persisted)) {
            curator.start();
            assertThat(curator.blockUntilConnected(30, TimeUnit.SECONDS)).isTrue();
            ZooKeeperClient publisher =
                    new ZooKeeperClient(
                            new CuratorFrameworkWithUnhandledErrorListener(
                                    curator, (message, error) -> {}),
                            new Configuration().set(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir));
            BulkLoadManager publishingManager =
                    new BulkLoadManager(
                            publisher, client.getCurrentEpoch().getCoordinatorEpochZkVersion());
            if (!persisted) {
                assertThatThrownBy(
                                () ->
                                        publishingManager.commitBucketSnapshot(
                                                LOAD_ID, bucket, snapshots))
                        .isInstanceOf(KeeperException.ConnectionLossException.class);
                assertBucketNotCommitted();
                return;
            }
            assertThat(publishingManager.commitBucketSnapshot(LOAD_ID, bucket, snapshots))
                    .contains(client.getLeaderAndIsr(bucket).get());
        }
        assertThat(client.getSelectedRecoverySnapshotId(bucket)).contains(snapshotId);
    }

    @Test
    void testPartitionRecoveryReplaysWalAndPreservesSuccessorOnRetry() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("pt", DataTypes.STRING())
                        .primaryKey("a", "pt")
                        .build();
        start(
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1, "a")
                        .partitionedBy("pt")
                        .build(),
                "p1");
        putRows(genKvRecords(schema.getRowType(), new Object[] {0, "base", "p1"}));
        long recoveryOffset = cluster.triggerAndWaitSnapshot(bucket).getLogOffset();
        List<KvRecord> beforeAllocation =
                genKvRecords(schema.getRowType(), new Object[] {3, "before-allocation", "p1"});
        putRows(beforeAllocation);
        allocate();
        assertThat(offset).isEqualTo(recoveryOffset);
        Tuple2<byte[], byte[]> imported =
                uploadCheckpoint(
                        genKvRecords(schema.getRowType(), new Object[] {1, "imported", "p1"}));
        List<KvRecord> tail =
                genKvRecords(schema.getRowType(), new Object[] {2, "during-upload", "p1"});
        putRows(tail);
        Tuple2<byte[], byte[]> appended = getKeyValuePairs(tail).get(0);
        // A second commit validated the import before the ordinary successor was published.
        Optional<LeaderAndIsr> replayedRole =
                commitAfterRoleRead(() -> commitAndAdvanceRecoverySnapshot(imported));
        assertThat(replayedRole).isEmpty();
        long successor = client.getSelectedRecoverySnapshotId(bucket).get();
        assertThat(successor).isGreaterThan(snapshotId);
        commit();
        AllocateBulkLoadBucketSnapshotResponse replay =
                coordinator
                        .allocateBulkLoadBucketSnapshot(allocateRequest)
                        .get(30, TimeUnit.SECONDS);
        assertThat(replay.getState()).isEqualTo(BulkLoadBucket.COMMITTED);
        assertThat(replay.getSnapshotId()).isEqualTo(snapshotId);
        assertThat(replay.getLogOffset()).isEqualTo(offset);
        assertThat(client.getSelectedRecoverySnapshotId(bucket)).contains(successor);
        assertThat(client.getLeaderAndIsr(bucket).get().leaderEpoch())
                .isEqualTo(expectedLeaderEpoch);
        assertLookupRow(getKeyValuePairs(beforeAllocation).get(0));
        assertLookupRow(appended);
    }

    private void commitAndAdvanceRecoverySnapshot(Tuple2<byte[], byte[]> imported)
            throws Exception {
        commit();
        assertRecovered(imported);
        long successor = cluster.triggerAndWaitSnapshot(bucket).getSnapshotID();
        retry(
                Duration.ofSeconds(30),
                () -> assertThat(client.getSelectedRecoverySnapshotId(bucket)).contains(successor));
    }

    private static CuratorFramework losePublicationResponse(
            ZooKeeperClient client, boolean persisted) {
        CuratorFramework existing = client.getCuratorClient();
        return CuratorFrameworkFactory.builder()
                .connectString(existing.getZookeeperClient().getCurrentConnectionString())
                .namespace(existing.getNamespace())
                .retryPolicy(new RetryNTimes(0, 0))
                .zookeeperFactory(
                        (address, sessionTimeout, watcher, readOnly) ->
                                new ZooKeeper(address, sessionTimeout, watcher, readOnly) {
                                    @Override
                                    public List<OpResult> multi(Iterable<Op> operations)
                                            throws InterruptedException, KeeperException {
                                        if (persisted) {
                                            super.multi(operations);
                                        }
                                        throw new KeeperException.ConnectionLossException();
                                    }
                                })
                .build();
    }

    private void putRows(List<KvRecord> rows) throws Exception {
        PutKvRequest request =
                newPutKvRequest(
                        bucket.getTableId(),
                        bucket.getPartitionId(),
                        bucket.getBucket(),
                        -1,
                        toKvRecordBatch(rows));
        request.getBucketsReqAt(0).setRoutingBucketCount(1);
        PbPutKvRespForBucket result =
                leader.putKv(request).get(30, TimeUnit.SECONDS).getBucketsRespAt(0);
        assertThat(result.hasErrorCode())
                .as(result.hasErrorMessage() ? result.getErrorMessage() : "")
                .isFalse();
    }

    private void assertLookupRow(Tuple2<byte[], byte[]> row) throws Exception {
        LookupRequest request = newLookupRequest(bucket.getTableId(), bucket.getBucket(), row.f0);
        request.getBucketsReqAt(0).setRoutingBucketCount(1);
        if (bucket.getPartitionId() != null) {
            request.getBucketsReqAt(0).setPartitionId(bucket.getPartitionId());
        }
        assertLookupResponse(leader.lookup(request).get(5, TimeUnit.SECONDS), row.f1);
    }

    private void assertBucketNotCommitted() throws Exception {
        BulkLoadTransaction transaction =
                client.getBulkLoadTransaction(bucket.getTableId(), LOAD_ID).get().data();
        assertThat(client.getBulkLoadBucket(transaction, bucket).get().state)
                .isEqualTo(BulkLoadBucket.ALLOCATED);
        assertThat(client.getSelectedRecoverySnapshotId(bucket)).isEmpty();
        assertThat(client.getTableBucketSnapshot(bucket, snapshotId)).isEmpty();
    }

    private Optional<LeaderAndIsr> commitAllocatedSnapshot() throws Exception {
        return manager.commitBucketSnapshot(LOAD_ID, bucket, snapshots);
    }

    private Optional<LeaderAndIsr> commitAfterRoleRead(RunnableWithException action)
            throws Exception {
        ZooKeeperClient racingClient =
                new ZooKeeperClient(
                        client.getCuratorFrameworkWrapper(),
                        new Configuration().set(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir)) {
                    private boolean firstRead = true;

                    @Override
                    public Optional<LeaderAndIsr> getLeaderAndIsr(TableBucket tableBucket)
                            throws Exception {
                        Optional<LeaderAndIsr> role = super.getLeaderAndIsr(tableBucket);
                        if (firstRead) {
                            firstRead = false;
                            action.run();
                        }
                        return role;
                    }
                };
        return new BulkLoadManager(
                        racingClient, client.getCurrentEpoch().getCoordinatorEpochZkVersion())
                .commitBucketSnapshot(LOAD_ID, bucket, snapshots);
    }

    private FsPath uploadSnapshot(long id, long logOffset) throws Exception {
        FsPath dir = snapshotDirectory(id);
        return writeManifest(
                dir, id, logOffset, 0L, Collections.emptyList(), Collections.emptyList());
    }

    private FsPath snapshotDirectory(long id) {
        FsPath tabletDir =
                FlussPaths.remoteKvTabletDir(
                        FlussPaths.remoteKvDir(remoteDataDir), physical, bucket);
        return FlussPaths.remoteKvSnapshotDir(tabletDir, id);
    }

    private FsPath writeManifest(
            FsPath dir,
            long id,
            long logOffset,
            long rowCount,
            List<KvSnapshotFileMetadata.FileHandle> sharedFiles,
            List<KvSnapshotFileMetadata.FileHandle> privateFiles)
            throws Exception {
        Files.createDirectories(Paths.get(dir.toUri()));
        FsPath metadata = CompletedSnapshot.getMetadataFilePath(dir);
        long size =
                privateFiles.stream().mapToLong(KvSnapshotFileMetadata.FileHandle::getSize).sum();
        Files.write(
                Paths.get(metadata.toUri()),
                KvSnapshotFileMetadataJsonSerde.toJson(
                        new KvSnapshotFileMetadata(
                                bucket,
                                id,
                                dir.toString(),
                                sharedFiles,
                                privateFiles,
                                size,
                                logOffset,
                                rowCount,
                                null)));
        return metadata;
    }

    private void seedRunningTarget(TableInfo table) throws Exception {
        Long partitionId = bucket.getPartitionId();
        String path = BulkLoadTransactionZNode.path(table.getTableId(), LOAD_ID);
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode transaction = mapper.createObjectNode();
        transaction
                .put("version", 1)
                .put("state", BulkLoadTransaction.RUNNING)
                .put("created_time", 10);
        transaction
                .putObject("table_path")
                .put("database_name", table.getTablePath().getDatabaseName())
                .put("table_name", table.getTablePath().getTableName());
        transaction
                .put("schema_id", table.getSchemaId())
                .put("remote_data_dir", table.getRemoteDataDir());
        createNode(path, mapper.writeValueAsBytes(transaction));

        String targetPath = BulkLoadTargetZNode.path(table.getTableId(), LOAD_ID, partitionId);
        BulkLoadTarget target =
                new BulkLoadTarget(
                        partitionId,
                        physical.getPartitionName(),
                        table.getNumBuckets(),
                        remoteDataDir,
                        false);
        createNode(targetPath, BulkLoadTargetZNode.encode(target));
        createNode(targetPath + "/buckets", new byte[0]);
    }

    private void createNode(String path, byte[] data) throws Exception {
        client.getCuratorClient().create().creatingParentsIfNeeded().forPath(path, data);
    }
}

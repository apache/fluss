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
import org.apache.fluss.exception.FencedLeaderEpochException;
import org.apache.fluss.exception.InvalidBulkLoadSnapshotException;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.exception.NotCoordinatorLeaderException;
import org.apache.fluss.exception.UnknownServerException;
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
import org.apache.fluss.rpc.messages.AcquireKvSnapshotLeaseRequest;
import org.apache.fluss.rpc.messages.AdjustIsrResponse;
import org.apache.fluss.rpc.messages.AllocateBulkLoadBucketSnapshotRequest;
import org.apache.fluss.rpc.messages.AllocateBulkLoadBucketSnapshotResponse;
import org.apache.fluss.rpc.messages.CommitBulkLoadBucketSnapshotRequest;
import org.apache.fluss.rpc.messages.CommitKvSnapshotResponse;
import org.apache.fluss.rpc.messages.DropKvSnapshotLeaseRequest;
import org.apache.fluss.rpc.messages.LookupRequest;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.PbAdjustIsrRespForBucket;
import org.apache.fluss.rpc.messages.PbBulkLoadHandle;
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrRespForBucket;
import org.apache.fluss.rpc.messages.PbPutKvRespForBucket;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadManager;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.coordinator.event.AdjustIsrReceivedEvent;
import org.apache.fluss.server.coordinator.event.CommitKvSnapshotEvent;
import org.apache.fluss.server.coordinator.event.CoordinatorEventManager;
import org.apache.fluss.server.coordinator.lease.KvSnapshotLeaseManager;
import org.apache.fluss.server.entity.CommitKvSnapshotData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotHandle;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotStore;
import org.apache.fluss.server.kv.snapshot.KvFileHandleAndLocalPath;
import org.apache.fluss.server.metadata.CoordinatorMetadataCache;
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
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.Op;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.OpResult;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.ZooKeeper;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.types.Tuple2;

import io.github.fluss_contrib.rocksdb.Checkpoint;
import io.github.fluss_contrib.rocksdb.CompactionOptions;
import io.github.fluss_contrib.rocksdb.FlushOptions;
import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.RocksDB;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.fluss.metadata.TableBucketSnapshot.NO_SNAPSHOT_ID;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.server.testutils.KvTestUtils.assertLookupResponse;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createPartition;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newLookupRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeAdjustIsrRequest;
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
    private CoordinatorEventProcessor publicationProcessor;
    private KvSnapshotLeaseManager publicationLeases;
    private TestCoordinatorChannelManager publicationChannel;
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
    void testRetentionReclaimsUnreusedImportedSst() throws Exception {
        prepareAllocatedBucket();
        FsPath dir = snapshotDirectory(snapshotId);
        Path sharedDir =
                Files.createDirectories(
                        Paths.get(FlussPaths.remoteKvSharedDir(dir.getParent()).toUri()));
        Path sst = Files.write(sharedDir.resolve("uploaded"), new byte[] {1});
        writeManifest(
                dir,
                snapshotId,
                offset,
                null,
                Collections.singletonList(
                        new KvSnapshotFileMetadata.FileHandle(
                                new FsPath(sst.toUri()).toString(), 1, "000001.sst")),
                Collections.emptyList());
        int oldEpoch = client.getLeaderAndIsr(bucket).get().leaderEpoch();
        long old = client.allocateBucketSnapshotId(bucket);
        CompletedSnapshot stale =
                new CompletedSnapshotHandle(old, uploadSnapshot(old, offset), offset)
                        .retrieveCompleteSnapshot();
        ManuallyTriggeredScheduledExecutorService io =
                new ManuallyTriggeredScheduledExecutorService();
        CoordinatorEventProcessor processor = startPublicationProcessor(client, io);
        CompletableFuture<Void> commit =
                manager.commitBucketSnapshot(
                        LOAD_ID, bucket, snapshots, processor.getCoordinatorEventManager());
        retry(Duration.ofSeconds(30), () -> assertThat(io.numQueuedRunnables()).isOne());
        CompletedSnapshotStore store = snapshots.getOrCreateCompletedSnapshotStore(TABLE, bucket);
        // The old ordinary commit reads back the selected import before its files are registered.
        assertThatThrownBy(() -> store.add(stale, NO_SNAPSHOT_ID, oldEpoch))
                .isInstanceOf(FencedLeaderEpochException.class);
        io.triggerAll();
        commit.get(30, TimeUnit.SECONDS);
        assertThat(store.getPhysicalStorageRemoteKvSize()).isOne();
        long next = client.allocateBucketSnapshotId(bucket);
        store.add(
                new CompletedSnapshotHandle(next, uploadSnapshot(next, offset), offset)
                        .retrieveCompleteSnapshot(),
                snapshotId,
                client.getLeaderAndIsr(bucket).get().leaderEpoch());
        assertThat(sst).doesNotExist();
    }

    @Test
    void testSnapshotOffsetMustMatchAllocation() throws Exception {
        prepareAllocatedBucket();
        uploadSnapshot(snapshotId, offset + 1);
        assertThatThrownBy(this::commitAllocatedSnapshot)
                .isInstanceOf(InvalidBulkLoadSnapshotException.class)
                .hasMessageContaining(
                        "log_offset %d, but Allocate returned %d", offset + 1, offset);
        assertBucketNotCommitted();
    }

    @Test
    void testShutdownRejectsQueuedPublication() throws Exception {
        prepareAllocatedBucket();
        uploadSnapshot(snapshotId, offset);
        CoordinatorEventManager events =
                startPublicationProcessor(client, ForkJoinPool.commonPool())
                        .getCoordinatorEventManager();
        CompletableFuture<Void> processing = new CompletableFuture<>();
        CompletableFuture<Void> resume = new CompletableFuture<>();
        events.put(
                new AccessContextEvent<>(
                        context -> {
                            processing.complete(null);
                            return resume.join();
                        }));
        try {
            processing.get(30, TimeUnit.SECONDS);
            CompletableFuture<Void> commit =
                    manager.commitBucketSnapshot(LOAD_ID, bucket, snapshots, events);
            assertThat(commit).isNotDone();
            CompletableFuture<Void> closed = CompletableFuture.runAsync(events::close);
            try {
                assertThatThrownBy(() -> commit.get(30, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(NotCoordinatorLeaderException.class);
                assertBucketNotCommitted();
            } finally {
                resume.complete(null);
                closed.get(30, TimeUnit.SECONDS);
            }
        } finally {
            resume.complete(null);
        }
    }

    @Test
    void testRejectedRegistrationLeavesDurableCommitRetryable() throws Exception {
        prepareAllocatedBucket();
        uploadSnapshot(snapshotId, offset);
        ExecutorService closed = Executors.newSingleThreadExecutor();
        closed.shutdown();
        CoordinatorEventManager events =
                startPublicationProcessor(client, closed).getCoordinatorEventManager();
        assertThatThrownBy(
                        () ->
                                manager.commitBucketSnapshot(LOAD_ID, bucket, snapshots, events)
                                        .get(30, TimeUnit.SECONDS))
                .hasCauseInstanceOf(UnknownServerException.class)
                .hasRootCauseInstanceOf(RejectedExecutionException.class);
        assertThat(client.getSelectedRecoverySnapshotId(bucket)).contains(snapshotId);
        manager.commitBucketSnapshot(LOAD_ID, bucket, snapshots, events).get(30, TimeUnit.SECONDS);
    }

    @Test
    void testCommitReportsTableDeletedAfterValidation() throws Exception {
        prepareAllocatedBucket();
        uploadSnapshot(snapshotId, offset);
        CoordinatorEventManager events =
                startPublicationProcessor(client, ForkJoinPool.commonPool())
                        .getCoordinatorEventManager();
        assertThatThrownBy(
                        () ->
                                manager.commitBucketSnapshot(
                                                LOAD_ID,
                                                bucket,
                                                snapshots,
                                                event -> {
                                                    try {
                                                        client.deleteTable(TABLE);
                                                    } catch (Exception failure) {
                                                        throw new AssertionError(failure);
                                                    }
                                                    events.put(event);
                                                })
                                        .get(30, TimeUnit.SECONDS))
                .hasCauseInstanceOf(BulkLoadNotFoundException.class);
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
                .hasCauseInstanceOf(NotCoordinatorLeaderException.class);
        assertBucketNotCommitted();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCommitRequiresFilesInTheirScopeDirectory(boolean shared) throws Exception {
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
                .isInstanceOf(InvalidBulkLoadSnapshotException.class)
                .hasMessageContaining(files.get(0).getPath());
        assertBucketNotCommitted();
    }

    @Test
    void testCommitRejectsDuplicateRestorePathAcrossScopes() throws Exception {
        prepareAllocatedBucket();
        FsPath dir = snapshotDirectory(snapshotId);
        FsPath shared = new FsPath(FlussPaths.remoteKvSharedDir(dir.getParent()), "shared-file");
        FsPath privateFile = new FsPath(dir, "private-file");
        for (FsPath path : Arrays.asList(shared, privateFile)) {
            Files.createDirectories(Paths.get(path.getParent().toUri()));
            Files.write(Paths.get(path.toUri()), new byte[] {1});
        }
        writeManifest(
                dir,
                snapshotId,
                offset,
                null,
                Collections.singletonList(
                        new KvSnapshotFileMetadata.FileHandle(shared.toString(), 1, "same.sst")),
                Collections.singletonList(
                        new KvSnapshotFileMetadata.FileHandle(
                                privateFile.toString(), 1, "same.sst")));
        assertThatThrownBy(this::commitAllocatedSnapshot)
                .isInstanceOf(InvalidBulkLoadSnapshotException.class)
                .hasMessageContaining("local_path 'same.sst' more than once");
    }

    @AfterEach
    void closeCluster() throws Exception {
        if (publicationProcessor != null) {
            publicationProcessor.shutdown();
            publicationLeases.close();
            publicationChannel.close();
        }
        if (cluster != null) {
            cluster.close();
        }
    }

    private void start(TableDescriptor descriptor, String partitionName) throws Exception {
        Configuration conf =
                new Configuration()
                        .set(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, 1)
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
                db.flush(flush);
            }
            checkpoint.createCheckpoint(directory.toString());
        }
        List<KvSnapshotFileMetadata.FileHandle> shared = new ArrayList<>();
        List<KvSnapshotFileMetadata.FileHandle> privateFiles = new ArrayList<>();
        try (DirectoryStream<Path> paths =
                Files.newDirectoryStream(directory, Files::isRegularFile)) {
            for (Path file : paths) {
                String name = file.getFileName().toString();
                boolean sst = name.endsWith(".sst");
                if (sst) {
                    Path sharedDir =
                            Files.createDirectories(
                                    Paths.get(
                                            FlussPaths.remoteKvSharedDir(dir.getParent()).toUri()));
                    file = Files.move(file, sharedDir.resolve(UUID.randomUUID().toString()));
                }
                (sst ? shared : privateFiles)
                        .add(
                                new KvSnapshotFileMetadata.FileHandle(
                                        new FsPath(file.toUri()).toString(),
                                        Files.size(file),
                                        name));
            }
        }
        writeManifest(dir, snapshotId, offset, (long) rows.size(), shared, privateFiles);
        return values.get(0);
    }

    private void assertRecovered(Tuple2<byte[], byte[]> imported) throws Exception {
        assertThat(client.getSelectedRecoverySnapshotId(bucket)).contains(snapshotId);
        retry(Duration.ofSeconds(30), () -> assertLookupRow(imported));
        assertThat(cluster.waitAndGetLeaderReplica(bucket).getLeaderEpoch())
                .isEqualTo(expectedLeaderEpoch);
    }

    @Test
    void testOrdinaryCommitDispatchedBeforeImportCannotReplaceIt() throws Exception {
        start();
        allocate();
        Tuple2<byte[], byte[]> imported =
                uploadCheckpoint(genKvRecords(new Object[] {1, "imported"}));
        LeaderAndIsr before = client.getLeaderAndIsr(bucket).get();
        long ordinaryId = client.allocateBucketSnapshotId(bucket);
        CompletedSnapshot ordinary =
                new CompletedSnapshotHandle(ordinaryId, uploadSnapshot(ordinaryId, offset), offset)
                        .retrieveCompleteSnapshot();
        ManuallyTriggeredScheduledExecutorService io =
                new ManuallyTriggeredScheduledExecutorService();
        CoordinatorEventProcessor processor = startPublicationProcessor(client, io);
        snapshots = processor.completedSnapshotStoreManager();
        CoordinatorEventManager events = processor.getCoordinatorEventManager();
        CompletableFuture<CommitKvSnapshotResponse> waiting = new CompletableFuture<>();
        events.put(
                new CommitKvSnapshotEvent(
                        new CommitKvSnapshotData(
                                ordinary,
                                client.getCurrentEpoch().getCoordinatorEpoch(),
                                before.leaderEpoch(),
                                NO_SNAPSHOT_ID),
                        waiting));
        CompletableFuture<Void> commit =
                manager.commitBucketSnapshot(LOAD_ID, bucket, snapshots, events);
        retry(Duration.ofSeconds(30), () -> assertThat(io.numQueuedRunnables()).isEqualTo(2));
        io.trigger();
        assertThatThrownBy(() -> waiting.get(30, TimeUnit.SECONDS))
                .hasCauseInstanceOf(FencedLeaderEpochException.class);
        io.triggerAll();
        commit.get(30, TimeUnit.SECONDS);
        assertThat(
                        snapshots
                                .getOrCreateCompletedSnapshotStore(TABLE, bucket)
                                .getActiveSnapshotIds())
                .containsExactly(snapshotId);
        assertThat(notifyRole(client.getLeaderAndIsr(bucket).get()).hasErrorCode()).isFalse();
        assertRecovered(imported);
    }

    @Test
    void testAdvancedRecoveryOffsetRejectsCommit() throws Exception {
        start();
        allocate();
        uploadSnapshot(snapshotId, offset);
        List<KvRecord> rows = genKvRecords(new Object[] {1, "ordinary"});
        putRows(rows);
        // The ordinary RPC has entered the I/O executor and stored its ZK reference. Its
        // selection and retention can still be finishing under the bucket lock.
        CompletedSnapshot ordinary = cluster.triggerAndWaitSnapshot(bucket);
        assertThat(ordinary.getLogOffset()).isGreaterThan(offset);
        assertThatThrownBy(this::commit).hasRootCauseInstanceOf(BulkLoadConflictException.class);
        assertThat(client.getSelectedRecoverySnapshotId(bucket)).contains(ordinary.getSnapshotID());
        assertThat(client.getTableBucketSnapshot(bucket, snapshotId)).isEmpty();
        assertThat(
                        snapshots
                                .getOrCreateCompletedSnapshotStore(TABLE, bucket)
                                .getActiveSnapshotIds())
                .containsExactly(ordinary.getSnapshotID());
        cluster.stopTabletServer(0);
        cluster.startTabletServer(0);
        leader =
                cluster.newTabletServerClientForNode(
                        cluster.waitAndGetLeaderReplica(bucket).getLeaderId());
        retry(Duration.ofSeconds(30), () -> assertLookupRow(getKeyValuePairs(rows).get(0)));
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
        commitAllocatedSnapshot();
        LeaderAndIsr published = client.getLeaderAndIsr(bucket).get();
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
        // Stop the Coordinator after durable publication and before local registration.
        ManuallyTriggeredScheduledExecutorService io =
                new ManuallyTriggeredScheduledExecutorService();
        CoordinatorEventProcessor processor = startPublicationProcessor(client, io);
        CompletableFuture<Void> pending =
                manager.commitBucketSnapshot(
                        LOAD_ID, bucket, snapshots, processor.getCoordinatorEventManager());
        retry(Duration.ofSeconds(30), () -> assertThat(io.numQueuedRunnables()).isOne());
        assertThat(pending).isNotDone();
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
    @CsvSource({"false,true", "true,true", "true,false"})
    void testConnectionLossDuringPublication(boolean persisted, boolean canVerify)
            throws Exception {
        start();
        allocate();
        uploadCheckpoint(genKvRecords(new Object[] {1, "imported"}));
        LeaderAndIsr initial = client.getLeaderAndIsr(bucket).get();
        assertThat(adjustIsr(initial).getBucketEpoch()).isEqualTo(initial.bucketEpoch() + 1);
        LeaderAndIsr beforeCommit = client.getLeaderAndIsr(bucket).get();
        try (CuratorFramework curator = losePublicationResponse(client, persisted, canVerify)) {
            curator.start();
            assertThat(curator.blockUntilConnected(30, TimeUnit.SECONDS)).isTrue();
            ZooKeeperClient publisher =
                    new ZooKeeperClient(
                            new CuratorFrameworkWithUnhandledErrorListener(
                                    curator, (message, error) -> {}),
                            new Configuration().set(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir));
            CoordinatorEventProcessor processor =
                    startPublicationProcessor(publisher, ForkJoinPool.commonPool());
            AccessContextEvent<Void> populateAssignment =
                    new AccessContextEvent<>(
                            context -> {
                                context.setLiveTabletServers(cluster.getTabletServerInfos());
                                context.updateBucketReplicaAssignment(bucket, beforeCommit.isr());
                                return null;
                            });
            processor.getCoordinatorEventManager().put(populateAssignment);
            populateAssignment.getResultFuture().get(30, TimeUnit.SECONDS);

            BulkLoadManager publishingManager =
                    new BulkLoadManager(
                            publisher, client.getCurrentEpoch().getCoordinatorEpochZkVersion());
            if (!canVerify) {
                assertThatThrownBy(
                                () ->
                                        publishingManager
                                                .commitBucketSnapshot(
                                                        LOAD_ID,
                                                        bucket,
                                                        snapshots,
                                                        processor.getCoordinatorEventManager())
                                                .get(30, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(NetworkException.class);
                assertThat(adjustPublicationIsr(beforeCommit).getErrorCode())
                        .isEqualTo(Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION.code());
                assertThat(client.getLeaderAndIsr(bucket).get().leaderEpoch())
                        .isEqualTo(expectedLeaderEpoch);
                return;
            }
            if (!persisted) {
                assertThatThrownBy(
                                () ->
                                        publishingManager
                                                .commitBucketSnapshot(
                                                        LOAD_ID,
                                                        bucket,
                                                        snapshots,
                                                        processor.getCoordinatorEventManager())
                                                .get(30, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(KeeperException.ConnectionLossException.class);
                assertBucketNotCommitted();
                assertThat(adjustPublicationIsr(beforeCommit).hasErrorCode()).isFalse();
                return;
            }
            publishingManager
                    .commitBucketSnapshot(
                            LOAD_ID, bucket, snapshots, processor.getCoordinatorEventManager())
                    .get(30, TimeUnit.SECONDS);
            LeaderAndIsr published = client.getLeaderAndIsr(bucket).get();
            assertThat(published.bucketEpoch()).isEqualTo(beforeCommit.bucketEpoch() + 1);
            assertThat(adjustPublicationIsr(beforeCommit).getErrorCode())
                    .isEqualTo(Errors.FENCED_LEADER_EPOCH_EXCEPTION.code());
            assertThat(client.getSelectedRecoverySnapshotId(bucket)).contains(snapshotId);
            assertThat(adjustPublicationIsr(published).hasErrorCode()).isFalse();
        }
    }

    private PbAdjustIsrRespForBucket adjustPublicationIsr(LeaderAndIsr role) throws Exception {
        CompletableFuture<AdjustIsrResponse> response = new CompletableFuture<>();
        publicationProcessor
                .getCoordinatorEventManager()
                .put(new AdjustIsrReceivedEvent(Collections.singletonMap(bucket, role), response));
        return response.get(30, TimeUnit.SECONDS).getTablesRespAt(0).getBucketsRespAt(0);
    }

    private PbAdjustIsrRespForBucket adjustIsr(LeaderAndIsr role) throws Exception {
        return coordinator
                .adjustIsr(
                        makeAdjustIsrRequest(role.leader(), Collections.singletonMap(bucket, role)))
                .get(30, TimeUnit.SECONDS)
                .getTablesRespAt(0)
                .getBucketsRespAt(0);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPartitionRecoveryReplaysWalAndPreservesSuccessorOnRetry(boolean leaseImport)
            throws Exception {
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
                        genKvRecords(
                                schema.getRowType(),
                                new Object[] {1, "imported", "p1"},
                                new Object[] {100, "second SST", "p1"}));
        List<KvRecord> tail =
                genKvRecords(schema.getRowType(), new Object[] {2, "during-upload", "p1"});
        putRows(tail);
        Tuple2<byte[], byte[]> appended = getKeyValuePairs(tail).get(0);
        FsPath dir = snapshotDirectory(snapshotId);
        CompletedSnapshot importedSnapshot =
                new CompletedSnapshotHandle(
                                snapshotId, CompletedSnapshot.getMetadataFilePath(dir), offset)
                        .retrieveCompleteSnapshot();
        List<KvFileHandleAndLocalPath> shared =
                importedSnapshot.getKvSnapshotHandle().getSharedKvFileHandles();
        assertThat(shared).hasSize(2);
        String importedSst = shared.get(0).getKvFileHandle().getFilePath();
        KvFileHandleAndLocalPath exclusive = shared.get(1);
        ManuallyTriggeredScheduledExecutorService io =
                new ManuallyTriggeredScheduledExecutorService();
        CoordinatorEventProcessor processor = startPublicationProcessor(client, io);
        CompletableFuture<Void> commit =
                manager.commitBucketSnapshot(
                        LOAD_ID, bucket, snapshots, processor.getCoordinatorEventManager());
        retry(Duration.ofSeconds(30), () -> assertThat(io.numQueuedRunnables()).isOne());
        cluster.stopTabletServer(0);
        cluster.startTabletServer(0);
        leader =
                cluster.newTabletServerClientForNode(
                        cluster.waitAndGetLeaderReplica(bucket).getLeaderId());
        retry(Duration.ofSeconds(30), () -> assertLookupRow(imported));
        if (leaseImport) {
            acquireImportLease();
        }
        // Rewrite just one imported SST. The checkpoint must reuse the other remote object.
        try (CompactionOptions options = new CompactionOptions()) {
            cluster.waitAndGetLeaderReplica(bucket)
                    .getKvTablet()
                    .getRocksDBKv()
                    .getDb()
                    .compactFiles(
                            options,
                            Collections.singletonList(exclusive.getLocalPath()),
                            1,
                            -1,
                            null);
        }
        commit(); // The durable import is already committed while its original RPC is pending.
        int successorEpoch = client.getLeaderAndIsr(bucket).get().leaderEpoch();
        CompletedSnapshot next = cluster.triggerAndWaitSnapshot(bucket);
        assertThat(next.getKvSnapshotHandle().getSharedKvFileHandles())
                .extracting(file -> file.getKvFileHandle().getFilePath())
                .contains(importedSst)
                .doesNotContain(exclusive.getKvFileHandle().getFilePath());
        if (leaseImport) {
            verifyLeasedSstSurvivesOrdinaryRetention(schema, next, importedSst);
        }
        io.triggerAll();
        commit.get(30, TimeUnit.SECONDS);
        if (leaseImport) {
            coordinator
                    .dropKvSnapshotLease(
                            new DropKvSnapshotLeaseRequest().setLeaseId("leased-import"))
                    .get(30, TimeUnit.SECONDS);
            putRows(genKvRecords(schema.getRowType(), new Object[] {5, "after-lease", "p1"}));
            cluster.triggerAndWaitSnapshot(bucket);
            retry(
                    Duration.ofSeconds(30),
                    () -> assertThat(Paths.get(new FsPath(importedSst).toUri())).doesNotExist());
        }
        retry(Duration.ofSeconds(30), () -> assertThat(Paths.get(dir.toUri())).doesNotExist());
        retry(
                Duration.ofSeconds(30),
                () ->
                        assertThat(
                                        Paths.get(
                                                new FsPath(
                                                                exclusive
                                                                        .getKvFileHandle()
                                                                        .getFilePath())
                                                        .toUri()))
                                .doesNotExist());
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
        assertThat(client.getLeaderAndIsr(bucket).get().leaderEpoch()).isEqualTo(successorEpoch);
        assertLookupRow(getKeyValuePairs(beforeAllocation).get(0));
        cluster.stopTabletServer(0);
        cluster.startTabletServer(0);
        leader =
                cluster.newTabletServerClientForNode(
                        cluster.waitAndGetLeaderReplica(bucket).getLeaderId());
        retry(Duration.ofSeconds(30), () -> assertLookupRow(imported));
        assertLookupRow(appended);
    }

    private void acquireImportLease() throws Exception {
        AcquireKvSnapshotLeaseRequest lease =
                new AcquireKvSnapshotLeaseRequest()
                        .setLeaseId("leased-import")
                        .setLeaseDurationMs(300000);
        lease.addSnapshotsToLease()
                .setTableId(bucket.getTableId())
                .addBucketSnapshot()
                .setBucketId(bucket.getBucket())
                .setPartitionId(bucket.getPartitionId())
                .setSnapshotId(snapshotId);
        coordinator.acquireKvSnapshotLease(lease).get(30, TimeUnit.SECONDS);
    }

    private void verifyLeasedSstSurvivesOrdinaryRetention(
            Schema schema, CompletedSnapshot next, String importedSst) throws Exception {
        putRows(genKvRecords(schema.getRowType(), new Object[] {4, "compacted", "p1"}));
        RocksDB db = cluster.waitAndGetLeaderReplica(bucket).getKvTablet().getRocksDBKv().getDb();
        try (CompactionOptions options = new CompactionOptions()) {
            List<String> inputs = new ArrayList<>();
            db.getLiveFilesMetaData().forEach(file -> inputs.add(file.fileName()));
            db.compactFiles(options, inputs, 6, -1, null);
        }
        CompletedSnapshot compacted = cluster.triggerAndWaitSnapshot(bucket);
        assertThat(compacted.getKvSnapshotHandle().getSharedKvFileHandles())
                .extracting(file -> file.getKvFileHandle().getFilePath())
                .doesNotContain(importedSst);
        // The sole cleanup worker has retired the last ordinary snapshot that referenced this SST.
        Path retired = Paths.get(snapshotDirectory(next.getSnapshotID()).toUri());
        retry(Duration.ofSeconds(30), () -> assertThat(retired).doesNotExist());
        Path leasedSst = Paths.get(new FsPath(importedSst).toUri());
        assertThat(client.getTableBucketSnapshot(bucket, snapshotId)).isPresent();
        assertThat(leasedSst).as("SST referenced by the leased import").exists();
    }

    private static CuratorFramework losePublicationResponse(
            ZooKeeperClient client, boolean persisted, boolean canVerify) {
        CuratorFramework existing = client.getCuratorClient();
        return CuratorFrameworkFactory.builder()
                .connectString(existing.getZookeeperClient().getCurrentConnectionString())
                .namespace(existing.getNamespace())
                .retryPolicy(new RetryNTimes(0, 0))
                .zookeeperFactory(
                        (address, sessionTimeout, watcher, readOnly) ->
                                new ZooKeeper(address, sessionTimeout, watcher, readOnly) {
                                    private boolean attempted;

                                    @Override
                                    public List<OpResult> multi(Iterable<Op> operations)
                                            throws InterruptedException, KeeperException {
                                        if (attempted && canVerify) {
                                            return super.multi(operations);
                                        }
                                        if (!attempted && persisted) {
                                            super.multi(operations);
                                        }
                                        attempted = true;
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

    // Run the real publication handler with no online replicas when testing durable state
    // independently of notification delivery (or injecting a fault in the publishing ZK session).
    private CoordinatorEventProcessor startPublicationProcessor(
            ZooKeeperClient publisher, ExecutorService io) throws Exception {
        CoordinatorContext context = new CoordinatorContext(client.getCurrentEpoch());
        context.putTablePath(bucket.getTableId(), TABLE);
        context.putBucketLeaderAndIsr(bucket, client.getLeaderAndIsr(bucket).get());
        Configuration conf =
                new Configuration()
                        .set(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir)
                        .set(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, 1);
        CoordinatorMetadataCache cache = new CoordinatorMetadataCache();
        publicationLeases =
                new KvSnapshotLeaseManager(
                        60_000,
                        publisher,
                        remoteDataDir,
                        SystemClock.getInstance(),
                        TestingMetricGroups.COORDINATOR_METRICS);
        publicationChannel = new TestCoordinatorChannelManager();
        publicationProcessor =
                new CoordinatorEventProcessor(
                        publisher,
                        cache,
                        publicationChannel,
                        context,
                        new ReplicaCapacityController(conf, cache),
                        null,
                        null,
                        TestingMetricGroups.COORDINATOR_METRICS,
                        conf,
                        io,
                        null,
                        publicationLeases,
                        null,
                        SystemClock.getInstance());
        publicationProcessor.getCoordinatorEventManager().start();
        return publicationProcessor;
    }

    private void commitAllocatedSnapshot() throws Exception {
        manager.commitBucketSnapshot(
                        LOAD_ID,
                        bucket,
                        snapshots,
                        startPublicationProcessor(client, ForkJoinPool.commonPool())
                                .getCoordinatorEventManager())
                .get(30, TimeUnit.SECONDS);
    }

    private FsPath uploadSnapshot(long id, long logOffset) throws Exception {
        FsPath dir = snapshotDirectory(id);
        return writeManifest(
                dir, id, logOffset, null, Collections.emptyList(), Collections.emptyList());
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
            @Nullable Long rowCount,
            List<KvSnapshotFileMetadata.FileHandle> sharedFiles,
            List<KvSnapshotFileMetadata.FileHandle> privateFiles)
            throws Exception {
        Files.createDirectories(Paths.get(dir.toUri()));
        FsPath metadata = CompletedSnapshot.getMetadataFilePath(dir);
        long size =
                Stream.concat(sharedFiles.stream(), privateFiles.stream())
                        .mapToLong(KvSnapshotFileMetadata.FileHandle::getSize)
                        .sum();
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
        BulkLoadTransaction transaction =
                new BulkLoadTransaction(
                        table.getTableId(),
                        LOAD_ID,
                        table.getTablePath(),
                        table.getSchemaId(),
                        table.getRemoteDataDir(),
                        BulkLoadTransaction.RUNNING,
                        10,
                        null,
                        null);
        createNode(path, BulkLoadTransactionZNode.encode(transaction));

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

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
import org.apache.fluss.metadata.TableBucketReplica;
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
import org.apache.fluss.rpc.messages.PbBulkLoadHandle;
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrRespForBucket;
import org.apache.fluss.rpc.messages.PbPutKvRespForBucket;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadManager;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.coordinator.event.AdjustIsrReceivedEvent;
import org.apache.fluss.server.coordinator.event.CommitKvSnapshotEvent;
import org.apache.fluss.server.coordinator.event.CoordinatorEvent;
import org.apache.fluss.server.coordinator.event.CoordinatorEventManager;
import org.apache.fluss.server.coordinator.event.DeadTabletServerEvent;
import org.apache.fluss.server.coordinator.lease.KvSnapshotLeaseManager;
import org.apache.fluss.server.coordinator.statemachine.BucketState;
import org.apache.fluss.server.coordinator.statemachine.ReplicaState;
import org.apache.fluss.server.entity.AdjustIsrResultForBucket;
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
import org.apache.fluss.server.zk.data.ZkData.LeaderAndIsrZNode;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadBucket;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTarget;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.fluss.shaded.curator5.org.apache.curator.retry.RetryNTimes;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.Op;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.OpResult;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.ZooKeeper;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;
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
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.fluss.metadata.TableBucketSnapshot.NO_SNAPSHOT_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR_PK;
import static org.apache.fluss.server.testutils.KvTestUtils.assertLookupResponse;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createPartition;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newLookupRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getAdjustIsrResponseData;
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
    private final FlussScheduler publicationScheduler = new FlussScheduler(1);

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
                        DATA1_TABLE_DESCRIPTOR_PK.withBucketCount(1),
                        null,
                        false);
        bucket = new TableBucket(tableId, 0);
        TableInfo table = metadata.getTable(TABLE);
        remoteDataDir = table.getRemoteDataDir();
        physical = PhysicalTablePath.of(TABLE);
        seedRunningTarget(table);
        int epoch =
                client.fenceBecomeCoordinatorLeader("bucket-commit").getCoordinatorEpochZkVersion();
        client.registerLeaderAndIsr(
                bucket,
                new LeaderAndIsr(0, 0).newLeaderAndIsr(Collections.singletonList(0)),
                epoch);
        manager = new BulkLoadManager(client, epoch, ForkJoinPool.commonPool());
        snapshots =
                new CompletedSnapshotStoreManager(
                        1,
                        Runnable::run,
                        client,
                        TestingMetricGroups.COORDINATOR_METRICS,
                        snapshot -> false);
    }

    private void prepareAllocatedBucket() throws Exception {
        prepareBucket();
        BulkLoadBucket allocation = manager.allocateBucketSnapshot(LOAD_ID, bucket);
        snapshotId = allocation.snapshotId;
        offset = allocation.logOffset;
    }

    @Test
    void testSnapshotOffsetMustMatchAllocation() throws Exception {
        prepareAllocatedBucket();
        uploadSnapshot(snapshotId, offset + 1);
        assertThatThrownBy(this::validateCommitBeforePublication)
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
                startPublicationProcessor(client, ForkJoinPool.commonPool());
        CompletableFuture<Void> resume = new CompletableFuture<>();
        events.put(new AccessContextEvent<>(context -> resume.join()));
        CompletableFuture<Void> closed;
        try {
            CompletableFuture<Void> commit =
                    manager.commitBucketSnapshot(LOAD_ID, bucket, snapshots, events);
            closed = CompletableFuture.runAsync(events::close);
            assertThatThrownBy(() -> commit.get(30, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(NotCoordinatorLeaderException.class);
        } finally {
            resume.complete(null);
        }
        closed.get(30, TimeUnit.SECONDS);
        assertBucketNotCommitted();
    }

    @Test
    void testCommitReportsTableDeletedAfterValidation() throws Exception {
        prepareAllocatedBucket();
        uploadSnapshot(snapshotId, offset);
        CoordinatorEventManager events =
                startPublicationProcessor(client, ForkJoinPool.commonPool());
        ManuallyTriggeredScheduledExecutorService io =
                new ManuallyTriggeredScheduledExecutorService();
        BulkLoadManager publishingManager =
                new BulkLoadManager(
                        client, client.getCurrentEpoch().getCoordinatorEpochZkVersion(), io);
        CompletableFuture<CoordinatorEvent> publication = new CompletableFuture<>();
        CompletableFuture<Void> commit =
                publishingManager.commitBucketSnapshot(
                        LOAD_ID, bucket, snapshots, publication::complete);
        client.deleteTable(TABLE);
        events.put(publication.get(30, TimeUnit.SECONDS));
        AccessContextEvent<Void> nextEvent = new AccessContextEvent<>(context -> null);
        events.put(nextEvent);
        nextEvent.getResultFuture().get(30, TimeUnit.SECONDS);
        assertThat(commit).isNotDone();
        io.triggerAll();
        assertThatThrownBy(() -> commit.get(30, TimeUnit.SECONDS))
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
        FsPath parent = shared ? dir : new FsPath(remote.toUri());
        List<KvSnapshotFileMetadata.FileHandle> files =
                Collections.singletonList(uploadFile(parent, "data.sst"));
        writeManifest(
                dir,
                snapshotId,
                offset,
                0L,
                shared ? files : Collections.emptyList(),
                shared ? Collections.emptyList() : files);
        assertThatThrownBy(this::validateCommitBeforePublication)
                .isInstanceOf(InvalidBulkLoadSnapshotException.class)
                .hasMessageContaining(files.get(0).getPath());
        assertBucketNotCommitted();
    }

    @Test
    void testCommitRejectsDuplicateRestorePathAcrossScopes() throws Exception {
        prepareAllocatedBucket();
        FsPath dir = snapshotDirectory(snapshotId);
        writeManifest(
                dir,
                snapshotId,
                offset,
                null,
                Collections.singletonList(
                        uploadFile(FlussPaths.remoteKvSharedDir(dir.getParent()), "same.sst")),
                Collections.singletonList(uploadFile(dir, "same.sst")));
        assertThatThrownBy(this::validateCommitBeforePublication)
                .isInstanceOf(InvalidBulkLoadSnapshotException.class)
                .hasMessageContaining("local_path 'same.sst' more than once");
    }

    @AfterEach
    void closeCluster() throws Exception {
        if (publicationProcessor != null) {
            publicationProcessor.shutdown();
            publicationScheduler.shutdown();
            publicationLeases.close();
            publicationChannel.close();
        }
        if (cluster != null) {
            cluster.close();
        }
    }

    private void start(TableDescriptor descriptor, String partitionName) throws Exception {
        start(descriptor, partitionName, 1);
    }

    private void start(TableDescriptor descriptor, String partitionName, int replicas)
            throws Exception {
        Configuration conf =
                new Configuration()
                        .set(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, 1)
                        .set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1))
                        .set(ConfigOptions.SERVER_IO_POOL_SIZE, 1);
        cluster =
                FlussClusterExtension.builder()
                        .setNumOfTabletServers(replicas)
                        .setClusterConf(conf)
                        .build();
        cluster.start();
        long tableId = createTable(cluster, TABLE, descriptor.withReplicationFactor(replicas));
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
                        client,
                        client.getCurrentEpoch().getCoordinatorEpochZkVersion(),
                        ForkJoinPool.commonPool());
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
        start(DATA1_TABLE_DESCRIPTOR_PK.withBucketCount(1), null);
    }

    private void startReplicatedBucket() throws Exception {
        start(DATA1_TABLE_DESCRIPTOR_PK.withBucketCount(1), null, 2);
        retry(
                Duration.ofSeconds(30),
                () -> assertThat(client.getLeaderAndIsr(bucket).get().isr()).hasSize(2));
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

    private void restartTabletServer() throws Exception {
        cluster.stopTabletServer(0);
        cluster.startTabletServer(0);
        leader =
                cluster.newTabletServerClientForNode(
                        cluster.waitAndGetLeaderReplica(bucket).getLeaderId());
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
        CoordinatorEventManager events = startPublicationProcessor(client, io);
        snapshots = publicationProcessor.completedSnapshotStoreManager();
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
        CompletedSnapshotStore store = snapshots.getOrCreateCompletedSnapshotStore(TABLE, bucket);
        assertThat(store.getActiveSnapshotIds()).containsExactly(snapshotId);
        assertThat(notifyRole(client.getLeaderAndIsr(bucket).get()).hasErrorCode()).isFalse();
        assertRecovered(imported);

        // A successor without shared files retires the import's only SST reference.
        String importedSst = importedSharedFiles().get(0).getKvFileHandle().getFilePath();
        long successor = client.allocateBucketSnapshotId(bucket);
        store.add(
                new CompletedSnapshotHandle(successor, uploadSnapshot(successor, offset), offset)
                        .retrieveCompleteSnapshot(),
                snapshotId,
                expectedLeaderEpoch);
        io.triggerAll();
        assertThat(Paths.get(new FsPath(importedSst).toUri())).doesNotExist();
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
        restartTabletServer();
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
    void testRejectedRegistrationAllowsRetryAndTakeoverRecovery() throws Exception {
        start();
        allocate();
        Tuple2<byte[], byte[]> imported =
                uploadCheckpoint(genKvRecords(new Object[] {1, "imported"}));
        LeaderAndIsr role = client.getLeaderAndIsr(bucket).get();
        LeaderAndIsr unpublished =
                role.newLeaderAndIsr(role.leader(), role.isr(), role.standbyReplicas());
        assertThat(notifyRole(unpublished).getErrorCode())
                .isEqualTo(Errors.LEADER_NOT_AVAILABLE_EXCEPTION.code());
        assertThat(cluster.waitAndGetLeaderReplica(bucket).getLeaderEpoch())
                .isEqualTo(role.leaderEpoch());
        ExecutorService closed = Executors.newSingleThreadExecutor();
        closed.shutdown();
        CoordinatorEventManager events = startPublicationProcessor(client, closed);
        CompletableFuture<Void> original =
                manager.commitBucketSnapshot(LOAD_ID, bucket, snapshots, events);
        assertThatThrownBy(() -> original.get(30, TimeUnit.SECONDS))
                .hasCauseInstanceOf(UnknownServerException.class)
                .hasRootCauseInstanceOf(RejectedExecutionException.class);
        commit(); // Retry acknowledges the durable publication before a new Coordinator takes over.
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
    void testImmediatelyVerifyLostPublicationResponse(boolean persisted) throws Exception {
        prepareAllocatedBucket();
        uploadSnapshot(snapshotId, offset);
        // An ordinary snapshot with a larger ID predates the recovery selection znode.
        long previousId = client.allocateBucketSnapshotId(bucket);
        FsPath previous = uploadSnapshot(previousId, offset);
        client.registerTableBucketSnapshot(
                bucket, new BucketSnapshot(previousId, offset, previous.toString()));
        LeaderAndIsr before = client.getLeaderAndIsr(bucket).get();
        try (CuratorFramework curator =
                losePublicationResponse(
                        client, persisted, new AtomicBoolean(true), new CountDownLatch(0), null)) {
            CoordinatorEventManager events =
                    startPublicationProcessor(startPublisher(curator), ForkJoinPool.commonPool());
            CompletableFuture<Void> commit =
                    manager.commitBucketSnapshot(LOAD_ID, bucket, snapshots, events);
            if (persisted) {
                commit.get(30, TimeUnit.SECONDS);
                assertThat(client.getSelectedRecoverySnapshotId(bucket)).contains(snapshotId);
                assertThat(client.getLeaderAndIsr(bucket))
                        .contains(
                                before.newLeaderAndIsr(
                                        before.leader(), before.isr(), before.standbyReplicas()));
                assertThat(Paths.get(previous.toUri())).doesNotExist();
            } else {
                assertThatThrownBy(() -> commit.get(30, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(KeeperException.ConnectionLossException.class);
                assertBucketNotCommitted();
                assertThat(client.getTableBucketLatestSnapshot(bucket).get().getSnapshotId())
                        .isEqualTo(previousId);
                assertThat(Paths.get(previous.toUri())).exists();
            }
            Optional<LeaderAndIsr> cached =
                    accessPublicationContext(context -> context.getBucketLeaderAndIsr(bucket));
            assertThat(cached).isEqualTo(client.getLeaderAndIsr(bucket));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUnconfirmedPublicationIsolatesBucketAndRecoversWithoutRetry(boolean persisted)
            throws Exception {
        startReplicatedBucket();
        List<KvRecord> base = genKvRecords(new Object[] {2, "original"});
        putRows(base);
        allocate();
        Tuple2<byte[], byte[]> imported =
                uploadCheckpoint(genKvRecords(new Object[] {1, "imported"}));
        LeaderAndIsr beforeCommit = client.getLeaderAndIsr(bucket).get();
        AtomicBoolean verificationAvailable = new AtomicBoolean(false);
        CountDownLatch failedChecks = new CountDownLatch(2);
        try (CuratorFramework curator =
                losePublicationResponse(
                        client, persisted, verificationAvailable, failedChecks, null)) {
            CompletableFuture<Void> commit = commitWithPublicationFault(curator, beforeCommit);
            assertThatThrownBy(() -> commit.get(30, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(NetworkException.class);
            verifyMixedIsrBatchAndFollowerLoss(beforeCommit);
            int expected = persisted ? expectedLeaderEpoch : beforeCommit.leaderEpoch();
            assertThat(client.getLeaderAndIsr(bucket).get().leaderEpoch()).isEqualTo(expected);
            // Both the inline check and a background check failed before connectivity returned.
            assertThat(failedChecks.await(30, TimeUnit.SECONDS)).isTrue();
            verificationAvailable.set(true);
            retry(
                    Duration.ofSeconds(30),
                    () -> {
                        LeaderAndIsr current = client.getLeaderAndIsr(bucket).get();
                        assertThat(current.leaderEpoch()).isEqualTo(expected);
                        assertThat(current.isr()).containsExactly(beforeCommit.leader());
                        assertThat(cluster.waitAndGetLeaderReplica(bucket).getLeaderEpoch())
                                .isEqualTo(expected);
                        assertLookupRow(persisted ? imported : getKeyValuePairs(base).get(0));
                    });
        }
    }

    private CompletableFuture<Void> commitWithPublicationFault(
            CuratorFramework curator, LeaderAndIsr beforeCommit) throws Exception {
        ZooKeeperClient publisher = startPublisher(curator);
        CoordinatorEventManager events =
                startPublicationProcessor(publisher, ForkJoinPool.commonPool());
        TableInfo table = cluster.waitAndGetLeaderReplica(bucket).getTableInfo();
        accessPublicationContext(
                context -> {
                    context.putTableInfo(table);
                    context.setCoordinatorServerInfo(cluster.getCoordinatorServerInfo());
                    context.setLiveTabletServers(cluster.getTabletServerInfos());
                    context.updateBucketReplicaAssignment(bucket, beforeCommit.isr());
                    context.putBucketState(bucket, BucketState.OnlineBucket);
                    for (int id : beforeCommit.isr()) {
                        context.putReplicaState(
                                new TableBucketReplica(bucket, id), ReplicaState.OnlineReplica);
                    }
                    return null;
                });
        return manager.commitBucketSnapshot(LOAD_ID, bucket, snapshots, events);
    }

    @Test
    void testRetryDeferredLeaderElectionAfterFailedRoleWrite() throws Exception {
        startReplicatedBucket();
        allocate();
        Tuple2<byte[], byte[]> imported =
                uploadCheckpoint(genKvRecords(new Object[] {1, "imported"}));
        LeaderAndIsr initial = client.getLeaderAndIsr(bucket).get();
        int follower =
                initial.isr().stream().filter(id -> id != initial.leader()).findFirst().get();
        AtomicBoolean verificationAvailable = new AtomicBoolean(false);
        try (CuratorFramework curator =
                losePublicationResponse(
                        client,
                        true,
                        verificationAvailable,
                        new CountDownLatch(0),
                        LeaderAndIsrZNode.path(bucket))) {
            CompletableFuture<Void> commit = commitWithPublicationFault(curator, initial);
            assertThatThrownBy(() -> commit.get(30, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(NetworkException.class);
            leader = cluster.newTabletServerClientForNode(follower);
            publicationChannel.setGateways(Collections.singletonMap(follower, leader));
            publicationProcessor
                    .getCoordinatorEventManager()
                    .put(new DeadTabletServerEvent(initial.leader()));
            BucketState state = accessPublicationContext(context -> context.getBucketState(bucket));
            assertThat(state).isEqualTo(BucketState.OfflineBucket);
            // The first deferred election fails before its role write reaches ZooKeeper.
            verificationAvailable.set(true);
            retry(
                    Duration.ofSeconds(30),
                    () -> {
                        assertThat(client.getLeaderAndIsr(bucket).get().leader())
                                .isEqualTo(follower);
                        assertLookupRow(imported);
                    });
        }
    }

    private ZooKeeperClient startPublisher(CuratorFramework curator) throws Exception {
        curator.start();
        assertThat(curator.blockUntilConnected(30, TimeUnit.SECONDS)).isTrue();
        return new ZooKeeperClient(
                new CuratorFrameworkWithUnhandledErrorListener(curator, (message, error) -> {}),
                new Configuration().set(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir));
    }

    private void verifyMixedIsrBatchAndFollowerLoss(LeaderAndIsr beforeCommit) throws Exception {
        long otherId =
                createTable(
                        cluster,
                        TablePath.of("db", "other"),
                        DATA1_TABLE_DESCRIPTOR_PK.withBucketCount(2).withReplicationFactor(1));
        TableBucket first = new TableBucket(otherId, 0);
        TableBucket other =
                cluster.waitAndGetLeaderReplica(first).getLeaderId() == beforeCommit.leader()
                        ? first
                        : new TableBucket(otherId, 1);
        cluster.waitAndGetLeaderReplica(other);
        LeaderAndIsr otherRole = client.getLeaderAndIsr(other).get();
        assertThat(otherRole.leader()).isEqualTo(beforeCommit.leader());
        int follower =
                beforeCommit.isr().stream()
                        .filter(id -> id != beforeCommit.leader())
                        .findFirst()
                        .get();
        CoordinatorEventManager events = publicationProcessor.getCoordinatorEventManager();
        accessPublicationContext(
                context -> {
                    context.putTablePath(otherId, TablePath.of("db", "other"));
                    context.putBucketLeaderAndIsr(other, otherRole);
                    context.updateBucketReplicaAssignment(other, otherRole.isr());
                    return null;
                });
        publicationChannel.setGateways(Collections.singletonMap(beforeCommit.leader(), leader));
        Map<TableBucket, LeaderAndIsr> batch = new HashMap<>();
        batch.put(bucket, beforeCommit);
        batch.put(other, otherRole);
        CompletableFuture<AdjustIsrResponse> adjusted = new CompletableFuture<>();
        events.put(new AdjustIsrReceivedEvent(batch, adjusted));
        Map<TableBucket, AdjustIsrResultForBucket> results =
                getAdjustIsrResponseData(adjusted.get(30, TimeUnit.SECONDS));
        assertThat(results.get(bucket).getErrorCode()).isEqualTo(Errors.NETWORK_EXCEPTION.code());
        assertThat(results.get(other).failed()).isFalse();
        assertThat(client.getLeaderAndIsr(other).get().bucketEpoch())
                .isEqualTo(otherRole.bucketEpoch() + 1);
        events.put(new DeadTabletServerEvent(follower));
        ReplicaState state =
                accessPublicationContext(
                        context ->
                                context.getReplicaState(new TableBucketReplica(bucket, follower)));
        assertThat(state).isEqualTo(ReplicaState.OfflineReplica);
    }

    private <T> T accessPublicationContext(Function<CoordinatorContext, T> action)
            throws Exception {
        AccessContextEvent<T> event = new AccessContextEvent<>(action);
        publicationProcessor.getCoordinatorEventManager().put(event);
        return event.getResultFuture().get(30, TimeUnit.SECONDS);
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
                        genKvRecords(
                                schema.getRowType(),
                                new Object[] {1, "imported", "p1"},
                                new Object[] {100, "second SST", "p1"}));
        List<KvRecord> tail =
                genKvRecords(schema.getRowType(), new Object[] {2, "during-upload", "p1"});
        putRows(tail);
        Tuple2<byte[], byte[]> appended = getKeyValuePairs(tail).get(0);
        FsPath dir = snapshotDirectory(snapshotId);
        List<KvFileHandleAndLocalPath> shared = importedSharedFiles();
        assertThat(shared).hasSize(2);
        String importedSst = shared.get(0).getKvFileHandle().getFilePath();
        KvFileHandleAndLocalPath exclusive = shared.get(1);
        String exclusiveSst = exclusive.getKvFileHandle().getFilePath();
        ManuallyTriggeredScheduledExecutorService io =
                new ManuallyTriggeredScheduledExecutorService();
        CompletableFuture<Void> commit = publishBeforeRegistration(io);
        restartTabletServer();
        retry(Duration.ofSeconds(30), () -> assertLookupRow(imported));
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
                .doesNotContain(exclusiveSst);
        io.triggerAll();
        commit.get(30, TimeUnit.SECONDS);
        assertThat(client.getSelectedRecoverySnapshotId(bucket)).contains(next.getSnapshotID());
        awaitDeletion(dir);
        awaitDeletion(new FsPath(exclusiveSst));
        commit();
        AllocateBulkLoadBucketSnapshotResponse replay =
                coordinator
                        .allocateBulkLoadBucketSnapshot(allocateRequest)
                        .get(30, TimeUnit.SECONDS);
        assertThat(replay.getState()).isEqualTo(BulkLoadBucket.COMMITTED);
        assertThat(replay.getSnapshotId()).isEqualTo(snapshotId);
        assertThat(replay.getLogOffset()).isEqualTo(offset);
        assertThat(client.getSelectedRecoverySnapshotId(bucket)).contains(next.getSnapshotID());
        assertThat(client.getLeaderAndIsr(bucket).get().leaderEpoch()).isEqualTo(successorEpoch);
        assertLookupRow(getKeyValuePairs(beforeAllocation).get(0));
        restartTabletServer();
        retry(Duration.ofSeconds(30), () -> assertLookupRow(imported));
        assertLookupRow(appended);
    }

    private CompletableFuture<Void> publishBeforeRegistration(
            ManuallyTriggeredScheduledExecutorService io) throws Exception {
        CoordinatorEventManager events = startPublicationProcessor(client, io);
        CompletableFuture<Void> commit =
                manager.commitBucketSnapshot(LOAD_ID, bucket, snapshots, events);
        retry(Duration.ofSeconds(30), () -> assertThat(io.numQueuedRunnables()).isOne());
        return commit;
    }

    private static void awaitDeletion(FsPath path) {
        retry(Duration.ofSeconds(30), () -> assertThat(Paths.get(path.toUri())).doesNotExist());
    }

    @Test
    void testLeaseProtectsImportedSstUntilReleased() throws Exception {
        start();
        allocate();
        Tuple2<byte[], byte[]> imported =
                uploadCheckpoint(genKvRecords(new Object[] {1, "imported"}));
        ManuallyTriggeredScheduledExecutorService io =
                new ManuallyTriggeredScheduledExecutorService();
        CompletableFuture<Void> commit = publishBeforeRegistration(io);
        restartTabletServer();
        retry(Duration.ofSeconds(30), () -> assertLookupRow(imported));
        AcquireKvSnapshotLeaseRequest lease =
                new AcquireKvSnapshotLeaseRequest()
                        .setLeaseId("leased-import")
                        .setLeaseDurationMs(300000);
        lease.addSnapshotsToLease()
                .setTableId(bucket.getTableId())
                .addBucketSnapshot()
                .setBucketId(bucket.getBucket())
                .setSnapshotId(snapshotId);
        coordinator.acquireKvSnapshotLease(lease).get(30, TimeUnit.SECONDS);

        String importedSst = importedSharedFiles().get(0).getKvFileHandle().getFilePath();
        putRows(genKvRecords(new Object[] {2, "successor"}));
        CompletedSnapshot next = cluster.triggerAndWaitSnapshot(bucket);
        assertThat(next.getKvSnapshotHandle().getSharedKvFileHandles())
                .extracting(file -> file.getKvFileHandle().getFilePath())
                .contains(importedSst);
        putRows(genKvRecords(new Object[] {3, "compacted"}));
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
        awaitDeletion(snapshotDirectory(next.getSnapshotID()));
        Path leasedSst = Paths.get(new FsPath(importedSst).toUri());
        assertThat(client.getTableBucketSnapshot(bucket, snapshotId)).isPresent();
        assertThat(leasedSst).as("SST referenced by the leased import").exists();
        io.triggerAll();
        commit.get(30, TimeUnit.SECONDS);
        assertThat(client.getSelectedRecoverySnapshotId(bucket))
                .contains(compacted.getSnapshotID());
        coordinator
                .dropKvSnapshotLease(new DropKvSnapshotLeaseRequest().setLeaseId("leased-import"))
                .get(30, TimeUnit.SECONDS);
        putRows(genKvRecords(new Object[] {4, "released"}));
        cluster.triggerAndWaitSnapshot(bucket);
        awaitDeletion(new FsPath(importedSst));
    }

    private static CuratorFramework losePublicationResponse(
            ZooKeeperClient client,
            boolean persisted,
            AtomicBoolean verificationAvailable,
            CountDownLatch failedChecks,
            @Nullable String failFirstRoleWritePath) {
        CuratorFramework existing = client.getCuratorClient();
        return CuratorFrameworkFactory.builder()
                .connectString(existing.getZookeeperClient().getCurrentConnectionString())
                .namespace(existing.getNamespace())
                .retryPolicy(new RetryNTimes(0, 0))
                .zookeeperFactory(
                        (address, sessionTimeout, watcher, readOnly) ->
                                new ZooKeeper(address, sessionTimeout, watcher, readOnly) {
                                    private boolean attempted;
                                    private boolean rejectedRoleWrite;

                                    @Override
                                    public List<OpResult> multi(Iterable<Op> operations)
                                            throws InterruptedException, KeeperException {
                                        if (verificationAvailable.get()
                                                && failFirstRoleWritePath != null
                                                && !rejectedRoleWrite) {
                                            for (Op op : operations) {
                                                if (op.getType() == OpCode.setData
                                                        && op.getPath()
                                                                .endsWith(failFirstRoleWritePath)) {
                                                    rejectedRoleWrite = true;
                                                    throw new KeeperException
                                                            .ConnectionLossException();
                                                }
                                            }
                                        }
                                        if (attempted) {
                                            if (verificationAvailable.get()
                                                    || !operations
                                                            .iterator()
                                                            .next()
                                                            .getPath()
                                                            .contains("/bulk_load/")) {
                                                return super.multi(operations);
                                            }
                                            failedChecks.countDown();
                                        } else {
                                            attempted = true;
                                            if (persisted) {
                                                super.multi(operations);
                                            }
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

    // Run the real publication handler with no online replicas when testing durable state
    // independently of notification delivery (or injecting a fault in the publishing ZK session).
    private CoordinatorEventManager startPublicationProcessor(
            ZooKeeperClient publisher, ExecutorService io) throws Exception {
        CoordinatorContext context = new CoordinatorContext(client.getCurrentEpoch());
        context.putTablePath(bucket.getTableId(), TABLE);
        context.putBucketLeaderAndIsr(bucket, client.getLeaderAndIsr(bucket).get());
        Configuration conf =
                new Configuration()
                        .set(ConfigOptions.ZOOKEEPER_RETRY_WAIT, Duration.ofMillis(100))
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
        publicationScheduler.startup();
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
                        publicationScheduler,
                        SystemClock.getInstance());
        publicationProcessor.getCoordinatorEventManager().start();
        return publicationProcessor.getCoordinatorEventManager();
    }

    private void validateCommitBeforePublication() throws Exception {
        manager.commitBucketSnapshot(
                LOAD_ID,
                bucket,
                snapshots,
                event -> {
                    throw new AssertionError("Invalid snapshot reached publication");
                });
    }

    private void commitAllocatedSnapshot() throws Exception {
        manager.commitBucketSnapshot(
                        LOAD_ID,
                        bucket,
                        snapshots,
                        startPublicationProcessor(client, ForkJoinPool.commonPool()))
                .get(30, TimeUnit.SECONDS);
    }

    private List<KvFileHandleAndLocalPath> importedSharedFiles() throws Exception {
        return new CompletedSnapshotHandle(
                        snapshotId,
                        CompletedSnapshot.getMetadataFilePath(snapshotDirectory(snapshotId)),
                        offset)
                .retrieveCompleteSnapshot()
                .getKvSnapshotHandle()
                .getSharedKvFileHandles();
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

    private static KvSnapshotFileMetadata.FileHandle uploadFile(FsPath directory, String localPath)
            throws Exception {
        Path parent = Files.createDirectories(Paths.get(directory.toUri()));
        Path file = Files.write(parent.resolve(localPath), new byte[] {1});
        return new KvSnapshotFileMetadata.FileHandle(
                new FsPath(file.toUri()).toString(), 1, localPath);
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

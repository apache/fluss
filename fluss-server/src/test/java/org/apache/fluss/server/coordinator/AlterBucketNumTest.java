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

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.TabletServerInfo;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.exception.TooManyBucketsException;
import org.apache.fluss.lake.lakestorage.LakeCatalog;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.entity.TablePropertyChanges;
import org.apache.fluss.server.zk.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.ZkVersion;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Stream;

import static org.apache.fluss.config.ConfigOptions.DEFAULT_LISTENER_NAME;
import static org.apache.fluss.metadata.ResolvedPartitionSpec.fromPartitionName;
import static org.apache.fluss.server.utils.TableAssignmentUtils.generateAssignment;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for ALTER TABLE SET ('bucket.num' = 'N') per-partition bucket count rescale. */
class AlterBucketNumTest {

    private static final String DEFAULT_DB = "db";

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zookeeperClient;
    private static MetadataManager metadataManager;
    private static String remoteDataDir;

    @BeforeAll
    static void beforeAll() throws Exception {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
        metadataManager =
                new MetadataManager(
                        zookeeperClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));

        // register coordinator server
        zookeeperClient.registerCoordinatorLeader(
                new CoordinatorAddress(
                        "1", Endpoint.fromListenersString("CLIENT://localhost:10012")));
        zookeeperClient.fenceBecomeCoordinatorLeader("1");

        // register 3 tablet servers
        for (int i = 0; i < 3; i++) {
            zookeeperClient.registerTabletServer(
                    i,
                    new TabletServerRegistration(
                            "rack" + i,
                            Collections.singletonList(
                                    new Endpoint("host" + i, 1000, DEFAULT_LISTENER_NAME)),
                            System.currentTimeMillis()));
        }

        // create database
        metadataManager.createDatabase(DEFAULT_DB, DatabaseDescriptor.builder().build(), false);
        remoteDataDir = zookeeperClient.getDefaultRemoteDataDir();
    }

    // ====================== Lake Propagation Tests ======================

    @Test
    void testAlterBucketNumOnLakeTablePassesValidationButAbortsWithoutLakeCatalog()
            throws Exception {
        // A lake table is no longer rejected by validation; the ALTER proceeds to the lake
        // propagation, which aborts here because this harness wires no lake catalog. Covers the
        // lakeCatalog == null branch (distinct from a propagation call that fails, tested below).
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_lake_table_alter_allowed");
        int originalBucketCount = 4;
        createSeededLakePartitionedTable(metadataManager, tablePath, originalBucketCount);

        // Lake First: the ALTER validates and then attempts to propagate the new bucket count to
        // the lake side BEFORE the Fluss ZK commit. This unit-test harness has no real lake
        // catalog wired in, so the propagation step fails with a FlussRuntimeException whose
        // message clearly points at the propagation stage.
        assertThatThrownBy(() -> alterBucketNum(metadataManager, tablePath, "8"))
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("propagate ALTER bucket.num to the lake side");

        // The propagation failure aborts the ALTER BEFORE the Fluss ZK commit, so table-level and
        // pre-existing partition state must both be unchanged.
        assertThat(metadataManager.getTable(tablePath).getNumBuckets())
                .isEqualTo(originalBucketCount);
        Optional<PartitionRegistration> pre = zookeeperClient.getPartition(tablePath, "2024-01");
        assertThat(pre).isPresent();
        assertThat(pre.get().getBucketCount()).isEqualTo(originalBucketCount);
    }

    @Test
    void testAlterBucketNumLakePropagationFailureAbortsAlter() throws Exception {
        CountingLakeCatalog stub = new CountingLakeCatalog(true);
        MetadataManager mm = buildMetadataManagerWithLakeCatalog(stub);
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_lake_alter_persistent_fail");
        int originalBucketCount = 4;
        createSeededLakePartitionedTable(mm, tablePath, originalBucketCount);

        // A lake failure fails loud: the ALTER aborts BEFORE the Fluss ZK commit with a clear
        // error telling the operator nothing was changed on the Fluss side and to re-run the
        // ALTER once the lake is reachable.
        assertThatThrownBy(() -> alterBucketNum(mm, tablePath, "8"))
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("to the lake schema failed")
                .hasMessageContaining("The Fluss side was NOT changed")
                .hasMessageContaining("Re-run the same ALTER");

        // Propagation is attempted exactly once.
        assertThat(stub.attempts.get()).isEqualTo(1);

        // Lake First: the Fluss ZK commit never ran, so table-level bucket count and the
        // pre-existing partition are both unchanged.
        assertThat(mm.getTable(tablePath).getNumBuckets()).isEqualTo(originalBucketCount);
        Optional<PartitionRegistration> pre = zookeeperClient.getPartition(tablePath, "2024-01");
        assertThat(pre).isPresent();
        assertThat(pre.get().getBucketCount()).isEqualTo(originalBucketCount);
    }

    @Test
    void testAlterBucketNumLakePropagationSucceedsFirstTry() throws Exception {
        CountingLakeCatalog stub = new CountingLakeCatalog(false);
        MetadataManager mm = buildMetadataManagerWithLakeCatalog(stub);
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_lake_alter_success");
        int newBucketCount = 8;
        createSeededLakePartitionedTable(mm, tablePath, 4);

        alterBucketNum(mm, tablePath, String.valueOf(newBucketCount));

        // Propagation succeeded on the first attempt.
        assertThat(stub.attempts.get()).isEqualTo(1);
        assertThat(stub.lastBucketCount).isEqualTo(newBucketCount);
        assertThat(mm.getTable(tablePath).getNumBuckets()).isEqualTo(newBucketCount);
    }

    @Test
    void testAlterBucketNumSkipsLakePropagationForUnawareBucketTable() throws Exception {
        // A lake table WITHOUT bucket keys is an Unaware Bucket table in Paimon (BUCKET = -1
        // encodes the bucket MODE); propagating a positive BUCKET would flip its mode. The
        // propagation must be skipped entirely while the Fluss-side rescale still succeeds.
        CountingLakeCatalog stub = new CountingLakeCatalog(false);
        MetadataManager mm = buildMetadataManagerWithLakeCatalog(stub);
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_lake_alter_unaware_skip");
        int originalBucketCount = 4;
        int newBucketCount = 8;
        TableDescriptor unawareLakeTable =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("b", DataTypes.STRING())
                                        .build())
                        .distributedBy(originalBucketCount)
                        .partitionedBy("b")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FORMAT.key(), "paimon")
                        .build()
                        .withReplicationFactor(3);
        mm.createTable(
                tablePath,
                remoteDataDir,
                unawareLakeTable,
                generateAssignment(originalBucketCount, 3, getTabletServers()),
                false);

        alterBucketNum(mm, tablePath, String.valueOf(newBucketCount));

        // no call ever reached the lake catalog, and the Fluss side still rescaled
        assertThat(stub.attempts.get()).isEqualTo(0);
        assertThat(mm.getTable(tablePath).getNumBuckets()).isEqualTo(newBucketCount);
    }

    /**
     * Builds a coordinator-side MetadataManager and reflectively injects the given stub as the
     * cluster lake catalog, so lake propagation can be exercised without a real Paimon catalog.
     */
    private static MetadataManager buildMetadataManagerWithLakeCatalog(LakeCatalog stub)
            throws Exception {
        LakeCatalogDynamicLoader loader =
                new LakeCatalogDynamicLoader(new Configuration(), null, true);
        Field containerField =
                LakeCatalogDynamicLoader.class.getDeclaredField("lakeCatalogContainer");
        containerField.setAccessible(true);
        Object container = containerField.get(loader);
        Field catalogField = container.getClass().getDeclaredField("lakeCatalog");
        catalogField.setAccessible(true);
        catalogField.set(container, stub);
        return new MetadataManager(zookeeperClient, new Configuration(), loader);
    }

    /**
     * Creates a lake-enabled, partitioned Fixed Bucket table with the given original bucket count
     * and seeds one pre-existing partition "2024-01" carrying that bucket count.
     */
    private static void createSeededLakePartitionedTable(
            MetadataManager mm, TablePath tablePath, int originalBucketCount) throws Exception {
        TableDescriptor lakeTable =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("b", DataTypes.STRING())
                                        .build())
                        .distributedBy(originalBucketCount, "a")
                        .partitionedBy("b")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FORMAT.key(), "paimon")
                        .build()
                        .withReplicationFactor(3);
        TableAssignment tableAssignment =
                generateAssignment(originalBucketCount, 3, getTabletServers());
        mm.createTable(tablePath, remoteDataDir, lakeTable, tableAssignment, false);
        TableInfo tableInfo = mm.getTable(tablePath);
        mm.createPartition(
                tablePath,
                tableInfo.getTableId(),
                remoteDataDir,
                new PartitionAssignment(
                        tableInfo.getTableId(), tableAssignment.getBucketAssignments()),
                fromPartitionName(tableInfo.getPartitionKeys(), "2024-01"),
                false,
                originalBucketCount);
    }

    /**
     * A stub lake catalog that counts bucket-count propagations (a "bucket.num" SetOption through
     * alterTable) and can simulate transient faults.
     */
    private static final class CountingLakeCatalog implements LakeCatalog {
        private final AtomicInteger attempts = new AtomicInteger();
        private volatile boolean failing;
        private volatile Integer lastBucketCount;

        CountingLakeCatalog(boolean failing) {
            this.failing = failing;
        }

        @Override
        public void createTable(
                TablePath tablePath, TableDescriptor tableDescriptor, Context context) {
            // not used by these tests
        }

        @Override
        public void alterTable(
                TablePath tablePath, java.util.List<TableChange> tableChanges, Context context) {
            for (TableChange change : tableChanges) {
                if (change instanceof TableChange.SetOption
                        && "bucket.num".equals(((TableChange.SetOption) change).getKey())) {
                    attempts.incrementAndGet();
                    if (failing) {
                        throw new RuntimeException("simulated transient lake failure");
                    }
                    lastBucketCount = Integer.parseInt(((TableChange.SetOption) change).getValue());
                }
            }
        }
    }

    @Test
    void testAlterBucketNumRejectedOnNonPartitionedTable() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_non_partitioned_reject");
        TableDescriptor logTable =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("b", DataTypes.STRING())
                                        .build())
                        .distributedBy(4)
                        .build()
                        .withReplicationFactor(3);

        TableAssignment tableAssignment = generateAssignment(4, 3, getTabletServers());
        metadataManager.createTable(tablePath, remoteDataDir, logTable, tableAssignment, false);

        // ALTER bucket.num on non-partitioned table should be rejected
        assertThatThrownBy(() -> alterBucketNum(metadataManager, tablePath, "8"))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("Cannot alter 'bucket.num' on non-partitioned table");
    }

    // ========================== Success Tests ==========================

    @ParameterizedTest(name = "bucketNum {0} -> {1}")
    @CsvSource({"3, 6", "6, 3"})
    void testBackfillOnlyAffectsPartitionsWithoutBucketCount(
            int originalBucketCount, int newBucketCount) throws Exception {
        TablePath tablePath =
                TablePath.of(
                        DEFAULT_DB,
                        "test_backfill_idempotent_" + originalBucketCount + "_" + newBucketCount);
        TableAssignment tableAssignment =
                generateAssignment(originalBucketCount, 3, getTabletServers());
        metadataManager.createTable(
                tablePath,
                remoteDataDir,
                partitionedLogTable(originalBucketCount),
                tableAssignment,
                false);
        TableInfo tableInfo = metadataManager.getTable(tablePath);
        long tableId = tableInfo.getTableId();

        // Create two partitions with bucketCount = 3
        PartitionAssignment partitionAssignment =
                new PartitionAssignment(tableId, tableAssignment.getBucketAssignments());
        metadataManager.createPartition(
                tablePath,
                tableId,
                remoteDataDir,
                partitionAssignment,
                fromPartitionName(tableInfo.getPartitionKeys(), "legacy"),
                false,
                originalBucketCount);
        metadataManager.createPartition(
                tablePath,
                tableId,
                remoteDataDir,
                partitionAssignment,
                fromPartitionName(tableInfo.getPartitionKeys(), "new"),
                false,
                originalBucketCount);

        // Simulate a legacy partition by overwriting its registration with null bucketCount.
        // This models a partition created before per-partition bucket count was introduced.
        Optional<PartitionRegistration> legacyReg =
                zookeeperClient.getPartition(tablePath, "legacy");
        assertThat(legacyReg).isPresent();
        PartitionRegistration nullBucketCountReg =
                new PartitionRegistration(
                        legacyReg.get().getTableId(),
                        legacyReg.get().getPartitionId(),
                        legacyReg.get().getRemoteDataDir());
        zookeeperClient.updatePartitionRegistration(tablePath, "legacy", nullBucketCountReg);

        // Verify: "legacy" has null bucketCount, "new" has 3
        Optional<PartitionRegistration> beforeLegacy =
                zookeeperClient.getPartition(tablePath, "legacy");
        assertThat(beforeLegacy).isPresent();
        assertThat(beforeLegacy.get().getBucketCount()).isNull();

        Optional<PartitionRegistration> beforeNew = zookeeperClient.getPartition(tablePath, "new");
        assertThat(beforeNew).isPresent();
        assertThat(beforeNew.get().getBucketCount()).isEqualTo(originalBucketCount);

        // ALTER bucket.num in both directions (scale-up 3->6 and scale-down 6->3)
        alterBucketNum(metadataManager, tablePath, String.valueOf(newBucketCount));

        // Verify: table-level bucket count was updated and persisted in ZK
        assertThat(metadataManager.getTable(tablePath).getNumBuckets()).isEqualTo(newBucketCount);

        // Verify: "legacy" was backfilled with actual bucket count (from assignment size)
        Optional<PartitionRegistration> afterLegacy =
                zookeeperClient.getPartition(tablePath, "legacy");
        assertThat(afterLegacy).isPresent();
        assertThat(afterLegacy.get().getBucketCount()).isEqualTo(originalBucketCount);

        // Verify: "new" still has the original bucketCount (not overwritten to the new value)
        Optional<PartitionRegistration> afterNew = zookeeperClient.getPartition(tablePath, "new");
        assertThat(afterNew).isPresent();
        assertThat(afterNew.get().getBucketCount()).isEqualTo(originalBucketCount);
    }

    @Test
    void testAlterBucketNumIsMutuallyExclusiveWithPartitionCreation() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_alter_vs_create_race");
        int originalBucketCount = 4;
        TableAssignment tableAssignment =
                generateAssignment(originalBucketCount, 3, getTabletServers());
        metadataManager.createTable(
                tablePath,
                remoteDataDir,
                partitionedLogTable(originalBucketCount),
                tableAssignment,
                false);
        TableInfo tableInfo = metadataManager.getTable(tablePath);
        long tableId = tableInfo.getTableId();

        // an existing partition created with the original bucket count (4)
        PartitionAssignment partitionAssignment =
                new PartitionAssignment(tableId, tableAssignment.getBucketAssignments());
        metadataManager.createPartition(
                tablePath,
                tableId,
                remoteDataDir,
                partitionAssignment,
                fromPartitionName(tableInfo.getPartitionKeys(), "2024-01"),
                false,
                originalBucketCount);

        ReadWriteLock rescaleLock = metadataManager.getBucketRescaleLock(tablePath);

        // Concurrent partition creations share the read lock (they still run in parallel): a second
        // read lock can be acquired while the first is held.
        Lock firstReader = rescaleLock.readLock();
        Lock secondReader = rescaleLock.readLock();
        firstReader.lock();
        try {
            assertThat(secondReader.tryLock(2, TimeUnit.SECONDS)).isTrue();
            secondReader.unlock();
        } finally {
            firstReader.unlock();
        }

        // The partition-creation paths (CoordinatorService.createPartition / AutoPartitionManager)
        // and the drop-partition path (CoordinatorService.dropPartition) all hold this same read
        // lock across their critical section, so this test covers the mutual exclusion for both.
        // Simulate an in-flight creation by holding the read lock, then assert a real ALTER
        // bucket.num (which must take the write lock) cannot proceed until it is released.
        Lock creationReadLock = rescaleLock.readLock();
        creationReadLock.lock();

        CountDownLatch alterStarted = new CountDownLatch(1);
        AtomicReference<Throwable> alterError = new AtomicReference<>();
        Thread alterThread =
                new Thread(
                        () -> {
                            alterStarted.countDown();
                            try {
                                alterBucketNum(metadataManager, tablePath, "8");
                            } catch (Throwable t) {
                                alterError.set(t);
                            }
                        },
                        "alter-bucket-num-thread");

        try {
            alterThread.start();
            assertThat(alterStarted.await(5, TimeUnit.SECONDS)).isTrue();

            // Wait until the ALTER thread is parked waiting for the write lock.
            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline
                    && alterThread.getState() != Thread.State.WAITING
                    && alterThread.getState() != Thread.State.TIMED_WAITING) {
                Thread.sleep(20);
            }

            // While the creation read lock is held, ALTER is blocked: table-level stays 4.
            assertThat(alterThread.isAlive()).isTrue();
            assertThat(metadataManager.getTable(tablePath).getNumBuckets())
                    .isEqualTo(originalBucketCount);
        } finally {
            creationReadLock.unlock();
        }

        // After releasing the read lock, ALTER acquires the write lock and completes.
        alterThread.join(10_000);
        assertThat(alterThread.isAlive()).isFalse();
        assertThat(alterError.get()).isNull();
        assertThat(metadataManager.getTable(tablePath).getNumBuckets()).isEqualTo(8);

        // old partition keeps its original bucket count
        Optional<PartitionRegistration> oldPartition =
                zookeeperClient.getPartition(tablePath, "2024-01");
        assertThat(oldPartition).isPresent();
        assertThat(oldPartition.get().getBucketCount()).isEqualTo(originalBucketCount);
    }

    private enum StaleFence {
        TABLE_VERSION,
        PARTITION_VERSION,
        COORDINATOR_EPOCH
    }

    @ParameterizedTest
    @EnumSource(StaleFence.class)
    void testBackfillCommitRejectsStaleFence(StaleFence fence) throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_fence_" + fence.name().toLowerCase());
        int originalBucketCount = 4;
        TableAssignment tableAssignment =
                generateAssignment(originalBucketCount, 3, getTabletServers());
        metadataManager.createTable(
                tablePath,
                remoteDataDir,
                partitionedLogTable(originalBucketCount),
                tableAssignment,
                false);

        String partitionName = "2024-01";
        if (fence == StaleFence.PARTITION_VERSION) {
            TableInfo tableInfo = metadataManager.getTable(tablePath);
            metadataManager.createPartition(
                    tablePath,
                    tableInfo.getTableId(),
                    remoteDataDir,
                    new PartitionAssignment(
                            tableInfo.getTableId(), tableAssignment.getBucketAssignments()),
                    fromPartitionName(tableInfo.getPartitionKeys(), partitionName),
                    false,
                    originalBucketCount);
        }

        // Capture fresh versions, then make exactly the fenced dimension stale (simulating a
        // concurrent read-modify-write or a deposed coordinator).
        ZooKeeperClient.VersionedData<TableRegistration> table =
                zookeeperClient.getTableWithVersion(tablePath).get();
        int tableVersion = table.zkVersion();
        Map<String, ZooKeeperClient.VersionedData<PartitionRegistration>> backfills =
                new HashMap<>();
        int epochVersion = ZkVersion.MATCH_ANY_VERSION.getVersion();
        switch (fence) {
            case TABLE_VERSION:
                zookeeperClient.updateTable(tablePath, table.data());
                break;
            case PARTITION_VERSION:
                ZooKeeperClient.VersionedData<PartitionRegistration> partition =
                        zookeeperClient.getPartitionWithVersion(tablePath, partitionName).get();
                zookeeperClient.updatePartitionRegistration(
                        tablePath, partitionName, partition.data());
                backfills.put(partitionName, partition);
                break;
            case COORDINATOR_EPOCH:
                epochVersion = zookeeperClient.getCurrentEpoch().getCoordinatorEpochZkVersion() + 1;
                break;
        }

        int staleEpochVersion = epochVersion;
        assertThatThrownBy(
                        () ->
                                zookeeperClient.updateTableWithPartitionBucketCountBackfill(
                                        tablePath,
                                        table.data().withBucketCount(8),
                                        tableVersion,
                                        backfills,
                                        staleEpochVersion))
                .isInstanceOf(KeeperException.BadVersionException.class);
        // The atomic transaction rejected everything: table-level unchanged.
        assertThat(metadataManager.getTable(tablePath).getNumBuckets())
                .isEqualTo(originalBucketCount);

        // With every dimension fresh the same commit succeeds.
        ZooKeeperClient.VersionedData<TableRegistration> freshTable =
                zookeeperClient.getTableWithVersion(tablePath).get();
        Map<String, ZooKeeperClient.VersionedData<PartitionRegistration>> freshBackfills =
                new HashMap<>();
        if (fence == StaleFence.PARTITION_VERSION) {
            freshBackfills.put(
                    partitionName,
                    zookeeperClient.getPartitionWithVersion(tablePath, partitionName).get());
        }
        zookeeperClient.updateTableWithPartitionBucketCountBackfill(
                tablePath,
                freshTable.data().withBucketCount(8),
                freshTable.zkVersion(),
                freshBackfills,
                zookeeperClient.getCurrentEpoch().getCoordinatorEpochZkVersion());
        assertThat(metadataManager.getTable(tablePath).getNumBuckets()).isEqualTo(8);
    }

    @ParameterizedTest(name = "newBucketNum={0}")
    @MethodSource("outOfRangeBucketNums")
    void testAlterBucketNumRejectedOutOfRange(
            String newBucketNum, Class<? extends Throwable> expectedException, String message)
            throws Exception {
        TablePath tablePath =
                TablePath.of(DEFAULT_DB, "test_alter_bucket_num_out_of_range_" + newBucketNum);
        int originalBucketCount = 4;
        metadataManager.createTable(
                tablePath,
                remoteDataDir,
                partitionedLogTable(originalBucketCount),
                generateAssignment(originalBucketCount, 3, getTabletServers()),
                false);

        assertThatThrownBy(() -> alterBucketNum(metadataManager, tablePath, newBucketNum))
                .isInstanceOf(expectedException)
                .hasMessageContaining(message);
        // table-level bucket count unchanged
        assertThat(metadataManager.getTable(tablePath).getNumBuckets())
                .isEqualTo(originalBucketCount);
    }

    private static Stream<Arguments> outOfRangeBucketNums() {
        return Stream.of(
                Arguments.of("0", InvalidAlterTableException.class, "at least 1"),
                Arguments.of(
                        String.valueOf(ConfigOptions.MAX_BUCKET_NUM.defaultValue() + 1),
                        TooManyBucketsException.class,
                        "exceeding the maximum"));
    }

    @Test
    void testAlterBucketNumRetriesOnceThenSucceeds() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_alter_bucket_num_retry_success");
        int originalBucketCount = 4;
        metadataManager.createTable(
                tablePath,
                remoteDataDir,
                partitionedLogTable(originalBucketCount),
                generateAssignment(originalBucketCount, 3, getTabletServers()),
                false);

        // A ZK client that throws BadVersion on the first bucket-count commit, then delegates.
        // This deterministically exercises the retry loop in alterTableProperties: attempt 1
        // hits BadVersion, attempt 2 re-reads a fresh version and commits successfully.
        AtomicInteger commitCalls = new AtomicInteger();
        MetadataManager retryMetadataManager =
                metadataManagerOver(zkClientFailingCommits(1, commitCalls));

        alterBucketNum(retryMetadataManager, tablePath, "8");

        // The retry loop must have re-invoked the commit exactly once after the injected failure.
        assertThat(commitCalls.get()).isEqualTo(2);
        // Table-level bucket count is now the new value, confirming the retried attempt succeeded.
        assertThat(metadataManager.getTable(tablePath).getNumBuckets()).isEqualTo(8);
    }

    @Test
    void testAlterBucketNumFailsAfterMaxRetries() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_alter_bucket_num_retry_exhaust");
        int originalBucketCount = 4;
        metadataManager.createTable(
                tablePath,
                remoteDataDir,
                partitionedLogTable(originalBucketCount),
                generateAssignment(originalBucketCount, 3, getTabletServers()),
                false);

        // Every commit throws BadVersion so the retry loop exhausts its budget and wraps the
        // failure into FlussRuntimeException with the "after 3 retries" message.
        AtomicInteger commitCalls = new AtomicInteger();
        MetadataManager exhaustRetryManager =
                metadataManagerOver(zkClientFailingCommits(Integer.MAX_VALUE, commitCalls));

        assertThatThrownBy(() -> alterBucketNum(exhaustRetryManager, tablePath, "8"))
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("after 3 retries")
                .hasCauseInstanceOf(KeeperException.BadVersionException.class);
        // Commit was attempted exactly MAX_ALTER_TABLE_RETRIES=3 times.
        assertThat(commitCalls.get()).isEqualTo(3);
        // Nothing was persisted.
        assertThat(metadataManager.getTable(tablePath).getNumBuckets())
                .isEqualTo(originalBucketCount);
    }

    @Test
    void testAlterBackfillRejectsPartitionMissingRegistration() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_alter_bucket_num_missing_reg");
        int originalBucketCount = 4;
        String victimPartition = "2024-01";
        createTableWithLegacyPartition(tablePath, victimPartition);

        // Wrap the ZK client so getPartitionWithVersion returns empty for the victim, simulating a
        // race where getPartitions() listed the partition but the individual znode has vanished.
        Configuration wrapperConfig = new Configuration();
        wrapperConfig.set(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir);
        ZooKeeperClient missingRegClient =
                new ZooKeeperClient(sharedZkWrapper(), wrapperConfig) {
                    @Override
                    public Optional<ZooKeeperClient.VersionedData<PartitionRegistration>>
                            getPartitionWithVersion(TablePath tp, String partitionName)
                                    throws Exception {
                        if (victimPartition.equals(partitionName)) {
                            return Optional.empty();
                        }
                        return super.getPartitionWithVersion(tp, partitionName);
                    }
                };
        MetadataManager missingRegManager = metadataManagerOver(missingRegClient);

        assertThatThrownBy(() -> alterBucketNum(missingRegManager, tablePath, "8"))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("is listed but its registration is missing");
        // Table-level bucket count MUST remain unchanged: partial commit is unacceptable.
        assertThat(metadataManager.getTable(tablePath).getNumBuckets())
                .isEqualTo(originalBucketCount);
    }

    @Test
    void testAlterBackfillRejectsPartitionMissingAssignment() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_alter_bucket_num_missing_assign");
        int originalBucketCount = 4;
        String victimPartition = "2024-02";
        long victimPartitionId = createTableWithLegacyPartition(tablePath, victimPartition);

        Configuration wrapperConfig = new Configuration();
        wrapperConfig.set(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir);
        ZooKeeperClient missingAssignClient =
                new ZooKeeperClient(sharedZkWrapper(), wrapperConfig) {
                    @Override
                    public Optional<PartitionAssignment> getPartitionAssignment(long partitionId)
                            throws Exception {
                        if (partitionId == victimPartitionId) {
                            return Optional.empty();
                        }
                        return super.getPartitionAssignment(partitionId);
                    }
                };
        MetadataManager missingAssignManager = metadataManagerOver(missingAssignClient);

        assertThatThrownBy(() -> alterBucketNum(missingAssignManager, tablePath, "8"))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("has no readable bucket assignment");
        assertThat(metadataManager.getTable(tablePath).getNumBuckets())
                .isEqualTo(originalBucketCount);
    }

    // ========================== Helpers ==========================

    private static TabletServerInfo[] getTabletServers() {
        return new TabletServerInfo[] {
            new TabletServerInfo(0, "rack0"),
            new TabletServerInfo(1, "rack1"),
            new TabletServerInfo(2, "rack2")
        };
    }

    /** A partitioned log table: INT column "a", STRING partition key "dt". */
    private static TableDescriptor partitionedLogTable(int bucketCount) {
        return TableDescriptor.builder()
                .schema(
                        Schema.newBuilder()
                                .column("a", DataTypes.INT())
                                .column("dt", DataTypes.STRING())
                                .build())
                .distributedBy(bucketCount)
                .partitionedBy("dt")
                .build()
                .withReplicationFactor(3);
    }

    private static void alterBucketNum(
            MetadataManager manager, TablePath tablePath, String newBucketNum) {
        TablePropertyChanges.Builder builder = TablePropertyChanges.builder();
        builder.setCustomProperty("bucket.num", newBucketNum);
        manager.alterTableProperties(
                tablePath,
                Collections.singletonList(TableChange.set("bucket.num", newBucketNum)),
                builder.build(),
                false,
                null,
                (currentTable, updatedTable) -> {},
                (currentTable, updatedTable) -> {},
                ZkVersion.MATCH_ANY_VERSION.getVersion());
    }

    /**
     * Creates a partitioned log table with one partition whose persisted bucket count is then
     * cleared, so an ALTER bucket.num must enter the backfill path for it. Returns the partition
     * id.
     */
    private static long createTableWithLegacyPartition(TablePath tablePath, String partitionName)
            throws Exception {
        int originalBucketCount = 4;
        TableAssignment tableAssignment =
                generateAssignment(originalBucketCount, 3, getTabletServers());
        metadataManager.createTable(
                tablePath,
                remoteDataDir,
                partitionedLogTable(originalBucketCount),
                tableAssignment,
                false);
        TableInfo tableInfo = metadataManager.getTable(tablePath);
        metadataManager.createPartition(
                tablePath,
                tableInfo.getTableId(),
                remoteDataDir,
                new PartitionAssignment(
                        tableInfo.getTableId(), tableAssignment.getBucketAssignments()),
                fromPartitionName(tableInfo.getPartitionKeys(), partitionName),
                false,
                originalBucketCount);
        ZooKeeperClient.VersionedData<PartitionRegistration> versioned =
                zookeeperClient.getPartitionWithVersion(tablePath, partitionName).get();
        zookeeperClient.updatePartitionRegistration(
                tablePath,
                partitionName,
                new PartitionRegistration(
                        versioned.data().getTableId(),
                        versioned.data().getPartitionId(),
                        versioned.data().getRemoteDataDir(),
                        null));
        return versioned.data().getPartitionId();
    }

    /** Builds a MetadataManager over a decorated ZK client sharing the test cluster. */
    private static MetadataManager metadataManagerOver(ZooKeeperClient decoratedClient) {
        return new MetadataManager(
                decoratedClient,
                new Configuration(),
                new LakeCatalogDynamicLoader(new Configuration(), null, true));
    }

    /** Shares the test ZK connection so decorating subclasses can override single methods. */
    private static CuratorFrameworkWithUnhandledErrorListener sharedZkWrapper() throws Exception {
        Field wrapperField = ZooKeeperClient.class.getDeclaredField("curatorFrameworkWrapper");
        wrapperField.setAccessible(true);
        return (CuratorFrameworkWithUnhandledErrorListener) wrapperField.get(zookeeperClient);
    }

    /**
     * A ZK client sharing the test connection whose bucket-count commit throws BadVersion for the
     * first {@code failures} calls (counted in {@code commitCalls}) and delegates afterwards.
     */
    private static ZooKeeperClient zkClientFailingCommits(int failures, AtomicInteger commitCalls)
            throws Exception {
        Configuration wrapperConfig = new Configuration();
        wrapperConfig.set(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir);
        return new ZooKeeperClient(sharedZkWrapper(), wrapperConfig) {
            @Override
            public void updateTableWithPartitionBucketCountBackfill(
                    TablePath tp,
                    TableRegistration reg,
                    int expectedTableZkVersion,
                    Map<String, ZooKeeperClient.VersionedData<PartitionRegistration>> backfills,
                    int expectedCoordinatorEpochZkVersion)
                    throws Exception {
                if (commitCalls.getAndIncrement() < failures) {
                    throw new KeeperException.BadVersionException();
                }
                super.updateTableWithPartitionBucketCountBackfill(
                        tp,
                        reg,
                        expectedTableZkVersion,
                        backfills,
                        expectedCoordinatorEpochZkVersion);
            }
        };
    }
}

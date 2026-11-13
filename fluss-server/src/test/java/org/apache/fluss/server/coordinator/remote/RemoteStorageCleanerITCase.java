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

package org.apache.fluss.server.coordinator.remote;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.coordinator.statemachine.ReplicaState;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.FlussPaths;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.fluss.record.TestData.DATA1_PARTITIONED_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR_PK;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newDropPartitionRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newDropTableRequest;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for {@link RemoteStorageCleaner}. */
class RemoteStorageCleanerITCase {

    private static final TableDescriptor DATA1_PARTITIONED_TABLE_DESCRIPTOR_PK =
            TableDescriptor.builder()
                    .schema(
                            Schema.newBuilder()
                                    .column("a", DataTypes.INT())
                                    .withComment("a is first column")
                                    .column("b", DataTypes.STRING())
                                    .withComment("b is second column")
                                    .primaryKey("a", "b")
                                    .build())
                    .distributedBy(3)
                    .partitionedBy("b")
                    .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                    .property(
                            ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                            AutoPartitionTimeUnit.YEAR)
                    .build();

    private static final List<String> REMOTE_DATA_DIRS = Arrays.asList("dir1", "dir2", "dir3");

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(new Configuration())
                    .setRemoteDirNames(REMOTE_DATA_DIRS)
                    .build();

    private ZooKeeperClient zkClient;
    private CoordinatorGateway coordinatorGateway;

    @BeforeEach
    void beforeEach() {
        zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        coordinatorGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDeleteNonPartitionedTable(boolean isPkTable) throws Exception {
        // Create table
        String tablePrefix = isPkTable ? "pk_table_cleaner_" : "log_table_cleaner_";
        TablePath tablePath = TablePath.of("test_db", tablePrefix + "1");
        TableDescriptor tableDescriptor =
                isPkTable ? DATA1_TABLE_DESCRIPTOR_PK : DATA1_TABLE_DESCRIPTOR;

        long tableId =
                RpcMessageTestUtils.createTable(
                        FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);
        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);

        // Get remote data dir for table
        Optional<TableRegistration> tableOpt = zkClient.getTable(tablePath);
        assertThat(tableOpt).isPresent();
        String remoteTableDataDir = tableOpt.get().remoteDataDir;
        assertThat(remoteTableDataDir).isNotNull();

        // Mock remote log dirs
        FsPath remoteLogTableDir =
                FlussPaths.remoteTableDir(
                        FlussPaths.remoteLogDir(remoteTableDataDir), tablePath, tableId);
        remoteLogTableDir.getFileSystem().mkdirs(remoteLogTableDir);
        assertThat(remoteLogTableDir.getFileSystem().exists(remoteLogTableDir)).isTrue();

        // Mock remote kv dirs
        FsPath remoteKvTableDir;
        if (isPkTable) {
            remoteKvTableDir =
                    FlussPaths.remoteTableDir(
                            FlussPaths.remoteKvDir(remoteTableDataDir), tablePath, tableId);
            remoteKvTableDir.getFileSystem().mkdirs(remoteKvTableDir);
            assertThat(remoteKvTableDir.getFileSystem().exists(remoteKvTableDir)).isTrue();
        } else {
            remoteKvTableDir = null;
        }

        // Mock remote lake dirs
        FsPath remoteLakeDir =
                FlussPaths.remoteLakeTableSnapshotDir(remoteTableDataDir, tablePath, tableId);
        remoteLakeDir.getFileSystem().mkdirs(remoteLakeDir);
        assertThat(remoteLakeDir.getFileSystem().exists(remoteLakeDir)).isTrue();

        // Drop table
        coordinatorGateway
                .dropTable(
                        newDropTableRequest(
                                tablePath.getDatabaseName(), tablePath.getTableName(), false))
                .get();
        assertThat(zkClient.tableExist(tablePath)).isFalse();

        // Verify remote log dir is deleted
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(remoteLogTableDir.getFileSystem().exists(remoteLogTableDir))
                                .isFalse());

        // Verify remote kv dir is deleted for PK table
        if (isPkTable) {
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(remoteKvTableDir.getFileSystem().exists(remoteKvTableDir))
                                    .isFalse());
        }

        // Verify remote lake dir is deleted
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(remoteLakeDir.getFileSystem().exists(remoteLakeDir)).isFalse());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDeletePartitionedTable(boolean isPkTable) throws Exception {
        String tableName = isPkTable ? "pk_partitioned_cleaner" : "partitioned_cleaner";
        TablePath tablePath = TablePath.of("test_db", tableName);
        TableDescriptor tableDescriptor =
                TableDescriptor.builder(
                                isPkTable
                                        ? DATA1_PARTITIONED_TABLE_DESCRIPTOR_PK
                                        : DATA1_PARTITIONED_TABLE_DESCRIPTOR)
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, false)
                        .build()
                        .withReplicationFactor(1);
        String tableRemoteDir = FLUSS_CLUSTER_EXTENSION.getRemoteDataDir("dir1");
        String firstPartitionRemoteDir = FLUSS_CLUSTER_EXTENSION.getRemoteDataDir("dir2");
        String lastPartitionRemoteDir = FLUSS_CLUSTER_EXTENSION.getRemoteDataDir("dir3");
        Configuration conf = new Configuration();
        MetadataManager metadataManager =
                new MetadataManager(zkClient, conf, new LakeCatalogDynamicLoader(conf, null, true));
        metadataManager.createDatabase(
                tablePath.getDatabaseName(), DatabaseDescriptor.builder().build(), true);
        long tableId =
                metadataManager.createTable(
                        tablePath, tableRemoteDir, tableDescriptor, null, false);
        retryVerifyContext(ctx -> assertThat(ctx.getTableInfoById(tableId)).isNotNull());

        // Explicit roots and assignments keep table cleanup from hiding missed partition cleanup.
        TablePartition firstPartition =
                createPartition(tablePath, tableId, "first", firstPartitionRemoteDir, 0);
        TablePartition lastPartition =
                createPartition(tablePath, tableId, "last", lastPartitionRemoteDir, 1);
        List<FsPath> remoteDirs = new ArrayList<>();
        remoteDirs.addAll(
                createPartitionRemoteData(
                        tablePath, firstPartition, "first", firstPartitionRemoteDir, isPkTable));
        remoteDirs.addAll(
                createPartitionRemoteData(
                        tablePath, lastPartition, "last", lastPartitionRemoteDir, isPkTable));
        FsPath remoteLogTableDir =
                FlussPaths.remoteTableDir(
                        FlussPaths.remoteLogDir(tableRemoteDir), tablePath, tableId);
        FsPath remoteLakeDir =
                FlussPaths.remoteLakeTableSnapshotDir(tableRemoteDir, tablePath, tableId);
        remoteDirs.add(remoteLogTableDir);
        remoteDirs.add(remoteLakeDir);
        createRemoteData(remoteLogTableDir);
        createRemoteData(remoteLakeDir);
        if (isPkTable) {
            FsPath remoteKvTableDir =
                    FlussPaths.remoteTableDir(
                            FlussPaths.remoteKvDir(tableRemoteDir), tablePath, tableId);
            createRemoteData(remoteKvTableDir);
            remoteDirs.add(remoteKvTableDir);
        }

        // Controlled deletion-response ordering is covered by ReplicaDeletionInterleavingTest.
        coordinatorGateway
                .dropTable(
                        newDropTableRequest(
                                tablePath.getDatabaseName(), tablePath.getTableName(), false))
                .get();
        assertThat(zkClient.tableExist(tablePath)).isFalse();

        retryVerifyContext(
                ctx -> {
                    for (TablePartition partition : Arrays.asList(firstPartition, lastPartition)) {
                        assertThat(ctx.getPartitionInfoById(partition.getPartitionId())).isNull();
                        assertThat(ctx.containsPartitionId(partition.getPartitionId())).isFalse();
                        assertThat(ctx.isPartitionQueuedForDeletion(partition)).isFalse();
                    }
                    assertThat(ctx.getTableInfoById(tableId)).isNull();
                    assertThat(ctx.getTablePathById(tableId)).isNull();
                    assertThat(ctx.allTables()).doesNotContainKey(tableId);
                    assertThat(ctx.getTablesToBeDeleted()).doesNotContain(tableId);
                });
        retry(
                Duration.ofMinutes(2),
                () -> {
                    for (FsPath dir : remoteDirs) {
                        assertThat(dir.getFileSystem().exists(dir)).isFalse();
                    }
                });
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDeleteSinglePartition(boolean isPkTable) throws Exception {
        // Create partitioned table
        String tableName =
                isPkTable ? "pk_partition_drop_cleaner_table" : "partition_drop_cleaner_table";
        TablePath tablePath = TablePath.of("test_db", tableName);
        // Keep this partition's lifecycle under test control instead of the auto-partitioner.
        TableDescriptor tableDescriptor =
                TableDescriptor.builder(
                                isPkTable
                                        ? DATA1_PARTITIONED_TABLE_DESCRIPTOR_PK
                                        : DATA1_PARTITIONED_TABLE_DESCRIPTOR)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, false)
                        .build();

        long tableId =
                RpcMessageTestUtils.createTable(
                        FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);

        // Get remote data dir for table
        Optional<TableRegistration> tableOpt = zkClient.getTable(tablePath);
        assertThat(tableOpt).isPresent();
        String remoteTableDataDir = tableOpt.get().remoteDataDir;
        assertThat(remoteTableDataDir).isNotNull();

        // Create a manual partition
        String partitionName = "2026";
        PartitionSpec partitionSpec =
                new PartitionSpec(Collections.singletonMap("b", partitionName));
        RpcMessageTestUtils.createPartition(
                FLUSS_CLUSTER_EXTENSION, tablePath, partitionSpec, false);

        // Wait for the partition to be ready
        Optional<PartitionRegistration> partitionOpt =
                zkClient.getPartition(tablePath, partitionName);
        assertThat(partitionOpt).isPresent();
        long partitionId = partitionOpt.get().getPartitionId();
        TableBucket tb = new TableBucket(tableId, partitionId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);

        // Get remote partition dir
        String partitionRemoteDataDir = partitionOpt.get().getRemoteDataDir();
        assertThat(partitionRemoteDataDir).isNotNull();

        TablePartition tablePartition = new TablePartition(tableId, partitionId);
        List<FsPath> remotePartitionDirs =
                createPartitionRemoteData(
                        tablePath,
                        tablePartition,
                        partitionName,
                        partitionRemoteDataDir,
                        isPkTable);

        // Create remote lake data that must survive partition deletion.
        FsPath remoteLakeDir =
                FlussPaths.remoteLakeTableSnapshotDir(remoteTableDataDir, tablePath, tableId);
        createRemoteData(remoteLakeDir);

        // Drop partition
        coordinatorGateway
                .dropPartition(newDropPartitionRequest(tablePath, partitionSpec, false))
                .get();

        // Wait partition is dropped
        FLUSS_CLUSTER_EXTENSION.waitUntilPartitionsDropped(
                tablePath, Collections.singletonList(partitionName));

        // Verify remote partition dirs are deleted.
        retry(
                Duration.ofMinutes(1),
                () -> {
                    for (FsPath dir : remotePartitionDirs) {
                        assertThat(dir.getFileSystem().exists(dir)).isFalse();
                    }
                });

        // Verify remote lake dir still exists (only partition is deleted, not table)
        assertThat(remoteLakeDir.getFileSystem().exists(remoteLakeDir)).isTrue();

        // Verify table still exists
        assertThat(zkClient.tableExist(tablePath)).isTrue();
    }

    private TablePartition createPartition(
            TablePath tablePath,
            long tableId,
            String partitionName,
            String remoteDataDir,
            int serverId)
            throws Exception {
        long partitionId = zkClient.getPartitionIdAndIncrement();
        TablePartition partition = new TablePartition(tableId, partitionId);
        PartitionAssignment assignment =
                new PartitionAssignment(
                        tableId, Collections.singletonMap(0, BucketAssignment.of(serverId)));
        zkClient.registerPartitionAssignmentAndMetadata(
                partitionId, partitionName, assignment, remoteDataDir, tablePath, tableId, 1);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(new TableBucket(tableId, partitionId, 0));
        retryVerifyContext(
                ctx -> {
                    assertThat(ctx.getAllReplicasForPartition(tableId, partitionId)).hasSize(1);
                    assertThat(ctx.areAllReplicasInState(partition, ReplicaState.OnlineReplica))
                            .isTrue();
                });
        return partition;
    }

    private List<FsPath> createPartitionRemoteData(
            TablePath tablePath,
            TablePartition partition,
            String partitionName,
            String remoteDataDir,
            boolean isPkTable)
            throws Exception {
        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath, partitionName);
        List<FsPath> dirs = new ArrayList<>();
        dirs.add(
                FlussPaths.remotePartitionDir(
                        FlussPaths.remoteLogDir(remoteDataDir), physicalTablePath, partition));
        if (isPkTable) {
            dirs.add(
                    FlussPaths.remotePartitionDir(
                            FlussPaths.remoteKvDir(remoteDataDir), physicalTablePath, partition));
        }
        for (FsPath dir : dirs) {
            createRemoteData(dir);
        }
        return dirs;
    }

    private void createRemoteData(FsPath dir) throws Exception {
        dir.getFileSystem().mkdirs(dir);
        try (OutputStream out =
                dir.getFileSystem()
                        .create(new FsPath(dir, "test-data"), FileSystem.WriteMode.NO_OVERWRITE)) {
            out.write(1);
        }
    }

    private void retryVerifyContext(Consumer<CoordinatorContext> verifyFunction) {
        retry(
                Duration.ofSeconds(30),
                () -> {
                    AccessContextEvent<Void> event =
                            new AccessContextEvent<>(
                                    ctx -> {
                                        verifyFunction.accept(ctx);
                                        return null;
                                    });
                    FLUSS_CLUSTER_EXTENSION
                            .getCoordinatorServer()
                            .getCoordinatorEventProcessor()
                            .getCoordinatorEventManager()
                            .put(event);
                    try {
                        event.getResultFuture().get(30, TimeUnit.SECONDS);
                    } catch (Throwable t) {
                        throw ExceptionUtils.stripExecutionException(t);
                    }
                });
    }
}

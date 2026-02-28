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
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.FlussPaths;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
        FsPath remoteTableDataDir = tableOpt.get().remoteDataDir;
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
        // Create partitioned table
        String tableName = isPkTable ? "pk_partitioned_cleaner" : "partitioned_cleaner";
        TablePath tablePath = TablePath.of("test_db", tableName);
        TableDescriptor tableDescriptor =
                isPkTable
                        ? DATA1_PARTITIONED_TABLE_DESCRIPTOR_PK
                        : DATA1_PARTITIONED_TABLE_DESCRIPTOR;

        long tableId =
                RpcMessageTestUtils.createTable(
                        FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);

        // Get remote data dir for table
        Optional<TableRegistration> tableOpt = zkClient.getTable(tablePath);
        assertThat(tableOpt).isPresent();
        FsPath remoteTableDataDir = tableOpt.get().remoteDataDir;
        assertThat(remoteTableDataDir).isNotNull();

        // Wait for auto partitions to be created
        Map<String, Long> partitions =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath);
        assertThat(partitions).isNotEmpty();

        // Get one partition's remote dir for verification
        String firstPartitionName = partitions.keySet().iterator().next();
        Long firstPartitionId = partitions.get(firstPartitionName);
        Optional<PartitionRegistration> partitionOpt =
                zkClient.getPartition(tablePath, firstPartitionName);
        assertThat(partitionOpt).isPresent();
        FsPath partitionRemoteDataDir = partitionOpt.get().getRemoteDataDir();
        assertThat(partitionRemoteDataDir).isNotNull();

        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath, firstPartitionName);
        TablePartition tablePartition = new TablePartition(tableId, firstPartitionId);

        // Mock remote log dirs
        FsPath remoteLogPartitionDir =
                FlussPaths.remotePartitionDir(
                        FlussPaths.remoteLogDir(partitionRemoteDataDir),
                        physicalTablePath,
                        tablePartition);
        remoteLogPartitionDir.getFileSystem().mkdirs(remoteLogPartitionDir);
        assertThat(remoteLogPartitionDir.getFileSystem().exists(remoteLogPartitionDir)).isTrue();

        // Mock remote kv dirs
        FsPath remoteKvPartitionDir;
        if (isPkTable) {
            remoteKvPartitionDir =
                    FlussPaths.remotePartitionDir(
                            FlussPaths.remoteKvDir(partitionRemoteDataDir),
                            physicalTablePath,
                            tablePartition);
            remoteKvPartitionDir.getFileSystem().mkdirs(remoteKvPartitionDir);
            assertThat(remoteKvPartitionDir.getFileSystem().exists(remoteKvPartitionDir)).isTrue();
        } else {
            remoteKvPartitionDir = null;
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

        // Verify remote log partition dir is deleted
        retry(
                Duration.ofMinutes(2),
                () ->
                        assertThat(
                                        remoteLogPartitionDir
                                                .getFileSystem()
                                                .exists(remoteLogPartitionDir))
                                .isFalse());

        // Verify remote kv partition dir is deleted for PK table
        if (isPkTable) {
            retry(
                    Duration.ofMinutes(2),
                    () ->
                            assertThat(
                                            remoteKvPartitionDir
                                                    .getFileSystem()
                                                    .exists(remoteKvPartitionDir))
                                    .isFalse());
        }

        // Verify remote lake dir is deleted
        retry(
                Duration.ofMinutes(2),
                () -> assertThat(remoteLakeDir.getFileSystem().exists(remoteLakeDir)).isFalse());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDeleteSinglePartition(boolean isPkTable) throws Exception {
        // Create partitioned table
        String tableName =
                isPkTable ? "pk_partition_drop_cleaner_table" : "partition_drop_cleaner_table";
        TablePath tablePath = TablePath.of("test_db", tableName);
        TableDescriptor tableDescriptor =
                isPkTable
                        ? DATA1_PARTITIONED_TABLE_DESCRIPTOR_PK
                        : DATA1_PARTITIONED_TABLE_DESCRIPTOR;

        long tableId =
                RpcMessageTestUtils.createTable(
                        FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);

        // Get remote data dir for table
        Optional<TableRegistration> tableOpt = zkClient.getTable(tablePath);
        assertThat(tableOpt).isPresent();
        FsPath remoteTableDataDir = tableOpt.get().remoteDataDir;
        assertThat(remoteTableDataDir).isNotNull();

        // Create a manual partition
        String partitionName = "manual_partition";
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
        FsPath partitionRemoteDataDir = partitionOpt.get().getRemoteDataDir();
        assertThat(partitionRemoteDataDir).isNotNull();

        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath, partitionName);
        TablePartition tablePartition = new TablePartition(tableId, partitionId);

        // Mock remote log dirs
        FsPath remoteLogPartitionDir =
                FlussPaths.remotePartitionDir(
                        FlussPaths.remoteLogDir(partitionRemoteDataDir),
                        physicalTablePath,
                        tablePartition);
        remoteLogPartitionDir.getFileSystem().mkdirs(remoteLogPartitionDir);
        assertThat(remoteLogPartitionDir.getFileSystem().exists(remoteLogPartitionDir)).isTrue();

        // Mock remote kv dirs
        FsPath remoteKvPartitionDir;
        if (isPkTable) {
            remoteKvPartitionDir =
                    FlussPaths.remotePartitionDir(
                            FlussPaths.remoteKvDir(partitionRemoteDataDir),
                            physicalTablePath,
                            tablePartition);
            remoteKvPartitionDir.getFileSystem().mkdirs(remoteKvPartitionDir);
            assertThat(remoteKvPartitionDir.getFileSystem().exists(remoteKvPartitionDir)).isTrue();
        } else {
            remoteKvPartitionDir = null;
        }

        // Mock remote lake dirs
        FsPath remoteLakeDir =
                FlussPaths.remoteLakeTableSnapshotDir(remoteTableDataDir, tablePath, tableId);
        remoteLakeDir.getFileSystem().mkdirs(remoteLakeDir);
        File remoteLakeDirFile = new File(remoteLakeDir.getPath());
        assertThat(remoteLakeDirFile.exists()).isTrue();

        // Drop partition
        coordinatorGateway
                .dropPartition(newDropPartitionRequest(tablePath, partitionSpec, false))
                .get();

        // Wait partition is dropped
        FLUSS_CLUSTER_EXTENSION.waitUntilPartitionsDropped(
                tablePath, Collections.singletonList(partitionName));

        // Verify remote log partition dir is deleted
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(
                                        remoteLogPartitionDir
                                                .getFileSystem()
                                                .exists(remoteLogPartitionDir))
                                .isFalse());

        // Verify remote kv partition dir is deleted for PK table
        if (isPkTable) {
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(
                                            remoteKvPartitionDir
                                                    .getFileSystem()
                                                    .exists(remoteKvPartitionDir))
                                    .isFalse());
        }

        // Verify remote lake dir still exists (only partition is deleted, not table)
        assertThat(remoteLakeDirFile.exists()).isTrue();

        // Verify table still exists
        assertThat(zkClient.tableExist(tablePath)).isTrue();
    }
}

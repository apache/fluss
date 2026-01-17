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
import org.junit.jupiter.api.Test;
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
        File remoteLogTableDirFile = new File(remoteLogTableDir.getPath());

        // Mock remote kv dirs
        File remoteKvTableDirFile = null;
        if (isPkTable) {
            FsPath remoteKvTableDir =
                    FlussPaths.remoteTableDir(
                            FlussPaths.remoteKvDir(remoteTableDataDir), tablePath, tableId);
            remoteKvTableDir.getFileSystem().mkdirs(remoteKvTableDir);
            remoteKvTableDirFile = new File(remoteKvTableDir.getPath());
        }

        // Mock remote lake dirs
        FsPath remoteLakeDir =
                FlussPaths.remoteLakeTableSnapshotDir(
                        remoteTableDataDir.getPath(), tablePath, tableId);
        remoteLakeDir.getFileSystem().mkdirs(remoteLakeDir);
        File remoteLakeDirFile = new File(remoteLakeDir.getPath());

        // Drop table
        coordinatorGateway
                .dropTable(
                        newDropTableRequest(
                                tablePath.getDatabaseName(), tablePath.getTableName(), false))
                .get();
        assertThat(zkClient.tableExist(tablePath)).isFalse();

        // Verify remote log dir is deleted
        retry(Duration.ofMinutes(1), () -> assertThat(remoteLogTableDirFile.exists()).isFalse());

        // Verify remote kv dir is deleted for PK table
        if (isPkTable) {
            File finalRemoteKvTableDirFile = remoteKvTableDirFile;
            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat(finalRemoteKvTableDirFile.exists()).isFalse());
        }

        // Verify remote lake dir is deleted
        // retry(Duration.ofMinutes(1), () -> assertThat(remoteLakeDirFile.exists()).isFalse());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDeletePartitionedTable(boolean isPkTable) throws Exception {
        // Create partitioned table
        String tablePrefix = isPkTable ? "pk_partitioned_cleaner_" : "partitioned_cleaner_";
        TablePath tablePath = TablePath.of("test_db", tablePrefix + "table");
        TableDescriptor tableDescriptor =
                isPkTable
                        ? DATA1_PARTITIONED_TABLE_DESCRIPTOR_PK
                        : DATA1_PARTITIONED_TABLE_DESCRIPTOR;

        long tableId =
                RpcMessageTestUtils.createTable(
                        FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);

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
        File remoteLogPartitionDirFile = new File(remoteLogPartitionDir.getPath());

        // Mock remote kv dirs
        File remoteKvPartitionDirFile = null;
        if (isPkTable) {
            FsPath remoteKvPartitionDir =
                    FlussPaths.remotePartitionDir(
                            FlussPaths.remoteKvDir(partitionRemoteDataDir),
                            physicalTablePath,
                            tablePartition);
            remoteKvPartitionDir.getFileSystem().mkdirs(remoteKvPartitionDir);
            remoteKvPartitionDirFile = new File(remoteKvPartitionDir.getPath());
        }

        // Mock remote lake dirs
        FsPath remoteLakeDir =
                FlussPaths.remoteLakeTableSnapshotDir(
                        FLUSS_CLUSTER_EXTENSION.getRemoteDataDir(), tablePath, tableId);
        remoteLakeDir.getFileSystem().mkdirs(remoteLakeDir);
        File remoteLakeDirFile = new File(remoteLakeDir.getPath());

        // Drop table
        coordinatorGateway
                .dropTable(
                        newDropTableRequest(
                                tablePath.getDatabaseName(), tablePath.getTableName(), false))
                .get();
        assertThat(zkClient.tableExist(tablePath)).isFalse();

        // Verify remote log partition dir is deleted
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(remoteLogPartitionDirFile.exists()).isFalse());

        // Verify remote kv partition dir is deleted for PK table
        if (isPkTable) {
            File finalRemoteKvPartitionDirFile = remoteKvPartitionDirFile;
            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat(finalRemoteKvPartitionDirFile.exists()).isFalse());
        }

        // Verify remote lake dir is deleted
        retry(Duration.ofMinutes(1), () -> assertThat(remoteLakeDirFile.exists()).isFalse());
    }

    @Test
    void testDeleteSinglePartition() throws Exception {
        // Create partitioned table
        TablePath tablePath = TablePath.of("test_db", "partition_drop_cleaner_table");

        long tableId =
                RpcMessageTestUtils.createTable(
                        FLUSS_CLUSTER_EXTENSION, tablePath, DATA1_PARTITIONED_TABLE_DESCRIPTOR);

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
        File remoteLogPartitionDirFile = new File(remoteLogPartitionDir.getPath());

        // Mock remote lake dirs
        FsPath remoteLakeDir =
                FlussPaths.remoteLakeTableSnapshotDir(
                        partitionRemoteDataDir.getPath(), tablePath, tableId);
        remoteLakeDir.getFileSystem().mkdirs(remoteLakeDir);
        File remoteLakeDirFile = new File(remoteLakeDir.getPath());

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
                () -> assertThat(remoteLogPartitionDirFile.exists()).isFalse());

        // Verify remote lake dir still exists (only partition is deleted, not table)
        assertThat(remoteLakeDirFile.exists()).isTrue();

        // Verify table still exists
        assertThat(zkClient.tableExist(tablePath)).isTrue();
    }
}

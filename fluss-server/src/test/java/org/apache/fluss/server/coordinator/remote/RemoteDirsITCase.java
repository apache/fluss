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
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.fluss.record.TestData.DATA1_PARTITIONED_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR_PK;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for multi remote data directories functionality. */
class RemoteDirsITCase {

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

    private static final List<String> REMOTE_DIR_NAMES = Arrays.asList("dir1", "dir2", "dir3");

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .setRemoteDirNames(REMOTE_DIR_NAMES)
                    .build();

    private ZooKeeperClient zkClient;

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        conf.set(ConfigOptions.AUTO_PARTITION_CHECK_INTERVAL, Duration.ofSeconds(1));

        return conf;
    }

    @BeforeEach
    void setup() {
        zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCreateMultipleTablesWithRoundRobin(boolean isPrimaryKeyTable) throws Exception {
        // Create multiple tables and verify they use different remote dirs via round-robin
        TableDescriptor tableDescriptor =
                isPrimaryKeyTable ? DATA1_TABLE_DESCRIPTOR_PK : DATA1_TABLE_DESCRIPTOR;
        String tablePrefix = isPrimaryKeyTable ? "pk_table_" : "non_pk_table_";

        List<FsPath> remoteDirsUsed = new ArrayList<>();
        int tableCount = 6; // Create more tables than dirs to see round-robin in action

        for (int i = 0; i < tableCount; i++) {
            TablePath tablePath = TablePath.of("test_db", tablePrefix + i);
            RpcMessageTestUtils.createTable(FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);

            // Get the table registration to check remoteDataDir
            Optional<TableRegistration> tableOpt = zkClient.getTable(tablePath);
            assertThat(tableOpt).isPresent();
            TableRegistration table = tableOpt.get();

            assertThat(table.remoteDataDir).isNotNull();
            remoteDirsUsed.add(table.remoteDataDir);
        }

        // Verify round-robin distribution: each dir should be used at least once
        Map<String, Integer> dirUsageCount = new HashMap<>();
        for (FsPath dir : remoteDirsUsed) {
            dirUsageCount.merge(dir.toString(), 1, Integer::sum);
        }

        // With round-robin, all configured dirs should be used
        assertThat(dirUsageCount.keySet()).hasSize(REMOTE_DIR_NAMES.size());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCreatePartitionsWithRoundRobin(boolean isPrimaryKeyTable) throws Exception {
        // Create a partitioned table and add multiple partitions
        // Each partition should get a different remoteDataDir via round-robin
        String tablePrefix = isPrimaryKeyTable ? "pk_partitioned_" : "partitioned_";
        TablePath tablePath = TablePath.of("test_db", tablePrefix + "table_2");

        TableDescriptor tableDescriptor =
                isPrimaryKeyTable
                        ? DATA1_PARTITIONED_TABLE_DESCRIPTOR_PK
                        : DATA1_PARTITIONED_TABLE_DESCRIPTOR;
        RpcMessageTestUtils.createTable(FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);

        Optional<TableRegistration> tableOpt = zkClient.getTable(tablePath);
        assertThat(tableOpt).isPresent();
        TableRegistration table = tableOpt.get();
        // Partitioned table should NOT have remoteDataDir set at table level
        assertThat(table.remoteDataDir).isNotNull();

        // Create multiple partitions using partition column "b"
        int partitionCount = 6;
        List<String> partitionNames = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            String partitionName = "p" + i;
            partitionNames.add(partitionName);
            PartitionSpec partitionSpec =
                    new PartitionSpec(Collections.singletonMap("b", partitionName));
            RpcMessageTestUtils.createPartition(
                    FLUSS_CLUSTER_EXTENSION, tablePath, partitionSpec, false);
        }

        // Verify each partition has remoteDataDir set and round-robin is applied
        Set<String> usedRemoteDirs = new HashSet<>();
        for (String partitionName : partitionNames) {
            Optional<PartitionRegistration> partitionOpt =
                    zkClient.getPartition(tablePath, partitionName);
            assertThat(partitionOpt).isPresent();
            PartitionRegistration partition = partitionOpt.get();

            assertThat(partition.getRemoteDataDir()).isNotNull();
            usedRemoteDirs.add(partition.getRemoteDataDir().toString());
        }

        // All configured dirs should be used
        assertThat(usedRemoteDirs).hasSize(REMOTE_DIR_NAMES.size());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testAutoPartitionWithMultipleRemoteDirs(boolean isPrimaryKeyTable) throws Exception {
        // Create an auto-partitioned table and verify partitions use different remote dirs
        String tablePrefix = isPrimaryKeyTable ? "auto_pk_partitioned_" : "auto_partitioned_";
        TablePath tablePath = TablePath.of("test_db", tablePrefix + "table");
        TableDescriptor tableDescriptor =
                isPrimaryKeyTable
                        ? DATA1_PARTITIONED_TABLE_DESCRIPTOR_PK
                        : DATA1_PARTITIONED_TABLE_DESCRIPTOR;

        RpcMessageTestUtils.createTable(FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);

        // Wait for auto partitions to be created
        Map<String, Long> partitions =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath);
        assertThat(partitions).isNotEmpty();

        // Verify partitions use remote dirs
        Set<String> usedRemoteDirs = new HashSet<>();
        for (String partitionName : partitions.keySet()) {
            Optional<PartitionRegistration> partitionOpt =
                    zkClient.getPartition(tablePath, partitionName);
            assertThat(partitionOpt).isPresent();
            PartitionRegistration partition = partitionOpt.get();

            assertThat(partition.getRemoteDataDir()).isNotNull();
            usedRemoteDirs.add(partition.getRemoteDataDir().toString());
        }

        // At least one remote dir should be used
        assertThat(usedRemoteDirs).isNotEmpty();
    }

    @Test
    void testMixedTableAndPartitionCreation() throws Exception {
        // Create a mix of non-partitioned tables and partitioned table partitions
        // to verify round-robin works correctly across both types

        // Create 2 non-partitioned tables
        for (int i = 0; i < 2; i++) {
            TablePath tablePath = TablePath.of("test_db", "mixed_non_pk_" + i);
            RpcMessageTestUtils.createTable(
                    FLUSS_CLUSTER_EXTENSION, tablePath, DATA1_TABLE_DESCRIPTOR_PK);
        }

        // Create a partitioned table
        TablePath partitionedTablePath = TablePath.of("test_db", "mixed_partitioned");
        RpcMessageTestUtils.createTable(
                FLUSS_CLUSTER_EXTENSION, partitionedTablePath, DATA1_PARTITIONED_TABLE_DESCRIPTOR);

        // Create partitions using partition column "b"
        RpcMessageTestUtils.createPartition(
                FLUSS_CLUSTER_EXTENSION,
                partitionedTablePath,
                new PartitionSpec(Collections.singletonMap("b", "p1")),
                false);
        RpcMessageTestUtils.createPartition(
                FLUSS_CLUSTER_EXTENSION,
                partitionedTablePath,
                new PartitionSpec(Collections.singletonMap("b", "p2")),
                false);

        // Create 2 more non-partitioned tables
        for (int i = 2; i < 4; i++) {
            TablePath tablePath = TablePath.of("test_db", "mixed_non_pk_" + i);
            RpcMessageTestUtils.createTable(
                    FLUSS_CLUSTER_EXTENSION, tablePath, DATA1_TABLE_DESCRIPTOR_PK);
        }

        // Collect all remote dirs used
        Set<String> allUsedDirs = new HashSet<>();

        // Check non-partitioned tables
        for (int i = 0; i < 4; i++) {
            TablePath tablePath = TablePath.of("test_db", "mixed_non_pk_" + i);
            Optional<TableRegistration> tableOpt = zkClient.getTable(tablePath);
            assertThat(tableOpt).isPresent();
            assertThat(tableOpt.get().remoteDataDir).isNotNull();
            allUsedDirs.add(tableOpt.get().remoteDataDir.toString());
        }

        // Check partitions
        for (String p : Arrays.asList("p1", "p2")) {
            Optional<PartitionRegistration> partitionOpt =
                    zkClient.getPartition(partitionedTablePath, p);
            assertThat(partitionOpt).isPresent();
            assertThat(partitionOpt.get().getRemoteDataDir()).isNotNull();
            allUsedDirs.add(partitionOpt.get().getRemoteDataDir().toString());
        }

        // All remote dirs should have been used (6 items, 3 dirs, round-robin)
        assertThat(allUsedDirs).hasSize(REMOTE_DIR_NAMES.size());
    }
}

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

package org.apache.fluss.server.kv.snapshot;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createPartition;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * ITCase for verifying KV snapshot can be uploaded to multiple remote directories with round-robin
 * distribution.
 */
public class KvSnapshotMultipleDirsITCase {

    private static final List<String> REMOTE_DIR_NAMES = Arrays.asList("dir1", "dir2", "dir3");

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .setRemoteDirNames(REMOTE_DIR_NAMES)
                    .build();

    private ZooKeeperClient zkClient;

    @BeforeEach
    void setup() {
        zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
    }

    @Test
    void testKvSnapshotToMultipleDirsForNonPartitionedTable() throws Exception {
        // Create multiple tables (more than number of remote dirs) to ensure round-robin
        // distribution. Each table's KV snapshot should be uploaded to different remote dirs.
        int tableCount = 6;
        List<TablePath> tablePaths = new ArrayList<>();
        List<Long> tableIds = new ArrayList<>();

        // Create tables with primary key (KV tables)
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA_PK)
                        .distributedBy(1) // Single bucket for simpler verification
                        .build();

        for (int i = 0; i < tableCount; i++) {
            TablePath tablePath = TablePath.of("test_db", String.format("kv_snapshot_table_%d", i));
            tablePaths.add(tablePath);
            long tableId =
                    RpcMessageTestUtils.createTable(
                            FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);
            tableIds.add(tableId);
        }

        // Write data to each table to trigger KV snapshot
        for (int t = 0; t < tableCount; t++) {
            TableBucket tb = new TableBucket(tableIds.get(t), 0);
            FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);

            int leaderId = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
            TabletServerGateway leaderGateway =
                    FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leaderId);

            // Write KV data to trigger snapshot
            KvRecordBatch kvRecordBatch =
                    genKvRecordBatch(
                            Tuple2.of("k1", new Object[] {1, "v1"}),
                            Tuple2.of("k2", new Object[] {2, "v2"}));

            PutKvRequest putKvRequest = newPutKvRequest(tableIds.get(t), 0, 1, kvRecordBatch);
            leaderGateway.putKv(putKvRequest).get();
        }

        // Wait for all tables' snapshots to be completed
        for (int t = 0; t < tableCount; t++) {
            TableBucket tb = new TableBucket(tableIds.get(t), 0);
            // Wait for snapshot 0 to be finished
            FLUSS_CLUSTER_EXTENSION.waitUntilSnapshotFinished(tb, 0);
        }

        // Collect the remote data directories used by each table
        Set<String> usedRemoteDataDirs = new HashSet<>();
        for (int t = 0; t < tableCount; t++) {
            Optional<TableRegistration> tableOpt = zkClient.getTable(tablePaths.get(t));
            assertThat(tableOpt).isPresent();
            TableRegistration table = tableOpt.get();

            assertThat(table.remoteDataDir).isNotNull();
            usedRemoteDataDirs.add(table.remoteDataDir.toString());

            // Verify the remote KV snapshot files actually exist
            FsPath remoteKvDir = FlussPaths.remoteKvDir(table.remoteDataDir);
            TableBucket tb = new TableBucket(tableIds.get(t), 0);
            FsPath remoteKvTabletDir =
                    FlussPaths.remoteKvTabletDir(
                            remoteKvDir, PhysicalTablePath.of(tablePaths.get(t)), tb);
            assertThat(remoteKvTabletDir.getFileSystem().exists(remoteKvTabletDir)).isTrue();
            FileStatus[] fileStatuses =
                    remoteKvTabletDir.getFileSystem().listStatus(remoteKvTabletDir);
            assertThat(fileStatuses).isNotEmpty();
        }

        // All configured remote dirs should be used due to round-robin distribution
        assertThat(usedRemoteDataDirs).hasSameSizeAs(REMOTE_DIR_NAMES);
    }

    @Test
    void testKvSnapshotToMultipleDirsForPartitionedTable() throws Exception {
        // Create a partitioned table and add multiple partitions (more than number of remote dirs)
        // to ensure round-robin distribution. Each partition's KV snapshot should be uploaded to
        // different remote dirs.
        int partitionCount = 6;
        TablePath tablePath = TablePath.of("test_db", "partitioned_kv_snapshot_table");

        // Create partitioned table with primary key
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .primaryKey("a", "b")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1) // Single bucket for simpler verification
                        .partitionedBy("b")
                        .build();

        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);

        // Create partitions
        List<String> partitionNames = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            String partitionName = "p" + i;
            partitionNames.add(partitionName);
            PartitionSpec partitionSpec =
                    new PartitionSpec(Collections.singletonMap("b", partitionName));
            createPartition(FLUSS_CLUSTER_EXTENSION, tablePath, partitionSpec, false);
        }

        // Get partition IDs from ZK
        Map<String, Long> partitionNameToId = zkClient.getPartitionNameAndIds(tablePath);
        assertThat(partitionNameToId).hasSize(partitionCount);

        // Wait for all partitions to be ready and write data to trigger KV snapshot
        for (int p = 0; p < partitionCount; p++) {
            String partitionName = partitionNames.get(p);
            Long partitionId = partitionNameToId.get(partitionName);
            TableBucket tb = new TableBucket(tableId, partitionId, 0);
            FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);

            int leaderId = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
            TabletServerGateway leaderGateway =
                    FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leaderId);

            // Write KV data to trigger snapshot
            KvRecordBatch kvRecordBatch =
                    genKvRecordBatch(
                            Tuple2.of("k1", new Object[] {1, "v1"}),
                            Tuple2.of("k2", new Object[] {2, "v2"}));

            PutKvRequest putKvRequest = newPutKvRequest(tableId, partitionId, 0, 1, kvRecordBatch);
            leaderGateway.putKv(putKvRequest).get();
        }

        // Wait for all partitions' snapshots to be completed
        for (int p = 0; p < partitionCount; p++) {
            Long partitionId = partitionNameToId.get(partitionNames.get(p));
            TableBucket tb = new TableBucket(tableId, partitionId, 0);
            // Wait for snapshot 0 to be finished
            FLUSS_CLUSTER_EXTENSION.waitUntilSnapshotFinished(tb, 0);
        }

        // Collect the remote data directories used by each partition
        Set<String> usedRemoteDataDirs = new HashSet<>();
        for (int p = 0; p < partitionCount; p++) {
            String partitionName = partitionNames.get(p);
            Long partitionId = partitionNameToId.get(partitionName);
            Optional<PartitionRegistration> partitionOpt =
                    zkClient.getPartition(tablePath, partitionName);
            assertThat(partitionOpt).isPresent();
            PartitionRegistration partition = partitionOpt.get();

            assertThat(partition.getRemoteDataDir()).isNotNull();
            usedRemoteDataDirs.add(partition.getRemoteDataDir().toString());

            // Verify the remote KV snapshot files actually exist
            FsPath remoteKvDir = FlussPaths.remoteKvDir(partition.getRemoteDataDir());
            TableBucket tb = new TableBucket(tableId, partitionId, 0);
            FsPath remoteKvTabletDir =
                    FlussPaths.remoteKvTabletDir(
                            remoteKvDir, PhysicalTablePath.of(tablePath, partitionName), tb);
            assertThat(remoteKvTabletDir.getFileSystem().exists(remoteKvTabletDir)).isTrue();
            FileStatus[] fileStatuses =
                    remoteKvTabletDir.getFileSystem().listStatus(remoteKvTabletDir);
            assertThat(fileStatuses).isNotEmpty();
        }

        // All configured remote dirs should be used due to round-robin distribution
        assertThat(usedRemoteDataDirs).hasSameSizeAs(REMOTE_DIR_NAMES);
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_BUCKET_NUMBER, 1);
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // Set a shorter interval for testing purpose
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1));
        return conf;
    }
}

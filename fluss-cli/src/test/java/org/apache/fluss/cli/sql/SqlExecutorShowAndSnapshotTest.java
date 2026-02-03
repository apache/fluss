/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.cli.sql;

import org.apache.fluss.cli.config.ConnectionConfig;
import org.apache.fluss.cli.config.ConnectionManager;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.metadata.KvSnapshotMetadata;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.exception.KvSnapshotNotExistException;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SqlExecutorShowAndSnapshotTest {

    @Test
    void testShowTableExists() throws Exception {
        Admin admin = mock(Admin.class);
        when(admin.tableExists(TablePath.of("db1", "tbl")))
                .thenReturn(CompletableFuture.completedFuture(true));

        String output = executeSql(admin, "SHOW TABLE EXISTS db1.tbl");

        assertThat(output).contains("true");
    }

    @Test
    void testShowCreateTable() throws Exception {
        Admin admin = mock(Admin.class);
        TablePath tablePath = TablePath.of("db1", "tbl");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3).build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, 0L, 0L);
        when(admin.getTableInfo(tablePath))
                .thenReturn(CompletableFuture.completedFuture(tableInfo));

        String output = executeSql(admin, "SHOW CREATE TABLE db1.tbl");

        assertThat(output).contains("CREATE TABLE db1.tbl");
        assertThat(output).contains("PRIMARY KEY");
    }

    @Test
    void testShowPartitions() throws Exception {
        Admin admin = mock(Admin.class);
        TablePath tablePath = TablePath.of("db1", "tbl");
        PartitionInfo partitionInfo =
                new PartitionInfo(1L, ResolvedPartitionSpec.fromPartitionValue("dt", "2024-01-01"));
        when(admin.listPartitionInfos(tablePath))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                Collections.singletonList(partitionInfo)));

        String output = executeSql(admin, "SHOW PARTITIONS FROM db1.tbl");

        assertThat(output).contains("Partitions in table 'db1.tbl'");
        assertThat(output).contains("2024-01-01");
    }

    @Test
    void testShowOffsets() throws Exception {
        Admin admin = mock(Admin.class);
        ListOffsetsResult listOffsetsResult = mock(ListOffsetsResult.class);
        when(admin.listOffsets(
                        eq(TablePath.of("db1", "tbl")), any(List.class), any(OffsetSpec.class)))
                .thenReturn(listOffsetsResult);
        Map<Integer, Long> offsets = new HashMap<>();
        offsets.put(0, 100L);
        when(listOffsetsResult.all()).thenReturn(CompletableFuture.completedFuture(offsets));

        String output = executeSql(admin, "SHOW OFFSETS FROM db1.tbl BUCKETS (0) AT LATEST");

        assertThat(output).contains("Offsets for db1.tbl");
        assertThat(output).contains("0\t100");
    }

    @Test
    void testShowKvSnapshots() throws Exception {
        Admin admin = mock(Admin.class);
        KvSnapshots snapshots = mock(KvSnapshots.class);
        when(admin.getLatestKvSnapshots(TablePath.of("db1", "tbl")))
                .thenReturn(CompletableFuture.completedFuture(snapshots));
        when(snapshots.getBucketIds()).thenReturn(Collections.singleton(0));
        when(snapshots.getSnapshotId(0)).thenReturn(OptionalLong.of(10L));
        when(snapshots.getLogOffset(0)).thenReturn(OptionalLong.of(20L));

        String output = executeSql(admin, "SHOW KV SNAPSHOTS FROM db1.tbl");

        assertThat(output).contains("KV Snapshots for db1.tbl");
        assertThat(output).contains("0\t10\t20");
    }

    @Test
    void testShowKvSnapshotMetadataNotFound() throws Exception {
        Admin admin = mock(Admin.class);
        TablePath tablePath = TablePath.of("db1", "tbl");
        Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).primaryKey("id").build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3).build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, 0L, 0L);
        when(admin.getTableInfo(tablePath))
                .thenReturn(CompletableFuture.completedFuture(tableInfo));
        CompletableFuture<KvSnapshotMetadata> failed = new CompletableFuture<>();
        failed.completeExceptionally(new KvSnapshotNotExistException("missing"));
        when(admin.getKvSnapshotMetadata(eq(new TableBucket(1L, 0)), eq(10L))).thenReturn(failed);

        String output =
                executeSql(admin, "SHOW KV SNAPSHOT METADATA FROM db1.tbl BUCKET 0 SNAPSHOT 10");

        assertThat(output).contains("snapshot id not found");
    }

    @Test
    void testShowLakeSnapshotNone() throws Exception {
        Admin admin = mock(Admin.class);
        CompletableFuture<LakeSnapshot> failed = new CompletableFuture<>();
        failed.completeExceptionally(new LakeTableSnapshotNotExistException("missing"));
        when(admin.getLatestLakeSnapshot(TablePath.of("db1", "tbl"))).thenReturn(failed);

        String output = executeSql(admin, "SHOW LAKE SNAPSHOT FROM db1.tbl");

        assertThat(output).contains("no lake snapshot has been committed yet");
    }

    @Test
    void testShowKvSnapshotMetadata() throws Exception {
        Admin admin = mock(Admin.class);
        TablePath tablePath = TablePath.of("db1", "tbl");
        Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).primaryKey("id").build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3).build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, 0L, 0L);
        when(admin.getTableInfo(tablePath))
                .thenReturn(CompletableFuture.completedFuture(tableInfo));

        KvSnapshotMetadata metadata = mock(KvSnapshotMetadata.class);
        when(metadata.toString()).thenReturn("mock-metadata");
        when(admin.getKvSnapshotMetadata(eq(new TableBucket(1L, 0)), eq(10L)))
                .thenReturn(CompletableFuture.completedFuture(metadata));

        String output =
                executeSql(admin, "SHOW KV SNAPSHOT METADATA FROM db1.tbl BUCKET 0 SNAPSHOT 10");

        assertThat(output).contains("mock-metadata");
    }

    @Test
    void testShowLakeSnapshot() throws Exception {
        Admin admin = mock(Admin.class);
        LakeSnapshot snapshot = mock(LakeSnapshot.class);
        when(snapshot.getSnapshotId()).thenReturn(3L);
        Map<TableBucket, Long> bucketOffsets = new HashMap<>();
        bucketOffsets.put(new TableBucket(1L, 0), 100L);
        when(snapshot.getTableBucketsOffset()).thenReturn(bucketOffsets);
        when(admin.getLatestLakeSnapshot(TablePath.of("db1", "tbl")))
                .thenReturn(CompletableFuture.completedFuture(snapshot));

        String output = executeSql(admin, "SHOW LAKE SNAPSHOT FROM db1.tbl");

        assertThat(output).contains("Lake Snapshot for db1.tbl: 3");
        assertThat(output).contains("bucket=0} -> 100");
    }

    @Test
    void testUseDatabaseNotExist() throws Exception {
        Admin admin = mock(Admin.class);
        when(admin.databaseExists("nonexistent"))
                .thenReturn(CompletableFuture.completedFuture(false));

        assertThatThrownBy(() -> executeSql(admin, "USE nonexistent"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Database does not exist: nonexistent");
    }

    @Test
    void testShowTablesWithoutDatabase() throws Exception {
        Admin admin = mock(Admin.class);
        Connection connection = mock(Connection.class);
        when(connection.getAdmin()).thenReturn(admin);

        SqlExecutor executor =
                new SqlExecutor(
                        new StubConnectionManager(connection), new PrintWriter(new StringWriter()));

        assertThatThrownBy(() -> executor.executeSql("SHOW TABLES"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SHOW TABLES requires database specification");
    }

    private static String executeSql(Admin admin, String sql) throws Exception {
        Connection connection = mock(Connection.class);
        when(connection.getAdmin()).thenReturn(admin);
        ConnectionManager connectionManager = new StubConnectionManager(connection);
        StringWriter writer = new StringWriter();
        SqlExecutor executor = new SqlExecutor(connectionManager, new PrintWriter(writer));
        executor.executeSql(sql);
        return writer.toString();
    }

    private static class StubConnectionManager extends ConnectionManager {
        private final Connection connection;

        StubConnectionManager(Connection connection) {
            super(new ConnectionConfig(new org.apache.fluss.config.Configuration()));
            this.connection = connection;
        }

        @Override
        public Connection getConnection() {
            return connection;
        }
    }
}

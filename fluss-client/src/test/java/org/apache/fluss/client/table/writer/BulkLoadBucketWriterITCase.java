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

package org.apache.fluss.client.table.writer;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.metadata.BulkLoadBuildContext;
import org.apache.fluss.client.metadata.BulkLoadHandle;
import org.apache.fluss.client.metadata.BulkLoadInfo;
import org.apache.fluss.client.table.BucketCalculator;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.BulkLoadIncompleteException;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryRow.BinaryRowFormat;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.serializer.RowSerializer;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.FlussPaths;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises Java BulkLoad through a real coordinator and tablet server. */
class BulkLoadBucketWriterITCase {

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(
                            new Configuration()
                                    .set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1)))
                    .build();

    @Test
    void testPartitionBucketCountsAndSnapshotRecovery(@TempDir Path tempDir) throws Exception {
        TablePath path = TablePath.of(BUILTIN_DATABASE, "bulkload_partitioned");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("p", DataTypes.STRING())
                        .column("payload", DataTypes.STRING())
                        .primaryKey("id", "p")
                        .build();
        List<String> partitions = Arrays.asList("old", "new");
        String loadId = UUID.randomUUID().toString();
        int key;
        try (Connection conn = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = conn.getAdmin()) {
            await(
                    admin.createTable(
                            path,
                            TableDescriptor.builder()
                                    .schema(schema)
                                    .partitionedBy("p")
                                    .kvFormat(KvFormat.INDEXED)
                                    .distributedBy(2, "id")
                                    .build(),
                            false));
            await(admin.createPartition(path, partitionSpec("old"), false));
            await(
                    admin.alterTable(
                            path,
                            Collections.singletonList(TableChange.modifyBucketCount(4)),
                            false));
            await(admin.createPartition(path, partitionSpec("new"), false));
            CLUSTER.waitUntilTableReady(path);
            try (Table table = conn.getTable(path)) {
                BulkLoad bulkLoad = table.newBulkLoad();
                BulkLoadBuildContext context = await(bulkLoad.begin(loadId, partitions));
                TableInfo tableInfo = table.getTableInfo();
                BucketCalculator calculator = BucketCalculator.forBucketKeyRow(tableInfo);
                key =
                        IntStream.range(0, 100)
                                .filter(
                                        id -> {
                                            byte[] encoded = calculator.encodeKey(row(id));
                                            return calculator.getBucketId(encoded, 4) >= 2;
                                        })
                                .findFirst()
                                .getAsInt();
                byte[] encoded = calculator.encodeKey(row(key));
                int oldBucket = calculator.getBucketId(encoded, 2);
                int newBucket = calculator.getBucketId(encoded, 4);
                RowSerializer serializer =
                        new RowSerializer(
                                schema.getRowType().getChildren().toArray(new DataType[0]),
                                BinaryRowFormat.INDEXED);
                InternalRow oldRow = serializer.toBinaryRow(row(key, "old", "previous layout"));
                InternalRow newRow = row(key, "new", "current layout");

                PhysicalTablePath oldPath = PhysicalTablePath.of(path, "old");
                BulkLoadBuildContext.Target old = context.getTarget(oldPath);
                BulkLoadBuildContext.Target current =
                        context.getTarget(PhysicalTablePath.of(path, "new"));
                assertThat(old.getBucketCount()).isEqualTo(2);
                assertThat(current.getBucketCount()).isEqualTo(4);
                assertThat(old.bucketOf(oldRow)).isEqualTo(oldBucket);
                assertThat(current.bucketOf(newRow)).isEqualTo(newBucket);
                Path workDir = Files.createDirectory(tempDir.resolve("work"));
                Path linkedWorkDir =
                        Files.createSymbolicLink(tempDir.resolve("linked-work"), workDir);
                try (Connection worker =
                                ConnectionFactory.createConnection(
                                        new Configuration(CLUSTER.getClientConfig())
                                                .set(
                                                        ConfigOptions.CLIENT_IO_TMP_DIR,
                                                        linkedWorkDir.toString())
                                                .set(
                                                        ConfigOptions.KV_COMPRESSION_PER_LEVEL,
                                                        Collections.singletonList(
                                                                ConfigOptions.KvCompressionType
                                                                        .NO)));
                        Table workerTable = worker.getTable(path)) {
                    BulkLoad workerBulkLoad = workerTable.newBulkLoad();
                    loadTarget(workerBulkLoad, old, oldBucket, oldRow);
                    loadTarget(workerBulkLoad, current, newBucket, newRow);
                }
                FsPath tabletDir =
                        FlussPaths.remoteKvTabletDir(
                                FlussPaths.remoteKvDir(old.getRemoteDataDir()),
                                old.getPath(),
                                new TableBucket(
                                        context.getHandle().getTableId(),
                                        old.getPartitionId(),
                                        oldBucket));
                try (Stream<Path> files = Files.walk(Paths.get(tabletDir.toUri()))) {
                    List<Path> optionsFiles =
                            files.filter(
                                            file ->
                                                    file.getFileName()
                                                            .toString()
                                                            .contains("-OPTIONS-"))
                                    .collect(Collectors.toList());
                    assertThat(optionsFiles).isNotEmpty();
                    for (Path optionsFile : optionsFiles) {
                        assertThat(new String(Files.readAllBytes(optionsFile), UTF_8))
                                .contains("compression_per_level=kNoCompression");
                    }
                }
                await(bulkLoad.commit(context.getHandle()));
                await(bulkLoad.commit(context.getHandle()));
                assertThat(await(bulkLoad.getInfo(context.getHandle())).getState())
                        .isEqualTo(BulkLoadInfo.State.COMMITTED);
                assertThat(bulkLoad.tryOpenBucketWriter(old, oldBucket)).isEmpty();

                CLUSTER.stopTabletServer(0);
                CLUSTER.startTabletServer(0);
                for (BulkLoadBuildContext.Target target : context.getTargets()) {
                    for (int bucket = 0; bucket < target.getBucketCount(); bucket++) {
                        CLUSTER.waitUntilAllReplicaReady(
                                new TableBucket(
                                        tableInfo.getTableId(), target.getPartitionId(), bucket));
                    }
                }
            }
        }
        try (Connection conn = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Table table = conn.getTable(path)) {
            Lookuper lookuper = table.newLookup().createLookuper();
            assertThatRow(await(lookuper.lookup(row(key, "old"))).getSingletonRow())
                    .withSchema(schema.getRowType())
                    .isEqualTo(row(key, "old", "previous layout"));
            assertThatRow(await(lookuper.lookup(row(key, "new"))).getSingletonRow())
                    .withSchema(schema.getRowType())
                    .isEqualTo(row(key, "new", "current layout"));
        }
    }

    @Test
    void testAbortPreservesPublishedBucket(@TempDir Path tempDir) throws Exception {
        TablePath path = TablePath.of(BUILTIN_DATABASE, "bulkload_partial");
        TablePath otherPath = TablePath.of(BUILTIN_DATABASE, "bulkload_other");
        InternalRow imported = row(7, "published before abort");
        Path workDir = tempDir.resolve("client");
        Path deprecatedWorkDir = tempDir.resolve("scanner");
        Path siblingDir = workDir.resolve("another-operation");
        Configuration conf =
                new Configuration(CLUSTER.getClientConfig())
                        .set(ConfigOptions.CLIENT_IO_TMP_DIR, workDir.toString())
                        .set(ConfigOptions.CLIENT_SCANNER_IO_TMP_DIR, deprecatedWorkDir.toString());
        try (Connection conn = ConnectionFactory.createConnection(conf);
                Admin admin = conn.getAdmin()) {
            await(admin.createTable(path, descriptor(2), false));
            await(admin.createTable(otherPath, descriptor(1), false));
            CLUSTER.waitUntilTableReady(path);
            CLUSTER.waitUntilTableReady(otherPath);
            try (Table table = conn.getTable(path);
                    Table otherTable = conn.getTable(otherPath)) {
                BulkLoad bulkLoad = table.newBulkLoad();
                BulkLoadBuildContext context =
                        await(
                                bulkLoad.begin(
                                        UUID.randomUUID().toString(), Collections.emptyList()));
                BulkLoadHandle handle = context.getHandle();
                BulkLoadBuildContext.Target target = context.getTarget(PhysicalTablePath.of(path));
                int bucket = target.bucketOf(imported);
                try (BulkLoadBucketWriter writer =
                        bulkLoad.tryOpenBucketWriter(target, bucket).get()) {
                    try (Stream<Path> entries = Files.list(workDir)) {
                        assertThat(entries)
                                .singleElement()
                                .satisfies(dir -> assertThat(dir).isDirectory());
                    }
                    Files.createDirectory(siblingDir);
                    writer.write(row(7, "superseded payload"));
                    writer.write(imported);
                    writer.commit();
                }
                try (Stream<Path> entries = Files.list(workDir)) {
                    assertThat(entries).containsExactly(siblingDir);
                }
                assertThat(deprecatedWorkDir).doesNotExist();
                BulkLoad otherLoad = otherTable.newBulkLoad();
                assertThatThrownBy(() -> otherLoad.tryOpenBucketWriter(target, bucket))
                        .isInstanceOf(IllegalArgumentException.class);
                assertThatThrownBy(() -> otherLoad.getInfo(handle))
                        .isInstanceOf(IllegalArgumentException.class);
                assertThatThrownBy(() -> otherLoad.commit(handle))
                        .isInstanceOf(IllegalArgumentException.class);
                assertThatThrownBy(() -> otherLoad.abort(handle, null))
                        .isInstanceOf(IllegalArgumentException.class);

                assertThatThrownBy(() -> await(bulkLoad.commit(handle)))
                        .hasRootCauseInstanceOf(BulkLoadIncompleteException.class);
                assertThat(await(bulkLoad.listInProgress(Collections.emptyList())))
                        .containsExactly(handle);
                await(bulkLoad.abort(handle, "cancelled"));
                BulkLoadInfo info = await(bulkLoad.getInfo(handle));
                assertThat(info.getState()).isEqualTo(BulkLoadInfo.State.ABORTED);
                assertThat(info.getAbortMessage()).isEqualTo("cancelled");
                assertThat(info.getTargets())
                        .singleElement()
                        .satisfies(
                                progress ->
                                        assertThat(progress.getCommittedBucketCount())
                                                .isEqualTo(1L));
                assertThat(await(bulkLoad.listInProgress(Collections.emptyList()))).isEmpty();
                restartTabletAndWait(new TableBucket(handle.getTableId(), bucket));
            }
        }
        assertImportedRow(path, 7, imported);
    }

    @Test
    void testResumeAfterCoordinatorUnavailableDuringBucketCommit() throws Exception {
        TablePath path = TablePath.of(BUILTIN_DATABASE, "bulkload_resume");
        String loadId = UUID.randomUUID().toString();
        InternalRow imported = row(11, "uploaded by the first worker");
        try (Connection conn = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = conn.getAdmin()) {
            await(admin.createTable(path, descriptor(1), false));
            CLUSTER.waitUntilTableReady(path);
            try (Table table = conn.getTable(path)) {
                BulkLoad bulkLoad = table.newBulkLoad();
                BulkLoadBuildContext context =
                        await(bulkLoad.begin(loadId, Collections.emptyList()));
                try (BulkLoadBucketWriter writer =
                        bulkLoad.tryOpenBucketWriter(
                                        context.getTarget(PhysicalTablePath.of(path)), 0)
                                .get()) {
                    writer.write(imported);
                    CLUSTER.stopCoordinatorServer();
                    try {
                        for (int attempt = 0; attempt < 2; attempt++) {
                            assertThatThrownBy(writer::commit)
                                    .isInstanceOf(IOException.class)
                                    .hasMessageContaining("Failed to commit the BulkLoad snapshot")
                                    .hasCauseInstanceOf(NetworkException.class);
                        }
                    } finally {
                        CLUSTER.startCoordinatorServer();
                    }
                }
            }
        }
        try (Connection conn = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Table table = conn.getTable(path)) {
            BulkLoad bulkLoad = table.newBulkLoad();
            BulkLoadBuildContext context = await(bulkLoad.begin(loadId, Collections.emptyList()));
            assertThat(await(bulkLoad.getInfo(context.getHandle())).getTargets())
                    .singleElement()
                    .satisfies(
                            progress -> {
                                assertThat(progress.getAllocatedBucketCount()).isEqualTo(1L);
                                assertThat(progress.getCommittedBucketCount()).isZero();
                            });
            assertThat(
                            bulkLoad.tryOpenBucketWriter(
                                    context.getTarget(PhysicalTablePath.of(path)), 0))
                    .isEmpty();
            await(bulkLoad.commit(context.getHandle()));
            restartTabletAndWait(new TableBucket(context.getHandle().getTableId(), 0));
        }
        assertImportedRow(path, 11, imported);
    }

    @Test
    void testTaggedTtlTableCannotPublishSnapshot() throws Exception {
        TablePath path = TablePath.of(BUILTIN_DATABASE, "bulkload_tagged");
        String loadId = UUID.randomUUID().toString();
        try (Connection conn = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = conn.getAdmin()) {
            await(
                    admin.createTable(
                            path,
                            descriptor(1)
                                    .withProperties(
                                            Collections.singletonMap(
                                                    ConfigOptions.TABLE_KV_TTL.key(), "1 h")),
                            false));
            CLUSTER.waitUntilTableReady(path);
            try (Table table = conn.getTable(path)) {
                BulkLoad bulkLoad = table.newBulkLoad();
                BulkLoadBuildContext context =
                        await(bulkLoad.begin(loadId, Collections.emptyList()));
                BulkLoadBuildContext.Target target = context.getTarget(PhysicalTablePath.of(path));
                assertThatThrownBy(() -> bulkLoad.tryOpenBucketWriter(target, 0))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("tagged KV values or row TTL");
                assertThat(await(bulkLoad.getInfo(context.getHandle())).getTargets())
                        .singleElement()
                        .satisfies(
                                progress ->
                                        assertThat(progress.getCommittedBucketCount()).isZero());

                ZooKeeperClient zk = CLUSTER.getZooKeeperClient();
                long tableId = context.getHandle().getTableId();
                TableBucket bucket = new TableBucket(tableId, 0);
                BulkLoadTransaction transaction =
                        zk.getBulkLoadTransaction(tableId, loadId).get().data();
                long snapshotId = zk.getBulkLoadBucket(transaction, bucket).get().snapshotId;
                FsPath tabletDir =
                        FlussPaths.remoteKvTabletDir(
                                FlussPaths.remoteKvDir(target.getRemoteDataDir()),
                                target.getPath(),
                                bucket);
                FsPath metadataPath =
                        CompletedSnapshot.getMetadataFilePath(
                                FlussPaths.remoteKvSnapshotDir(tabletDir, snapshotId));
                assertThat(metadataPath.getFileSystem().exists(metadataPath)).isFalse();
            }
        }
    }

    private static void loadTarget(
            BulkLoad bulkLoad, BulkLoadBuildContext.Target target, int dataBucket, InternalRow row)
            throws Exception {
        for (int bucket = 0; bucket < target.getBucketCount(); bucket++) {
            try (BulkLoadBucketWriter writer = bulkLoad.tryOpenBucketWriter(target, bucket).get()) {
                if (bucket == dataBucket) {
                    writer.write(row);
                }
                writer.commit();
            }
        }
    }

    private static <T> T await(CompletableFuture<T> result) throws Exception {
        return result.get(30, TimeUnit.SECONDS);
    }

    private static TableDescriptor descriptor(int bucketCount) {
        return TableDescriptor.builder()
                .schema(DATA1_SCHEMA_PK)
                .distributedBy(bucketCount, "a")
                .build();
    }

    private static PartitionSpec partitionSpec(String value) {
        return new PartitionSpec(Collections.singletonMap("p", value));
    }

    private static void restartTabletAndWait(TableBucket bucket) throws Exception {
        CLUSTER.stopTabletServer(0);
        CLUSTER.startTabletServer(0);
        CLUSTER.waitUntilAllReplicaReady(bucket);
    }

    private static void assertImportedRow(TablePath path, int key, InternalRow expected)
            throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Table table = conn.getTable(path)) {
            Lookuper lookuper = table.newLookup().createLookuper();
            assertThatRow(await(lookuper.lookup(row(key))).getSingletonRow())
                    .withSchema(DATA1_SCHEMA_PK.getRowType())
                    .isEqualTo(expected);
        }
    }
}

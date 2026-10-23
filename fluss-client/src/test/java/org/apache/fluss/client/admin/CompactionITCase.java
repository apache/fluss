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

package org.apache.fluss.client.admin;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.cluster.compaction.CompactionProgress;
import org.apache.fluss.cluster.compaction.CompactionStatus;
import org.apache.fluss.cluster.compaction.CompactionType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.KvTestUtils;
import org.apache.fluss.server.utils.ResourceGuard;
import org.apache.fluss.types.DataTypes;

import io.github.fluss_contrib.rocksdb.RocksIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.server.kv.KvTabletTestUtils.flushAndWait;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end tests for manual compaction and its committed snapshots. */
class CompactionITCase {

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("value", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    @RegisterExtension
    public static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(clusterConfig())
                    .build();

    private static Configuration clusterConfig() {
        Configuration config = new Configuration();
        config.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 1);
        config.set(ConfigOptions.KV_PARTITIONED_INDEX_FILTERS, true);
        config.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ZERO);
        config.set(ConfigOptions.NETTY_CLIENT_NUM_NETWORK_THREADS, 1);
        return config;
    }

    @Test
    void testMigrateAndRecoverCommittedSnapshot(@TempDir Path recoveryDir) throws Exception {
        Configuration ordinaryIndex = new Configuration();
        ordinaryIndex.set(ConfigOptions.KV_PARTITIONED_INDEX_FILTERS, false);
        for (int server = 0; server < 3; server++) {
            CLUSTER.restartTabletServer(server, ordinaryIndex);
        }
        TablePath path = TablePath.of("compaction", "migration");
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            TableInfo info = createTable(admin, path);
            List<String> expectedRows = new ArrayList<>();
            try (Table table = connection.getTable(path)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                for (int id = 0; id < 300; id++) {
                    writer.upsert(row(id, "value-" + id));
                    expectedRows.add(id + ":value-" + id);
                }
                writer.flush();
            }
            Collections.sort(expectedRows);
            Map<Integer, Long> previousSnapshots = new HashMap<>();
            Set<Integer> leaders = new HashSet<>();
            for (int bucket = 0; bucket < 3; bucket++) {
                TableBucket tb = new TableBucket(info.getTableId(), bucket);
                Replica replica = CLUSTER.waitAndGetLeaderReplica(tb);
                flushAndWait(replica.getKvTablet(), replica.getLogHighWatermark());
                long initialSnapshotId =
                        replica.getKvSnapshotManager()
                                .triggerSnapshotAfterCompaction()
                                .get(30, TimeUnit.SECONDS)
                                .getSnapshotId();
                CompletedSnapshot snapshot =
                        CLUSTER.waitUntilSnapshotFinished(tb, initialSnapshotId);
                previousSnapshots.put(bucket, snapshot.getSnapshotID());
                leaders.add(CLUSTER.waitAndGetLeaderReplica(tb).getLeaderId());
                KvTablet tablet = CLUSTER.waitAndGetLeaderReplica(tb).getKvTablet();
                assertThat(tablet.isPartitionedIndexEnabled()).isFalse();
                assertThat(tablet.inspectSstProperties().fileCount()).isPositive();
                assertThat(tablet.inspectSstProperties().matches(false)).isTrue();
            }
            assertThat(leaders).hasSize(3);
            Configuration partitionedIndex = new Configuration();
            partitionedIndex.set(ConfigOptions.KV_PARTITIONED_INDEX_FILTERS, true);
            for (int server = 0; server < 3; server++) {
                CLUSTER.restartTabletServer(server, partitionedIndex);
            }
            CLUSTER.waitUntilTableReady(info.getTableId());
            for (int bucket = 0; bucket < 3; bucket++) {
                KvTablet tablet =
                        CLUSTER.waitAndGetLeaderReplica(new TableBucket(info.getTableId(), bucket))
                                .getKvTablet();
                assertThat(tablet.isPartitionedIndexEnabled()).isTrue();
                assertThat(tablet.inspectSstProperties().matches(false)).isTrue();
                assertThat(tablet.inspectSstProperties().fileCount()).isPositive();
            }

            awaitSucceeded(admin, admin.compact(path, CompactionType.MIGRATE).get());
            List<String> recoveredRows = new ArrayList<>();
            Map<Integer, Set<String>> migratedFiles = new HashMap<>();
            for (int bucket = 0; bucket < 3; bucket++) {
                TableBucket tb = new TableBucket(info.getTableId(), bucket);
                KvTablet tablet = CLUSTER.waitAndGetLeaderReplica(tb).getKvTablet();
                assertThat(tablet.inspectSstProperties().matches(true)).isTrue();
                migratedFiles.put(bucket, liveSstFiles(tablet));
                long snapshotId =
                        admin.getLatestKvSnapshots(path).get().getSnapshotId(bucket).getAsLong();
                assertThat(snapshotId).isGreaterThan(previousSnapshots.get(bucket));
                previousSnapshots.put(bucket, snapshotId);
                CompletedSnapshot snapshot = CLUSTER.waitUntilSnapshotFinished(tb, snapshotId);
                try (RocksDBKv recovered =
                                KvTestUtils.buildFromSnapshotHandle(
                                        snapshot.getKvSnapshotHandle(),
                                        recoveryDir.resolve("bucket-" + bucket));
                        ResourceGuard.Lease ignored =
                                recovered.getResourceGuard().acquireResource()) {
                    assertThat(recovered.inspectSstProperties().fileCount()).isPositive();
                    assertThat(recovered.inspectSstProperties().matches(true)).isTrue();
                    recoveredRows.addAll(readRows(recovered, info));
                }
            }
            Collections.sort(recoveredRows);
            assertThat(recoveredRows).containsExactlyElementsOf(expectedRows);

            String second = admin.compact(path, CompactionType.MIGRATE).get();
            awaitSucceeded(admin, second);
            for (int bucket = 0; bucket < 3; bucket++) {
                KvTablet tablet =
                        CLUSTER.waitAndGetLeaderReplica(new TableBucket(info.getTableId(), bucket))
                                .getKvTablet();
                assertThat(liveSstFiles(tablet)).isEqualTo(migratedFiles.get(bucket));
                assertThat(admin.getLatestKvSnapshots(path).get().getSnapshotId(bucket).getAsLong())
                        .isGreaterThan(previousSnapshots.get(bucket));
            }
            CompactionProgress beforeRestart = admin.getCompaction(second).get();
            CLUSTER.stopCoordinatorServer();
            CLUSTER.startCoordinatorServer();
            try (Connection recoveredConnection =
                            ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                    Admin recoveredAdmin = recoveredConnection.getAdmin()) {
                awaitSucceeded(recoveredAdmin, second);
                CompactionProgress afterRestart = recoveredAdmin.getCompaction(second).get();
                assertThat(afterRestart.info()).isEqualTo(beforeRestart.info());
                assertThat(afterRestart.succeeded()).isEqualTo(beforeRestart.succeeded());
                assertThat(afterRestart.failed()).isEqualTo(beforeRestart.failed());
                assertThat(afterRestart.cancelled()).isEqualTo(beforeRestart.cancelled());
                assertThat(afterRestart.lastProgressTime())
                        .isEqualTo(beforeRestart.lastProgressTime());
            }
        }
    }

    @Test
    void testOptimizeAndReclaimWithForegroundWrites() throws Exception {
        TablePath path = TablePath.of("compaction", "foreground");
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            TableInfo info = createTable(admin, path);
            for (CompactionType type :
                    Arrays.asList(CompactionType.OPTIMIZE, CompactionType.RECLAIM)) {
                Map<Integer, Long> previousSnapshots = new HashMap<>();
                for (int bucket = 0; bucket < 3; bucket++) {
                    previousSnapshots.put(
                            bucket,
                            admin.getLatestKvSnapshots(path)
                                    .get()
                                    .getSnapshotId(bucket)
                                    .orElse(-1));
                }
                AtomicBoolean stop = new AtomicBoolean();
                AtomicInteger completedWrites = new AtomicInteger();
                ExecutorService executor = Executors.newSingleThreadExecutor();
                Future<?> foreground =
                        executor.submit(
                                () -> {
                                    try (Table table = connection.getTable(path)) {
                                        UpsertWriter writer = table.newUpsert().createWriter();
                                        Lookuper lookuper = table.newLookup().createLookuper();
                                        while (!stop.get()) {
                                            int round = completedWrites.get();
                                            for (int id = 0; id < 30; id++) {
                                                writer.upsert(row(id, "round-" + round));
                                            }
                                            writer.flush();
                                            assertThatRow(
                                                            lookuper.lookup(row(0))
                                                                    .get(10, TimeUnit.SECONDS)
                                                                    .getSingletonRow())
                                                    .withSchema(SCHEMA.getRowType())
                                                    .isEqualTo(row(0, "round-" + round));
                                            completedWrites.incrementAndGet();
                                        }
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                });
                try {
                    retry(
                            Duration.ofSeconds(30),
                            () -> assertThat(completedWrites.get()).isPositive());
                    String operation = admin.compact(path, type).get(10, TimeUnit.SECONDS);
                    int atSubmission = completedWrites.get();
                    retry(
                            Duration.ofSeconds(30),
                            () -> assertThat(completedWrites.get()).isGreaterThan(atSubmission));
                    awaitSucceeded(admin, operation);
                    for (int bucket = 0; bucket < 3; bucket++) {
                        long snapshotId =
                                admin.getLatestKvSnapshots(path)
                                        .get()
                                        .getSnapshotId(bucket)
                                        .getAsLong();
                        assertThat(snapshotId).isGreaterThan(previousSnapshots.get(bucket));
                        assertThat(
                                        CLUSTER.waitUntilSnapshotFinished(
                                                        new TableBucket(info.getTableId(), bucket),
                                                        snapshotId)
                                                .getSnapshotID())
                                .isEqualTo(snapshotId);
                    }
                } finally {
                    stop.set(true);
                    try {
                        foreground.get(30, TimeUnit.SECONDS);
                    } finally {
                        executor.shutdownNow();
                    }
                }
                try (Table table = connection.getTable(path)) {
                    UpsertWriter writer = table.newUpsert().createWriter();
                    writer.upsert(row(100, type.name()));
                    writer.delete(row(0, null));
                    writer.flush();
                    Lookuper lookuper = table.newLookup().createLookuper();
                    assertThatRow(lookuper.lookup(row(100)).get().getSingletonRow())
                            .withSchema(SCHEMA.getRowType())
                            .isEqualTo(row(100, type.name()));
                    assertThat(lookuper.lookup(row(0)).get().getRowList()).isEmpty();
                }
            }
        }
    }

    private TableInfo createTable(Admin admin, TablePath path) throws Exception {
        admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
        admin.createTable(
                        path,
                        TableDescriptor.builder().schema(SCHEMA).distributedBy(3, "id").build(),
                        false)
                .get();
        TableInfo info = admin.getTableInfo(path).get();
        CLUSTER.waitUntilTableReady(info.getTableId());
        return info;
    }

    private static void awaitSucceeded(Admin admin, String operation) {
        retry(
                Duration.ofSeconds(60),
                () -> {
                    CompactionProgress progress =
                            admin.getCompaction(operation).get(10, TimeUnit.SECONDS);
                    assertThat(progress.info().status())
                            .as("%s", progress.firstError())
                            .isEqualTo(CompactionStatus.SUCCEEDED);
                    assertThat(progress.succeeded()).isEqualTo(progress.total());
                    assertThat(progress.total()).isEqualTo(3);
                });
    }

    private static Set<String> liveSstFiles(KvTablet tablet) throws Exception {
        RocksDBKv kv = tablet.getRocksDBKv();
        try (ResourceGuard.Lease ignored = kv.getResourceGuard().acquireResource()) {
            return new HashSet<>(
                    kv.getDb()
                            .getPropertiesOfAllTables(kv.getDefaultColumnFamilyHandle())
                            .keySet());
        }
    }

    private static List<String> readRows(RocksDBKv kv, TableInfo info) {
        ValueDecoder decoder =
                new ValueDecoder(
                        new TestingSchemaGetter(info.getSchemaInfo()),
                        info.getTableConfig().getKvFormat(),
                        KvValueLayout.fromTableConfig(info.getTableConfig()));
        List<String> rows = new ArrayList<>();
        try (RocksIterator iterator = kv.getDb().newIterator(kv.getDefaultColumnFamilyHandle())) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                InternalRow row = decoder.decodeValue(iterator.value()).row;
                rows.add(row.getInt(0) + ":" + row.getString(1));
            }
        }
        return rows;
    }
}

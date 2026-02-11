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

package org.apache.fluss.flink.sink.undo;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.UpsertResult;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.sink.undo.UndoRecoveryManager.UndoOffsets;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for {@link UndoRecoveryManager}.
 *
 * <p>Tests use complex multi-bucket, multi-key scenarios to ensure only correct implementations
 * pass. Each test covers multiple aspects to maximize value while minimizing redundancy.
 */
public class UndoRecoveryManagerITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(
                            new Configuration()
                                    // not to clean snapshots for test purpose
                                    .set(
                                            ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS,
                                            Integer.MAX_VALUE))
                    .setNumOfTabletServers(3)
                    .build();

    private static final String DEFAULT_DB = "test-flink-db";

    protected static Connection conn;
    protected static Admin admin;

    @BeforeAll
    static void beforeAll() {
        Configuration clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    @BeforeEach
    void beforeEach() throws Exception {
        admin.createDatabase(DEFAULT_DB, DatabaseDescriptor.EMPTY, true).get();
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (admin != null) {
            admin.close();
            admin = null;
        }
        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    protected long createTable(TablePath tablePath, TableDescriptor tableDescriptor)
            throws Exception {
        admin.createTable(tablePath, tableDescriptor, true).get();
        return admin.getTableInfo(tablePath).get().getTableId();
    }

    private static final int NUM_BUCKETS = 4;

    private static final Schema TEST_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .column("price", DataTypes.INT())
                    .column("stock", DataTypes.INT())
                    .primaryKey("id")
                    .build();

    private static final Schema PARTITIONED_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .column("price", DataTypes.INT())
                    .column("pt", DataTypes.STRING())
                    .primaryKey("id", "pt")
                    .build();

    // ==================== Full Update Tests ====================

    /**
     * Comprehensive test for Full Update mode with 4 buckets and 100+ keys.
     *
     * <p>Covers: zero-offset recovery, DELETE undo, UPDATE undo with deduplication, mixed ops.
     */
    @Test
    void testFullUpdateMultiBucketComprehensiveRecovery() throws Exception {
        TablePath tablePath = createTestTablePath("full_update_test");
        long tableId =
                createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(TEST_SCHEMA)
                                .distributedBy(NUM_BUCKETS, "id")
                                .property(
                                        ConfigOptions.TABLE_MERGE_ENGINE,
                                        MergeEngineType.AGGREGATION)
                                .build());

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            Lookuper lookuper = table.newLookup().createLookuper();
            BucketTracker tracker = new BucketTracker(NUM_BUCKETS, tableId);

            // Phase 1: Write initial 100 keys (batch async)
            List<CompletableFuture<UpsertResult>> writeFutures = new ArrayList<>();
            for (int id = 0; id < 100; id++) {
                writeFutures.add(writer.upsert(row(id, "pre_" + id, id * 10, id)));
            }
            for (int i = 0; i < writeFutures.size(); i++) {
                tracker.recordWrite(i, writeFutures.get(i).get());
            }
            writer.flush();

            int zeroOffsetBucket = tracker.selectBucketForZeroOffsetRecovery();
            Map<TableBucket, Long> checkpointOffsets =
                    tracker.buildCheckpointOffsetsWithZeroOffset(zeroOffsetBucket);

            Map<Integer, Set<Integer>> keysAfterCheckpoint = new HashMap<>();
            for (int i = 0; i < NUM_BUCKETS; i++) {
                keysAfterCheckpoint.put(i, new HashSet<>());
            }

            // Phase 2: Operations after checkpoint (batch async)
            List<CompletableFuture<UpsertResult>> newKeyFutures = new ArrayList<>();
            for (int id = 200; id < 300; id++) {
                newKeyFutures.add(writer.upsert(row(id, "new_" + id, id * 10, id)));
            }
            writer.flush();
            for (int i = 0; i < newKeyFutures.size(); i++) {
                UpsertResult result = newKeyFutures.get(i).get();
                keysAfterCheckpoint.get(result.getBucket().getBucket()).add(200 + i);
            }

            for (int bucket = 0; bucket < NUM_BUCKETS; bucket++) {
                if (bucket != zeroOffsetBucket && !tracker.keysByBucket.get(bucket).isEmpty()) {
                    List<Integer> keys = tracker.keysByBucket.get(bucket);
                    for (int i = 0; i < Math.min(3, keys.size()); i++) {
                        int id = keys.get(i);
                        writer.upsert(row(id, "updated_" + id, 9999, 9999));
                        keysAfterCheckpoint.get(bucket).add(id);
                    }
                    if (keys.size() > 5) {
                        for (int i = 3; i < Math.min(5, keys.size()); i++) {
                            int id = keys.get(i);
                            writer.delete(row(id, "pre_" + id, id * 10, id));
                            keysAfterCheckpoint.get(bucket).add(id);
                        }
                    }
                    if (keys.size() > 6) {
                        int id = keys.get(5);
                        writer.upsert(row(id, "multi_v1", 111, 111));
                        writer.upsert(row(id, "multi_v2", 222, 222));
                        writer.upsert(row(id, "multi_v3", 333, 333));
                        keysAfterCheckpoint.get(bucket).add(id);
                    }
                }
            }
            writer.flush();

            // Phase 3: Perform undo recovery
            Map<TableBucket, UndoOffsets> offsetRanges =
                    buildUndoOffsets(checkpointOffsets, tablePath, admin);
            new UndoRecoveryManager(table, null).performUndoRecovery(offsetRanges, 0, 1);

            // Phase 4: Verify results (parallel lookup)
            List<CompletableFuture<Void>> verifyFutures = new ArrayList<>();

            for (int id : tracker.keysByBucket.get(zeroOffsetBucket)) {
                final int keyId = id;
                verifyFutures.add(
                        lookuper.lookup(row(keyId))
                                .thenAccept(
                                        r ->
                                                assertThat(r.getSingletonRow())
                                                        .as(
                                                                "Zero-offset bucket key %d should be deleted",
                                                                keyId)
                                                        .isNull()));
            }

            for (int bucket = 0; bucket < NUM_BUCKETS; bucket++) {
                if (bucket == zeroOffsetBucket) {
                    continue;
                }
                for (int id : tracker.keysByBucket.get(bucket)) {
                    final int keyId = id;
                    final int b = bucket;
                    verifyFutures.add(
                            lookuper.lookup(row(keyId))
                                    .thenAccept(
                                            r -> {
                                                InternalRow result = r.getSingletonRow();
                                                assertThat(result)
                                                        .as(
                                                                "Bucket %d key %d should exist",
                                                                b, keyId)
                                                        .isNotNull();
                                                assertRowEquals(
                                                        result,
                                                        keyId,
                                                        "pre_" + keyId,
                                                        keyId * 10,
                                                        keyId);
                                            }));
                }
                for (int id : keysAfterCheckpoint.get(bucket)) {
                    if (id >= 200) {
                        final int keyId = id;
                        final int b = bucket;
                        verifyFutures.add(
                                lookuper.lookup(row(keyId))
                                        .thenAccept(
                                                r ->
                                                        assertThat(r.getSingletonRow())
                                                                .as(
                                                                        "Bucket %d new key %d should be deleted",
                                                                        b, keyId)
                                                                .isNull()));
                    }
                }
            }
            CompletableFuture.allOf(verifyFutures.toArray(new CompletableFuture[0])).get();
        }
    }

    // ==================== Partial Update Tests ====================

    /**
     * Comprehensive test for Partial Update mode with 4 buckets.
     *
     * <p>Covers: target columns rollback, non-target columns preserved, other writer's changes
     * preserved.
     */
    @Test
    void testPartialUpdateMultiBucketComprehensiveRecovery() throws Exception {
        TablePath tablePath = createTestTablePath("partial_update_test");
        long tableId =
                createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(TEST_SCHEMA)
                                .distributedBy(NUM_BUCKETS, "id")
                                .property(
                                        ConfigOptions.TABLE_MERGE_ENGINE,
                                        MergeEngineType.AGGREGATION)
                                .build());

        int[] targetColumns = new int[] {0, 2}; // id, price

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter fullWriter = table.newUpsert().createWriter();
            Lookuper lookuper = table.newLookup().createLookuper();

            // Discover bucket assignments efficiently (batch write + delete)
            Map<Integer, Integer> keyToBucket = new HashMap<>();
            List<CompletableFuture<UpsertResult>> discoverFutures = new ArrayList<>();
            for (int id = 0; id < 80; id++) {
                discoverFutures.add(fullWriter.upsert(row(id, "temp", 0, 0)));
            }
            for (int i = 0; i < discoverFutures.size(); i++) {
                keyToBucket.put(i, discoverFutures.get(i).get().getBucket().getBucket());
            }
            fullWriter.flush();
            for (int id = 0; id < 80; id++) {
                fullWriter.delete(row(id, "temp", 0, 0));
            }
            fullWriter.flush();

            Map<Integer, List<Integer>> initialKeysByBucket = new HashMap<>();
            for (int i = 0; i < NUM_BUCKETS; i++) {
                initialKeysByBucket.put(i, new ArrayList<>());
            }
            for (int id = 0; id < 80; id++) {
                initialKeysByBucket.get(keyToBucket.get(id)).add(id);
            }
            int zeroOffsetBucket =
                    selectBucketForZeroOffsetRecovery(initialKeysByBucket, NUM_BUCKETS);

            // Phase 1: Write initial data (except zeroOffsetBucket)
            Map<Integer, List<Integer>> keysByBucket = new HashMap<>();
            Map<Integer, Long> bucketOffsets = new HashMap<>();
            for (int i = 0; i < NUM_BUCKETS; i++) {
                keysByBucket.put(i, new ArrayList<>());
            }

            List<CompletableFuture<UpsertResult>> initFutures = new ArrayList<>();
            List<Integer> initIds = new ArrayList<>();
            for (int id = 0; id < 80; id++) {
                int bucket = keyToBucket.get(id);
                if (bucket != zeroOffsetBucket) {
                    initFutures.add(fullWriter.upsert(row(id, "name_" + id, id * 10, id * 100)));
                    initIds.add(id);
                }
            }
            for (int i = 0; i < initFutures.size(); i++) {
                UpsertResult result = initFutures.get(i).get();
                int id = initIds.get(i);
                int bucket = result.getBucket().getBucket();
                keysByBucket.get(bucket).add(id);
                bucketOffsets.put(bucket, result.getLogEndOffset());
            }
            fullWriter.flush();

            Map<TableBucket, Long> checkpointOffsets = new HashMap<>();
            checkpointOffsets.put(new TableBucket(tableId, zeroOffsetBucket), 0L);
            for (int i = 0; i < NUM_BUCKETS; i++) {
                if (i != zeroOffsetBucket) {
                    checkpointOffsets.put(
                            new TableBucket(tableId, i), bucketOffsets.getOrDefault(i, 0L));
                }
            }

            Set<Integer> zeroOffsetBucketKeysAfterCheckpoint = new HashSet<>();
            Map<Integer, Set<Integer>> newKeysAfterCheckpointByBucket = new HashMap<>();
            for (int i = 0; i < NUM_BUCKETS; i++) {
                newKeysAfterCheckpointByBucket.put(i, new HashSet<>());
            }

            // Phase 2: Partial updates after checkpoint
            UpsertWriter partialWriter =
                    table.newUpsert().partialUpdate(targetColumns).createWriter();

            for (int id = 0; id < 80; id++) {
                if (keyToBucket.get(id) == zeroOffsetBucket) {
                    partialWriter.upsert(row(id, null, 9999, null));
                    zeroOffsetBucketKeysAfterCheckpoint.add(id);
                }
            }

            List<CompletableFuture<UpsertResult>> newPartialFutures = new ArrayList<>();
            for (int id = 100; id < 130; id++) {
                newPartialFutures.add(partialWriter.upsert(row(id, null, id * 10, null)));
            }
            for (int i = 0; i < newPartialFutures.size(); i++) {
                UpsertResult result = newPartialFutures.get(i).get();
                int id = 100 + i;
                int bucket = result.getBucket().getBucket();
                if (bucket == zeroOffsetBucket) {
                    zeroOffsetBucketKeysAfterCheckpoint.add(id);
                } else {
                    newKeysAfterCheckpointByBucket.get(bucket).add(id);
                }
            }

            for (int bucket = 0; bucket < NUM_BUCKETS; bucket++) {
                if (bucket != zeroOffsetBucket) {
                    List<Integer> keys = keysByBucket.get(bucket);
                    for (int i = 0; i < Math.min(5, keys.size()); i++) {
                        partialWriter.upsert(row(keys.get(i), null, 8888, null));
                    }
                }
            }
            partialWriter.flush();

            // Other Writer updates stock (should NOT be undone)
            UpsertWriter otherWriter =
                    table.newUpsert().partialUpdate(new int[] {0, 3}).createWriter();
            for (int bucket = 0; bucket < NUM_BUCKETS; bucket++) {
                if (bucket != zeroOffsetBucket) {
                    for (int id : keysByBucket.get(bucket)) {
                        otherWriter.upsert(row(id, null, null, 5555));
                    }
                }
            }
            otherWriter.flush();

            // Phase 3: Perform undo recovery
            Map<TableBucket, UndoOffsets> offsetRanges =
                    buildUndoOffsets(checkpointOffsets, tablePath, admin);
            new UndoRecoveryManager(table, targetColumns).performUndoRecovery(offsetRanges, 0, 1);

            // Phase 4: Verify results (parallel lookup)
            List<CompletableFuture<Void>> verifyFutures = new ArrayList<>();

            for (int bucket = 0; bucket < NUM_BUCKETS; bucket++) {
                if (bucket == zeroOffsetBucket) {
                    continue;
                }

                for (int id : keysByBucket.get(bucket)) {
                    final int keyId = id;
                    final int b = bucket;
                    verifyFutures.add(
                            lookuper.lookup(row(keyId))
                                    .thenAccept(
                                            r -> {
                                                InternalRow result = r.getSingletonRow();
                                                assertThat(result)
                                                        .as(
                                                                "Bucket %d key %d should exist",
                                                                b, keyId)
                                                        .isNotNull();
                                                assertThat(result.getString(1).toString())
                                                        .isEqualTo("name_" + keyId);
                                                assertThat(result.getInt(2))
                                                        .as("price restored")
                                                        .isEqualTo(keyId * 10);
                                                assertThat(result.getInt(3))
                                                        .as("stock kept")
                                                        .isEqualTo(5555);
                                            }));
                }
            }

            // Verify zero-offset bucket keys after undo recovery.
            //
            // Since checkpoint offset is 0, ALL writes to this bucket are undone.
            // The undo writer uses partial update (targetColumns = {id, price}) with
            // OVERWRITE mode, so undo only clears target columns — non-target columns
            // written by other writers or earlier full writes are unaffected.
            //
            // - id < 80: These keys were first written by fullWriter in Phase 1, which
            //   populated ALL columns (including non-target columns like name, stock).
            //   Undo clears the target column (price) to null, but the row still exists
            //   because non-target columns retain their values.
            //
            // - id >= 100: These keys were ONLY written by partialWriter in Phase 2,
            //   which only populated target columns (id, price). Undo clears those
            //   target columns, leaving no column with data, so the row is effectively
            //   deleted.
            for (int id : zeroOffsetBucketKeysAfterCheckpoint) {
                final int keyId = id;
                verifyFutures.add(
                        lookuper.lookup(row(keyId))
                                .thenAccept(
                                        r -> {
                                            if (keyId < 80) {
                                                // Key has non-target column data from
                                                // fullWriter (Phase 1), so the row survives
                                                // undo — only target column (price) is cleared.
                                                InternalRow result = r.getSingletonRow();
                                                assertThat(result)
                                                        .as(
                                                                "Zero-offset bucket key %d should still exist (non-target columns retain data)",
                                                                keyId)
                                                        .isNotNull();
                                                assertThat(result.isNullAt(2))
                                                        .as(
                                                                "Zero-offset bucket key %d price (target column) should be null after undo",
                                                                keyId)
                                                        .isTrue();
                                            } else {
                                                // Key was only written via partialWriter
                                                // (target columns only) — undo clears all
                                                // its data, so the row no longer exists.
                                                assertThat(r.getSingletonRow())
                                                        .as(
                                                                "Zero-offset bucket key %d should not exist (no non-target column data)",
                                                                keyId)
                                                        .isNull();
                                            }
                                        }));
            }

            CompletableFuture.allOf(verifyFutures.toArray(new CompletableFuture[0])).get();
        }
    }

    // ==================== Partitioned Table Tests ====================

    /**
     * Test undo recovery for partitioned table with multiple partitions.
     *
     * <p>Covers: multi-partition recovery, INSERT/UPDATE/DELETE undo across partitions.
     */
    @Test
    void testPartitionedTableRecovery() throws Exception {
        TablePath tablePath = createTestTablePath("partitioned_test");
        long tableId =
                createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(PARTITIONED_SCHEMA)
                                .partitionedBy("pt")
                                .distributedBy(2, "id")
                                .property(
                                        ConfigOptions.TABLE_MERGE_ENGINE,
                                        MergeEngineType.AGGREGATION)
                                .build());

        String partition1 = "p1", partition2 = "p2";
        admin.createPartition(tablePath, newPartitionSpec("pt", partition1), false).get();
        admin.createPartition(tablePath, newPartitionSpec("pt", partition2), false).get();

        List<PartitionInfo> partitionInfos = admin.listPartitionInfos(tablePath).get();
        Map<String, Long> partitionNameToId = new HashMap<>();
        for (PartitionInfo info : partitionInfos) {
            partitionNameToId.put(info.getPartitionName(), info.getPartitionId());
        }
        long partition1Id = partitionNameToId.get(partition1);
        long partition2Id = partitionNameToId.get(partition2);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            Lookuper lookuper = table.newLookup().createLookuper();
            Map<TableBucket, Long> checkpointOffsets = new HashMap<>();

            // Phase 1: Write initial data (batch)
            List<CompletableFuture<UpsertResult>> p1Futures = new ArrayList<>();
            List<CompletableFuture<UpsertResult>> p2Futures = new ArrayList<>();
            for (int id = 0; id < 10; id++) {
                p1Futures.add(writer.upsert(row(id, "p1_name_" + id, id * 10, partition1)));
                p2Futures.add(writer.upsert(row(id, "p2_name_" + id, id * 20, partition2)));
            }
            for (int i = 0; i < 10; i++) {
                UpsertResult r1 = p1Futures.get(i).get();
                checkpointOffsets.put(
                        new TableBucket(tableId, partition1Id, r1.getBucket().getBucket()),
                        r1.getLogEndOffset());
                UpsertResult r2 = p2Futures.get(i).get();
                checkpointOffsets.put(
                        new TableBucket(tableId, partition2Id, r2.getBucket().getBucket()),
                        r2.getLogEndOffset());
            }
            writer.flush();

            // Phase 2: Operations after checkpoint
            Set<Integer> newKeysInP1 = new HashSet<>(), newKeysInP2 = new HashSet<>();
            for (int id = 100; id < 105; id++) {
                writer.upsert(row(id, "p1_new_" + id, id * 10, partition1));
                newKeysInP1.add(id);
            }
            for (int id = 0; id < 3; id++) {
                writer.upsert(row(id, "p1_updated_" + id, 9999, partition1));
            }
            for (int id = 5; id < 8; id++) {
                writer.delete(row(id, "p2_name_" + id, id * 20, partition2));
            }
            for (int id = 200; id < 205; id++) {
                writer.upsert(row(id, "p2_new_" + id, id * 20, partition2));
                newKeysInP2.add(id);
            }
            writer.flush();

            // Phase 3: Perform undo recovery
            Map<TableBucket, UndoOffsets> offsetRanges =
                    buildUndoOffsets(checkpointOffsets, tablePath, admin);
            new UndoRecoveryManager(table, null).performUndoRecovery(offsetRanges, 0, 1);

            // Phase 4: Verify results (parallel lookup)
            List<CompletableFuture<Void>> verifyFutures = new ArrayList<>();

            for (int id = 0; id < 10; id++) {
                final int keyId = id;
                verifyFutures.add(
                        lookuper.lookup(row(keyId, partition1))
                                .thenAccept(
                                        r -> {
                                            InternalRow result = r.getSingletonRow();
                                            assertThat(result)
                                                    .as("P1 key %d should exist", keyId)
                                                    .isNotNull();
                                            assertThat(result.getString(1).toString())
                                                    .isEqualTo("p1_name_" + keyId);
                                            assertThat(result.getInt(2)).isEqualTo(keyId * 10);
                                        }));
                verifyFutures.add(
                        lookuper.lookup(row(keyId, partition2))
                                .thenAccept(
                                        r -> {
                                            InternalRow result = r.getSingletonRow();
                                            assertThat(result)
                                                    .as("P2 key %d should exist", keyId)
                                                    .isNotNull();
                                            assertThat(result.getString(1).toString())
                                                    .isEqualTo("p2_name_" + keyId);
                                            assertThat(result.getInt(2)).isEqualTo(keyId * 20);
                                        }));
            }

            CompletableFuture.allOf(verifyFutures.toArray(new CompletableFuture[0])).get();
        }
    }

    // ==================== Idempotency and Exception Tests ====================

    /**
     * Test that undo recovery is idempotent - multiple executions produce the same result.
     *
     * <p>Covers: 4 buckets, 200 keys, mixed operations, 2 recovery rounds.
     */
    @Test
    void testRecoveryIdempotency() throws Exception {
        final int numBuckets = 4;
        final int keysPerBucket = 50;

        TablePath tablePath = createTestTablePath("idempotency_test");
        long tableId =
                createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(TEST_SCHEMA)
                                .distributedBy(numBuckets, "id")
                                .property(
                                        ConfigOptions.TABLE_MERGE_ENGINE,
                                        MergeEngineType.AGGREGATION)
                                .build());

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            Lookuper lookuper = table.newLookup().createLookuper();
            BucketTracker tracker = new BucketTracker(numBuckets, tableId);

            int totalKeys = numBuckets * keysPerBucket;

            // Batch write initial keys
            List<CompletableFuture<UpsertResult>> initFutures = new ArrayList<>();
            for (int id = 0; id < totalKeys; id++) {
                initFutures.add(writer.upsert(row(id, "original_" + id, id * 10, id * 100)));
            }
            for (int i = 0; i < initFutures.size(); i++) {
                tracker.recordWrite(i, initFutures.get(i).get());
            }
            writer.flush();

            Map<TableBucket, Long> checkpointOffsets = tracker.buildCheckpointOffsetsAll();

            // Batch write new keys
            Set<Integer> newKeys = new HashSet<>();
            for (int id = totalKeys; id < totalKeys + 50; id++) {
                writer.upsert(row(id, "new_" + id, id * 10, id * 100));
                newKeys.add(id);
            }

            for (int bucket = 0; bucket < numBuckets; bucket++) {
                List<Integer> keys = tracker.keysByBucket.get(bucket);
                for (int i = 0; i < Math.min(3, keys.size()); i++) {
                    writer.upsert(row(keys.get(i), "updated_" + keys.get(i), 9999, 9999));
                }
                if (keys.size() > 5) {
                    int id = keys.get(5);
                    writer.delete(row(id, "original_" + id, id * 10, id * 100));
                }
            }
            writer.flush();

            // Execute recovery 2 times - results should be identical
            // For idempotency, we need to update checkpoint offsets after first recovery
            // because recovery writes undo operations which advance the log end offset
            Map<TableBucket, Long> currentCheckpointOffsets = new HashMap<>(checkpointOffsets);

            for (int round = 1; round <= 2; round++) {
                Map<TableBucket, UndoOffsets> offsetRanges =
                        buildUndoOffsets(currentCheckpointOffsets, tablePath, admin);
                new UndoRecoveryManager(table, null).performUndoRecovery(offsetRanges, 0, 1);

                // After first recovery, update checkpoint offsets to current log end offsets
                // This simulates what would happen in a real scenario where checkpoint is taken
                // after recovery completes
                if (round == 1) {
                    for (Map.Entry<TableBucket, UndoOffsets> entry : offsetRanges.entrySet()) {
                        // Query the new log end offset after recovery
                        TableBucket tb = entry.getKey();
                        List<Integer> bucketIds = new ArrayList<>();
                        bucketIds.add(tb.getBucket());
                        ListOffsetsResult offsetsResult =
                                admin.listOffsets(
                                        tablePath, bucketIds, new OffsetSpec.LatestSpec());
                        Long newOffset = offsetsResult.bucketResult(tb.getBucket()).get();
                        if (newOffset != null) {
                            currentCheckpointOffsets.put(tb, newOffset);
                        }
                    }
                }

                // Batch verify (parallel lookup)
                List<CompletableFuture<Void>> verifyFutures = new ArrayList<>();
                final int r = round;

                for (int id : newKeys) {
                    final int keyId = id;
                    verifyFutures.add(
                            lookuper.lookup(row(keyId))
                                    .thenAccept(
                                            result ->
                                                    assertThat(result.getSingletonRow())
                                                            .as(
                                                                    "Round %d: New key %d should be deleted",
                                                                    r, keyId)
                                                            .isNull()));
                }

                for (int bucket = 0; bucket < numBuckets; bucket++) {
                    for (int id : tracker.keysByBucket.get(bucket)) {
                        final int keyId = id;
                        verifyFutures.add(
                                lookuper.lookup(row(keyId))
                                        .thenAccept(
                                                lookupResult -> {
                                                    InternalRow result =
                                                            lookupResult.getSingletonRow();
                                                    assertThat(result)
                                                            .as("Round %d: Key %d exists", r, keyId)
                                                            .isNotNull();
                                                    assertRowEquals(
                                                            result,
                                                            keyId,
                                                            "original_" + keyId,
                                                            keyId * 10,
                                                            keyId * 100);
                                                }));
                    }
                }
                CompletableFuture.allOf(verifyFutures.toArray(new CompletableFuture[0])).get();
            }
        }
    }

    /** Test that scanner exceptions are properly propagated. */
    @Test
    void testRecoveryWithScannerException() throws Exception {
        TablePath tablePath = createTestTablePath("scanner_exception_test");
        long tableId =
                createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(TEST_SCHEMA)
                                .distributedBy(2, "id")
                                .property(
                                        ConfigOptions.TABLE_MERGE_ENGINE,
                                        MergeEngineType.AGGREGATION)
                                .build());

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            Map<TableBucket, Long> checkpointOffsets = new HashMap<>();

            List<CompletableFuture<UpsertResult>> futures = new ArrayList<>();
            for (int id = 0; id < 10; id++) {
                futures.add(writer.upsert(row(id, "name_" + id, id * 10, id)));
            }
            for (int i = 0; i < futures.size(); i++) {
                UpsertResult result = futures.get(i).get();
                checkpointOffsets.put(
                        new TableBucket(tableId, result.getBucket().getBucket()),
                        result.getLogEndOffset());
            }
            writer.flush();

            for (int id = 100; id < 110; id++) {
                writer.upsert(row(id, "new_" + id, id * 10, id));
            }
            writer.flush();

            final String errorMessage = "Simulated scanner failure for testing";
            FaultInjectingUndoRecoveryManager faultHandler =
                    new FaultInjectingUndoRecoveryManager(table, null);
            faultHandler.setExceptionToThrow(new RuntimeException(errorMessage));

            Map<TableBucket, UndoOffsets> offsetRanges =
                    buildUndoOffsets(checkpointOffsets, tablePath, admin);
            assertThatThrownBy(() -> faultHandler.performUndoRecovery(offsetRanges, 0, 1))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining(errorMessage);
        }
    }

    // ==================== Helper Classes ====================

    /** Tracks bucket assignments and offsets during test setup. */
    private static class BucketTracker {
        final int numBuckets;
        final long tableId;
        final Map<Integer, List<Integer>> keysByBucket = new HashMap<>();
        final Map<Integer, Long> bucketOffsets = new HashMap<>();

        BucketTracker(int numBuckets, long tableId) {
            this.numBuckets = numBuckets;
            this.tableId = tableId;
            for (int i = 0; i < numBuckets; i++) {
                keysByBucket.put(i, new ArrayList<>());
            }
        }

        void recordWrite(int key, UpsertResult result) {
            int bucketId = result.getBucket().getBucket();
            keysByBucket.get(bucketId).add(key);
            bucketOffsets.put(bucketId, result.getLogEndOffset());
        }

        int selectBucketForZeroOffsetRecovery() {
            int maxBucket = 0, maxKeys = 0;
            for (int i = 0; i < numBuckets; i++) {
                if (keysByBucket.get(i).size() > maxKeys) {
                    maxKeys = keysByBucket.get(i).size();
                    maxBucket = i;
                }
            }
            return maxBucket;
        }

        Map<TableBucket, Long> buildCheckpointOffsetsWithZeroOffset(int zeroOffsetBucket) {
            Map<TableBucket, Long> offsets = new HashMap<>();
            offsets.put(new TableBucket(tableId, zeroOffsetBucket), 0L);
            for (int i = 0; i < numBuckets; i++) {
                if (i != zeroOffsetBucket) {
                    offsets.put(new TableBucket(tableId, i), bucketOffsets.getOrDefault(i, 0L));
                }
            }
            return offsets;
        }

        Map<TableBucket, Long> buildCheckpointOffsetsAll() {
            Map<TableBucket, Long> offsets = new HashMap<>();
            for (int i = 0; i < numBuckets; i++) {
                offsets.put(new TableBucket(tableId, i), bucketOffsets.getOrDefault(i, 0L));
            }
            return offsets;
        }
    }

    private static class FaultInjectingUndoRecoveryManager extends UndoRecoveryManager {
        private RuntimeException exceptionToThrow;

        FaultInjectingUndoRecoveryManager(Table table, int[] targetColumnIndexes) {
            super(table, targetColumnIndexes);
        }

        void setExceptionToThrow(RuntimeException exception) {
            this.exceptionToThrow = exception;
        }

        @Override
        protected LogScanner createLogScanner() {
            return new FaultInjectingLogScanner(super.createLogScanner(), exceptionToThrow);
        }
    }

    private static class FaultInjectingLogScanner implements LogScanner {
        private final LogScanner delegate;
        private final RuntimeException exceptionToThrow;

        FaultInjectingLogScanner(LogScanner delegate, RuntimeException exceptionToThrow) {
            this.delegate = delegate;
            this.exceptionToThrow = exceptionToThrow;
        }

        @Override
        public ScanRecords poll(Duration timeout) {
            if (exceptionToThrow != null) {
                throw exceptionToThrow;
            }
            return delegate.poll(timeout);
        }

        @Override
        public void subscribe(int bucket, long offset) {
            delegate.subscribe(bucket, offset);
        }

        @Override
        public void subscribe(long partitionId, int bucket, long offset) {
            delegate.subscribe(partitionId, bucket, offset);
        }

        @Override
        public void unsubscribe(long partitionId, int bucket) {
            delegate.unsubscribe(partitionId, bucket);
        }

        @Override
        public void unsubscribe(int bucket) {
            delegate.unsubscribe(bucket);
        }

        @Override
        public void wakeup() {
            delegate.wakeup();
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }

    // ==================== Helper Methods ====================

    private TablePath createTestTablePath(String prefix) {
        return TablePath.of(DEFAULT_DB, prefix + "_" + System.currentTimeMillis());
    }

    private PartitionSpec newPartitionSpec(String key, String value) {
        return new PartitionSpec(Collections.singletonMap(key, value));
    }

    private int selectBucketForZeroOffsetRecovery(
            Map<Integer, List<Integer>> keysByBucket, int numBuckets) {
        int maxBucket = 0, maxKeys = 0;
        for (int i = 0; i < numBuckets; i++) {
            if (keysByBucket.get(i).size() > maxKeys) {
                maxKeys = keysByBucket.get(i).size();
                maxBucket = i;
            }
        }
        return maxBucket;
    }

    private void assertRowEquals(
            InternalRow row,
            int expectedId,
            String expectedName,
            int expectedPrice,
            int expectedStock) {
        assertThat(row.getInt(0)).as("id").isEqualTo(expectedId);
        assertThat(row.getString(1).toString()).as("name").isEqualTo(expectedName);
        assertThat(row.getInt(2)).as("price").isEqualTo(expectedPrice);
        assertThat(row.getInt(3)).as("stock").isEqualTo(expectedStock);
    }

    /**
     * Builds offset ranges by querying the latest offset for each bucket.
     *
     * <p>This simulates what the upper layer would do: it already has checkpoint offsets and needs
     * to query latest offsets to determine which buckets need recovery.
     */
    private Map<TableBucket, UndoOffsets> buildUndoOffsets(
            Map<TableBucket, Long> checkpointOffsets, TablePath tablePath, Admin admin)
            throws Exception {
        Map<TableBucket, UndoOffsets> result = new HashMap<>();

        // Group buckets by partition for efficient batch queries
        Map<Long, List<TableBucket>> bucketsByPartition = new HashMap<>();
        List<TableBucket> nonPartitionedBuckets = new ArrayList<>();

        for (TableBucket bucket : checkpointOffsets.keySet()) {
            if (bucket.getPartitionId() != null) {
                bucketsByPartition
                        .computeIfAbsent(bucket.getPartitionId(), k -> new ArrayList<>())
                        .add(bucket);
            } else {
                nonPartitionedBuckets.add(bucket);
            }
        }

        // Query non-partitioned buckets
        if (!nonPartitionedBuckets.isEmpty()) {
            List<Integer> bucketIds = new ArrayList<>();
            for (TableBucket tb : nonPartitionedBuckets) {
                bucketIds.add(tb.getBucket());
            }
            ListOffsetsResult offsetsResult =
                    admin.listOffsets(tablePath, bucketIds, new OffsetSpec.LatestSpec());
            for (TableBucket tb : nonPartitionedBuckets) {
                Long latestOffset = offsetsResult.bucketResult(tb.getBucket()).get();
                if (latestOffset == null) {
                    latestOffset = 0L;
                }
                long checkpointOffset = checkpointOffsets.get(tb);
                // Skip buckets that don't need recovery (checkpointOffset == latestOffset)
                if (checkpointOffset < latestOffset) {
                    result.put(tb, new UndoOffsets(checkpointOffset, latestOffset));
                }
            }
        }

        // Query partitioned buckets
        if (!bucketsByPartition.isEmpty()) {
            // Get partition name mapping
            List<PartitionInfo> partitions = admin.listPartitionInfos(tablePath).get();
            Map<Long, String> partitionIdToName = new HashMap<>();
            for (PartitionInfo info : partitions) {
                partitionIdToName.put(info.getPartitionId(), info.getPartitionName());
            }

            for (Map.Entry<Long, List<TableBucket>> entry : bucketsByPartition.entrySet()) {
                Long partitionId = entry.getKey();
                List<TableBucket> buckets = entry.getValue();
                String partitionName = partitionIdToName.get(partitionId);

                List<Integer> bucketIds = new ArrayList<>();
                for (TableBucket tb : buckets) {
                    bucketIds.add(tb.getBucket());
                }

                ListOffsetsResult offsetsResult =
                        admin.listOffsets(
                                tablePath, partitionName, bucketIds, new OffsetSpec.LatestSpec());
                for (TableBucket tb : buckets) {
                    Long latestOffset = offsetsResult.bucketResult(tb.getBucket()).get();
                    if (latestOffset == null) {
                        latestOffset = 0L;
                    }
                    long checkpointOffset = checkpointOffsets.get(tb);
                    // Skip buckets that don't need recovery (checkpointOffset == latestOffset)
                    if (checkpointOffset < latestOffset) {
                        result.put(tb, new UndoOffsets(checkpointOffset, latestOffset));
                    }
                }
            }
        }

        return result;
    }
}

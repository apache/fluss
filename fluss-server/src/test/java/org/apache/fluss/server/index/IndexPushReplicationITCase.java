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

package org.apache.fluss.server.index;

import org.apache.fluss.bucketing.FlussBucketingFunction;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.PbAddColumn;
import org.apache.fluss.rpc.messages.PutIndexRequest;
import org.apache.fluss.rpc.messages.PutIndexResponse;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IndexTableUtils;
import org.apache.fluss.utils.json.DataTypeJsonSerde;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createPartition;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newAlterTableRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newDropPartitionRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newGetTableInfoRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end ITCase for the WAL-driven index replication pipeline (WAL → {@link IndexReplicator} →
 * {@code IndexSendBuffer} → {@code IndexSender} → PutIndex to the Index Table leader).
 *
 * <p>Scenarios:
 *
 * <ul>
 *   <li>{@link #testInsertOnMainTablePushesEntryToIndexTable()} — INSERT pushes a single UPSERT to
 *       the Index Table.
 *   <li>{@link #testUpdateRewritesIndexEntry()} — UPDATE on the indexed column produces a DELETE on
 *       the old composite key and an UPSERT on the new composite key.
 *   <li>{@link #testDeleteRemovesIndexEntry()} — DELETE on the main table removes the index entry.
 *   <li>{@link #testEmptyWalBatchAdvancesSyncIndexOffset()} — empty WAL batches still advance index
 *       progress.
 *   <li>{@link #testAsyncVisibilityEventuallyVisible()} — with async {@code Schema.Index}
 *       visibility the PutKv ack does not wait for the push, but the index entry is eventually
 *       visible.
 *   <li>{@link #testDropRecreatePartitionRejectsLateOldIndexWrites()} — a dropped and recreated
 *       partition rejects delayed writes from the old incarnation and compacts only stale rows.
 *   <li>{@link #testSchemaEvolutionKeepsIndexReplicationHealthy()} — an index-safe ALTER TABLE ADD
 *       COLUMN is accepted on tables with secondary indexes and the replication pipeline keeps
 *       working across the schema evolution.
 * </ul>
 *
 * <p>Leader-failover scenarios (main-table and index-table leader switchover) are covered by {@link
 * IndexPushFailoverITCase}.
 *
 * <p>Each test uses a distinct main-table name so that the shared {@link FlussClusterExtension}
 * cluster does not see colliding paths across scenarios.
 */
class IndexPushReplicationITCase {

    private static final String DB = "test_db";
    private static final String INDEX_NAME = "idx_b";
    private static final String PARTITION_NAME = "p1";
    private static final int PRIMARY_KEY = 7;
    private static final String INDEXED_VALUE = "collision";

    /**
     * Single tablet server keeps replication trivial — both the main table's data leader and the
     * index table's leader are this one process so the index push is effectively a self-RPC. The
     * test still exercises the full {@code TabletServerGateway} → {@code KvManager.putAsLeader}
     * round-trip; we just don't need to coordinate ISR shrink/grow.
     */
    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(initConfig())
                    .build();

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 1);
        return conf;
    }

    private static final RowType PARTITIONED_MAIN_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("a", DataTypes.INT()),
                    new DataField("b", DataTypes.STRING()),
                    new DataField("p", DataTypes.STRING()));
    private static final RowType MAIN_BUCKET_KEY_TYPE =
            DataTypes.ROW(new DataField("a", DataTypes.INT().copy(false)));

    /** Bounded poll deadline for asynchronous index visibility checks. */
    private static final Duration INDEX_VISIBILITY_TIMEOUT = Duration.ofSeconds(30);

    /** A SYNC write is acknowledged only after its Index Table entry is visible. */
    @Test
    void testInsertOnMainTablePushesEntryToIndexTable() throws Exception {
        String mainName = "main_t_insert";
        Fixture f = setupTables(mainName, /* visibility */ null);

        KvRecordBatch batch = genKvRecordBatch(new Object[] {1, "hello"});
        PutKvRequest putKvRequest =
                newPutKvRequest(f.mainTableId, /* bucketId */ 0, /* acks */ 1, batch);
        f.mainGateway.putKv(putKvRequest).get();

        byte[] indexKey = encodeIndexKey(f.indexSpec, "hello", 1);

        assertIndexEntry(f.indexTableId, indexKey, targetBucket(f.indexSpec, "hello", 1), true);

        // The sync push completed before the ack, so the sync pushed offset must have advanced to
        // the write log end offset. A single row at log offset 0 advances it to 1.
        assertThat(f.mainLeaderReplica.getSyncIndexPushedOffset())
                .as(
                        "sync index pushed offset must equal the write log end offset after a sync write")
                .isEqualTo(1L);
    }

    /** Updating an indexed column deletes the old index key and writes the new key. */
    @Test
    void testUpdateRewritesIndexEntry() throws Exception {
        String mainName = "main_t_update";
        Fixture f = setupTables(mainName, /* visibility */ null);

        // (1) Insert (a=1, b="hello"); wait for the index entry to land.
        f.mainGateway
                .putKv(
                        newPutKvRequest(
                                f.mainTableId, 0, 1, genKvRecordBatch(new Object[] {1, "hello"})))
                .get();
        byte[] oldIndexKey = encodeIndexKey(f.indexSpec, "hello", 1);
        assertIndexEntry(f.indexTableId, oldIndexKey, targetBucket(f.indexSpec, "hello", 1), true);

        // (2) Update the same PK row with a different idx column value.
        f.mainGateway
                .putKv(
                        newPutKvRequest(
                                f.mainTableId, 0, 1, genKvRecordBatch(new Object[] {1, "world"})))
                .get();

        // (3) The old composite key disappears, the new one appears.
        byte[] newIndexKey = encodeIndexKey(f.indexSpec, "world", 1);
        assertIndexEntry(f.indexTableId, newIndexKey, targetBucket(f.indexSpec, "world", 1), true);
        assertIndexEntry(f.indexTableId, oldIndexKey, targetBucket(f.indexSpec, "hello", 1), false);
    }

    /** Deleting a main-table row removes its Index Table entry. */
    @Test
    void testDeleteRemovesIndexEntry() throws Exception {
        String mainName = "main_t_delete";
        Fixture f = setupTables(mainName, /* visibility */ null);

        // (1) Insert a row, wait for the index entry to land.
        f.mainGateway
                .putKv(
                        newPutKvRequest(
                                f.mainTableId, 0, 1, genKvRecordBatch(new Object[] {1, "hello"})))
                .get();
        byte[] indexKey = encodeIndexKey(f.indexSpec, "hello", 1);
        assertIndexEntry(f.indexTableId, indexKey, targetBucket(f.indexSpec, "hello", 1), true);

        // (2) Delete the row by sending a KvRecord with a null value (tombstone) for the same PK.
        KvRecordBatch deleteBatch =
                genKvRecordBatch(
                        Collections.singletonList(Tuple2.of(new Object[] {1}, /* value */ null)));
        f.mainGateway.putKv(newPutKvRequest(f.mainTableId, 0, 1, deleteBatch)).get();

        // (3) The index entry for ("hello", 1) goes away.
        assertIndexEntry(f.indexTableId, indexKey, targetBucket(f.indexSpec, "hello", 1), false);
    }

    @Test
    void testEmptyWalBatchAdvancesSyncIndexOffset() throws Exception {
        String mainName = "main_t_empty_wal_batch";
        Fixture f = setupTables(mainName, /* visibility */ null);

        KvRecordBatch deleteMissingKey =
                genKvRecordBatch(
                        Collections.singletonList(Tuple2.of(new Object[] {404}, /* value */ null)));

        f.mainGateway
                .putKv(newPutKvRequest(f.mainTableId, 0, 1, deleteMissingKey))
                .get(30, TimeUnit.SECONDS);

        assertThat(f.mainLeaderReplica.getSyncIndexPushedOffset())
                .as("empty WAL batches must advance index progress to the batch next offset")
                .isEqualTo(1L);
    }

    /** An ASYNC index becomes visible eventually after the main-table write is acknowledged. */
    @Test
    void testAsyncVisibilityEventuallyVisible() throws Exception {
        String mainName = "main_t_async";
        Fixture f = setupTables(mainName, IndexVisibility.ASYNC);

        f.mainGateway
                .putKv(
                        newPutKvRequest(
                                f.mainTableId, 0, 1, genKvRecordBatch(new Object[] {1, "hello"})))
                .get();

        byte[] indexKey = encodeIndexKey(f.indexSpec, "hello", 1);
        waitForIndexEntry(f.indexTableId, indexKey, targetBucket(f.indexSpec, "hello", 1), true);
    }

    /** A recreated partition must be isolated from delayed writes of its previous incarnation. */
    @Test
    void testDropRecreatePartitionRejectsLateOldIndexWrites() throws Exception {
        String mainName = "main_t_partition_recreate";
        TablePath mainPath = TablePath.of(DB, mainName);
        Fixture fixture = setupPartitionedTables(mainName);
        createPartition(FLUSS_CLUSTER_EXTENSION, mainPath, partitionSpec(), false);
        long oldPartitionId =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionsCreated(mainPath, 1).get(PARTITION_NAME);

        int mainBucket = mainBucket(PRIMARY_KEY);
        TableBucket oldSource = new TableBucket(fixture.mainTableId, oldPartitionId, mainBucket);
        Replica oldMainReplica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(oldSource);
        int oldMainLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(oldSource);
        IndexSpec oldSpec =
                IndexSpecFactory.buildIndexSpecs(
                                oldMainReplica.getTableInfo(),
                                oldSource,
                                FLUSS_CLUSTER_EXTENSION
                                        .getTabletServerById(oldMainLeader)
                                        .getMetadataCache())
                        .get(0);
        IndexSpec.IndexEntry oldEntry = oldSpec.encodeEntry(partitionedMainRow());
        byte[] oldKey = oldEntry.key();
        BinaryRow oldValue = oldEntry.value().copy();
        int indexBucket = oldEntry.targetBucket();
        Replica target =
                FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(
                        new TableBucket(fixture.indexTableId, indexBucket));
        PutIndexRequest delayedUpsert =
                newPutIndexRequest(
                        fixture.indexTableId,
                        oldSpec,
                        oldSource,
                        indexBucket,
                        2L,
                        oldKey,
                        oldValue);
        PutIndexRequest delayedDelete =
                newPutIndexRequest(
                        fixture.indexTableId, oldSpec, oldSource, indexBucket, 3L, oldKey, null);
        byte[] oldProgressKey = oldSpec.encodeProgress(oldSource, indexBucket, 1L).key();

        putPartitionedMainRow(fixture.mainTableId, oldPartitionId, mainBucket);
        assertVisible(target, oldKey, true, "old partition incarnation is indexed");
        assertVisible(target, oldProgressKey, true, "old partition progress is durable");
        KvTablet targetKv = target.getKvTablet();
        org.apache.fluss.server.kv.KvTabletTestUtils.flushAndWait(targetKv, Long.MAX_VALUE);
        assertTaggedValue(targetKv.getRocksDBKv().get(oldKey), oldPartitionId, "old value tag");

        FLUSS_CLUSTER_EXTENSION
                .newCoordinatorClient()
                .dropPartition(newDropPartitionRequest(mainPath, partitionSpec(), false))
                .get();
        TabletServerMetadataCache metadataCache =
                FLUSS_CLUSTER_EXTENSION
                        .getTabletServerById(
                                FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(
                                        new TableBucket(fixture.indexTableId, indexBucket)))
                        .getMetadataCache();
        waitUntil(
                () ->
                        metadataCache
                                .getPartitionTombstone(fixture.mainTableId)
                                .isTombstoned(oldPartitionId),
                INDEX_VISIBILITY_TIMEOUT,
                "old partition tombstone publication");
        assertVisible(target, oldKey, false, "old incarnation is filtered immediately");
        assertVisible(target, oldProgressKey, false, "old progress is filtered immediately");
        assertThat(targetKv.getRocksDBKv().get(oldKey)).isNotNull();

        createPartition(FLUSS_CLUSTER_EXTENSION, mainPath, partitionSpec(), false);
        long newPartitionId = waitForRecreatedPartition(mainPath, oldPartitionId);
        TableBucket newSource = new TableBucket(fixture.mainTableId, newPartitionId, mainBucket);
        Replica newMainReplica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(newSource);
        int newMainLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(newSource);
        IndexSpec newSpec =
                IndexSpecFactory.buildIndexSpecs(
                                newMainReplica.getTableInfo(),
                                newSource,
                                FLUSS_CLUSTER_EXTENSION
                                        .getTabletServerById(newMainLeader)
                                        .getMetadataCache())
                        .get(0);
        IndexSpec.IndexEntry newEntry = newSpec.encodeEntry(partitionedMainRow());
        byte[] newKey = newEntry.key();
        assertThat(newKey).isNotEqualTo(oldKey);
        assertThat(newEntry.targetBucket()).isEqualTo(indexBucket);
        byte[] newProgressKey = newSpec.encodeProgress(newSource, indexBucket, 1L).key();

        putPartitionedMainRow(fixture.mainTableId, newPartitionId, mainBucket);
        assertVisible(target, newKey, true, "new partition incarnation is indexed");
        assertVisible(target, newProgressKey, true, "new partition progress is durable");
        org.apache.fluss.server.kv.KvTabletTestUtils.flushAndWait(targetKv, Long.MAX_VALUE);
        assertTaggedValue(targetKv.getRocksDBKv().get(newKey), newPartitionId, "new value tag");

        long walBeforeDelayed = target.getLogTablet().localLogEndOffset();
        putIndexMutation(delayedUpsert);
        putIndexMutation(delayedDelete);
        assertThat(target.getLogTablet().localLogEndOffset()).isEqualTo(walBeforeDelayed);
        assertVisible(target, newKey, true, "late old writes cannot affect the new incarnation");

        RocksDBKv rocks = targetKv.getRocksDBKv();
        rocks.getDb().compactRange();
        assertThat(rocks.get(oldKey)).isNull();
        assertThat(rocks.get(oldProgressKey)).isNull();
        assertTaggedValue(rocks.get(newKey), newPartitionId, "new row survives compaction");
        assertVisible(target, newKey, true, "new incarnation survives compaction");
    }

    /**
     * An index-safe {@code ALTER TABLE ADD COLUMN} on a table with secondary indexes must be
     * accepted, and the WAL-driven index replication must stay healthy across the schema evolution:
     * re-deriving the index specs on the leader still succeeds, and records written under the old
     * schema id keep replicating (WAL decoding keys off the record's schema id).
     */
    @Test
    void testSchemaEvolutionKeepsIndexReplicationHealthy() throws Exception {
        String mainName = "main_t_schema_evolution";
        TablePath mainPath = TablePath.of(DB, mainName);
        Fixture f = setupTables(mainName, /* visibility */ null);

        // (1) Write one batch under the original schema (id 1) and wait for the index entry.
        f.mainGateway
                .putKv(
                        newPutKvRequest(
                                f.mainTableId,
                                /* bucketId */ 0,
                                /* acks */ 1,
                                genKvRecordBatch(new Object[] {1, "before"})))
                .get(30, TimeUnit.SECONDS);
        assertIndexEntry(
                f.indexTableId,
                encodeIndexKey(f.indexSpec, "before", 1),
                targetBucket(f.indexSpec, "before", 1),
                true);

        // (2) ALTER TABLE ADD COLUMN at the last position — must be accepted for indexed tables.
        CoordinatorGateway adminGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        PbAddColumn addColumn = new PbAddColumn();
        addColumn
                .setColumnName("extra")
                .setDataTypeJson(
                        JsonSerdeUtils.writeValueAsBytes(
                                DataTypes.STRING(), DataTypeJsonSerde.INSTANCE))
                // LAST is the only supported position for ADD COLUMN.
                .setColumnPositionType(0);
        adminGateway
                .alterTable(
                        newAlterTableRequest(
                                mainPath,
                                Collections.emptyMap(),
                                Collections.emptyList(),
                                Collections.singletonList(addColumn),
                                false))
                .get(30, TimeUnit.SECONDS);

        // (3) The evolved schema (id 2) is committed and propagates to the tablet servers.
        GetTableInfoResponse tableInfoResponse =
                adminGateway.getTableInfo(newGetTableInfoRequest(mainPath)).get();
        assertThat(tableInfoResponse.getSchemaId()).isEqualTo(2);
        assertThat(
                        TableDescriptor.fromJsonBytes(tableInfoResponse.getTableJson())
                                .getSchema()
                                .getColumnNames())
                .containsExactly("a", "b", "extra");
        for (int bucket = 0; bucket < 3; bucket++) {
            Replica mainReplica =
                    FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(
                            new TableBucket(f.mainTableId, bucket));
            waitUntil(
                    () -> mainReplica.getSchemaGetter().getLatestSchemaInfo().getSchemaId() >= 2,
                    Duration.ofMinutes(1),
                    "schema id 2 not propagated to main-table replica " + bucket);
        }

        // (4) Writes under the old schema id still replicate to the Index Table (mixed-schema WAL
        // decoding by record schema id).
        f.mainGateway
                .putKv(
                        newPutKvRequest(
                                f.mainTableId,
                                /* bucketId */ 0,
                                /* acks */ 1,
                                genKvRecordBatch(new Object[] {2, "after"})))
                .get(30, TimeUnit.SECONDS);
        waitForIndexEntry(
                f.indexTableId,
                encodeIndexKey(f.indexSpec, "after", 2),
                targetBucket(f.indexSpec, "after", 2),
                true);
    }

    // ---------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------

    /** Per-scenario state: ids, paths, gateways, and the main-table leader replica handle. */
    private static final class Fixture {
        final long mainTableId;
        final long indexTableId;
        final Replica mainLeaderReplica;
        final TabletServerGateway mainGateway;
        @Nullable final IndexSpec indexSpec;

        Fixture(
                long mainTableId,
                long indexTableId,
                Replica mainLeaderReplica,
                TabletServerGateway mainGateway,
                @Nullable IndexSpec indexSpec) {
            this.mainTableId = mainTableId;
            this.indexTableId = indexTableId;
            this.mainLeaderReplica = mainLeaderReplica;
            this.mainGateway = mainGateway;
            this.indexSpec = indexSpec;
        }
    }

    /**
     * Creates the main table only. The Coordinator derives the matching Index Table as part of
     * {@code processCreateTable}. If {@code NotifyLeaderAndIsr} for the main table reaches the
     * TabletServer before the auto-derived Index Table's metadata broadcast, the scheduler init is
     * deferred and retried once the cache catches up. This helper polls until the scheduler is
     * wired so the rest of the test can proceed deterministically.
     */
    private static Fixture setupTables(String mainName, @Nullable IndexVisibility visibility)
            throws Exception {
        TablePath mainPath = TablePath.of(DB, mainName);

        TableDescriptor mainDescriptor = buildMainTableDescriptor(visibility);

        long mainTableId = createTable(FLUSS_CLUSTER_EXTENSION, mainPath, mainDescriptor);
        TablePath indexPath =
                TablePath.of(
                        DB, IndexTableUtils.indexTableName(mainPath.getTableName(), INDEX_NAME));
        TableBucket mainBucket = new TableBucket(mainTableId, 0);
        Replica mainLeaderReplica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(mainBucket);
        int mainLeaderServer = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(mainBucket);

        // Look up the derived Index Table id directly from ZK. The Coordinator persists it during
        // processCreateTable before returning.
        long indexTableId =
                FLUSS_CLUSTER_EXTENSION
                        .getZooKeeperClient()
                        .getTable(indexPath)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Auto-derived Index Table "
                                                        + indexPath
                                                        + " was not registered in ZK."))
                        .tableId;
        // Wait for ALL index bucket leaders to be ready (the push may target any bucket).
        for (int i = 0; i < INDEX_BUCKET_COUNT; i++) {
            FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(new TableBucket(indexTableId, i));
        }

        // Ensure the metadata cache on the main-table leader server has all index bucket leaders
        // resolved — the IndexReplicator needs getBucketLeader to return non-empty for the target
        // bucket, otherwise the push silently fails.
        final long idxTableId = indexTableId;
        TabletServerMetadataCache mainServerCache =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(mainLeaderServer).getMetadataCache();
        waitUntil(
                () -> {
                    for (int i = 0; i < INDEX_BUCKET_COUNT; i++) {
                        if (!mainServerCache.getBucketLeader(idxTableId, i).isPresent()) {
                            return false;
                        }
                    }
                    return true;
                },
                Duration.ofSeconds(30),
                "wait for all index bucket leaders to be resolved in metadata cache");

        TabletServerGateway mainGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(mainLeaderServer);
        IndexSpec indexSpec =
                IndexSpecFactory.buildIndexSpecs(
                                mainLeaderReplica.getTableInfo(), mainBucket, mainServerCache)
                        .get(0);
        return new Fixture(mainTableId, indexTableId, mainLeaderReplica, mainGateway, indexSpec);
    }

    /**
     * Sets up a partitioned main table so its auto-derived Index Table includes the {@code
     * __partition_id} system column and uses {@code KvFormat.COMPACTED}. Required for testing the
     * partition tombstone value filter which only installs on partitioned Index Tables.
     */
    private static Fixture setupPartitionedTables(String mainName) throws Exception {
        TablePath mainPath = TablePath.of(DB, mainName);

        Schema partitionedSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("p", DataTypes.STRING())
                        .primaryKey("a", "p")
                        .index(
                                INDEX_NAME,
                                IndexType.SECONDARY,
                                Collections.singletonList("b"),
                                IndexVisibility.SYNC,
                                3)
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(partitionedSchema)
                        .distributedBy(3, "a")
                        .partitionedBy("p")
                        .build();

        long mainTableId = createTable(FLUSS_CLUSTER_EXTENSION, mainPath, descriptor);
        TablePath indexPath =
                TablePath.of(
                        DB, IndexTableUtils.indexTableName(mainPath.getTableName(), INDEX_NAME));

        long indexTableId =
                FLUSS_CLUSTER_EXTENSION
                        .getZooKeeperClient()
                        .getTable(indexPath)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Auto-derived Index Table "
                                                        + indexPath
                                                        + " was not registered in ZK."))
                        .tableId;
        for (int i = 0; i < INDEX_BUCKET_COUNT; i++) {
            FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(new TableBucket(indexTableId, i));
        }

        return new Fixture(mainTableId, indexTableId, null, null, null);
    }

    private static final int INDEX_BUCKET_COUNT = 3;

    private static void assertIndexEntry(
            long indexTableId, byte[] key, int indexBucket, boolean expectPresent)
            throws Exception {
        boolean present = indexEntryPresent(indexTableId, key, indexBucket);
        assertThat(present)
                .as(
                        expectPresent
                                ? "index entry must be visible when the write is acknowledged"
                                : "index entry must be absent when the write is acknowledged")
                .isEqualTo(expectPresent);
    }

    /** Waits until an asynchronous Index Table update reaches the expected state. */
    private static void waitForIndexEntry(
            long indexTableId, byte[] key, int indexBucket, boolean expectPresent) {
        String desc =
                expectPresent
                        ? "wait for index entry to be visible on the Index Table"
                        : "wait for index entry to disappear from the Index Table";
        waitUntil(
                () -> {
                    try {
                        return indexEntryPresent(indexTableId, key, indexBucket) == expectPresent;
                    } catch (Exception e) {
                        return false;
                    }
                },
                INDEX_VISIBILITY_TIMEOUT,
                desc);
    }

    private static boolean indexEntryPresent(long indexTableId, byte[] key, int indexBucket)
            throws Exception {
        Replica replica =
                FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(
                        new TableBucket(indexTableId, indexBucket));
        return replica.lookups(Collections.singletonList(key)).get(0) != null;
    }

    private static Schema buildMainSchema(@Nullable IndexVisibility visibility) {
        return Schema.newBuilder()
                .column("a", DataTypes.INT())
                .column("b", DataTypes.STRING())
                .primaryKey("a")
                .index(
                        INDEX_NAME,
                        IndexType.SECONDARY,
                        Collections.singletonList("b"),
                        visibility == null ? IndexVisibility.SYNC : visibility,
                        3)
                .build();
    }

    private static TableDescriptor buildMainTableDescriptor(@Nullable IndexVisibility visibility) {
        return TableDescriptor.builder()
                .schema(buildMainSchema(visibility))
                .distributedBy(3, "a")
                .build();
    }

    private static byte[] encodeIndexKey(IndexSpec spec, String b, int a) {
        return spec.encodeEntry(GenericRow.of(a, fromString(b))).key();
    }

    private static int targetBucket(IndexSpec spec, String b, int a) {
        return spec.encodeEntry(GenericRow.of(a, fromString(b))).targetBucket();
    }

    private static PartitionSpec partitionSpec() {
        return new PartitionSpec(Collections.singletonMap("p", PARTITION_NAME));
    }

    private static GenericRow partitionedMainRow() {
        return GenericRow.of(PRIMARY_KEY, fromString(INDEXED_VALUE), fromString(PARTITION_NAME));
    }

    private static int mainBucket(int primaryKey) {
        GenericRow key = new GenericRow(1);
        key.setField(0, primaryKey);
        byte[] encoded = new CompactedKeyEncoder(MAIN_BUCKET_KEY_TYPE).encodeKey(key);
        return new FlussBucketingFunction().bucketing(encoded, 3);
    }

    private static void putPartitionedMainRow(long tableId, long partitionId, int bucket)
            throws Exception {
        KvRecordBatch records =
                genKvRecordBatch(
                        MAIN_BUCKET_KEY_TYPE,
                        PARTITIONED_MAIN_ROW_TYPE,
                        Collections.singletonList(
                                Tuple2.of(
                                        new Object[] {PRIMARY_KEY},
                                        new Object[] {
                                            PRIMARY_KEY, INDEXED_VALUE, PARTITION_NAME
                                        })));
        PutKvRequest request = newPutKvRequest(tableId, bucket, 1, records);
        request.getBucketsReqsList().get(0).setPartitionId(partitionId);
        int leader =
                FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(
                        new TableBucket(tableId, partitionId, bucket));
        assertSuccess(
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader).putKv(request).get());
    }

    private static PutIndexRequest newPutIndexRequest(
            long indexTableId,
            IndexSpec spec,
            TableBucket sourceBucket,
            int targetBucket,
            long sourceEndOffset,
            byte[] key,
            @Nullable BinaryRow value)
            throws Exception {
        IndexSpec.IndexEntry progress =
                spec.encodeProgress(sourceBucket, targetBucket, sourceEndOffset);
        IndexReplicator.BucketBatchBuilder builder =
                new IndexReplicator.BucketBatchBuilder(
                        (short) spec.getIndexSchemaId(), spec.getIndexKvFormat());
        if (value == null) {
            builder.appendDelete(key);
        } else {
            builder.appendUpsert(key, value);
        }
        BytesView records = builder.finish(progress.key(), progress.value());
        PutIndexRequest request =
                new PutIndexRequest()
                        .setTableId(indexTableId)
                        .setSourceTableId(sourceBucket.getTableId())
                        .setAcks(-1)
                        .setTimeoutMs(10_000);
        request.addBucketsReq()
                .setBucketId(targetBucket)
                .setSourcePartitionId(sourceBucket.getPartitionId())
                .setSourceBucketId(sourceBucket.getBucket())
                .setSourceEndOffset(sourceEndOffset)
                .setProgressKey(progress.key())
                .setRecordsBytesView(records);
        return request;
    }

    private static void putIndexMutation(PutIndexRequest request) throws Exception {
        int bucket = request.getBucketsReqsList().get(0).getBucketId();
        int leader =
                FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(
                        new TableBucket(request.getTableId(), bucket));
        PutIndexResponse response =
                FLUSS_CLUSTER_EXTENSION
                        .newTabletServerClientForNode(leader)
                        .putIndex(request)
                        .get();
        assertThat(response.getBucketsRespsList())
                .singleElement()
                .satisfies(result -> assertThat(result.hasErrorCode()).isFalse());
    }

    private static void assertVisible(
            Replica replica, byte[] key, boolean expected, String description) {
        waitUntil(
                () -> {
                    try {
                        return !replica.prefixLookup(key).isEmpty() == expected;
                    } catch (Exception e) {
                        return false;
                    }
                },
                INDEX_VISIBILITY_TIMEOUT,
                description);
    }

    private static void assertTaggedValue(byte[] value, long partitionId, String description) {
        assertThat(value).as(description).isNotNull();
        assertThat(KvValueLayout.TAGGED.readValueTag(MemorySegment.wrap(value)))
                .as(description)
                .isEqualTo(partitionId);
    }

    private static long waitForRecreatedPartition(TablePath mainPath, long oldPartitionId) {
        waitUntil(
                () ->
                        FLUSS_CLUSTER_EXTENSION
                                        .getZooKeeperClient()
                                        .getPartition(mainPath, PARTITION_NAME)
                                        .map(partition -> partition.getPartitionId())
                                        .orElse(oldPartitionId)
                                != oldPartitionId,
                INDEX_VISIBILITY_TIMEOUT,
                "new immutable partition incarnation");
        return FLUSS_CLUSTER_EXTENSION.waitUntilPartitionsCreated(mainPath, 1).get(PARTITION_NAME);
    }

    private static void assertSuccess(PutKvResponse response) {
        assertThat(response.getBucketsRespsList())
                .singleElement()
                .satisfies(result -> assertThat(result.hasErrorCode()).isFalse());
    }
}

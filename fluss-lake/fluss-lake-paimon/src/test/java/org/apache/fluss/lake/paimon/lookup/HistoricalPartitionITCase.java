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

package org.apache.fluss.lake.paimon.lookup;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.lake.paimon.testutils.FlinkPaimonTieringTestBase;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.flink.core.execution.JobClient;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.CloseableIterator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;
import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end IT case for historical partition writes, tiering, recovery, and lookup. */
class HistoricalPartitionITCase extends FlinkPaimonTieringTestBase {

    private static final String EXPIRED_PARTITION_NAME = "20240101";
    private static final String SECOND_EXPIRED_PARTITION_NAME = "20240102";
    private static final int INITIAL_PARTITION_RETENTION = 100000;
    private static final int EXPIRED_PARTITION_RETENTION = 1;
    private static final int PRE_RESCALE_BUCKET_NUM = 2;
    private static final int POST_RESCALE_BUCKET_NUM = 4;
    private static final int MAX_CANDIDATE_ID = 64;

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(initConfig())
                    .setNumOfTabletServers(3)
                    .build();

    @BeforeAll
    protected static void beforeAll() {
        FlinkPaimonTieringTestBase.beforeAll(FLUSS_CLUSTER_EXTENSION.getClientConfig());
    }

    @Test
    void testWriteAndTierHistoricalKvToPaimon() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "historical_write_tiering");
        Schema schema = partitionedPkSchema(true);
        long tableId =
                createTable(
                        tablePath,
                        partitionedDescriptor(schema, true, EXPIRED_PARTITION_RETENTION));

        try {
            long historicalPartitionId = waitUntilHistoricalPartitionReady(tablePath, tableId);

            InternalRow tieredRow = dataRow(true, 1, "unused", "Alice");
            assertThat(admin.listPartitionInfos(tablePath).get())
                    .noneMatch(p -> EXPIRED_PARTITION_NAME.equals(p.getPartitionName()));
            writeRows(tablePath, Collections.singletonList(tieredRow), false);
            // Historical writes must not recreate the expired original partition.
            assertThat(admin.listPartitionInfos(tablePath).get())
                    .noneMatch(p -> EXPIRED_PARTITION_NAME.equals(p.getPartitionName()));

            TableBucket historicalBucket = new TableBucket(tableId, historicalPartitionId, 0);
            assertThat(getLeaderReplica(historicalBucket).getLocalLogEndOffset()).isEqualTo(1);
            tierAndVerifyPaimonRows(
                    tablePath, historicalBucket, 1L, "1|" + EXPIRED_PARTITION_NAME + "|Alice");

            // Leave an untiered update after the Paimon snapshot. Restart recovery must apply this
            // changelog over the tiered row.
            InternalRow updatedRow = dataRow(true, 1, "unused", "Alice-updated");
            writeRows(tablePath, Collections.singletonList(updatedRow), false);
            // FULL changelog emits UPDATE_BEFORE and UPDATE_AFTER for the update.
            assertThat(getLeaderReplica(historicalBucket).getLocalLogEndOffset()).isEqualTo(3);
            assertThat(getLeaderReplica(historicalBucket).getLakeLogEndOffset()).isEqualTo(1);

            restartLeaderAndVerifyLookup(tablePath, historicalBucket, schema, updatedRow);

            // Verify that the recovered local state can resolve the previous value for another
            // update, and that a newly started tiering job can synchronize the resulting changelog.
            InternalRow postRecoveryRow = dataRow(true, 1, "unused", "Alice-after-recovery");
            writeRows(tablePath, Collections.singletonList(postRecoveryRow), false);
            assertThat(getLeaderReplica(historicalBucket).getLocalLogEndOffset()).isEqualTo(5);
            assertThat(getLeaderReplica(historicalBucket).getLakeLogEndOffset()).isEqualTo(1);
            tierAndVerifyPaimonRows(
                    tablePath,
                    historicalBucket,
                    5L,
                    "1|" + EXPIRED_PARTITION_NAME + "|Alice-after-recovery");
        } finally {
            dropTable(tablePath);
        }
    }

    @Test
    void testWriteAndTierHistoricalLogToPaimon() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "historical_log_write_tiering");
        long tableId =
                createTable(
                        tablePath,
                        partitionedDescriptor(
                                partitionedLogSchema(), true, EXPIRED_PARTITION_RETENTION));

        try {
            long historicalPartitionId = waitUntilHistoricalPartitionReady(tablePath, tableId);
            List<InternalRow> rows =
                    Arrays.asList(
                            row(1, EXPIRED_PARTITION_NAME, "Alice"),
                            row(2, EXPIRED_PARTITION_NAME, "Bob"));

            writeRows(tablePath, rows, true);
            // Historical writes must not recreate the expired original partition.
            assertThat(admin.listPartitionInfos(tablePath).get())
                    .noneMatch(p -> EXPIRED_PARTITION_NAME.equals(p.getPartitionName()));

            TableBucket historicalBucket = new TableBucket(tableId, historicalPartitionId, 0);
            assertThat(getLeaderReplica(historicalBucket).getLocalLogEndOffset()).isEqualTo(2);
            tierAndVerifyPaimonRows(
                    tablePath,
                    historicalBucket,
                    2L,
                    "1|" + EXPIRED_PARTITION_NAME + "|Alice",
                    "2|" + EXPIRED_PARTITION_NAME + "|Bob");
        } finally {
            dropTable(tablePath);
        }
    }

    @ParameterizedTest(name = "defaultBucketKey={0}")
    @ValueSource(booleans = {true, false})
    void testLookupExpiredPartitionFromPaimon(boolean defaultBucketKey) throws Exception {
        TablePath tablePath =
                TablePath.of(
                        DEFAULT_DB,
                        defaultBucketKey
                                ? "historical_lookup_default_bucket"
                                : "historical_lookup_bucket_subset");
        Schema oldSchema = partitionedPkSchema(defaultBucketKey);
        long tableId = createTable(tablePath, partitionedDescriptor(oldSchema, false));

        // Enable historical lookup through ALTER TABLE to cover dynamic creation of the
        // coordinator-owned historical system partition.
        admin.alterTable(
                        tablePath,
                        Collections.singletonList(
                                TableChange.set(
                                        ConfigOptions.TABLE_DATALAKE_HISTORICAL_PARTITION_ENABLED
                                                .key(),
                                        "true")),
                        false)
                .get();
        // The ALTER RPC must not complete until the required system partition is persisted.
        Optional<PartitionRegistration> historicalPartition =
                FLUSS_CLUSTER_EXTENSION
                        .getZooKeeperClient()
                        .getPartition(tablePath, HISTORICAL_PARTITION_VALUE);
        assertThat(historicalPartition).isPresent();
        long historicalPartitionId = historicalPartition.get().getPartitionId();
        FLUSS_CLUSTER_EXTENSION.waitUntilTablePartitionReady(tableId, historicalPartitionId);

        // Keep the initial retention wide enough so this old partition can be created and written
        // through the normal Fluss path before it is treated as historical.
        PartitionSpec expiredPartitionSpec = partitionSpec(EXPIRED_PARTITION_NAME);
        admin.createPartition(tablePath, expiredPartitionSpec, false).get();
        long partitionId = getPartitionId(tablePath, EXPIRED_PARTITION_NAME);
        FLUSS_CLUSTER_EXTENSION.waitUntilTablePartitionReady(tableId, partitionId);

        PartitionSpec secondExpiredPartitionSpec = partitionSpec(SECOND_EXPIRED_PARTITION_NAME);
        admin.createPartition(tablePath, secondExpiredPartitionSpec, false).get();
        long secondPartitionId = getPartitionId(tablePath, SECOND_EXPIRED_PARTITION_NAME);
        FLUSS_CLUSTER_EXTENSION.waitUntilTablePartitionReady(tableId, secondPartitionId);

        InternalRow expectedOldRow = dataRow(defaultBucketKey, 1, "sub-1", "Alice");
        InternalRow expectedSecondPartitionRow =
                dataRow(defaultBucketKey, 3, "sub-3", "Carol", SECOND_EXPIRED_PARTITION_NAME);
        writeRows(tablePath, Arrays.asList(expectedOldRow, expectedSecondPartitionRow), false);

        TableBucket tableBucket = new TableBucket(tableId, partitionId, 0);
        TableBucket secondTableBucket = new TableBucket(tableId, secondPartitionId, 0);
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshots(
                Arrays.asList(tableBucket, secondTableBucket));

        JobClient jobClient = buildTieringJob(execEnv);
        try {
            assertReplicaStatus(tableBucket, 1);
            assertReplicaStatus(secondTableBucket, 1);
        } finally {
            jobClient.cancel().get();
        }

        admin.alterTable(
                        tablePath,
                        Collections.singletonList(
                                TableChange.addColumn(
                                        "extra",
                                        DataTypes.STRING(),
                                        "extra column",
                                        TableChange.ColumnPosition.last())),
                        false)
                .get();
        Schema evolvedSchema = evolvedPartitionedPkSchema(defaultBucketKey);

        InternalRow expectedNewRow =
                evolvedDataRow(defaultBucketKey, 2, "sub-2", "Bob", "new-value");
        writeRows(tablePath, Collections.singletonList(expectedNewRow), false);
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshots(Collections.singleton(tableBucket));

        jobClient = buildTieringJob(execEnv);
        try {
            assertReplicaStatus(tableBucket, 2);
        } finally {
            jobClient.cancel().get();
        }

        // Cache the normal partition route before retention cleanup. The table-level historical
        // partition option is captured when the lookuper is created and enables fallback after the
        // original partitions are deleted.
        try (Connection lookupConn = ConnectionFactory.createConnection(clientConf);
                Table table = lookupConn.getTable(tablePath)) {
            Lookuper lookuper = table.newLookup().createLookuper();
            InternalRow lookupRow =
                    lookuper.lookup(lookupKey(defaultBucketKey, 2, "sub-2"))
                            .get()
                            .getSingletonRow();
            assertThatRow(lookupRow)
                    .withSchema(evolvedSchema.getRowType())
                    .isEqualTo(expectedNewRow);

            admin.alterTable(
                            tablePath,
                            Collections.singletonList(
                                    TableChange.set(
                                            ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION.key(),
                                            String.valueOf(EXPIRED_PARTITION_RETENTION))),
                            false)
                    .get();

            // Lowering retention triggers immediate auto-partition cleanup. Wait until both expired
            // partitions are removed before verifying historical lookup fallback.
            waitUntilPartitionDropped(tablePath, EXPIRED_PARTITION_NAME);
            waitUntilPartitionDropped(tablePath, SECOND_EXPIRED_PARTITION_NAME);

            // Submit both lookups before waiting so different original partitions that map to the
            // same historical TableBucket can be batched into one RPC and matched from its
            // original_partition_name responses.
            CompletableFuture<LookupResult> firstPartitionLookup =
                    lookuper.lookup(lookupKey(defaultBucketKey, 1, "sub-1"));
            CompletableFuture<LookupResult> secondPartitionLookup =
                    lookuper.lookup(
                            lookupKey(defaultBucketKey, 3, "sub-3", SECOND_EXPIRED_PARTITION_NAME));

            lookupRow = firstPartitionLookup.get().getSingletonRow();
            assertThatRow(lookupRow)
                    .withSchema(evolvedSchema.getRowType())
                    .isEqualTo(evolvedDataRow(defaultBucketKey, 1, "sub-1", "Alice", null));

            lookupRow = secondPartitionLookup.get().getSingletonRow();
            assertThatRow(lookupRow)
                    .withSchema(evolvedSchema.getRowType())
                    .isEqualTo(
                            evolvedDataRow(
                                    defaultBucketKey,
                                    3,
                                    "sub-3",
                                    "Carol",
                                    null,
                                    SECOND_EXPIRED_PARTITION_NAME));

            lookupRow =
                    lookuper.lookup(lookupKey(defaultBucketKey, 2, "sub-2"))
                            .get()
                            .getSingletonRow();
            assertThatRow(lookupRow)
                    .withSchema(evolvedSchema.getRowType())
                    .isEqualTo(expectedNewRow);
        }
        dropTable(tablePath);
    }

    /**
     * Changing bucket.num must not disable historical point lookup. The old partition keeps the
     * bucket layout it was tiered with while later partitions use the new count, so the lookup has
     * to be served from the bucket the lake data actually lives in.
     */
    @ParameterizedTest(name = "defaultBucketKey={0}")
    @ValueSource(booleans = {true, false})
    void testLookupExpiredPartitionAfterBucketNumRescale(boolean defaultBucketKey)
            throws Exception {
        TablePath tablePath =
                TablePath.of(
                        DEFAULT_DB,
                        defaultBucketKey
                                ? "historical_rescale_default_bucket"
                                : "historical_rescale_bucket_subset");
        Schema schema = partitionedPkSchema(defaultBucketKey);
        long tableId =
                createTable(tablePath, partitionedPkDescriptor(schema, PRE_RESCALE_BUCKET_NUM));

        // A key that lands in different buckets under the two layouts, so routing with the wrong
        // bucket count cannot accidentally hit the right lake bucket.
        int lookupId = idRoutedDifferentlyAcrossLayouts(defaultBucketKey, schema);

        admin.alterTable(
                        tablePath,
                        Collections.singletonList(
                                TableChange.set(
                                        ConfigOptions.TABLE_DATALAKE_HISTORICAL_PARTITION_ENABLED
                                                .key(),
                                        "true")),
                        false)
                .get();
        Optional<PartitionRegistration> historicalPartition =
                FLUSS_CLUSTER_EXTENSION
                        .getZooKeeperClient()
                        .getPartition(tablePath, HISTORICAL_PARTITION_VALUE);
        assertThat(historicalPartition).isPresent();
        FLUSS_CLUSTER_EXTENSION.waitUntilTablePartitionReady(
                tableId, historicalPartition.get().getPartitionId());

        // The old partition is created and written before the rescale, so it is laid out with
        // PRE_RESCALE_BUCKET_NUM buckets.
        admin.createPartition(tablePath, partitionSpec(EXPIRED_PARTITION_NAME), false).get();
        long oldPartitionId = getPartitionId(tablePath, EXPIRED_PARTITION_NAME);
        FLUSS_CLUSTER_EXTENSION.waitUntilTablePartitionReady(tableId, oldPartitionId);
        assertThat(bucketCountActualOf(tablePath, EXPIRED_PARTITION_NAME))
                .isEqualTo(PRE_RESCALE_BUCKET_NUM);

        InternalRow expectedOldRow =
                dataRow(defaultBucketKey, lookupId, "sub-" + lookupId, "Alice");
        writeRows(tablePath, Collections.singletonList(expectedOldRow), false);

        admin.alterTable(
                        tablePath,
                        Collections.singletonList(
                                TableChange.set(
                                        "bucket.num", String.valueOf(POST_RESCALE_BUCKET_NUM))),
                        false)
                .get();

        // A partition created after the rescale uses the new count, establishing mixed layouts.
        admin.createPartition(tablePath, partitionSpec(SECOND_EXPIRED_PARTITION_NAME), false).get();
        long newPartitionId = getPartitionId(tablePath, SECOND_EXPIRED_PARTITION_NAME);
        FLUSS_CLUSTER_EXTENSION.waitUntilTablePartitionReady(tableId, newPartitionId);
        assertThat(bucketCountActualOf(tablePath, SECOND_EXPIRED_PARTITION_NAME))
                .isEqualTo(POST_RESCALE_BUCKET_NUM);
        writeRows(
                tablePath,
                Collections.singletonList(
                        dataRow(
                                defaultBucketKey,
                                lookupId,
                                "sub-" + lookupId,
                                "Carol",
                                SECOND_EXPIRED_PARTITION_NAME)),
                false);

        // Snapshot only the buckets the rows were actually written to; a primary-key table is
        // tiered from its KV snapshots.
        Set<TableBucket> tableBuckets = new HashSet<>();
        tableBuckets.add(
                new TableBucket(
                        tableId,
                        oldPartitionId,
                        lakeBucketOf(defaultBucketKey, schema, lookupId, PRE_RESCALE_BUCKET_NUM)));
        tableBuckets.add(
                new TableBucket(
                        tableId,
                        newPartitionId,
                        lakeBucketOf(defaultBucketKey, schema, lookupId, POST_RESCALE_BUCKET_NUM)));
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshots(tableBuckets);

        JobClient jobClient = buildTieringJob(execEnv);
        try {
            // The tiered lake data of the old partition keeps the bucket count it was written with,
            // not the new table-level one.
            retry(
                    Duration.ofMinutes(2),
                    () ->
                            assertThat(totalBucketsOfPartition(tablePath, EXPIRED_PARTITION_NAME))
                                    .containsExactly(PRE_RESCALE_BUCKET_NUM));
        } finally {
            jobClient.cancel().get();
        }

        try (Connection lookupConn = ConnectionFactory.createConnection(clientConf);
                Table table = lookupConn.getTable(tablePath)) {
            Lookuper lookuper = table.newLookup().createLookuper();

            admin.alterTable(
                            tablePath,
                            Collections.singletonList(
                                    TableChange.set(
                                            ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION.key(),
                                            String.valueOf(EXPIRED_PARTITION_RETENTION))),
                            false)
                    .get();
            waitUntilPartitionDropped(tablePath, EXPIRED_PARTITION_NAME);

            InternalRow lookupRow =
                    lookuper.lookup(lookupKey(defaultBucketKey, lookupId, "sub-" + lookupId))
                            .get()
                            .getSingletonRow();
            assertThatRow(lookupRow).withSchema(schema.getRowType()).isEqualTo(expectedOldRow);
        }
        dropTable(tablePath);
    }

    /**
     * Returns an id whose bucket differs between the pre-rescale and post-rescale layouts, so a
     * lookup routed with the wrong bucket count cannot accidentally read the right lake bucket.
     */
    private static int idRoutedDifferentlyAcrossLayouts(boolean defaultBucketKey, Schema schema) {
        for (int id = 1; id <= MAX_CANDIDATE_ID; id++) {
            if (lakeBucketOf(defaultBucketKey, schema, id, PRE_RESCALE_BUCKET_NUM)
                    != lakeBucketOf(defaultBucketKey, schema, id, POST_RESCALE_BUCKET_NUM)) {
                return id;
            }
        }
        throw new AssertionError(
                "No id within "
                        + MAX_CANDIDATE_ID
                        + " candidates is routed to different buckets by the "
                        + PRE_RESCALE_BUCKET_NUM
                        + "- and "
                        + POST_RESCALE_BUCKET_NUM
                        + "-bucket layouts, so this test could not detect routing with the wrong "
                        + "bucket count.");
    }

    /** Computes the lake bucket of a lookup key the same way the write and lookup paths do. */
    private static int lakeBucketOf(
            boolean defaultBucketKey, Schema schema, int id, int bucketNum) {
        RowType lookupRowType = schema.getRowType().project(schema.getPrimaryKeyColumnNames());
        KeyEncoder bucketKeyEncoder =
                KeyEncoder.ofBucketKeyEncoder(
                        lookupRowType, Collections.singletonList("id"), DataLakeFormat.PAIMON);
        byte[] bucketKey = bucketKeyEncoder.encodeKey(lookupKey(defaultBucketKey, id, "sub-" + id));
        return BucketingFunction.of(DataLakeFormat.PAIMON).bucketing(bucketKey, bucketNum);
    }

    /** Reads the bucket count a Fluss partition was created with. */
    private static int bucketCountActualOf(TablePath tablePath, String partitionName)
            throws Exception {
        Optional<PartitionRegistration> partition =
                FLUSS_CLUSTER_EXTENSION.getZooKeeperClient().getPartition(tablePath, partitionName);
        assertThat(partition).isPresent();
        return partition.get().getBucketCountActual();
    }

    /** Reads the bucket counts the tiered lake data of a partition was written with. */
    private static Set<Integer> totalBucketsOfPartition(TablePath tablePath, String partitionName)
            throws Exception {
        FileStoreTable fileStoreTable =
                (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
        List<Split> splits =
                fileStoreTable
                        .newReadBuilder()
                        .withPartitionFilter(Collections.singletonMap("dt", partitionName))
                        .newScan()
                        .plan()
                        .splits();
        assertThat(splits)
                .withFailMessage(
                        "No lake splits for partition %s; all lake splits: %s",
                        partitionName, fileStoreTable.newReadBuilder().newScan().plan().splits())
                .isNotEmpty();
        Set<Integer> totalBuckets = new HashSet<>();
        for (Split split : splits) {
            totalBuckets.add(((DataSplit) split).totalBuckets());
        }
        return totalBuckets;
    }

    @Override
    protected FlussClusterExtension getFlussClusterExtension() {
        return FLUSS_CLUSTER_EXTENSION;
    }

    private static long waitUntilHistoricalPartitionReady(TablePath tablePath, long tableId)
            throws Exception {
        Optional<PartitionRegistration> historicalPartition =
                FLUSS_CLUSTER_EXTENSION
                        .getZooKeeperClient()
                        .getPartition(tablePath, HISTORICAL_PARTITION_VALUE);
        assertThat(historicalPartition).isPresent();
        long partitionId = historicalPartition.get().getPartitionId();
        FLUSS_CLUSTER_EXTENSION.waitUntilTablePartitionReady(tableId, partitionId);
        return partitionId;
    }

    private List<String> readPaimonRows(TablePath tablePath) throws Exception {
        List<String> actualRows = new ArrayList<>();
        try (CloseableIterator<org.apache.paimon.data.InternalRow> rows =
                getPaimonRowCloseableIterator(tablePath)) {
            while (rows.hasNext()) {
                org.apache.paimon.data.InternalRow row = rows.next();
                actualRows.add(row.getInt(0) + "|" + row.getString(1) + "|" + row.getString(2));
            }
        }
        return actualRows;
    }

    private void tierAndVerifyPaimonRows(
            TablePath tablePath,
            TableBucket historicalBucket,
            long expectedLogEndOffset,
            String... expectedRows)
            throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            assertReplicaStatus(historicalBucket, expectedLogEndOffset);
            checkFlussOffsetsInSnapshot(
                    tablePath, Collections.singletonMap(historicalBucket, expectedLogEndOffset));
            assertThat(readPaimonRows(tablePath)).containsExactlyInAnyOrder(expectedRows);
        } finally {
            jobClient.cancel().get();
        }
    }

    private void restartLeaderAndVerifyLookup(
            TablePath tablePath,
            TableBucket historicalBucket,
            Schema schema,
            InternalRow expectedRow)
            throws Exception {
        int tabletServerId = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(historicalBucket);
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(tabletServerId);
        try {
            FLUSS_CLUSTER_EXTENSION.startTabletServer(tabletServerId);
            FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(historicalBucket);

            try (Connection connection = ConnectionFactory.createConnection(clientConf);
                    Table table = connection.getTable(tablePath)) {
                InternalRow actualRow =
                        table.newLookup()
                                .createLookuper()
                                .lookup(lookupKey(true, 1, "unused"))
                                .get()
                                .getSingletonRow();
                assertThatRow(actualRow).withSchema(schema.getRowType()).isEqualTo(expectedRow);
            }
        } finally {
            if (FLUSS_CLUSTER_EXTENSION.getTabletServerById(tabletServerId) == null) {
                FLUSS_CLUSTER_EXTENSION.startTabletServer(tabletServerId);
            }
        }
    }

    private static Schema partitionedPkSchema(boolean defaultBucketKey) {
        if (defaultBucketKey) {
            return Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("dt", DataTypes.STRING())
                    .column("name", DataTypes.STRING())
                    .primaryKey("id", "dt")
                    .build();
        }
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("sub_id", DataTypes.STRING())
                .column("dt", DataTypes.STRING())
                .column("name", DataTypes.STRING())
                .primaryKey("id", "sub_id", "dt")
                .build();
    }

    private static Schema partitionedLogSchema() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("dt", DataTypes.STRING())
                .column("name", DataTypes.STRING())
                .build();
    }

    private static Schema evolvedPartitionedPkSchema(boolean defaultBucketKey) {
        if (defaultBucketKey) {
            return Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("dt", DataTypes.STRING())
                    .column("name", DataTypes.STRING())
                    .column("extra", DataTypes.STRING())
                    .primaryKey("id", "dt")
                    .build();
        }
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("sub_id", DataTypes.STRING())
                .column("dt", DataTypes.STRING())
                .column("name", DataTypes.STRING())
                .column("extra", DataTypes.STRING())
                .primaryKey("id", "sub_id", "dt")
                .build();
    }

    private static TableDescriptor partitionedDescriptor(
            Schema schema, boolean historicalPartitionEnabled) {
        return partitionedDescriptor(
                schema, historicalPartitionEnabled, INITIAL_PARTITION_RETENTION);
    }

    private static TableDescriptor partitionedDescriptor(
            Schema schema, boolean historicalPartitionEnabled, int partitionRetention) {
        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .schema(schema)
                        // For primary-key tables, id is the default bucket key for (id, dt) and a
                        // strict subset of the physical primary key for (id, sub_id, dt).
                        .distributedBy(1, "id")
                        .partitionedBy("dt")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_KEY, "dt")
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.DAY)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION,
                                partitionRetention)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE, "UTC")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500));
        if (historicalPartitionEnabled) {
            builder.property(ConfigOptions.TABLE_DATALAKE_HISTORICAL_PARTITION_ENABLED, true);
        }
        return builder.build();
    }

    /** Same as {@link #partitionedDescriptor} but with an explicit table-level bucket count. */
    private static TableDescriptor partitionedPkDescriptor(Schema schema, int bucketNum) {
        return TableDescriptor.builder()
                .schema(schema)
                // This is the default bucket key for (id, dt), and a strict subset of the physical
                // primary key for (id, sub_id, dt).
                .distributedBy(bucketNum, "id")
                .partitionedBy("dt")
                .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                .property(ConfigOptions.TABLE_AUTO_PARTITION_KEY, "dt")
                .property(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, AutoPartitionTimeUnit.DAY)
                .property(
                        ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION,
                        INITIAL_PARTITION_RETENTION)
                .property(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE, "UTC")
                .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                .build();
    }

    private static InternalRow dataRow(
            boolean defaultBucketKey, int id, String subId, String name) {
        return dataRow(defaultBucketKey, id, subId, name, EXPIRED_PARTITION_NAME);
    }

    private static InternalRow dataRow(
            boolean defaultBucketKey, int id, String subId, String name, String partitionName) {
        return defaultBucketKey
                ? row(id, partitionName, name)
                : row(id, subId, partitionName, name);
    }

    private static InternalRow evolvedDataRow(
            boolean defaultBucketKey, int id, String subId, String name, String extra) {
        return evolvedDataRow(defaultBucketKey, id, subId, name, extra, EXPIRED_PARTITION_NAME);
    }

    private static InternalRow evolvedDataRow(
            boolean defaultBucketKey,
            int id,
            String subId,
            String name,
            String extra,
            String partitionName) {
        return defaultBucketKey
                ? row(id, partitionName, name, extra)
                : row(id, subId, partitionName, name, extra);
    }

    private static InternalRow lookupKey(boolean defaultBucketKey, int id, String subId) {
        return lookupKey(defaultBucketKey, id, subId, EXPIRED_PARTITION_NAME);
    }

    private static InternalRow lookupKey(
            boolean defaultBucketKey, int id, String subId, String partitionName) {
        return defaultBucketKey ? row(id, partitionName) : row(id, subId, partitionName);
    }

    private static PartitionSpec partitionSpec(String partitionName) {
        return new PartitionSpec(Collections.singletonMap("dt", partitionName));
    }

    private static long getPartitionId(TablePath tablePath, String partitionName) throws Exception {
        List<PartitionInfo> partitionInfos = admin.listPartitionInfos(tablePath).get();
        for (PartitionInfo partitionInfo : partitionInfos) {
            if (partitionName.equals(partitionInfo.getPartitionName())) {
                return partitionInfo.getPartitionId();
            }
        }
        throw new IllegalStateException("Partition " + partitionName + " does not exist.");
    }

    private static void waitUntilPartitionDropped(TablePath tablePath, String partitionName) {
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(admin.listPartitionInfos(tablePath).get())
                                .noneMatch(p -> partitionName.equals(p.getPartitionName())));
    }
}

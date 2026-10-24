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

package org.apache.fluss.client.table;

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.IndexTableUtils;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for secondary index lookup, covering non-partitioned and partitioned tables.
 */
class FlussTableSecondaryIndexITCase extends ClientToServerITCaseBase {

    private static final String DB = "test_db_sec_idx";

    /** Bounded poll deadline for index visibility checks. */
    private static final Duration INDEX_VISIBILITY_TIMEOUT = Duration.ofSeconds(30);

    @Test
    void indexTableRejectsPublicUpsertWriterCreation() throws Exception {
        TablePath mainPath = TablePath.of(DB, "test_public_index_write_guard");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .index(
                                "idx_name",
                                IndexType.SECONDARY,
                                Collections.singletonList("name"),
                                IndexVisibility.SYNC,
                                2)
                        .build();
        long mainTableId =
                createTable(
                        mainPath,
                        TableDescriptor.builder().schema(schema).distributedBy(2, "id").build(),
                        true);

        TablePath indexPath = indexTablePath(mainPath, "idx_name");
        waitAllReplicasReady(admin.getTableInfo(indexPath).get().getTableId(), 2);
        try (Table mainTable = conn.getTable(mainPath);
                Table indexTable = conn.getTable(indexPath)) {
            assertThat(indexTable.getTableInfo().isIndexTable()).isTrue();
            assertThat(countRows(indexTable)).isZero();

            assertThatThrownBy(indexTable::newUpsert)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(indexPath.toString())
                    .hasMessageContaining("internal secondary index table");

            assertThat(countRows(indexTable)).isZero();
            assertThat(mainTable.newUpsert()).isNotNull();
        }
    }

    private static int countRows(Table table) throws Exception {
        int rowCount = 0;
        try (BatchScanner scanner = table.newScan().limit(1).createBatchScanner()) {
            CloseableIterator<InternalRow> nextBatch;
            while ((nextBatch = scanner.pollBatch(Duration.ofSeconds(30))) != null) {
                try (CloseableIterator<InternalRow> batch = nextBatch) {
                    while (batch.hasNext()) {
                        batch.next();
                        rowCount++;
                    }
                }
            }
        }
        return rowCount;
    }

    private static InternalRow lookupSingleRow(Table table, String indexName, Object key)
            throws Exception {
        Lookuper lookuper = table.getSecondaryIndexLookuper(indexName);
        waitUntil(
                () -> !lookuper.lookup(row(key)).get().getRowList().isEmpty(),
                INDEX_VISIBILITY_TIMEOUT,
                "index lookup for " + indexName);
        List<InternalRow> rows = lookuper.lookup(row(key)).get().getRowList();
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getInt(0)).isEqualTo(1);
        return rows.get(0);
    }

    @Test
    void testSecondaryIndexLookup() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_sec_idx_lookup");

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("email", DataTypes.STRING())
                        .primaryKey("id")
                        .index(
                                "idx_name",
                                IndexType.SECONDARY,
                                Arrays.asList("name"),
                                IndexVisibility.SYNC,
                                3)
                        .index(
                                "idx_email",
                                IndexType.SECONDARY,
                                Arrays.asList("email"),
                                IndexVisibility.SYNC,
                                3)
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "id").build();

        createTable(tablePath, descriptor, true);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            upsertWriter.upsert(row(1, "Alice", "alice@example.com"));
            upsertWriter.upsert(row(2, "Bob", "bob@example.com"));
            upsertWriter.upsert(row(3, "Charlie", "charlie@example.com"));
            upsertWriter.upsert(row(4, "Diana", "diana@example.com"));
            upsertWriter.upsert(row(5, "Eve", "eve@example.com"));
            upsertWriter.flush();

            Lookuper nameLookuper = table.getSecondaryIndexLookuper("idx_name");
            Lookuper emailLookuper = table.getSecondaryIndexLookuper("idx_email");

            // SYNC index writes must be visible when flush returns.
            LookupResult aliceResult = nameLookuper.lookup(row("Alice")).get();
            assertThat(aliceResult.getRowList()).hasSize(1);
            InternalRow aliceRow = aliceResult.getRowList().get(0);
            assertThat(aliceRow.getInt(0)).isEqualTo(1);
            assertThat(aliceRow.getString(1).toString()).isEqualTo("Alice");
            assertThat(aliceRow.getString(2).toString()).isEqualTo("alice@example.com");

            LookupResult bobResult = emailLookuper.lookup(row("bob@example.com")).get();
            assertThat(bobResult.getRowList()).hasSize(1);
            InternalRow bobRow = bobResult.getRowList().get(0);
            assertThat(bobRow.getInt(0)).isEqualTo(2);
            assertThat(bobRow.getString(1).toString()).isEqualTo("Bob");
            assertThat(bobRow.getString(2).toString()).isEqualTo("bob@example.com");

            // Verify non-existent value returns empty.
            LookupResult notFoundResult = nameLookuper.lookup(row("NonExistent")).get();
            assertThat(notFoundResult.getRowList()).isEmpty();
        }
    }

    @Test
    void testCompositeSecondaryIndexLookup() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_composite_sec_idx_lookup");

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("city", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .primaryKey("id")
                        .index(
                                "idx_city_age",
                                IndexType.SECONDARY,
                                Arrays.asList("city", "age"),
                                IndexVisibility.SYNC,
                                3)
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "id").build();

        createTable(tablePath, descriptor, true);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            upsertWriter.upsert(row(1, "Shanghai", 30));
            upsertWriter.upsert(row(2, "Shanghai", 40));
            upsertWriter.upsert(row(3, "Beijing", 30));
            upsertWriter.flush();

            Lookuper lookuper = table.getSecondaryIndexLookuper("idx_city_age");

            LookupResult shanghai30 = lookuper.lookup(row("Shanghai", 30)).get();
            assertThat(shanghai30.getRowList()).hasSize(1);
            InternalRow shanghai30Row = shanghai30.getRowList().get(0);
            assertThat(shanghai30Row.getInt(0)).isEqualTo(1);
            assertThat(shanghai30Row.getString(1).toString()).isEqualTo("Shanghai");
            assertThat(shanghai30Row.getInt(2)).isEqualTo(30);

            LookupResult beijing30 = lookuper.lookup(row("Beijing", 30)).get();
            assertThat(beijing30.getRowList()).hasSize(1);
            assertThat(beijing30.getRowList().get(0).getInt(0)).isEqualTo(3);

            // Decisive for a composite key: both components exist on their own ("Beijing" on id 3,
            // 40 on id 2) but never together, so keying on either column alone would wrongly match.
            assertThat(lookuper.lookup(row("Beijing", 40)).get().getRowList()).isEmpty();

            // Moving only the second component must relocate the row across index keys.
            upsertWriter.upsert(row(1, "Shanghai", 31));
            upsertWriter.flush();
            assertThat(lookuper.lookup(row("Shanghai", 30)).get().getRowList()).isEmpty();
            LookupResult shanghai31 = lookuper.lookup(row("Shanghai", 31)).get();
            assertThat(shanghai31.getRowList()).hasSize(1);
            assertThat(shanghai31.getRowList().get(0).getInt(0)).isEqualTo(1);
        }
    }

    @Test
    void testPartitionedTableSecondaryIndexLookup() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_partitioned_sec_idx_lookup");

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("email", DataTypes.STRING())
                        .column("year", DataTypes.STRING())
                        .primaryKey("id", "year")
                        .index(
                                "idx_name",
                                IndexType.SECONDARY,
                                Arrays.asList("name"),
                                IndexVisibility.SYNC,
                                3)
                        .index(
                                "idx_email",
                                IndexType.SECONDARY,
                                Arrays.asList("email"),
                                IndexVisibility.SYNC,
                                3)
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "id")
                        .partitionedBy("year")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.YEAR)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE, 0)
                        .build();

        long mainTableId = createTable(tablePath, descriptor, true);

        // Wait for non-partitioned index tables to have their replicas ready.
        TablePath nameIdxPath = indexTablePath(tablePath, "idx_name");
        TablePath emailIdxPath = indexTablePath(tablePath, "idx_email");
        long nameIdxTableId = admin.getTableInfo(nameIdxPath).get().getTableId();
        long emailIdxTableId = admin.getTableInfo(emailIdxPath).get().getTableId();
        waitAllReplicasReady(nameIdxTableId, 3);
        waitAllReplicasReady(emailIdxTableId, 3);

        // Manually create partitions "2023" and "2024".
        admin.createPartition(tablePath, newPartitionSpec("year", "2023"), true).get();
        admin.createPartition(tablePath, newPartitionSpec("year", "2024"), true).get();

        // Wait for both partitions to be registered.
        FLUSS_CLUSTER_EXTENSION.waitUntilPartitionsCreated(tablePath, 2);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            // Write cross-partition data.
            upsertWriter.upsert(row(1, "Alice", "alice@example.com", "2023"));
            upsertWriter.upsert(row(2, "Bob", "bob@example.com", "2023"));
            upsertWriter.upsert(row(3, "Charlie", "charlie@example.com", "2024"));
            upsertWriter.upsert(row(4, "Diana", "diana@example.com", "2024"));
            upsertWriter.flush();

            Lookuper nameLookuper = table.getSecondaryIndexLookuper("idx_name");
            Lookuper emailLookuper = table.getSecondaryIndexLookuper("idx_email");

            // Wait for name index to become visible.
            waitUntil(
                    () -> {
                        LookupResult r = nameLookuper.lookup(row("Alice")).get();
                        return !r.getRowList().isEmpty();
                    },
                    INDEX_VISIBILITY_TIMEOUT,
                    "wait for idx_name entry 'Alice' (partition 2023) to become visible");

            // Verify name lookup returns data from correct partition (2023).
            LookupResult aliceResult = nameLookuper.lookup(row("Alice")).get();
            assertThat(aliceResult.getRowList()).hasSize(1);
            InternalRow aliceRow = aliceResult.getRowList().get(0);
            assertThat(aliceRow.getInt(0)).isEqualTo(1);
            assertThat(aliceRow.getString(1).toString()).isEqualTo("Alice");
            assertThat(aliceRow.getString(3).toString()).isEqualTo("2023");

            // Verify email lookup returns data from correct partition (2024).
            waitUntil(
                    () -> {
                        LookupResult r = emailLookuper.lookup(row("charlie@example.com")).get();
                        return !r.getRowList().isEmpty();
                    },
                    INDEX_VISIBILITY_TIMEOUT,
                    "wait for idx_email entry 'charlie@example.com' (partition 2024) to become visible");

            LookupResult charlieResult = emailLookuper.lookup(row("charlie@example.com")).get();
            assertThat(charlieResult.getRowList()).hasSize(1);
            InternalRow charlieRow = charlieResult.getRowList().get(0);
            assertThat(charlieRow.getInt(0)).isEqualTo(3);
            assertThat(charlieRow.getString(2).toString()).isEqualTo("charlie@example.com");
            assertThat(charlieRow.getString(3).toString()).isEqualTo("2024");
        }
    }

    @Test
    void testPartitionedIndexContinuesAfterDroppingAnotherPartition() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_partitioned_index_after_drop");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("year", DataTypes.STRING())
                        .primaryKey("id", "year")
                        .index(
                                "idx_name",
                                IndexType.SECONDARY,
                                Collections.singletonList("name"),
                                IndexVisibility.SYNC,
                                1)
                        .build();
        long mainTableId =
                createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(schema)
                                .distributedBy(1, "id")
                                .partitionedBy("year")
                                .build(),
                        true);
        TablePath indexTablePath = indexTablePath(tablePath, "idx_name");
        waitAllReplicasReady(admin.getTableInfo(indexTablePath).get().getTableId(), 1);

        admin.createPartition(tablePath, newPartitionSpec("year", "2023"), false).get();
        admin.createPartition(tablePath, newPartitionSpec("year", "2024"), false).get();
        FLUSS_CLUSTER_EXTENSION.waitUntilPartitionsCreated(tablePath, 2);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            Lookuper lookuper = table.getSecondaryIndexLookuper("idx_name");

            writer.upsert(row(1, "Alice", "2023"));
            writer.upsert(row(2, "Bob", "2024"));
            writer.flush();
            assertSinglePartitionedRow(lookuper, "Alice", 1, "2023");
            assertSinglePartitionedRow(lookuper, "Bob", 2, "2024");

            admin.dropPartition(tablePath, newPartitionSpec("year", "2023"), false).get();
            waitUntil(
                    () ->
                            admin.listPartitionInfos(tablePath, newPartitionSpec("year", "2023"))
                                    .get()
                                    .isEmpty(),
                    INDEX_VISIBILITY_TIMEOUT,
                    "wait for partition 2023 to be removed");

            // Reuse the existing writer and lookuper to prove an active client can continue after
            // an unrelated partition is dropped.
            writer.upsert(row(3, "Charlie", "2024"));
            writer.flush();

            waitUntil(
                    () -> lookuper.lookup(row("Alice")).get().getRowList().isEmpty(),
                    INDEX_VISIBILITY_TIMEOUT,
                    "wait for the dropped partition row to become invisible");
            assertThat(lookuper.lookup(row("Alice")).get().getRowList()).isEmpty();
            assertSinglePartitionedRow(lookuper, "Bob", 2, "2024");
            assertSinglePartitionedRow(lookuper, "Charlie", 3, "2024");
        }
    }

    @Test
    void testSecondaryIndexSurvivesRepeatedPartitionRecreation() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_partition_recreation_churn");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("year", DataTypes.STRING())
                        .primaryKey("id", "year")
                        .index(
                                "idx_name",
                                IndexType.SECONDARY,
                                Collections.singletonList("name"),
                                IndexVisibility.SYNC,
                                3)
                        .build();
        long mainTableId =
                createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(schema)
                                .distributedBy(3, "id")
                                .partitionedBy("year")
                                .build(),
                        true);
        waitAllReplicasReady(
                admin.getTableInfo(indexTablePath(tablePath, "idx_name")).get().getTableId(), 3);

        PartitionSpec stablePartition = newPartitionSpec("year", "stable");
        PartitionSpec churnPartition = newPartitionSpec("year", "churn");
        admin.createPartition(tablePath, stablePartition, false).get();
        long stablePartitionId =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionsCreated(tablePath, 1).get("stable");

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            Lookuper lookuper = table.getSecondaryIndexLookuper("idx_name");
            writer.upsert(row(1, "stable-anchor", "stable"));
            writer.flush();
            assertSinglePartitionedRow(lookuper, "stable-anchor", 1, "stable");

            long previousPartitionId = stablePartitionId;
            for (int generation = 0; generation < 4; generation++) {
                admin.createPartition(tablePath, churnPartition, false).get();
                Map<String, Long> partitions =
                        FLUSS_CLUSTER_EXTENSION.waitUntilPartitionsCreated(tablePath, 2);
                long partitionId = partitions.get("churn");
                assertThat(partitionId).isGreaterThan(previousPartitionId);

                String indexValue = "churn-generation-" + generation;
                int rowId = 100 + generation;
                writer.upsert(row(rowId, indexValue, "churn"));
                writer.flush();
                assertSinglePartitionedRow(lookuper, indexValue, rowId, "churn");
                assertSinglePartitionedRow(lookuper, "stable-anchor", 1, "stable");

                admin.dropPartition(tablePath, churnPartition, false).get();
                waitUntil(
                        () -> admin.listPartitionInfos(tablePath, churnPartition).get().isEmpty(),
                        INDEX_VISIBILITY_TIMEOUT,
                        "wait for churn generation " + generation + " to be removed");
                waitUntil(
                        () ->
                                FLUSS_CLUSTER_EXTENSION.getTabletServers().stream()
                                        .allMatch(
                                                tabletServer ->
                                                        tabletServer
                                                                .getMetadataCache()
                                                                .getInitializedPartitionTombstone(
                                                                        mainTableId)
                                                                .map(
                                                                        tombstone ->
                                                                                tombstone
                                                                                        .isTombstoned(
                                                                                                partitionId))
                                                                .orElse(false)),
                        INDEX_VISIBILITY_TIMEOUT,
                        "all tablet servers observe churn tombstone " + partitionId);
                waitUntil(
                        () -> lookuper.lookup(row(indexValue)).get().getRowList().isEmpty(),
                        INDEX_VISIBILITY_TIMEOUT,
                        "retired churn generation " + generation + " is invisible");
                assertSinglePartitionedRow(lookuper, "stable-anchor", 1, "stable");

                previousPartitionId = partitionId;
            }

            admin.dropPartition(tablePath, stablePartition, false).get();
            waitUntil(
                    () -> admin.listPartitionInfos(tablePath, stablePartition).get().isEmpty(),
                    INDEX_VISIBILITY_TIMEOUT,
                    "wait for stable partition to be removed");
            long highestRetiredPartitionId = previousPartitionId;
            waitUntil(
                    () ->
                            FLUSS_CLUSTER_EXTENSION.getTabletServers().stream()
                                    .allMatch(
                                            tabletServer ->
                                                    tabletServer
                                                            .getMetadataCache()
                                                            .getInitializedPartitionTombstone(
                                                                    mainTableId)
                                                            .map(
                                                                    tombstone ->
                                                                            tombstone.getFloor()
                                                                                            >= highestRetiredPartitionId
                                                                                    && tombstone
                                                                                            .getExplicitSet()
                                                                                            .isEmpty())
                                                            .orElse(false)),
                    INDEX_VISIBILITY_TIMEOUT,
                    "all tablet servers compact retired generations into the tombstone floor");
            waitUntil(
                    () -> lookuper.lookup(row("stable-anchor")).get().getRowList().isEmpty(),
                    INDEX_VISIBILITY_TIMEOUT,
                    "stable partition row is invisible after drop");

            admin.createPartition(tablePath, churnPartition, false).get();
            long latestPartitionId =
                    FLUSS_CLUSTER_EXTENSION.waitUntilPartitionsCreated(tablePath, 1).get("churn");
            assertThat(latestPartitionId).isGreaterThan(highestRetiredPartitionId);
            writer.upsert(row(999, "latest-generation", "churn"));
            writer.flush();
            assertSinglePartitionedRow(lookuper, "latest-generation", 999, "churn");
            for (int generation = 0; generation < 4; generation++) {
                assertThat(
                                lookuper.lookup(row("churn-generation-" + generation))
                                        .get()
                                        .getRowList())
                        .isEmpty();
            }
        }
    }

    @Test
    void testSecondaryIndexLookupReturnsEveryWrittenRow() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_massive_data");

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("email", DataTypes.STRING())
                        .primaryKey("id")
                        .index(
                                "idx_name",
                                IndexType.SECONDARY,
                                Arrays.asList("name"),
                                IndexVisibility.SYNC,
                                3)
                        .index(
                                "idx_email",
                                IndexType.SECONDARY,
                                Arrays.asList("email"),
                                IndexVisibility.SYNC,
                                3)
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "id").build();

        createTable(tablePath, descriptor, true);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            int totalRows = 120;
            int nameGroups = 12;
            for (int id = 0; id < totalRows; id++) {
                upsertWriter.upsert(row(id, "name-" + (id % nameGroups), "user-" + id));
            }
            upsertWriter.flush();

            Lookuper nameLookuper = table.getSecondaryIndexLookuper("idx_name");
            Lookuper emailLookuper = table.getSecondaryIndexLookuper("idx_email");

            for (int group = 0; group < nameGroups; group++) {
                String name = "name-" + group;
                List<InternalRow> rows = nameLookuper.lookup(row(name)).get().getRowList();
                assertThat(rows)
                        .extracting(r -> r.getInt(0))
                        .containsExactlyInAnyOrderElementsOf(
                                expectedIds(group, totalRows, nameGroups));
                assertThat(rows)
                        .allSatisfy(r -> assertThat(r.getString(1).toString()).isEqualTo(name));
            }
            for (int id = 0; id < totalRows; id++) {
                int expectedId = id;
                List<InternalRow> rows = emailLookuper.lookup(row("user-" + id)).get().getRowList();
                assertThat(rows)
                        .singleElement()
                        .satisfies(r -> assertThat(r.getInt(0)).isEqualTo(expectedId));
            }

            // Negative anchor: values that were never written must resolve to no index entry, so a
            // bug returning rows for absent keys would be caught.
            assertThat(
                            nameLookuper
                                    .lookup(row("NoSuchName_42"))
                                    .get(INDEX_VISIBILITY_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                                    .getRowList())
                    .isEmpty();
            assertThat(emailLookuper.lookup(row("missing-user")).get().getRowList()).isEmpty();
        }
    }

    @Test
    void testSecondaryIndexLookupWithIntIndexColumn() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_int_index_lookup");

        Schema schema =
                Schema.newBuilder()
                        .column("fact_id", DataTypes.INT())
                        .column("product_id", DataTypes.INT())
                        .column("amount", DataTypes.INT())
                        .primaryKey("fact_id")
                        .index(
                                "idx_product_id",
                                IndexType.SECONDARY,
                                Arrays.asList("product_id"),
                                IndexVisibility.SYNC,
                                3)
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "fact_id").build();

        createTable(tablePath, descriptor, true);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            for (int i = 1; i <= 15; i++) {
                upsertWriter.upsert(row(i, i, i * 100));
            }
            upsertWriter.flush();

            Lookuper indexLookuper = table.getSecondaryIndexLookuper("idx_product_id");

            // Wait for all entries to become visible
            for (int i = 1; i <= 15; i++) {
                final int productId = i;
                waitUntil(
                        () -> !indexLookuper.lookup(row(productId)).get().getRowList().isEmpty(),
                        INDEX_VISIBILITY_TIMEOUT,
                        "index lookup for product_id=" + productId);
            }

            // Verify specific lookup
            LookupResult result = indexLookuper.lookup(row(1)).get();
            assertThat(result.getRowList()).hasSize(1);
            InternalRow resultRow = result.getRowList().get(0);
            assertThat(resultRow.getInt(0)).isEqualTo(1);
            assertThat(resultRow.getInt(1)).isEqualTo(1);
            assertThat(resultRow.getInt(2)).isEqualTo(100);
        }
    }

    @Test
    void testSecondaryIndexLookupRoundTripsSupportedKeyTypes() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_index_key_type_round_trip");
        Schema schema =
                Schema.newBuilder()
                        .column("fact_id", DataTypes.INT())
                        .column("order_id", DataTypes.BIGINT())
                        .column("price", DataTypes.DECIMAL(10, 2))
                        .column("event_time", DataTypes.TIMESTAMP())
                        .column("payload", DataTypes.BYTES())
                        .column("amount", DataTypes.INT())
                        .primaryKey("fact_id")
                        .index(
                                "idx_order_id",
                                IndexType.SECONDARY,
                                Arrays.asList("order_id"),
                                IndexVisibility.SYNC,
                                3)
                        .index(
                                "idx_price",
                                IndexType.SECONDARY,
                                Arrays.asList("price"),
                                IndexVisibility.SYNC,
                                3)
                        .index(
                                "idx_event_time",
                                IndexType.SECONDARY,
                                Arrays.asList("event_time"),
                                IndexVisibility.SYNC,
                                3)
                        .index(
                                "idx_payload",
                                IndexType.SECONDARY,
                                Arrays.asList("payload"),
                                IndexVisibility.SYNC,
                                3)
                        .build();
        createTable(
                tablePath,
                TableDescriptor.builder().schema(schema).distributedBy(3, "fact_id").build(),
                true);

        long orderId = 9_000_000_000L;
        BigDecimal priceValue = new BigDecimal("123.45");
        Decimal price = Decimal.fromBigDecimal(priceValue, 10, 2);
        TimestampNtz eventTime = TimestampNtz.fromMillis(1_700_000_000_000L);
        byte[] payload = new byte[] {1, 2, 3};
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, orderId, price, eventTime, payload, 100));
            writer.flush();

            InternalRow bigintRow = lookupSingleRow(table, "idx_order_id", orderId);
            assertThat(bigintRow.getLong(1)).isEqualTo(orderId);

            InternalRow decimalRow = lookupSingleRow(table, "idx_price", price);
            assertThat(decimalRow.getDecimal(2, 10, 2).toBigDecimal())
                    .isEqualByComparingTo(priceValue);

            InternalRow timestampRow = lookupSingleRow(table, "idx_event_time", eventTime);
            assertThat(timestampRow.getTimestampNtz(3, 6)).isEqualTo(eventTime);

            InternalRow bytesRow = lookupSingleRow(table, "idx_payload", new byte[] {1, 2, 3});
            assertThat(bytesRow.getBytes(4)).containsExactly(1, 2, 3);
        }
    }

    @Test
    void testSecondaryIndexLookupWithBooleanIndexColumn() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_boolean_index_lookup");
        Schema schema =
                Schema.newBuilder()
                        .column("fact_id", DataTypes.INT())
                        .column("active", DataTypes.BOOLEAN())
                        .column("amount", DataTypes.INT())
                        .primaryKey("fact_id")
                        .index(
                                "idx_active",
                                IndexType.SECONDARY,
                                Arrays.asList("active"),
                                IndexVisibility.SYNC,
                                3)
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "fact_id").build();
        createTable(tablePath, descriptor, true);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, true, 100));
            writer.upsert(row(2, false, 200));
            writer.upsert(row(3, true, 300));
            writer.flush();

            Lookuper indexLookuper = table.getSecondaryIndexLookuper("idx_active");
            // A boolean index is low-cardinality: lookup(true) must return both true rows.
            waitUntil(
                    () -> indexLookuper.lookup(row(true)).get().getRowList().size() == 2,
                    INDEX_VISIBILITY_TIMEOUT,
                    "index lookup for active=true returns both rows");

            List<Integer> trueFactIds = new ArrayList<>();
            for (InternalRow r : indexLookuper.lookup(row(true)).get().getRowList()) {
                assertThat(r.getBoolean(1)).isTrue();
                trueFactIds.add(r.getInt(0));
            }
            assertThat(trueFactIds).containsExactlyInAnyOrder(1, 3);

            LookupResult falseResult = indexLookuper.lookup(row(false)).get();
            assertThat(falseResult.getRowList()).hasSize(1);
            assertThat(falseResult.getRowList().get(0).getInt(0)).isEqualTo(2);
        }
    }

    @Test
    void testPartialUpdateOmittingNotNullIndexColumnIsRejected() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_pu_notnull_idx_rejected");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT().copy(false))
                        .column("user_id", DataTypes.BIGINT().copy(false)) // NOT NULL index column
                        .column("note", DataTypes.STRING().copy(true))
                        .primaryKey("id")
                        .index(
                                "idx_user",
                                IndexType.SECONDARY,
                                Arrays.asList("user_id"),
                                IndexVisibility.SYNC,
                                3)
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "id").build();
        createTable(tablePath, descriptor, true);

        try (Table table = conn.getTable(tablePath)) {
            // A partial update that omits the NOT NULL index column must be rejected at writer
            // construction: index columns are NOT exempted from the partial-update nullable rule.
            assertThatThrownBy(() -> table.newUpsert().partialUpdate("id", "note").createWriter())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(
                            "Partial Update requires all columns except primary key to be nullable")
                    .hasMessageContaining("user_id");
        }
    }

    @Test
    void testPartialUpdatePreservesOmittedNullableIndexColumn() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_pu_nullable_idx_accepted");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT().copy(false))
                        .column("user_id", DataTypes.BIGINT().copy(true)) // nullable index column
                        .column("note", DataTypes.STRING().copy(true))
                        .primaryKey("id")
                        .index(
                                "idx_user",
                                IndexType.SECONDARY,
                                Arrays.asList("user_id"),
                                IndexVisibility.SYNC,
                                3)
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "id").build();
        createTable(tablePath, descriptor, true);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter fullWriter = table.newUpsert().createWriter();
            fullWriter.upsert(row(1L, 42L, "before"));
            fullWriter.flush();

            UpsertWriter partialWriter =
                    table.newUpsert().partialUpdate("id", "note").createWriter();
            partialWriter.upsert(row(1L, null, "after"));
            partialWriter.flush();

            InternalRow mainRow =
                    table.newLookup().createLookuper().lookup(row(1L)).get().getSingletonRow();
            assertThat(mainRow.getLong(0)).isEqualTo(1L);
            assertThat(mainRow.getLong(1)).isEqualTo(42L);
            assertThat(mainRow.getString(2).toString()).isEqualTo("after");

            LookupResult indexResult =
                    table.getSecondaryIndexLookuper("idx_user").lookup(row(42L)).get();
            assertThat(indexResult.getRowList()).hasSize(1);
            InternalRow indexedRow = indexResult.getRowList().get(0);
            assertThat(indexedRow.getLong(0)).isEqualTo(1L);
            assertThat(indexedRow.getLong(1)).isEqualTo(42L);
            assertThat(indexedRow.getString(2).toString()).isEqualTo("after");
        }
    }

    @Test
    void testSecondaryIndexLookupWithoutDistributedBy() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_no_dist_by_index");

        Schema schema =
                Schema.newBuilder()
                        .column("fact_id", DataTypes.INT())
                        .column("product_id", DataTypes.INT())
                        .column("amount", DataTypes.INT())
                        .primaryKey("fact_id")
                        .index(
                                "idx_product_id",
                                IndexType.SECONDARY,
                                Arrays.asList("product_id"),
                                IndexVisibility.SYNC,
                                3)
                        .build();

        TableDescriptor descriptor = TableDescriptor.builder().schema(schema).build();

        createTable(tablePath, descriptor, true);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            for (int i = 1; i <= 15; i++) {
                upsertWriter.upsert(row(i, i, i * 100));
            }
            upsertWriter.flush();

            // Verify PK lookup on main table works
            Lookuper pkLookuper = table.newLookup().createLookuper();
            for (int i = 1; i <= 3; i++) {
                LookupResult pkResult = pkLookuper.lookup(row(i)).get();
                assertThat(pkResult.getRowList()).hasSize(1);
            }

            // Verify secondary index lookup
            Lookuper indexLookuper = table.getSecondaryIndexLookuper("idx_product_id");

            for (int i = 1; i <= 15; i++) {
                final int pid = i;
                waitUntil(
                        () -> !indexLookuper.lookup(row(pid)).get().getRowList().isEmpty(),
                        INDEX_VISIBILITY_TIMEOUT,
                        "secondary index lookup for product_id=" + pid);
                LookupResult r = indexLookuper.lookup(row(pid)).get();
                assertThat(r.getRowList())
                        .as("Secondary index lookup for product_id=" + pid)
                        .hasSize(1);
            }
        }
    }

    @Test
    void testSparseIndexWithNullIndexColumn() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_sparse_null");

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("email", DataTypes.STRING())
                        .primaryKey("id")
                        .index(
                                "idx_name",
                                IndexType.SECONDARY,
                                Arrays.asList("name"),
                                IndexVisibility.SYNC,
                                3)
                        .index(
                                "idx_email",
                                IndexType.SECONDARY,
                                Arrays.asList("email"),
                                IndexVisibility.SYNC,
                                3)
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "id").build();

        createTable(tablePath, descriptor, true);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();

            // Write rows with NULL index columns — sparse index skips them
            upsertWriter.upsert(row(1, null, "test1@example.com"));
            upsertWriter.upsert(row(2, "TestName", null));
            upsertWriter.upsert(row(3, null, null));
            // Also write a normal row for contrast
            upsertWriter.upsert(row(4, "ValidName", "valid@example.com"));
            upsertWriter.flush();

            // Verify PK lookup still works for all rows
            Lookuper pkLookuper = table.newLookup().createLookuper();
            assertThat(pkLookuper.lookup(row(1)).get().getRowList()).hasSize(1);
            assertThat(pkLookuper.lookup(row(2)).get().getRowList()).hasSize(1);
            assertThat(pkLookuper.lookup(row(3)).get().getRowList()).hasSize(1);

            // Verify the valid row IS indexed
            Lookuper nameLookuper = table.getSecondaryIndexLookuper("idx_name");
            waitUntil(
                    () -> !nameLookuper.lookup(row("ValidName")).get().getRowList().isEmpty(),
                    INDEX_VISIBILITY_TIMEOUT,
                    "name index for ValidName");
            LookupResult validResult = nameLookuper.lookup(row("ValidName")).get();
            assertThat(validResult.getRowList()).hasSize(1);
            assertThat(validResult.getRowList().get(0).getInt(0)).isEqualTo(4);
        }
    }

    @Test
    void testSparseIndexUpdateToNull() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_sparse_update_null");

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("email", DataTypes.STRING())
                        .primaryKey("id")
                        .index(
                                "idx_name",
                                IndexType.SECONDARY,
                                Arrays.asList("name"),
                                IndexVisibility.SYNC,
                                3)
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "id").build();

        createTable(tablePath, descriptor, true);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();

            // Write a row with non-NULL index column
            upsertWriter.upsert(row(1, "Alice", "alice@example.com"));
            upsertWriter.flush();

            Lookuper nameLookuper = table.getSecondaryIndexLookuper("idx_name");

            // Verify index lookup works
            waitUntil(
                    () -> !nameLookuper.lookup(row("Alice")).get().getRowList().isEmpty(),
                    INDEX_VISIBILITY_TIMEOUT,
                    "Alice should be findable");
            assertThat(nameLookuper.lookup(row("Alice")).get().getRowList()).hasSize(1);

            // Update the row to have NULL index column
            upsertWriter.upsert(row(1, null, "alice_updated@example.com"));
            upsertWriter.flush();

            // Verify main table has updated value
            Lookuper pkLookuper = table.newLookup().createLookuper();
            waitUntil(
                    () -> {
                        InternalRow r = pkLookuper.lookup(row(1)).get().getRowList().get(0);
                        return r.isNullAt(1);
                    },
                    INDEX_VISIBILITY_TIMEOUT,
                    "main table should reflect NULL name");
            InternalRow result = pkLookuper.lookup(row(1)).get().getRowList().get(0);
            assertThat(result.isNullAt(1)).isTrue();
            assertThat(result.getString(2).toString()).isEqualTo("alice_updated@example.com");

            // Verify old index entry is removed (lookup by "Alice" → empty)
            waitUntil(
                    () -> nameLookuper.lookup(row("Alice")).get().getRowList().isEmpty(),
                    INDEX_VISIBILITY_TIMEOUT,
                    "Alice index entry should be deleted");
            assertThat(nameLookuper.lookup(row("Alice")).get().getRowList()).isEmpty();
        }
    }

    @Test
    void testIndexColumnRemainsNullableForSparseIndex() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_nullable_idx_col");

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("email", DataTypes.STRING())
                        .primaryKey("id")
                        .index(
                                "idx_name",
                                IndexType.SECONDARY,
                                Arrays.asList("name"),
                                IndexVisibility.SYNC,
                                3)
                        .index(
                                "idx_email",
                                IndexType.SECONDARY,
                                Arrays.asList("email"),
                                IndexVisibility.SYNC,
                                3)
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "id").build();

        createTable(tablePath, descriptor, true);

        // Verify schema nullability via admin API
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        Schema serverSchema = tableInfo.getSchema();
        RowType rowType = serverSchema.getRowType();

        assertThat(rowType.getField("name").getType().isNullable())
                .as("Index column 'name' should remain nullable for sparse index")
                .isTrue();
        assertThat(rowType.getField("email").getType().isNullable())
                .as("Index column 'email' should remain nullable for sparse index")
                .isTrue();
        assertThat(rowType.getField("id").getType().isNullable())
                .as("Primary key 'id' should be NOT NULL")
                .isFalse();
    }

    private static TablePath indexTablePath(TablePath mainTablePath, String indexName) {
        return TablePath.of(
                DB, IndexTableUtils.indexTableName(mainTablePath.getTableName(), indexName));
    }

    private static void assertSinglePartitionedRow(
            Lookuper lookuper, String indexValue, int expectedId, String expectedPartition) {
        AtomicReference<LookupResult> result = new AtomicReference<>();
        waitUntil(
                () -> {
                    LookupResult lookupResult = lookuper.lookup(row(indexValue)).get();
                    if (lookupResult.getRowList().size() != 1) {
                        return false;
                    }
                    result.set(lookupResult);
                    return true;
                },
                INDEX_VISIBILITY_TIMEOUT,
                "wait for exactly one index row for " + indexValue);

        InternalRow actual = result.get().getRowList().get(0);
        assertThat(actual.getInt(0)).isEqualTo(expectedId);
        assertThat(actual.getString(1).toString()).isEqualTo(indexValue);
        assertThat(actual.getString(2).toString()).isEqualTo(expectedPartition);
    }

    private static List<Integer> expectedIds(int group, int rowCount, int groupCount) {
        List<Integer> ids = new ArrayList<>();
        for (int id = group; id < rowCount; id += groupCount) {
            ids.add(id);
        }
        return ids;
    }
}

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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.client.table.getter.PartitionGetter;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.lake.paimon.testutils.FlinkPaimonTieringTestBase;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.types.Tuple2;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.CloseableIterator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** IT case for tiering tables to paimon. */
class PaimonTieringITCase extends FlinkPaimonTieringTestBase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(initConfig())
                    .setNumOfTabletServers(3)
                    .build();

    protected static final String DEFAULT_DB = "fluss";

    private static StreamExecutionEnvironment execEnv;
    private static Catalog paimonCatalog;

    @BeforeAll
    protected static void beforeAll() {
        FlinkPaimonTieringTestBase.beforeAll(FLUSS_CLUSTER_EXTENSION.getClientConfig());

        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(2);
        execEnv.enableCheckpointing(1000);
        paimonCatalog = getPaimonCatalog();
    }

    @Test
    void testTiering() throws Exception {
        // create a pk table, write some records and wait until snapshot finished
        TablePath t1 = TablePath.of(DEFAULT_DB, "pkTable");
        long t1Id = createPkTable(t1);
        TableBucket t1Bucket = new TableBucket(t1Id, 0);
        // write records
        List<InternalRow> rows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));
        writeRows(t1, rows, false);
        triggerAndWaitSnapshot(t1Id, 1);

        // then start tiering job
        JobClient jobClient = buildTieringJob(execEnv);

        try {
            // check the status of replica after synced
            assertReplicaStatus(t1Bucket, 3);
            // check data in paimon
            checkDataInPaimonPrimaryKeyTable(t1, rows);
            checkFlussOffsetsInSnapshot(t1, Collections.singletonMap(new TableBucket(t1Id, 0), 3L));

            // then, create another log table
            TablePath t2 = TablePath.of(DEFAULT_DB, "logTable");
            long t2Id = createLogTable(t2);
            TableBucket t2Bucket = new TableBucket(t2Id, 0);
            List<InternalRow> flussRows = new ArrayList<>();
            // write records
            for (int i = 0; i < 10; i++) {
                rows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));
                flussRows.addAll(rows);
                // write records
                writeRows(t2, rows, true);
            }
            // check the status of replica after synced;
            // note: we can't update log start offset for unaware bucket mode log table
            assertReplicaStatus(t2Bucket, 30);
            assertThat(getLeaderReplica(t2Bucket).getLogTablet().getLakeMaxTimestamp())
                    .isGreaterThan(-1);

            // check data in paimon
            checkDataInPaimonAppendOnlyTable(t2, flussRows, 0);

            // then write data to the pk tables
            // write records
            rows = Arrays.asList(row(1, "v111"), row(2, "v222"), row(3, "v333"));
            // write records
            writeRows(t1, rows, false);

            // check the status of replica of t2 after synced
            // not check start offset since we won't
            // update start log offset for primary key table
            assertReplicaStatus(t1Bucket, 9);

            checkDataInPaimonPrimaryKeyTable(t1, rows);

            // then create partitioned table and wait partitions are ready
            TablePath partitionedTablePath = TablePath.of(DEFAULT_DB, "partitionedTable");
            Tuple2<Long, TableDescriptor> tableIdAndDescriptor =
                    createPartitionedTable(partitionedTablePath);
            Map<Long, String> partitionNameByIds = waitUntilPartitions(partitionedTablePath);

            // now, write rows into partitioned table
            TableDescriptor partitionedTableDescriptor = tableIdAndDescriptor.f1;
            Map<String, List<InternalRow>> writtenRowsByPartition =
                    writeRowsIntoPartitionedTable(
                            partitionedTablePath, partitionedTableDescriptor, partitionNameByIds);
            long tableId = tableIdAndDescriptor.f0;

            Map<TableBucket, Long> expectedOffsets = new HashMap<>();

            // wait until synced to paimon
            for (Long partitionId : partitionNameByIds.keySet()) {
                TableBucket tableBucket = new TableBucket(tableId, partitionId, 0);
                assertReplicaStatus(tableBucket, 3);
                expectedOffsets.put(tableBucket, 3L);
            }

            // now, let's check data in paimon per partition
            // check data in paimon
            String partitionCol = partitionedTableDescriptor.getPartitionKeys().get(0);
            for (String partitionName : partitionNameByIds.values()) {
                checkDataInPaimonAppendOnlyPartitionedTable(
                        partitionedTablePath,
                        Collections.singletonMap(partitionCol, partitionName),
                        writtenRowsByPartition.get(partitionName),
                        0);
            }
            checkFlussOffsetsInSnapshot(partitionedTablePath, expectedOffsets);
        } finally {
            jobClient.cancel().get();
        }
    }

    private static Stream<Arguments> tieringAllTypesWriteArgs() {
        return Stream.of(Arguments.of(true), Arguments.of(false));
    }

    @ParameterizedTest
    @MethodSource("tieringAllTypesWriteArgs")
    void testTieringForAllTypes(boolean isPrimaryKeyTable) throws Exception {
        // create a table, write some records and wait until snapshot finished
        TablePath t1 =
                TablePath.of(
                        DEFAULT_DB,
                        isPrimaryKeyTable ? "pkTableForAllTypes" : "logTableForAllTypes");
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("c0", DataTypes.STRING())
                        .column("c1", DataTypes.BOOLEAN())
                        .column("c2", DataTypes.TINYINT())
                        .column("c3", DataTypes.SMALLINT())
                        .column("c4", DataTypes.INT())
                        .column("c5", DataTypes.BIGINT())
                        .column("c6", DataTypes.FLOAT())
                        .column("c7", DataTypes.DOUBLE())
                        // decimal not support for partition key
                        .column("c8", DataTypes.DECIMAL(10, 2))
                        .column("c9", DataTypes.CHAR(10))
                        .column("c10", DataTypes.STRING())
                        .column("c11", DataTypes.BYTES())
                        .column("c12", DataTypes.BINARY(5))
                        .column("c13", DataTypes.DATE())
                        .column("c14", DataTypes.TIME(6))
                        .column("c15", DataTypes.TIMESTAMP(6))
                        .column("c16", DataTypes.TIMESTAMP_LTZ(6));
        if (isPrimaryKeyTable) {
            builder.primaryKey(
                    "c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c9", "c10", "c11", "c12",
                    "c13", "c14", "c15", "c16");
        }
        List<String> partitionKeys =
                Arrays.asList(
                        "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c9", "c10", "c11", "c12", "c13",
                        "c14", "c15", "c16");
        TableDescriptor.Builder tableDescriptor =
                TableDescriptor.builder()
                        .schema(builder.build())
                        .distributedBy(1, "c0")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500));
        tableDescriptor.partitionedBy(partitionKeys);
        tableDescriptor.customProperties(Collections.emptyMap());
        tableDescriptor.properties(Collections.emptyMap());
        long t1Id = createTable(t1, tableDescriptor.build());

        // write records
        List<InternalRow> rows =
                Collections.singletonList(
                        row(
                                BinaryString.fromString("v0"),
                                true,
                                (byte) 1,
                                (short) 2,
                                3,
                                4L,
                                5.0f,
                                6.0,
                                Decimal.fromBigDecimal(new BigDecimal("0.09"), 10, 2),
                                BinaryString.fromString("v1"),
                                BinaryString.fromString("v2"),
                                "v3".getBytes(StandardCharsets.UTF_8),
                                new byte[] {1, 2, 3, 4, 5},
                                (int) LocalDate.of(2025, 10, 16).toEpochDay(),
                                (int)
                                        (LocalTime.of(10, 10, 10, 123000000).toNanoOfDay()
                                                / 1_000_000),
                                TimestampNtz.fromLocalDateTime(
                                        LocalDateTime.of(2025, 10, 16, 10, 10, 10, 123000000)),
                                TimestampLtz.fromInstant(
                                        Instant.parse("2025-10-16T10:10:10.123Z"))));
        writeRows(t1, rows, !isPrimaryKeyTable);

        TableInfo tableInfo = admin.getTableInfo(t1).get();
        List<PartitionInfo> partitionInfos = admin.listPartitionInfos(t1).get();
        assertThat(partitionInfos.size()).isEqualTo(1);
        PartitionGetter partitionGetter =
                new PartitionGetter(tableInfo.getRowType(), partitionKeys);
        String partition = partitionGetter.getPartition(rows.get(0));
        assertThat(partitionInfos.get(0).getPartitionName()).isEqualTo(partition);

        long partitionId = partitionInfos.get(0).getPartitionId();
        TableBucket t1Bucket = new TableBucket(t1Id, partitionId, 0);

        // then start tiering job
        JobClient jobClient = buildTieringJob(execEnv);

        try {
            // check the status of replica after synced
            assertReplicaStatus(t1Bucket, 1);

            // check data in paimon
            Iterator<org.apache.paimon.data.InternalRow> paimonRowIterator =
                    getPaimonRowCloseableIterator(t1);
            for (InternalRow expectedRow : rows) {
                org.apache.paimon.data.InternalRow row = paimonRowIterator.next();
                assertThat(row.getString(0).toString())
                        .isEqualTo(expectedRow.getString(0).toString());
                assertThat(row.getBoolean(1)).isEqualTo(expectedRow.getBoolean(1));
                assertThat(row.getByte(2)).isEqualTo(expectedRow.getByte(2));
                assertThat(row.getShort(3)).isEqualTo(expectedRow.getShort(3));
                assertThat(row.getInt(4)).isEqualTo(expectedRow.getInt(4));
                assertThat(row.getLong(5)).isEqualTo(expectedRow.getLong(5));
                assertThat(row.getFloat(6)).isEqualTo(expectedRow.getFloat(6));
                assertThat(row.getDouble(7)).isEqualTo(expectedRow.getDouble(7));
                assertThat(row.getDecimal(8, 10, 2).toBigDecimal())
                        .isEqualTo(expectedRow.getDecimal(8, 10, 2).toBigDecimal());
                assertThat(row.getString(9).toString())
                        .isEqualTo(expectedRow.getString(9).toString());
                assertThat(row.getString(10).toString())
                        .isEqualTo(expectedRow.getString(10).toString());
                assertThat(row.getBinary(11)).isEqualTo(expectedRow.getBytes(11));
                assertThat(row.getBinary(12)).isEqualTo(expectedRow.getBinary(12, 5));
                assertThat(row.getInt(13)).isEqualTo(expectedRow.getInt(13));
                assertThat(row.getInt(14)).isEqualTo(expectedRow.getInt(14));
                assertThat(row.getTimestamp(15, 6).getMillisecond())
                        .isEqualTo(expectedRow.getTimestampNtz(15, 6).getMillisecond());
                assertThat(row.getTimestamp(16, 6).getMillisecond())
                        .isEqualTo(expectedRow.getTimestampLtz(16, 6).getEpochMillisecond());

                checkFlussOffsetsInSnapshot(t1, Collections.singletonMap(t1Bucket, 1L));
            }
        } finally {
            jobClient.cancel().get();
        }
    }

    @Test
    void testTieringForAlterTable() throws Exception {
        TablePath t1 = TablePath.of(DEFAULT_DB, "pkTableAlter");
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "false");

        long t1Id = createPkTable(t1, 1, tableProperties, Collections.emptyMap());

        TableChange.SetOption setOption =
                TableChange.set(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true");
        List<TableChange> changes = Collections.singletonList(setOption);
        admin.alterTable(t1, changes, false).get();

        TableBucket t1Bucket = new TableBucket(t1Id, 0);

        // write records
        List<InternalRow> rows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));
        writeRows(t1, rows, false);
        triggerAndWaitSnapshot(t1Id, 1);

        // then start tiering job
        JobClient jobClient = buildTieringJob(execEnv);

        try {
            // check the status of replica after synced
            assertReplicaStatus(t1Bucket, 3);
            // check data in paimon
            checkDataInPaimonPrimaryKeyTable(t1, rows);
            checkFlussOffsetsInSnapshot(t1, Collections.singletonMap(t1Bucket, 3L));

            // then, create another log table
            TablePath t2 = TablePath.of(DEFAULT_DB, "logTableAlter");

            Map<String, String> logTableProperties = new HashMap<>();
            logTableProperties.put(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "false");
            long t2Id = createLogTable(t2, 1, false, logTableProperties, Collections.emptyMap());
            // enable lake
            admin.alterTable(t2, changes, false).get();

            TableBucket t2Bucket = new TableBucket(t2Id, 0);
            List<InternalRow> flussRows = new ArrayList<>();
            // write records
            for (int i = 0; i < 10; i++) {
                rows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));
                flussRows.addAll(rows);
                // write records
                writeRows(t2, rows, true);
            }
            // check the status of replica after synced;
            // note: we can't update log start offset for unaware bucket mode log table
            assertReplicaStatus(t2Bucket, 30);

            // check data in paimon
            checkDataInPaimonAppendOnlyTable(t2, flussRows, 0);

            // then write data to the pk tables
            // write records
            rows = Arrays.asList(row(1, "v111"), row(2, "v222"), row(3, "v333"));
            // write records
            writeRows(t1, rows, false);

            // check the status of replica of t2 after synced
            // not check start offset since we won't
            // update start log offset for primary key table
            assertReplicaStatus(t1Bucket, 9);

            checkDataInPaimonPrimaryKeyTable(t1, rows);

            // then create partitioned table and wait partitions are ready
            TablePath partitionedTablePath = TablePath.of(DEFAULT_DB, "partitionedTableAlter");
            Map<String, String> partitionTableProperties = new HashMap<>();
            partitionTableProperties.put(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "false");

            Tuple2<Long, TableDescriptor> tableIdAndDescriptor =
                    createPartitionedTable(
                            partitionedTablePath, partitionTableProperties, Collections.emptyMap());

            admin.alterTable(partitionedTablePath, changes, false).get();

            Map<Long, String> partitionNameByIds = waitUntilPartitions(partitionedTablePath);

            // now, write rows into partitioned table
            TableDescriptor partitionedTableDescriptor = tableIdAndDescriptor.f1;
            Map<String, List<InternalRow>> writtenRowsByPartition =
                    writeRowsIntoPartitionedTable(
                            partitionedTablePath, partitionedTableDescriptor, partitionNameByIds);
            long tableId = tableIdAndDescriptor.f0;

            // wait until synced to paimon
            Map<TableBucket, Long> expectedOffset = new HashMap<>();
            for (Long partitionId : partitionNameByIds.keySet()) {
                TableBucket tableBucket = new TableBucket(tableId, partitionId, 0);
                assertReplicaStatus(tableBucket, 3);
                expectedOffset.put(tableBucket, 3L);
            }

            // now, let's check data in paimon per partition
            // check data in paimon
            String partitionCol = partitionedTableDescriptor.getPartitionKeys().get(0);
            for (String partitionName : partitionNameByIds.values()) {
                checkDataInPaimonAppendOnlyPartitionedTable(
                        partitionedTablePath,
                        Collections.singletonMap(partitionCol, partitionName),
                        writtenRowsByPartition.get(partitionName),
                        0);
            }

            checkFlussOffsetsInSnapshot(partitionedTablePath, expectedOffset);
        } finally {
            jobClient.cancel().get();
        }
    }

    @Test
    void testTieringToDvEnabledTable() throws Exception {
        TablePath t1 = TablePath.of(DEFAULT_DB, "pkTableWithDv");
        long t1Id =
                createPkTable(
                        t1,
                        Collections.singletonMap("table.datalake.auto-compaction", "true"),
                        Collections.singletonMap("paimon.deletion-vectors.enabled", "true"));
        // write records
        List<InternalRow> rows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));
        writeRows(t1, rows, false);
        triggerAndWaitSnapshot(t1Id, 1);

        // then start tiering job
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            // check the status of replica after synced
            assertReplicaStatus(new TableBucket(t1Id, 0), 3);
            // check data in paimon
            checkDataInPaimonPrimaryKeyTable(t1, rows);
        } finally {
            jobClient.cancel().get();
        }
    }

    private Tuple2<Long, TableDescriptor> createPartitionedTable(TablePath partitionedTablePath)
            throws Exception {
        return createPartitionedTable(
                partitionedTablePath, Collections.emptyMap(), Collections.emptyMap());
    }

    private Tuple2<Long, TableDescriptor> createPartitionedTable(
            TablePath partitionedTablePath,
            Map<String, String> properties,
            Map<String, String> customProperties)
            throws Exception {
        TableDescriptor partitionedTableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("name", DataTypes.STRING())
                                        .column("date", DataTypes.STRING())
                                        .build())
                        .partitionedBy("date")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.YEAR)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .properties(properties)
                        .customProperties(customProperties)
                        .build();
        return Tuple2.of(
                createTable(partitionedTablePath, partitionedTableDescriptor),
                partitionedTableDescriptor);
    }

    private void checkDataInPaimonAppendOnlyTable(
            TablePath tablePath, List<InternalRow> expectedRows, long startingOffset)
            throws Exception {
        Iterator<org.apache.paimon.data.InternalRow> paimonRowIterator =
                getPaimonRowCloseableIterator(tablePath);
        Iterator<InternalRow> flussRowIterator = expectedRows.iterator();
        while (paimonRowIterator.hasNext()) {
            org.apache.paimon.data.InternalRow row = paimonRowIterator.next();
            InternalRow flussRow = flussRowIterator.next();
            assertThat(row.getInt(0)).isEqualTo(flussRow.getInt(0));
            assertThat(row.getString(1).toString()).isEqualTo(flussRow.getString(1).toString());
            // system columns are always the last three: __bucket, __offset, __timestamp
            int offsetIndex = row.getFieldCount() - 2;
            assertThat(row.getLong(offsetIndex)).isEqualTo(startingOffset++);
        }
        assertThat(flussRowIterator.hasNext()).isFalse();
    }

    private void checkDataInPaimonAppendOnlyPartitionedTable(
            TablePath tablePath,
            Map<String, String> partitionSpec,
            List<InternalRow> expectedRows,
            long startingOffset)
            throws Exception {
        Iterator<org.apache.paimon.data.InternalRow> paimonRowIterator =
                getPaimonRowCloseableIterator(tablePath, partitionSpec);
        Iterator<InternalRow> flussRowIterator = expectedRows.iterator();
        while (paimonRowIterator.hasNext()) {
            org.apache.paimon.data.InternalRow row = paimonRowIterator.next();
            InternalRow flussRow = flussRowIterator.next();
            assertThat(row.getInt(0)).isEqualTo(flussRow.getInt(0));
            assertThat(row.getString(1).toString()).isEqualTo(flussRow.getString(1).toString());
            assertThat(row.getString(2).toString()).isEqualTo(flussRow.getString(2).toString());
            // the idx 3 is __bucket, so use 4
            assertThat(row.getLong(4)).isEqualTo(startingOffset++);
        }
        assertThat(flussRowIterator.hasNext()).isFalse();
    }

    private CloseableIterator<org.apache.paimon.data.InternalRow> getPaimonRowCloseableIterator(
            TablePath tablePath, Map<String, String> partitionSpec) throws Exception {
        Identifier tableIdentifier =
                Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName());

        FileStoreTable table = (FileStoreTable) paimonCatalog.getTable(tableIdentifier);

        RecordReader<org.apache.paimon.data.InternalRow> reader =
                table.newRead()
                        .createReader(
                                table.newReadBuilder()
                                        .withPartitionFilter(partitionSpec)
                                        .newScan()
                                        .plan());
        return reader.toCloseableIterator();
    }

    @Test
    void testTieringWithAddColumn() throws Exception {
        // Test ADD COLUMN during tiering with "Lake First" strategy

        // 1. Create a datalake enabled table with initial schema (c1: INT, c2: STRING)
        TablePath tablePath = TablePath.of(DEFAULT_DB, "addColumnTable");
        long tableId = createLogTable(tablePath);
        TableBucket tableBucket = new TableBucket(tableId, 0);

        // 2. Write initial data before ADD COLUMN
        List<InternalRow> initialRows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));
        writeRows(tablePath, initialRows, true);

        // 3. Start tiering job
        JobClient jobClient = buildTieringJob(execEnv);

        try {
            // 4. Wait for initial data to be tiered
            assertReplicaStatus(tableBucket, 3);

            // 5. Execute ADD COLUMN (c3: INT, nullable)
            List<TableChange> addColumnChanges =
                    Collections.singletonList(
                            TableChange.addColumn(
                                    "c3",
                                    DataTypes.INT(),
                                    "new column",
                                    TableChange.ColumnPosition.last()));
            admin.alterTable(tablePath, addColumnChanges, false).get();

            // 6. Write more data after ADD COLUMN (with new column value)
            // schema now has 3 business columns (c1, c2, c3), so provide value for the new column
            List<InternalRow> newRows =
                    Arrays.asList(row(4, "v4", 40), row(5, "v5", 50), row(6, "v6", 60));
            writeRows(tablePath, newRows, true);

            // 7. Wait for new data to be tiered
            assertReplicaStatus(tableBucket, 6);

            // 8. Verify Paimon table has the new column with exact field names and order
            Identifier tableIdentifier =
                    Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName());
            FileStoreTable paimonTable = (FileStoreTable) paimonCatalog.getTable(tableIdentifier);
            List<String> fieldNames = paimonTable.rowType().getFieldNames();

            // Should have exact fields in order: a, b, c3, __bucket, __offset, __timestamp
            assertThat(fieldNames)
                    .containsExactly("a", "b", "c3", "__bucket", "__offset", "__timestamp");

            // 9. Verify both schema evolution and data correctness
            // For initial rows (before ADD COLUMN), c3 should be NULL
            // For new rows (after ADD COLUMN), c3 should have the provided values
            List<InternalRow> expectedRows = new ArrayList<>();
            // Initial rows with NULL for c3
            expectedRows.add(row(1, "v1", null));
            expectedRows.add(row(2, "v2", null));
            expectedRows.add(row(3, "v3", null));
            // New rows with c3 values
            expectedRows.add(row(4, "v4", 40));
            expectedRows.add(row(5, "v5", 50));
            expectedRows.add(row(6, "v6", 60));

            checkDataInPaimonAppendOnlyTable(tablePath, expectedRows, 0);

        } finally {
            jobClient.cancel().get();
        }
    }

    @Override
    protected FlussClusterExtension getFlussClusterExtension() {
        return FLUSS_CLUSTER_EXTENSION;
    }
}

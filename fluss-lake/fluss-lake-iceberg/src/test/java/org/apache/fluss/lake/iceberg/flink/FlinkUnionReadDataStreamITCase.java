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

package org.apache.fluss.lake.iceberg.flink;

import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.flink.source.FlussSource;
import org.apache.fluss.flink.source.FlussSourceBuilder;
import org.apache.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for union read through the DataStream {@link FlussSource}.
 *
 * <p>These tests mirror the Flink SQL union-read coverage ({@code FlinkUnionReadLogTableITCase} and
 * {@code FlinkUnionReadPrimaryKeyTableITCase}) but exercise the programmatic DataStream source.
 * Each test asserts the three properties that make a union read meaningful:
 *
 * <ul>
 *   <li>data tiered to the lake before tiering stopped is read back from the lake snapshot;
 *   <li>data written to Fluss after tiering stopped is read from the live Fluss log;
 *   <li>the union of both is returned exactly once (for PK tables the Fluss changelog is applied as
 *       -U/+U on top of the lake snapshot read as +I).
 * </ul>
 */
public class FlinkUnionReadDataStreamITCase extends FlinkUnionReadTestBase {

    private static final int[] FULL_COLS = new int[] {0, 1, 2};
    private static final int[] FULL_PARTITIONED_COLS = new int[] {0, 1, 2, 3};
    private static final int[] PROJECTED_ID_AMOUNT_COLS = new int[] {0, 2};

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUnionReadLogTable(boolean isPartitioned) throws Exception {
        JobClient tieringJob = buildTieringJob(execEnv);

        String tableName = "ds_union_log_" + (isPartitioned ? "partitioned" : "non_partitioned");
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        long tableId = createLogTable(tablePath, isPartitioned);
        List<String> partitions = partitionsOf(tablePath, isPartitioned);

        List<Row> firstBatch = new ArrayList<>();
        for (String partition : partitions) {
            firstBatch.addAll(appendLogRows(tablePath, 0, 5, partition));
        }
        waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, isPartitioned);
        assertIcebergContainsExactly(tablePath, isPartitioned, firstBatch);

        // written after tiering stops, so this batch lives only in Fluss
        tieringJob.cancel().get();
        List<Row> secondBatch = new ArrayList<>();
        for (String partition : partitions) {
            secondBatch.addAll(appendLogRows(tablePath, 100, 3, partition));
        }
        assertIcebergContainsExactly(tablePath, isPartitioned, firstBatch);

        List<Row> expected = new ArrayList<>(firstBatch);
        expected.addAll(secondBatch);
        FlussSource<RowData> source = buildSource(tableName);
        List<Row> actual =
                collect(source, expected.size(), isPartitioned ? FULL_PARTITIONED_COLS : FULL_COLS);

        assertRowsIgnoreOrder(actual, expected);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUnionReadPrimaryKeyTable(boolean isPartitioned) throws Exception {
        JobClient tieringJob = buildTieringJob(execEnv);

        String tableName = "ds_union_pk_" + (isPartitioned ? "partitioned" : "non_partitioned");
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        long tableId = createPkTable(tablePath, isPartitioned);
        List<String> partitions = partitionsOf(tablePath, isPartitioned);

        List<Row> snapshotRows = new ArrayList<>();
        for (String partition : partitions) {
            snapshotRows.addAll(upsertRows(tablePath, 1, 3, partition));
        }
        waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, isPartitioned);
        assertIcebergContainsExactly(tablePath, isPartitioned, snapshotRows);

        // update after tiering stops: lives only in the Fluss changelog, read as -U/+U on top of
        // the Iceberg snapshot read as +I
        tieringJob.cancel().get();
        List<Row> changelog = new ArrayList<>();
        for (String partition : partitions) {
            changelog.addAll(updateRow(tablePath, 1, partition));
        }
        assertIcebergContainsExactly(tablePath, isPartitioned, snapshotRows);

        List<Row> expected = new ArrayList<>(snapshotRows);
        expected.addAll(changelog);
        FlussSource<RowData> source = buildSource(tableName);
        List<Row> actual =
                collect(source, expected.size(), isPartitioned ? FULL_PARTITIONED_COLS : FULL_COLS);

        assertRowsIgnoreOrder(actual, expected);
    }

    // projection, filtering and boundedness are independent of partitioning, which the log/PK tests
    // above already cover, so these run on a single non-partitioned table
    @Test
    void testUnionReadWithProjectionPushdown() throws Exception {
        JobClient tieringJob = buildTieringJob(execEnv);

        String tableName = "ds_union_projection";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        long tableId = createLogTable(tablePath, false);

        List<Row> firstBatch = appendLogRows(tablePath, 0, 5, null);
        waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, false);
        assertIcebergContainsExactly(tablePath, false, firstBatch);

        tieringJob.cancel().get();
        List<Row> secondBatch = appendLogRows(tablePath, 100, 3, null);
        assertIcebergContainsExactly(tablePath, false, firstBatch);

        List<Row> written = new ArrayList<>(firstBatch);
        written.addAll(secondBatch);
        FlussSource<RowData> source = buildSource(tableName, "id", "amount");
        List<Row> actual = collect(source, written.size(), PROJECTED_ID_AMOUNT_COLS);

        List<Row> expected =
                written.stream()
                        .map(r -> Row.ofKind(RowKind.INSERT, r.getField(0), r.getField(2)))
                        .collect(Collectors.toList());

        assertRowsIgnoreOrder(actual, expected);
    }

    @Test
    void testUnionReadWithFilterPushdown() throws Exception {
        JobClient tieringJob = buildTieringJob(execEnv);

        String tableName = "ds_union_filter";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        long tableId = createLogTable(tablePath, false);

        // amounts 0..40, tiered to the lake snapshot
        List<Row> firstBatch = appendLogRows(tablePath, 0, 5, null);
        waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, false);
        assertIcebergContainsExactly(tablePath, false, firstBatch);

        // amounts 1000..1020, written after tiering stops so they live only in the Fluss log
        tieringJob.cancel().get();
        List<Row> secondBatch = appendLogRows(tablePath, 100, 3, null);
        assertIcebergContainsExactly(tablePath, false, firstBatch);

        // 'amount >= 100' prunes the lake snapshot file (max amount 40) during Iceberg planning and
        // keeps the Fluss batch (min amount 1000), so the union read must return exactly the Fluss
        // batch. If the filter were not pushed to the lake side, the snapshot rows would leak in.
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .field("amount", DataTypes.BIGINT())
                        .build();
        Predicate filter = new PredicateBuilder(rowType).greaterOrEqual(2, 100L);

        FlussSource<RowData> source =
                FlussSource.<RowData>builder()
                        .setBootstrapServers(bootstrapServers())
                        .setDatabase(DEFAULT_DB)
                        .setTable(tableName)
                        .setStartingOffsets(OffsetsInitializer.full())
                        .setScanPartitionDiscoveryIntervalMs(100L)
                        .setFilter(filter)
                        .setDeserializationSchema(new RowDataDeserializationSchema())
                        .build();
        List<Row> actual = collect(source, secondBatch.size(), FULL_COLS);

        assertRowsIgnoreOrder(actual, secondBatch);
    }

    @Test
    void testBoundedRequiresFullStartupModeFailsFast() throws Exception {
        String tableName = "ds_bounded_invalid_startup";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        createLogTable(tablePath, false);

        FlussSourceBuilder<RowData> builder =
                FlussSource.<RowData>builder()
                        .setBootstrapServers(bootstrapServers())
                        .setDatabase(DEFAULT_DB)
                        .setTable(tableName)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setBounded()
                        .setDeserializationSchema(new RowDataDeserializationSchema());

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Bounded (batch) read requires");
    }

    @Test
    void testBoundedRequiresDataLakeEnabledFailsFast() throws Exception {
        String tableName = "ds_bounded_no_lake";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        createNonLakeLogTable(tablePath);

        FlussSourceBuilder<RowData> builder =
                FlussSource.<RowData>builder()
                        .setBootstrapServers(bootstrapServers())
                        .setDatabase(DEFAULT_DB)
                        .setTable(tableName)
                        .setStartingOffsets(OffsetsInitializer.full())
                        .setBounded()
                        .setDeserializationSchema(new RowDataDeserializationSchema());

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Bounded (batch) read requires");
    }

    @Test
    void testBatchUnionReadLogTable() throws Exception {
        JobClient tieringJob = buildTieringJob(execEnv);

        String tableName = "ds_batch_union_log";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        long tableId = createLogTable(tablePath, false);

        List<Row> firstBatch = appendLogRows(tablePath, 0, 5, null);
        waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, false);
        assertIcebergContainsExactly(tablePath, false, firstBatch);

        tieringJob.cancel().get();
        List<Row> secondBatch = appendLogRows(tablePath, 100, 3, null);
        assertIcebergContainsExactly(tablePath, false, firstBatch);

        List<Row> expected = new ArrayList<>(firstBatch);
        expected.addAll(secondBatch);

        StreamExecutionEnvironment batchEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        batchEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);
        batchEnv.setParallelism(2);
        FlussSource<RowData> source =
                FlussSource.<RowData>builder()
                        .setBootstrapServers(bootstrapServers())
                        .setDatabase(DEFAULT_DB)
                        .setTable(tableName)
                        .setStartingOffsets(OffsetsInitializer.full())
                        .setBounded()
                        .setDeserializationSchema(new RowDataDeserializationSchema())
                        .build();
        List<Row> actual = collectBounded(batchEnv, source, FULL_COLS);

        assertRowsIgnoreOrder(actual, expected);
    }

    // ------------------------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------------------------

    private FlussSource<RowData> buildSource(String tableName, String... projectedFields) {
        FlussSourceBuilder<RowData> builder =
                FlussSource.<RowData>builder()
                        .setBootstrapServers(bootstrapServers())
                        .setDatabase(DEFAULT_DB)
                        .setTable(tableName)
                        .setStartingOffsets(OffsetsInitializer.full())
                        .setScanPartitionDiscoveryIntervalMs(100L)
                        .setDeserializationSchema(new RowDataDeserializationSchema());
        if (projectedFields.length > 0) {
            builder.setProjectedFields(projectedFields);
        }
        return builder.build();
    }

    private List<Row> collect(FlussSource<RowData> source, int expectedCount, int[] cols)
            throws Exception {
        DataStreamSource<RowData> stream =
                execEnv.fromSource(source, WatermarkStrategy.noWatermarks(), "Fluss Union Source");
        return stream.executeAndCollect(expectedCount).stream()
                .map(rowData -> toRow(rowData, cols))
                .collect(Collectors.toList());
    }

    private List<Row> collectBounded(
            StreamExecutionEnvironment env, FlussSource<RowData> source, int[] cols)
            throws Exception {
        DataStreamSource<RowData> stream =
                env.fromSource(
                        source, WatermarkStrategy.noWatermarks(), "Fluss Union Source (batch)");
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<RowData> iterator = stream.executeAndCollect()) {
            while (iterator.hasNext()) {
                rows.add(toRow(iterator.next(), cols));
            }
        }
        return rows;
    }

    private List<String> partitionsOf(TablePath tablePath, boolean isPartitioned) {
        if (!isPartitioned) {
            return Collections.singletonList(null);
        }
        return new ArrayList<>(waitUntilPartitions(tablePath).values());
    }

    private List<Row> appendLogRows(
            TablePath tablePath, int startId, int count, @Nullable String partition)
            throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        List<Row> expected = new ArrayList<>();
        for (int id = startId; id < startId + count; id++) {
            String name = "name_" + id;
            long amount = id * 10L;
            rows.add(internalRow(id, name, amount, partition));
            expected.add(expectedRow(RowKind.INSERT, id, name, amount, partition));
        }
        writeRows(tablePath, rows, true);
        return expected;
    }

    private List<Row> upsertRows(
            TablePath tablePath, int startId, int count, @Nullable String partition)
            throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        List<Row> expected = new ArrayList<>();
        for (int id = startId; id < startId + count; id++) {
            String name = "name_" + id;
            long amount = id * 10L;
            rows.add(internalRow(id, name, amount, partition));
            expected.add(expectedRow(RowKind.INSERT, id, name, amount, partition));
        }
        writeRows(tablePath, rows, false);
        return expected;
    }

    private List<Row> updateRow(TablePath tablePath, int id, @Nullable String partition)
            throws Exception {
        String newName = "updated_" + id;
        long newAmount = id * 100L;
        writeRows(
                tablePath,
                Collections.singletonList(internalRow(id, newName, newAmount, partition)),
                false);

        List<Row> expected = new ArrayList<>();
        expected.add(expectedRow(RowKind.UPDATE_BEFORE, id, "name_" + id, id * 10L, partition));
        expected.add(expectedRow(RowKind.UPDATE_AFTER, id, newName, newAmount, partition));
        return expected;
    }

    private static InternalRow internalRow(
            int id, String name, long amount, @Nullable String partition) {
        return partition == null ? row(id, name, amount) : row(id, name, amount, partition);
    }

    private static Row expectedRow(
            RowKind kind, int id, String name, long amount, @Nullable String partition) {
        return partition == null
                ? Row.ofKind(kind, id, name, amount)
                : Row.ofKind(kind, id, name, amount, partition);
    }

    private static Row toRow(RowData rowData, int[] cols) {
        Object[] fields = new Object[cols.length];
        for (int i = 0; i < cols.length; i++) {
            if (rowData.isNullAt(i)) {
                fields[i] = null;
                continue;
            }
            switch (cols[i]) {
                case 0:
                    fields[i] = rowData.getInt(i);
                    break;
                case 1:
                    fields[i] = rowData.getString(i).toString();
                    break;
                case 2:
                    fields[i] = rowData.getLong(i);
                    break;
                case 3:
                    fields[i] = rowData.getString(i).toString();
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected column index: " + cols[i]);
            }
        }
        return Row.ofKind(rowData.getRowKind(), fields);
    }

    private static void assertRowsIgnoreOrder(List<Row> actual, List<Row> expected) {
        assertThat(actual.stream().map(Row::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(
                        expected.stream().map(Row::toString).collect(Collectors.toList()));
    }

    private void assertIcebergContainsExactly(
            TablePath tablePath, boolean isPartitioned, List<Row> expectedValues) throws Exception {
        // tiering is asynchronous; poll once per second (a tight loop would exhaust file
        // descriptors) until the expected rows are visible, then assert exact contents
        waitUntil(
                () -> readIcebergUserRows(tablePath, isPartitioned).size() == expectedValues.size(),
                Duration.ofMinutes(2),
                Duration.ofSeconds(1),
                "Iceberg did not contain the expected number of rows for " + tablePath);
        List<Row> icebergValues = readIcebergUserRows(tablePath, isPartitioned);
        assertThat(icebergValues.stream().map(Row::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(
                        expectedValues.stream()
                                .map(r -> Row.of(toArray(r)).toString())
                                .collect(Collectors.toList()));
    }

    private List<Row> readIcebergUserRows(TablePath tablePath, boolean isPartitioned)
            throws Exception {
        // full generic scan reads all partitions and applies PK deletes; avoids the shared util's
        // log-table reader which drops same-offset data files from different partitions
        Table table = icebergCatalog.loadTable(toIceberg(tablePath));
        List<Row> rows = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record record : records) {
                int id = ((Number) record.getField("id")).intValue();
                Object nameField = record.getField("name");
                String name = nameField == null ? null : nameField.toString();
                long amount = ((Number) record.getField("amount")).longValue();
                if (isPartitioned) {
                    Object partitionField = record.getField("p");
                    String partition = partitionField == null ? null : partitionField.toString();
                    rows.add(Row.of(id, name, amount, partition));
                } else {
                    rows.add(Row.of(id, name, amount));
                }
            }
        }
        return rows;
    }

    private static Object[] toArray(Row row) {
        Object[] fields = new Object[row.getArity()];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = row.getField(i);
        }
        return fields;
    }

    private long createLogTable(TablePath tablePath, boolean isPartitioned) throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("amount", DataTypes.BIGINT());
        TableDescriptor.Builder tableBuilder =
                TableDescriptor.builder()
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500));
        if (isPartitioned) {
            schemaBuilder.column("p", DataTypes.STRING());
            tableBuilder
                    .partitionedBy("p")
                    .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                    .property(
                            ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                            AutoPartitionTimeUnit.YEAR);
        }
        tableBuilder.distributedBy(DEFAULT_BUCKET_NUM, "id").schema(schemaBuilder.build());
        return createTable(tablePath, tableBuilder.build());
    }

    private long createNonLakeLogTable(TablePath tablePath) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("amount", DataTypes.BIGINT())
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .distributedBy(DEFAULT_BUCKET_NUM, "id")
                        .schema(schema)
                        .build();
        return createTable(tablePath, descriptor);
    }

    private long createPkTable(TablePath tablePath, boolean isPartitioned) throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("amount", DataTypes.BIGINT());
        TableDescriptor.Builder tableBuilder =
                TableDescriptor.builder()
                        .distributedBy(DEFAULT_BUCKET_NUM)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500));
        if (isPartitioned) {
            schemaBuilder.column("p", DataTypes.STRING());
            schemaBuilder.primaryKey("id", "p");
            tableBuilder
                    .partitionedBy("p")
                    .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                    .property(
                            ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                            AutoPartitionTimeUnit.YEAR);
        } else {
            schemaBuilder.primaryKey("id");
        }
        tableBuilder.schema(schemaBuilder.build());
        return createTable(tablePath, tableBuilder.build());
    }

    private static String bootstrapServers() {
        return String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
    }
}

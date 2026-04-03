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

package org.apache.fluss.lake.paimon.flink;

import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.flink.source.FlussSource;
import org.apache.fluss.flink.source.FlussSourceBuilder;
import org.apache.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for Flink DataStream API union read from lake and Fluss. */
class FlinkDataStreamUnionReadITCase extends FlinkUnionReadTestBase {

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
    }

    private String getBootstrapServers() {
        return String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
    }

    private StreamExecutionEnvironment createBatchEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(2);
        return env;
    }

    private StreamExecutionEnvironment createStreamingEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        return env;
    }

    private FlussSourceBuilder<RowData> newFlussSourceBuilder(String tableName) {
        return FlussSource.<RowData>builder()
                .setBootstrapServers(getBootstrapServers())
                .setDatabase(DEFAULT_DB)
                .setTable(tableName)
                .setDeserializationSchema(new RowDataDeserializationSchema());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUnionReadLogTableFromDataStream(boolean isPartitioned) throws Exception {
        // Start tiering job (requires STREAMING mode)
        JobClient jobClient = buildTieringJob(execEnv);

        String tableName = "ds_logTable_" + (isPartitioned ? "partitioned" : "non_partitioned");
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);

        // Create log table with data lake enabled and write initial data
        long tableId = createLogTableWithDataLake(tablePath, isPartitioned);
        List<InternalRow> writtenRows = new ArrayList<>();
        if (isPartitioned) {
            Map<Long, String> partitionNameById = waitUntilPartitions(tablePath);
            for (String partition : partitionNameById.values()) {
                for (int i = 0; i < 3; i++) {
                    writtenRows.add(row(100 + i, "value_" + i, partition));
                }
            }
        } else {
            for (int i = 0; i < 5; i++) {
                writtenRows.add(row(100 + i, "value_" + i));
            }
        }
        writeRows(tablePath, writtenRows, true);

        // Wait until records have been synced to lake
        waitUntilBucketSynced(tablePath, tableId, 1, isPartitioned);

        // Create FlussSource with union read enabled and read using BATCH mode
        FlussSource<RowData> flussSource =
                newFlussSourceBuilder(tableName)
                        .setStartingOffsets(OffsetsInitializer.full())
                        .setLakeEnabled(true)
                        .setStreaming(false)
                        .build();

        DataStreamSource<RowData> stream =
                createBatchEnv()
                        .fromSource(
                                flussSource,
                                WatermarkStrategy.noWatermarks(),
                                "Fluss DataStream Union Read");

        // Verify all written rows are read
        List<RowData> results = stream.executeAndCollect(writtenRows.size());
        assertThat(results.size()).isEqualTo(writtenRows.size());

        jobClient.cancel().get();
    }

    @Test
    void testLakeEnabledWithoutLakeConfiguration() throws Exception {
        String tableName = "ds_no_lake_table";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);

        // Create a simple log table WITHOUT data lake configuration
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1, "a").build();
        admin.createTable(tablePath, tableDescriptor, true).get();

        // Write data
        List<InternalRow> writtenRows = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            writtenRows.add(row(i, "value_" + i));
        }
        writeRows(tablePath, writtenRows, true);

        // Create source with lakeEnabled=true (but table has no lake)
        FlussSource<RowData> flussSource =
                newFlussSourceBuilder(tableName)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setLakeEnabled(true)
                        .build();

        DataStreamSource<RowData> stream =
                createStreamingEnv()
                        .fromSource(
                                flussSource,
                                WatermarkStrategy.noWatermarks(),
                                "Fluss DataStream No Lake");

        // Should still read all data from Fluss
        List<RowData> results = stream.executeAndCollect(writtenRows.size());
        assertThat(results.size()).isEqualTo(writtenRows.size());
    }

    @Test
    void testLakeDisabledEvenWithLakeTable() throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);

        String tableName = "ds_lake_disabled_table";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        long tableId = createLogTableWithDataLake(tablePath, false);

        // Write data and wait for sync to lake
        List<InternalRow> writtenRows = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            writtenRows.add(row(i, "value_" + i));
        }
        writeRows(tablePath, writtenRows, true);
        waitUntilBucketSynced(tablePath, tableId, 1, false);

        // Create source with lakeEnabled=false (explicitly disabled)
        FlussSource<RowData> flussSource =
                newFlussSourceBuilder(tableName)
                        .setStartingOffsets(OffsetsInitializer.full())
                        .setLakeEnabled(false)
                        .build();

        DataStreamSource<RowData> stream =
                createStreamingEnv()
                        .fromSource(
                                flussSource,
                                WatermarkStrategy.noWatermarks(),
                                "Fluss DataStream Lake Disabled");

        // Should read from Fluss log, not lake snapshot
        List<RowData> results = stream.executeAndCollect(writtenRows.size());
        assertThat(results.size()).isEqualTo(writtenRows.size());

        jobClient.cancel().get();
    }

    private long createLogTableWithDataLake(TablePath tablePath, boolean isPartitioned)
            throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder().column("a", DataTypes.INT()).column("b", DataTypes.STRING());

        TableDescriptor.Builder tableBuilder =
                TableDescriptor.builder()
                        .distributedBy(1, "a")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500));

        if (isPartitioned) {
            schemaBuilder.column("c", DataTypes.STRING());
            tableBuilder.property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true);
            tableBuilder.partitionedBy("c");
            tableBuilder.property(
                    ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, AutoPartitionTimeUnit.YEAR);
        }
        tableBuilder.schema(schemaBuilder.build());
        return createTable(tablePath, tableBuilder.build());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUnionReadPrimaryKeyTableFromDataStream(boolean isPartitioned) throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);

        String tableName = "ds_pkTable_" + (isPartitioned ? "partitioned" : "non_partitioned");
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        long tableId = createPkTableWithDataLake(tablePath, isPartitioned);

        // Write initial data (keys 1, 2, 3)
        List<InternalRow> initialRows = new ArrayList<>();
        if (isPartitioned) {
            Map<Long, String> partitionNameById = waitUntilPartitions(tablePath);
            for (String partition : partitionNameById.values()) {
                initialRows.add(row(1, BinaryString.fromString("value_1"), partition));
                initialRows.add(row(2, BinaryString.fromString("value_2"), partition));
                initialRows.add(row(3, BinaryString.fromString("value_3"), partition));
            }
        } else {
            initialRows.add(row(1, BinaryString.fromString("value_1")));
            initialRows.add(row(2, BinaryString.fromString("value_2")));
            initialRows.add(row(3, BinaryString.fromString("value_3")));
        }
        writeRows(tablePath, initialRows, false);

        // Wait for sync to lake
        waitUntilBucketSynced(tablePath, tableId, 1, isPartitioned);

        // Write MORE data AFTER sync: update key 2, add key 4 (will be in log, not lake)
        List<InternalRow> newRows = new ArrayList<>();
        if (isPartitioned) {
            Map<Long, String> partitionNameById = waitUntilPartitions(tablePath);
            for (String partition : partitionNameById.values()) {
                newRows.add(row(2, BinaryString.fromString("value_2_updated"), partition));
                newRows.add(row(4, BinaryString.fromString("value_4"), partition));
            }
        } else {
            newRows.add(row(2, BinaryString.fromString("value_2_updated")));
            newRows.add(row(4, BinaryString.fromString("value_4")));
        }
        writeRows(tablePath, newRows, false);

        // Create FlussSource with union read enabled
        FlussSource<RowData> flussSource =
                newFlussSourceBuilder(tableName)
                        .setStartingOffsets(OffsetsInitializer.full())
                        .setLakeEnabled(true)
                        .setStreaming(false)
                        .build();

        DataStreamSource<RowData> stream =
                createBatchEnv()
                        .fromSource(
                                flussSource,
                                WatermarkStrategy.noWatermarks(),
                                "Fluss DataStream PK Union Read");

        // Collect and verify: 4 distinct keys per partition (key 2 deduplicated to latest)
        int expectedCount = isPartitioned ? 4 * waitUntilPartitions(tablePath).size() : 4;
        List<RowData> results = stream.executeAndCollect(expectedCount);
        assertThat(results.size()).isEqualTo(expectedCount);

        // Verify values - key 2 should be updated, original value should NOT be present
        Set<String> actualValues = new HashSet<>();
        for (RowData row : results) {
            actualValues.add(row.getString(1).toString());
        }
        assertThat(actualValues).contains("value_1", "value_2_updated", "value_3", "value_4");
        assertThat(actualValues).doesNotContain("value_2");

        jobClient.cancel().get();
    }

    private long createPkTableWithDataLake(TablePath tablePath, boolean isPartitioned)
            throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder().column("a", DataTypes.INT()).column("b", DataTypes.STRING());

        TableDescriptor.Builder tableBuilder =
                TableDescriptor.builder()
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500));

        if (isPartitioned) {
            schemaBuilder.column("c", DataTypes.STRING());
            tableBuilder.property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true);
            tableBuilder.partitionedBy("c");
            tableBuilder.property(
                    ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, AutoPartitionTimeUnit.YEAR);
            schemaBuilder.primaryKey("c", "a"); // partition key + primary key
        } else {
            schemaBuilder.primaryKey("a");
        }

        tableBuilder.schema(schemaBuilder.build());
        return createTable(tablePath, tableBuilder.build());
    }
}

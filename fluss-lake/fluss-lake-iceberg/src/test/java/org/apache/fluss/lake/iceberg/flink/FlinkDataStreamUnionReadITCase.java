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

package org.apache.fluss.lake.iceberg.flink;

import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.flink.source.FlussSource;
import org.apache.fluss.flink.source.FlussSourceBuilder;
import org.apache.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

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
        long tableId = createLogTableWithDataLake(tablePath);

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

    private long createLogTableWithDataLake(TablePath tablePath) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1, "a")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .build();
        return createTable(tablePath, tableDescriptor);
    }
}

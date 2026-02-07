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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.lake.iceberg.testutils.FlinkIcebergTieringTestBase;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for schema evolution (ADD COLUMN) with Iceberg tiering. */
class IcebergSchemaEvolutionITCase extends FlinkIcebergTieringTestBase {

    private static final String DEFAULT_DB = "fluss";

    private static StreamExecutionEnvironment execEnv;

    private static final Schema INITIAL_LOG_SCHEMA =
            Schema.newBuilder()
                    .column("f_int", DataTypes.INT())
                    .column("f_str", DataTypes.STRING())
                    .build();

    private static final Schema INITIAL_PK_SCHEMA =
            Schema.newBuilder()
                    .column("f_int", DataTypes.INT())
                    .column("f_str", DataTypes.STRING())
                    .primaryKey("f_int")
                    .build();

    private static final Schema COMPLEX_LOG_SCHEMA =
            Schema.newBuilder()
                    .column("f_int", DataTypes.INT())
                    .column("f_str", DataTypes.STRING())
                    .column("f_tags", DataTypes.ARRAY(DataTypes.STRING()))
                    .column("f_meta", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                    .build();

    @BeforeAll
    protected static void beforeAll() {
        FlinkIcebergTieringTestBase.beforeAll();
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(2);
        execEnv.enableCheckpointing(1000);
    }

    @Test
    void testSchemaEvolutionLogTable() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "schemaEvoLogTable");
        long tableId = createLogTable(tablePath, 1, false, INITIAL_LOG_SCHEMA);
        TableBucket bucket = new TableBucket(tableId, 0);

        // Write initial rows and start tiering
        List<InternalRow> initialRows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));
        writeRows(tablePath, initialRows, true);

        JobClient jobClient = buildTieringJob(execEnv);
        try {
            // Wait until initial data is tiered
            assertReplicaStatus(bucket, 3);
            checkDataInIcebergAppendOnlyTable(tablePath, initialRows, 0);

            // Verify initial Iceberg schema has no extra columns
            org.apache.iceberg.Table icebergTable = icebergCatalog.loadTable(toIceberg(tablePath));
            assertThat(icebergTable.schema().findField("f_new")).isNull();

            // ALTER TABLE ADD COLUMN via Admin API
            admin.alterTable(
                            tablePath,
                            Collections.singletonList(
                                    TableChange.addColumn(
                                            "f_new",
                                            DataTypes.STRING(),
                                            null,
                                            TableChange.ColumnPosition.last())),
                            false)
                    .get();

            // Verify the Iceberg schema now has the new column before system columns
            icebergTable.refresh();
            Types.NestedField newField = icebergTable.schema().findField("f_new");
            assertThat(newField).isNotNull();
            assertThat(newField.type()).isEqualTo(Types.StringType.get());
            assertThat(newField.isOptional()).isTrue();

            // Verify column ordering: new column should be before __bucket
            List<String> fieldNames =
                    icebergTable.schema().columns().stream()
                            .map(Types.NestedField::name)
                            .collect(Collectors.toList());
            int newColIdx = fieldNames.indexOf("f_new");
            int bucketIdx = fieldNames.indexOf(BUCKET_COLUMN_NAME);
            assertThat(newColIdx).isLessThan(bucketIdx);
        } finally {
            jobClient.cancel().get();
        }
    }

    @Test
    void testSchemaEvolutionPkTable() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "schemaEvoPkTable");
        long tableId = createPkTable(tablePath, 1, false, INITIAL_PK_SCHEMA);
        TableBucket bucket = new TableBucket(tableId, 0);

        // Write initial rows and trigger snapshot
        List<InternalRow> initialRows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));
        writeRows(tablePath, initialRows, false);
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);

        JobClient jobClient = buildTieringJob(execEnv);
        try {
            // Wait until initial data is tiered
            assertReplicaStatus(bucket, 3);

            // Verify initial data in Iceberg
            List<Record> records = getIcebergRecords(tablePath);
            assertThat(records).hasSize(3);

            // ALTER TABLE ADD COLUMN
            admin.alterTable(
                            tablePath,
                            Collections.singletonList(
                                    TableChange.addColumn(
                                            "f_new",
                                            DataTypes.INT(),
                                            "new column",
                                            TableChange.ColumnPosition.last())),
                            false)
                    .get();

            // Verify the Iceberg schema now has the new column
            org.apache.iceberg.Table icebergTable = icebergCatalog.loadTable(toIceberg(tablePath));
            Types.NestedField newField = icebergTable.schema().findField("f_new");
            assertThat(newField).isNotNull();
            assertThat(newField.type()).isEqualTo(Types.IntegerType.get());
            assertThat(newField.isOptional()).isTrue();
            assertThat(newField.doc()).isEqualTo("new column");

            // Verify column ordering
            List<String> fieldNames =
                    icebergTable.schema().columns().stream()
                            .map(Types.NestedField::name)
                            .collect(Collectors.toList());
            int newColIdx = fieldNames.indexOf("f_new");
            int bucketIdx = fieldNames.indexOf(BUCKET_COLUMN_NAME);
            assertThat(newColIdx).isLessThan(bucketIdx);
        } finally {
            jobClient.cancel().get();
        }
    }

    @Test
    void testSchemaEvolutionLogTableWithComplexTypes() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "schemaEvoComplexLogTable");
        long tableId = createLogTable(tablePath, 1, false, COMPLEX_LOG_SCHEMA);
        TableBucket bucket = new TableBucket(tableId, 0);

        // Write initial rows (nulls for complex type columns)
        List<InternalRow> initialRows =
                Arrays.asList(
                        row(1, "v1", null, null),
                        row(2, "v2", null, null),
                        row(3, "v3", null, null));
        writeRows(tablePath, initialRows, true);

        JobClient jobClient = buildTieringJob(execEnv);
        try {
            // Wait until initial data is tiered
            assertReplicaStatus(bucket, 3);

            // Verify initial Iceberg schema has complex type columns
            org.apache.iceberg.Table icebergTable = icebergCatalog.loadTable(toIceberg(tablePath));
            assertThat(icebergTable.schema().findField("f_tags").type().isListType()).isTrue();
            assertThat(icebergTable.schema().findField("f_meta").type().isMapType()).isTrue();

            // ALTER TABLE ADD COLUMN — this exercises the compatibility check
            // with complex types whose field IDs were reassigned by Iceberg
            admin.alterTable(
                            tablePath,
                            Collections.singletonList(
                                    TableChange.addColumn(
                                            "f_new",
                                            DataTypes.STRING(),
                                            null,
                                            TableChange.ColumnPosition.last())),
                            false)
                    .get();

            // Verify the Iceberg schema now has the new column
            icebergTable.refresh();
            Types.NestedField newField = icebergTable.schema().findField("f_new");
            assertThat(newField).isNotNull();
            assertThat(newField.type()).isEqualTo(Types.StringType.get());
            assertThat(newField.isOptional()).isTrue();

            // Verify column ordering: new column before system columns
            List<String> fieldNames =
                    icebergTable.schema().columns().stream()
                            .map(Types.NestedField::name)
                            .collect(Collectors.toList());
            int newColIdx = fieldNames.indexOf("f_new");
            int bucketIdx = fieldNames.indexOf(BUCKET_COLUMN_NAME);
            assertThat(newColIdx).isLessThan(bucketIdx);

            // Verify complex type columns are still intact
            assertThat(icebergTable.schema().findField("f_tags").type().isListType()).isTrue();
            assertThat(icebergTable.schema().findField("f_meta").type().isMapType()).isTrue();
        } finally {
            jobClient.cancel().get();
        }
    }
}

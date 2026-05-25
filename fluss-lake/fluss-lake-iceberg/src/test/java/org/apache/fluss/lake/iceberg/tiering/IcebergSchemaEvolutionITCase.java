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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
            assertThat(fieldNames.indexOf("f_new"))
                    .isLessThan(fieldNames.indexOf(BUCKET_COLUMN_NAME));

            // Post-ALTER rows: new rows carry f_new values; old rows must surface NULL
            // via Iceberg field-ID schema evolution.
            List<InternalRow> postAlterRows =
                    Arrays.asList(row(4, "v4", "newA"), row(5, "v5", "newB"));
            writeRows(tablePath, postAlterRows, true);
            assertReplicaStatus(bucket, 5);

            icebergTable.refresh();
            List<Record> records = getIcebergRecords(tablePath);
            assertThat(records).hasSize(5);
            Map<Integer, Object> fNewByFInt = new HashMap<>();
            for (Record record : records) {
                fNewByFInt.put((Integer) record.getField("f_int"), record.getField("f_new"));
            }
            assertThat(fNewByFInt.get(1)).isNull();
            assertThat(fNewByFInt.get(2)).isNull();
            assertThat(fNewByFInt.get(3)).isNull();
            assertThat(fNewByFInt.get(4)).isEqualTo("newA");
            assertThat(fNewByFInt.get(5)).isEqualTo("newB");
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
            assertThat(fieldNames.indexOf("f_new"))
                    .isLessThan(fieldNames.indexOf(BUCKET_COLUMN_NAME));

            // Post-ALTER upserts + snapshot trigger; verify all rows tier.
            List<InternalRow> postAlterRows = Arrays.asList(row(4, "v4", 100), row(5, "v5", 200));
            writeRows(tablePath, postAlterRows, false);
            FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);
            assertReplicaStatus(bucket, 5);

            icebergTable.refresh();
            List<Record> postAlterRecords = getIcebergRecords(tablePath);
            assertThat(postAlterRecords).hasSize(5);
            Map<Integer, Object> fNewByFInt = new HashMap<>();
            for (Record record : postAlterRecords) {
                fNewByFInt.put((Integer) record.getField("f_int"), record.getField("f_new"));
            }
            assertThat(fNewByFInt.get(1)).isNull();
            assertThat(fNewByFInt.get(2)).isNull();
            assertThat(fNewByFInt.get(3)).isNull();
            assertThat(fNewByFInt.get(4)).isEqualTo(100);
            assertThat(fNewByFInt.get(5)).isEqualTo(200);
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
            assertThat(fieldNames.indexOf("f_new"))
                    .isLessThan(fieldNames.indexOf(BUCKET_COLUMN_NAME));

            assertThat(icebergTable.schema().findField("f_tags").type().isListType()).isTrue();
            assertThat(icebergTable.schema().findField("f_meta").type().isMapType()).isTrue();

            // Post-ALTER rows alongside pre-existing complex columns.
            List<InternalRow> postAlterRows =
                    Arrays.asList(
                            row(4, "v4", null, null, "newA"), row(5, "v5", null, null, "newB"));
            writeRows(tablePath, postAlterRows, true);
            assertReplicaStatus(bucket, 5);

            icebergTable.refresh();
            List<Record> records = getIcebergRecords(tablePath);
            assertThat(records).hasSize(5);
            Map<Integer, Object> fNewByFInt = new HashMap<>();
            for (Record record : records) {
                fNewByFInt.put((Integer) record.getField("f_int"), record.getField("f_new"));
            }
            assertThat(fNewByFInt.get(1)).isNull();
            assertThat(fNewByFInt.get(2)).isNull();
            assertThat(fNewByFInt.get(3)).isNull();
            assertThat(fNewByFInt.get(4)).isEqualTo("newA");
            assertThat(fNewByFInt.get(5)).isEqualTo("newB");
        } finally {
            jobClient.cancel().get();
        }
    }

    @Test
    void testSchemaEvolutionLogTableAddComplexColumn() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "schemaEvoLogAddComplex");
        long tableId = createLogTable(tablePath, 1, false, INITIAL_LOG_SCHEMA);
        TableBucket bucket = new TableBucket(tableId, 0);
        List<InternalRow> initialRows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));
        writeRows(tablePath, initialRows, true);

        JobClient jobClient = buildTieringJob(execEnv);
        try {
            assertReplicaStatus(bucket, 3);
            admin.alterTable(
                            tablePath,
                            Collections.singletonList(
                                    TableChange.addColumn(
                                            "f_tags",
                                            DataTypes.ARRAY(DataTypes.STRING()),
                                            null,
                                            TableChange.ColumnPosition.last())),
                            false)
                    .get();

            org.apache.iceberg.Table icebergTable = icebergCatalog.loadTable(toIceberg(tablePath));
            Types.NestedField added = icebergTable.schema().findField("f_tags");
            assertThat(added).isNotNull();
            assertThat(added.type().isListType()).isTrue();
            assertThat(added.type().asListType().elementType()).isEqualTo(Types.StringType.get());
            List<String> fieldNames =
                    icebergTable.schema().columns().stream()
                            .map(Types.NestedField::name)
                            .collect(Collectors.toList());
            assertThat(fieldNames.indexOf("f_tags"))
                    .isLessThan(fieldNames.indexOf(BUCKET_COLUMN_NAME));

            // Post-ALTER rows for a complex new column. Writing null exercises the
            // data-plane path through the new ARRAY field ID without forcing the test
            // to build an InternalArray literal.
            List<InternalRow> postAlterRows = Arrays.asList(row(4, "v4", null), row(5, "v5", null));
            writeRows(tablePath, postAlterRows, true);
            assertReplicaStatus(bucket, 5);

            icebergTable.refresh();
            List<Record> records = getIcebergRecords(tablePath);
            assertThat(records).hasSize(5);
            for (Record record : records) {
                assertThat(record.getField("f_tags")).isNull();
            }
        } finally {
            jobClient.cancel().get();
        }
    }

    @Test
    void testSchemaEvolutionPkTableWithComplexPreExisting() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "schemaEvoPkComplex");
        Schema pkComplexSchema =
                Schema.newBuilder()
                        .column("f_int", DataTypes.INT())
                        .column("f_str", DataTypes.STRING())
                        .column("f_tags", DataTypes.ARRAY(DataTypes.STRING()))
                        .primaryKey("f_int")
                        .build();
        long tableId = createPkTable(tablePath, 1, false, pkComplexSchema);
        TableBucket bucket = new TableBucket(tableId, 0);

        List<InternalRow> initialRows =
                Arrays.asList(row(1, "v1", null), row(2, "v2", null), row(3, "v3", null));
        writeRows(tablePath, initialRows, false);
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);

        JobClient jobClient = buildTieringJob(execEnv);
        try {
            assertReplicaStatus(bucket, 3);
            admin.alterTable(
                            tablePath,
                            Collections.singletonList(
                                    TableChange.addColumn(
                                            "f_new",
                                            DataTypes.INT(),
                                            null,
                                            TableChange.ColumnPosition.last())),
                            false)
                    .get();

            org.apache.iceberg.Table icebergTable = icebergCatalog.loadTable(toIceberg(tablePath));
            assertThat(icebergTable.schema().findField("f_new").type())
                    .isEqualTo(Types.IntegerType.get());
            assertThat(icebergTable.schema().findField("f_tags").type().isListType()).isTrue();
            List<String> fieldNames =
                    icebergTable.schema().columns().stream()
                            .map(Types.NestedField::name)
                            .collect(Collectors.toList());
            assertThat(fieldNames.indexOf("f_new"))
                    .isLessThan(fieldNames.indexOf(BUCKET_COLUMN_NAME));

            // Post-ALTER upserts with f_new values + snapshot trigger.
            List<InternalRow> postAlterRows =
                    Arrays.asList(row(4, "v4", null, 100), row(5, "v5", null, 200));
            writeRows(tablePath, postAlterRows, false);
            FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);
            assertReplicaStatus(bucket, 5);

            icebergTable.refresh();
            List<Record> records = getIcebergRecords(tablePath);
            assertThat(records).hasSize(5);
            Map<Integer, Object> fNewByFInt = new HashMap<>();
            for (Record record : records) {
                fNewByFInt.put((Integer) record.getField("f_int"), record.getField("f_new"));
            }
            assertThat(fNewByFInt.get(1)).isNull();
            assertThat(fNewByFInt.get(2)).isNull();
            assertThat(fNewByFInt.get(3)).isNull();
            assertThat(fNewByFInt.get(4)).isEqualTo(100);
            assertThat(fNewByFInt.get(5)).isEqualTo(200);
        } finally {
            jobClient.cancel().get();
        }
    }
}

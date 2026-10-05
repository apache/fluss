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

import org.apache.fluss.lake.iceberg.FlussDataTypeToIcebergDataType;
import org.apache.fluss.lake.iceberg.testutils.FlinkIcebergTieringTestBase;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.LocalZonedTimestampNanoType;
import org.apache.fluss.types.TimestampNanoType;

import org.apache.flink.core.execution.JobClient;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Iceberg V3 features.
 *
 * <p>This test validates V3 type support with actual Fluss cluster and Iceberg tables.
 */
class IcebergV3TieringITCase extends FlinkIcebergTieringTestBase {

    private static final String DEFAULT_DB = "fluss";

    @BeforeAll
    protected static void beforeAll() {
        FlinkIcebergTieringTestBase.beforeAll();
    }

    @Test
    void testV3TypeConversion() {
        // Validate type conversion from Fluss to Iceberg V3 types
        TimestampNanoType tsNano = new TimestampNanoType();
        Type icebergTsNano = tsNano.accept(FlussDataTypeToIcebergDataType.INSTANCE);
        assertThat(icebergTsNano).isInstanceOf(Types.TimestampNanoType.class);
        assertThat(((Types.TimestampNanoType) icebergTsNano).shouldAdjustToUTC()).isFalse();

        LocalZonedTimestampNanoType tsNanoLtz = new LocalZonedTimestampNanoType();
        Type icebergTsNanoLtz = tsNanoLtz.accept(FlussDataTypeToIcebergDataType.INSTANCE);
        assertThat(icebergTsNanoLtz).isInstanceOf(Types.TimestampNanoType.class);
        assertThat(((Types.TimestampNanoType) icebergTsNanoLtz).shouldAdjustToUTC()).isTrue();
    }

    @Test
    void testBasicTieringWithStandardTypes() throws Exception {
        // E2E test: Create table, write data, tier to Iceberg, read back
        TablePath tablePath = TablePath.of(DEFAULT_DB, "v3_basic_pk_table");

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("ts", DataTypes.TIMESTAMP(6))
                        .column("ts_ltz", DataTypes.TIMESTAMP_LTZ(6))
                        .primaryKey("id")
                        .build();

        long tableId = createPkTable(tablePath, 1, false, schema);
        TableBucket tableBucket = new TableBucket(tableId, 0);

        List<InternalRow> rows =
                Arrays.asList(
                        row(
                                1,
                                "row1",
                                TimestampNtz.fromMillis(1698235273501L),
                                TimestampLtz.fromEpochMillis(1698235273400L)),
                        row(
                                2,
                                "row2",
                                TimestampNtz.fromMillis(1698235273502L),
                                TimestampLtz.fromEpochMillis(1698235273401L)),
                        row(
                                3,
                                "row3",
                                TimestampNtz.fromMillis(1698235273503L),
                                TimestampLtz.fromEpochMillis(1698235273402L)));

        writeRows(tablePath, rows, false);
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);

        JobClient jobClient = buildTieringJob(execEnv);
        try {
            assertReplicaStatus(tableBucket, 3);

            // Verify data in Iceberg
            List<Record> icebergRecords = getIcebergRecords(tablePath);
            assertThat(icebergRecords).hasSize(3);

            // Verify Iceberg table schema
            Table icebergTable = icebergCatalog.loadTable(toIceberg(tablePath));
            org.apache.iceberg.Schema icebergSchema = icebergTable.schema();

            assertThat(icebergSchema.findField("id").type()).isInstanceOf(Types.IntegerType.class);
            assertThat(icebergSchema.findField("name").type()).isInstanceOf(Types.StringType.class);
            assertThat(icebergSchema.findField("ts").type())
                    .isInstanceOf(Types.TimestampType.class);
            assertThat(icebergSchema.findField("ts_ltz").type())
                    .isInstanceOf(Types.TimestampType.class);
        } finally {
            jobClient.cancel().get();
        }
    }

    @Test
    void testIcebergTableSchemaValidation() throws Exception {
        // Validate Iceberg table schema matches Fluss schema after tiering
        TablePath tablePath = TablePath.of(DEFAULT_DB, "v3_schema_validation_table");

        Schema schema =
                Schema.newBuilder()
                        .column("pk_col", DataTypes.BIGINT())
                        .column("int_col", DataTypes.INT())
                        .column("str_col", DataTypes.STRING())
                        .column("bool_col", DataTypes.BOOLEAN())
                        .column("double_col", DataTypes.DOUBLE())
                        .primaryKey("pk_col")
                        .build();

        long tableId = createPkTable(tablePath, 1, false, schema);
        TableBucket tableBucket = new TableBucket(tableId, 0);

        List<InternalRow> rows =
                Arrays.asList(row(1L, 10, "test1", true, 1.5d), row(2L, 20, "test2", false, 2.5d));

        writeRows(tablePath, rows, false);
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);

        JobClient jobClient = buildTieringJob(execEnv);
        try {
            assertReplicaStatus(tableBucket, 2);

            Table icebergTable = icebergCatalog.loadTable(toIceberg(tablePath));
            org.apache.iceberg.Schema icebergSchema = icebergTable.schema();

            // Validate all column types
            assertThat(icebergSchema.findField("pk_col").type()).isInstanceOf(Types.LongType.class);
            assertThat(icebergSchema.findField("int_col").type())
                    .isInstanceOf(Types.IntegerType.class);
            assertThat(icebergSchema.findField("str_col").type())
                    .isInstanceOf(Types.StringType.class);
            assertThat(icebergSchema.findField("bool_col").type())
                    .isInstanceOf(Types.BooleanType.class);
            assertThat(icebergSchema.findField("double_col").type())
                    .isInstanceOf(Types.DoubleType.class);

            // Verify primary key is set
            assertThat(icebergSchema.identifierFieldIds()).isNotEmpty();

            // Verify data
            List<Record> records = getIcebergRecords(tablePath);
            assertThat(records).hasSize(2);
        } finally {
            jobClient.cancel().get();
        }
    }

    // Future V3 E2E tests:
    // - testV3NanosecondTimestampTiering() - when row value support is added
    // - testV3DeletionVectors() - when DVs are implemented
    // - testV3DefaultValues() - when default values are implemented
    // - testV3RowLineage() - when row lineage is implemented
}

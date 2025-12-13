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

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.lake.iceberg.testutils.FlinkIcebergTieringTestBase;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.DateTimeUtils;
import org.apache.fluss.utils.TypeUtils;
import org.apache.fluss.utils.types.Tuple2;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.data.Record;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.lake.committer.BucketOffset.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** The ITCase for tiering into iceberg. */
class IcebergTieringITCase extends FlinkIcebergTieringTestBase {

    protected static final String DEFAULT_DB = "fluss";

    private static StreamExecutionEnvironment execEnv;

    private static final Schema pkSchema =
            Schema.newBuilder()
                    .column("f_boolean", DataTypes.BOOLEAN())
                    .column("f_byte", DataTypes.TINYINT())
                    .column("f_short", DataTypes.SMALLINT())
                    .column("f_int", DataTypes.INT())
                    .column("f_long", DataTypes.BIGINT())
                    .column("f_float", DataTypes.FLOAT())
                    .column("f_double", DataTypes.DOUBLE())
                    .column("f_string", DataTypes.STRING())
                    .column("f_decimal1", DataTypes.DECIMAL(5, 2))
                    .column("f_decimal2", DataTypes.DECIMAL(20, 0))
                    .column("f_timestamp_ltz1", DataTypes.TIMESTAMP_LTZ(3))
                    .column("f_timestamp_ltz2", DataTypes.TIMESTAMP_LTZ(6))
                    .column("f_timestamp_ntz1", DataTypes.TIMESTAMP(3))
                    .column("f_timestamp_ntz2", DataTypes.TIMESTAMP(6))
                    .column("f_binary", DataTypes.BINARY(4))
                    .column("f_date", DataTypes.DATE())
                    .column("f_time", DataTypes.TIME())
                    .column("f_char", DataTypes.CHAR(3))
                    .column("f_bytes", DataTypes.BYTES())
                    .primaryKey("f_int")
                    .build();

    private static final Schema logSchema =
            Schema.newBuilder()
                    .column("f_int", DataTypes.INT())
                    .column("f_str", DataTypes.STRING())
                    .build();

    @BeforeAll
    protected static void beforeAll() {
        FlinkIcebergTieringTestBase.beforeAll();
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(2);
        execEnv.enableCheckpointing(1000);
    }

    @Test
    void testTiering() throws Exception {
        // create a pk table, write some records and wait until snapshot finished
        TablePath t1 = TablePath.of(DEFAULT_DB, "pkTable");
        long t1Id = createPkTable(t1, 1, false, pkSchema);
        TableBucket t1Bucket = new TableBucket(t1Id, 0);
        // write records
        List<InternalRow> rows =
                Arrays.asList(
                        row(
                                true,
                                (byte) 100,
                                (short) 200,
                                1,
                                1 + 400L,
                                500.1f,
                                600.0d,
                                "v1",
                                Decimal.fromUnscaledLong(900, 5, 2),
                                Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                TimestampLtz.fromEpochMillis(1698235273400L),
                                TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                TimestampNtz.fromMillis(1698235273501L),
                                TimestampNtz.fromMillis(1698235273501L, 8000),
                                new byte[] {5, 6, 7, 8},
                                TypeUtils.castFromString("2023-10-25", DataTypes.DATE()),
                                TypeUtils.castFromString("09:30:00.0", DataTypes.TIME()),
                                BinaryString.fromString("abc"),
                                new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                        row(
                                true,
                                (byte) 100,
                                (short) 200,
                                2,
                                2 + 400L,
                                500.1f,
                                600.0d,
                                "v2",
                                Decimal.fromUnscaledLong(900, 5, 2),
                                Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                TimestampLtz.fromEpochMillis(1698235273400L),
                                TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                TimestampNtz.fromMillis(1698235273501L),
                                TimestampNtz.fromMillis(1698235273501L, 8000),
                                new byte[] {5, 6, 7, 8},
                                TypeUtils.castFromString("2023-10-25", DataTypes.DATE()),
                                TypeUtils.castFromString("09:30:00.0", DataTypes.TIME()),
                                BinaryString.fromString("abc"),
                                new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                        row(
                                true,
                                (byte) 100,
                                (short) 200,
                                3,
                                3 + 400L,
                                500.1f,
                                600.0d,
                                "v3",
                                Decimal.fromUnscaledLong(900, 5, 2),
                                Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                TimestampLtz.fromEpochMillis(1698235273400L),
                                TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                TimestampNtz.fromMillis(1698235273501L),
                                TimestampNtz.fromMillis(1698235273501L, 8000),
                                new byte[] {5, 6, 7, 8},
                                TypeUtils.castFromString("2023-10-25", DataTypes.DATE()),
                                TypeUtils.castFromString("09:30:00.0", DataTypes.TIME()),
                                BinaryString.fromString("abc"),
                                new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));
        writeRows(t1, rows, false);
        waitUntilSnapshot(t1Id, 1, 0);

        // then start tiering job
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            // check the status of replica after synced
            assertReplicaStatus(t1Bucket, 3);

            checkDataInIcebergPrimaryKeyTable(t1, rows);
            // check snapshot property in iceberg
            Map<String, String> properties =
                    new HashMap<String, String>() {
                        {
                            put(
                                    FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY,
                                    "[{\"bucket\":0,\"offset\":3}]");
                        }
                    };
            checkSnapshotPropertyInIceberg(t1, properties);

            // test log table
            testLogTableTiering();

            // then write data to the pk tables
            // write records
            rows =
                    Arrays.asList(
                            row(
                                    true,
                                    (byte) 100,
                                    (short) 200,
                                    1,
                                    1 + 400L,
                                    500.1f,
                                    600.0d,
                                    "v111",
                                    Decimal.fromUnscaledLong(900, 5, 2),
                                    Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                    TimestampLtz.fromEpochMillis(1698235273400L),
                                    TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                    TimestampNtz.fromMillis(1698235273501L),
                                    TimestampNtz.fromMillis(1698235273501L, 8000),
                                    new byte[] {5, 6, 7, 8},
                                    TypeUtils.castFromString("2023-10-25", DataTypes.DATE()),
                                    TypeUtils.castFromString("09:30:00.0", DataTypes.TIME()),
                                    BinaryString.fromString("abc"),
                                    new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                            row(
                                    true,
                                    (byte) 100,
                                    (short) 200,
                                    2,
                                    2 + 400L,
                                    500.1f,
                                    600.0d,
                                    "v222",
                                    Decimal.fromUnscaledLong(900, 5, 2),
                                    Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                    TimestampLtz.fromEpochMillis(1698235273400L),
                                    TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                    TimestampNtz.fromMillis(1698235273501L),
                                    TimestampNtz.fromMillis(1698235273501L, 8000),
                                    new byte[] {5, 6, 7, 8},
                                    TypeUtils.castFromString("2023-10-25", DataTypes.DATE()),
                                    TypeUtils.castFromString("09:30:00.0", DataTypes.TIME()),
                                    BinaryString.fromString("abc"),
                                    new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                            row(
                                    true,
                                    (byte) 100,
                                    (short) 200,
                                    3,
                                    3 + 400L,
                                    500.1f,
                                    600.0d,
                                    "v333",
                                    Decimal.fromUnscaledLong(900, 5, 2),
                                    Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                    TimestampLtz.fromEpochMillis(1698235273400L),
                                    TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                    TimestampNtz.fromMillis(1698235273501L),
                                    TimestampNtz.fromMillis(1698235273501L, 8000),
                                    new byte[] {5, 6, 7, 8},
                                    TypeUtils.castFromString("2023-10-25", DataTypes.DATE()),
                                    TypeUtils.castFromString("09:30:00.0", DataTypes.TIME()),
                                    BinaryString.fromString("abc"),
                                    new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));
            // write records
            writeRows(t1, rows, false);

            // check the status of replica of t1 after synced
            // not check start offset since we won't
            // update start log offset for primary key table
            // 3 initial + (3 deletes + 3 inserts) = 9
            assertReplicaStatus(t1Bucket, 9);

            checkDataInIcebergPrimaryKeyTable(t1, rows);

            // then create partitioned table and wait partitions are ready
            testPartitionedTableTiering();
        } finally {
            jobClient.cancel().get();
        }
    }

    private void checkDataInIcebergPrimaryKeyTable(
            TablePath tablePath, List<InternalRow> expectedRows) throws Exception {
        Iterator<Record> acturalIterator = getIcebergRecords(tablePath).iterator();
        Iterator<InternalRow> iterator = expectedRows.iterator();
        while (iterator.hasNext() && acturalIterator.hasNext()) {
            InternalRow row = iterator.next();
            Record record = acturalIterator.next();
            assertThat(record.get(0)).isEqualTo(row.getBoolean(0));
            assertThat(record.get(1)).isEqualTo((int) row.getByte(1));
            assertThat(record.get(2)).isEqualTo((int) row.getShort(2));
            assertThat(record.get(3)).isEqualTo(row.getInt(3));
            assertThat(record.get(4)).isEqualTo(row.getLong(4));
            assertThat(record.get(5)).isEqualTo(row.getFloat(5));
            assertThat(record.get(6)).isEqualTo(row.getDouble(6));
            assertThat(record.get(7)).isEqualTo(row.getString(7).toString());
            // Iceberg expects BigDecimal for decimal types.
            assertThat(record.get(8)).isEqualTo(row.getDecimal(8, 5, 2).toBigDecimal());
            assertThat(record.get(9)).isEqualTo(row.getDecimal(9, 20, 0).toBigDecimal());
            assertThat(record.get(10))
                    .isEqualTo(
                            OffsetDateTime.ofInstant(
                                    row.getTimestampLtz(10, 3).toInstant(), ZoneOffset.UTC));
            assertThat(record.get(11))
                    .isEqualTo(
                            OffsetDateTime.ofInstant(
                                    row.getTimestampLtz(11, 6).toInstant(), ZoneOffset.UTC));
            assertThat(record.get(12)).isEqualTo(row.getTimestampNtz(12, 6).toLocalDateTime());
            assertThat(record.get(13)).isEqualTo(row.getTimestampNtz(13, 6).toLocalDateTime());
            // Iceberg's Record interface expects ByteBuffer for binary types.
            assertThat(record.get(14)).isEqualTo(ByteBuffer.wrap(row.getBinary(14, 4)));
            assertThat(record.get(15))
                    .isEqualTo(DateTimeUtils.toLocalDate(row.getInt(15)))
                    .isEqualTo(LocalDate.of(2023, 10, 25));
            assertThat(record.get(16))
                    .isEqualTo(DateTimeUtils.toLocalTime(row.getInt(16)))
                    .isEqualTo(LocalTime.of(9, 30, 0, 0));
            assertThat(record.get(17)).isEqualTo(row.getChar(17, 3).toString());
            assertThat(record.get(18)).isEqualTo(ByteBuffer.wrap(row.getBytes(18)));
        }
        assertThat(acturalIterator.hasNext()).isFalse();
        assertThat(iterator.hasNext()).isFalse();
    }

    private Tuple2<Long, TableDescriptor> createPartitionedTable(TablePath partitionedTablePath)
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
                        .build();
        return Tuple2.of(
                createTable(partitionedTablePath, partitionedTableDescriptor),
                partitionedTableDescriptor);
    }

    private void testLogTableTiering() throws Exception {
        // then, create another log table
        TablePath t2 = TablePath.of(DEFAULT_DB, "logTable");
        long t2Id = createLogTable(t2, 1, false, logSchema);
        TableBucket t2Bucket = new TableBucket(t2Id, 0);
        List<InternalRow> flussRows = new ArrayList<>();
        List<InternalRow> rows;
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

        // check data in iceberg
        checkDataInIcebergAppendOnlyTable(t2, flussRows, 0);
    }

    private void testPartitionedTableTiering() throws Exception {
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

        // wait until synced to iceberg
        for (Long partitionId : partitionNameByIds.keySet()) {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, 0);
            assertReplicaStatus(tableBucket, 3);
        }

        // now, let's check data in iceberg per partition
        // check data in iceberg
        String partitionCol = partitionedTableDescriptor.getPartitionKeys().get(0);
        for (String partitionName : partitionNameByIds.values()) {
            checkDataInIcebergAppendOnlyPartitionedTable(
                    partitionedTablePath,
                    Collections.singletonMap(partitionCol, partitionName),
                    writtenRowsByPartition.get(partitionName),
                    0);
        }

        Map<String, String> properties =
                new HashMap<String, String>() {
                    {
                        put(
                                FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY,
                                "["
                                        + "{\"partition_id\":0,\"bucket\":0,\"offset\":3},"
                                        + "{\"partition_id\":1,\"bucket\":0,\"offset\":3}"
                                        + "]");
                    }
                };

        checkSnapshotPropertyInIceberg(partitionedTablePath, properties);
    }
}

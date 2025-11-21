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

package org.apache.fluss.lake.values;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.TypeUtils;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.lake.committer.BucketOffset.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test {@link ValuesLake} with Flink. */
public class ValuesTieringITCase extends FlinkValuesTieringTestBase {
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
                    .primaryKey("f_string")
                    .build();

    @BeforeAll
    protected static void beforeAll() {
        FlinkValuesTieringTestBase.beforeAll();
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
        List<InternalRow> expectedRows = new ArrayList<>();
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
        expectedRows.addAll(rows);
        writeRows(t1, rows);
        waitUntilSnapshot(t1Id, 1, 0);

        // then start tiering job
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            // check the status of replica after synced
            assertReplicaStatus(t1Bucket, 3);

            checkDataInValuesPrimaryKeyTable(t1, rows);
            // check snapshot property in iceberg
            Map<String, String> properties =
                    new HashMap<String, String>() {
                        {
                            put(
                                    FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY,
                                    "[{\"bucket\":0,\"offset\":3}]");
                        }
                    };
            checkSnapshotPropertyInValues(t1, properties);

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
            expectedRows.addAll(rows);
            // write records
            writeRows(t1, rows);

            // check the status of replica of t1 after synced
            // not check start offset since we won't
            // update start log offset for primary key table
            // 3 initial + (3 deletes + 3 inserts) = 9
            assertReplicaStatus(t1Bucket, expectedRows.size());

            checkDataInValuesPrimaryKeyTable(t1, expectedRows);
        } finally {
            jobClient.cancel().get();
        }
    }

    private void checkDataInValuesPrimaryKeyTable(
            TablePath tablePath, List<InternalRow> expectedRows) throws Exception {
        Iterator<InternalRow> acturalIterator = getValuesRecords(tablePath).iterator();
        Iterator<InternalRow> iterator = expectedRows.iterator();
        while (iterator.hasNext() && acturalIterator.hasNext()) {
            InternalRow row = iterator.next();
            InternalRow record = acturalIterator.next();
            assertThat(record.getBoolean(0)).isEqualTo(row.getBoolean(0));
            assertThat(record.getByte(1)).isEqualTo(row.getByte(1));
            assertThat(record.getShort(2)).isEqualTo(row.getShort(2));
            assertThat(record.getInt(3)).isEqualTo(row.getInt(3));
            assertThat(record.getLong(4)).isEqualTo(row.getLong(4));
            assertThat(record.getFloat(5)).isEqualTo(row.getFloat(5));
            assertThat(record.getDouble(6)).isEqualTo(row.getDouble(6));
            assertThat(record.getString(7)).isEqualTo(row.getString(7));
            // Iceberg expects BigDecimal for decimal types.
            assertThat(record.getDecimal(8, 5, 2).toBigDecimal())
                    .isEqualTo(row.getDecimal(8, 5, 2).toBigDecimal());
            assertThat(record.getDecimal(9, 20, 0).toBigDecimal())
                    .isEqualTo(row.getDecimal(9, 20, 0).toBigDecimal());
            assertThat(record.getTimestampLtz(10, 3).toInstant())
                    .isEqualTo(record.getTimestampLtz(10, 3).toInstant());
            assertThat(record.getTimestampLtz(11, 6).toInstant())
                    .isEqualTo(row.getTimestampLtz(11, 6).toInstant());
            assertThat(record.getTimestampNtz(12, 6).toLocalDateTime())
                    .isEqualTo(row.getTimestampNtz(12, 6).toLocalDateTime());
            assertThat(record.getTimestampNtz(13, 6).toLocalDateTime())
                    .isEqualTo(row.getTimestampNtz(13, 6).toLocalDateTime());
            // Iceberg's Record interface expects ByteBuffer for binary types.
            assertThat(record.getBinary(14, 4)).isEqualTo(row.getBinary(14, 4));
            assertThat(record.getInt(15)).isEqualTo(row.getInt(15));
            assertThat(record.getInt(16)).isEqualTo(row.getInt(16));
            assertThat(record.getChar(17, 3)).isEqualTo(row.getChar(17, 3));
            assertThat(record.getBinary(18, 10)).isEqualTo(row.getBinary(18, 10));
        }
        assertThat(acturalIterator.hasNext()).isFalse();
        assertThat(iterator.hasNext()).isFalse();
    }
}

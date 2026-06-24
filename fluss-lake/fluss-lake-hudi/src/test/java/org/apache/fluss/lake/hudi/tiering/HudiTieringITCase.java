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

package org.apache.fluss.lake.hudi.tiering;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.lake.hudi.testutils.FlinkHudiTieringTestBase;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.TypeUtils;
import org.apache.fluss.utils.types.Tuple2;

import org.apache.flink.core.execution.JobClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** IT case for tiering tables to Hudi. */
class HudiTieringITCase extends FlinkHudiTieringTestBase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(initConfig())
                    .setNumOfTabletServers(3)
                    .build();

    private static final Schema PK_SCHEMA =
            Schema.newBuilder()
                    .column("f_boolean", DataTypes.BOOLEAN())
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

    private static final Schema LOG_SCHEMA =
            Schema.newBuilder()
                    .column("f_int", DataTypes.INT())
                    .column("f_str", DataTypes.STRING())
                    .build();

    @BeforeAll
    protected static void beforeAll() {
        FlinkHudiTieringTestBase.beforeAll(FLUSS_CLUSTER_EXTENSION.getClientConfig());
    }

    @Test
    void testTiering() throws Exception {
        TablePath pkTablePath = TablePath.of(DEFAULT_DB, "pkTable");
        long pkTableId = createPkTable(pkTablePath, 1, false, PK_SCHEMA);
        TableBucket pkTableBucket = new TableBucket(pkTableId, 0);

        List<InternalRow> rows = Arrays.asList(pkRow(1, "v1"), pkRow(2, "v2"), pkRow(3, "v3"));
        writeRows(pkTablePath, rows, false);
        waitUntilSnapshot(pkTableId, 1);

        execEnv.enableCheckpointing(1000);
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            assertReplicaStatus(pkTableBucket, 3);
            checkDataInHudiMORTable(pkTablePath, "", rows, 0);
            checkFlussOffsetsInSnapshot(pkTablePath, Collections.singletonMap(pkTableBucket, 3L));

            testLogTableTiering();

            rows = Arrays.asList(pkRow(1, "v111"), pkRow(2, "v222"), pkRow(3, "v333"));
            writeRows(pkTablePath, rows, false);

            // 3 initial records + 3 delete records + 3 insert records.
            assertReplicaStatus(pkTableBucket, 9);
            checkDataInHudiMORTable(pkTablePath, "", rows, 0);
            checkFlussOffsetsInSnapshot(pkTablePath, Collections.singletonMap(pkTableBucket, 9L));

            testPartitionedTableTiering();
        } finally {
            jobClient.cancel().get();
        }
    }

    @Test
    void testPkTableTieringWithAutoCompaction() throws Exception {
        TablePath pkTablePath = TablePath.of(DEFAULT_DB, "pkTableWithAutoCompaction");
        long pkTableId = createPkTable(pkTablePath, 1, true, PK_SCHEMA);
        TableBucket pkTableBucket = new TableBucket(pkTableId, 0);

        List<InternalRow> rows = Arrays.asList(pkRow(1, "v1"), pkRow(2, "v2"), pkRow(3, "v3"));
        writeRows(pkTablePath, rows, false);
        waitUntilSnapshot(pkTableId, 1);

        execEnv.enableCheckpointing(1000);
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            assertReplicaStatus(pkTableBucket, 3);
            checkDataInHudiMORTable(pkTablePath, "", rows, 0);
            checkFlussOffsetsInSnapshot(pkTablePath, Collections.singletonMap(pkTableBucket, 3L));

            rows = Arrays.asList(pkRow(1, "v111"), pkRow(2, "v222"), pkRow(3, "v333"));
            writeRows(pkTablePath, rows, false);

            // 3 initial records + 3 delete records + 3 insert records.
            assertReplicaStatus(pkTableBucket, 9);
            checkDataInHudiMORTable(pkTablePath, "", rows, 0);
            checkFlussOffsetsInSnapshot(pkTablePath, Collections.singletonMap(pkTableBucket, 9L));

            rows = Arrays.asList(pkRow(1, "v1111"), pkRow(2, "v2222"), pkRow(3, "v3333"));
            writeRows(pkTablePath, rows, false);

            // 9 previous records + 3 delete records + 3 insert records.
            assertReplicaStatus(pkTableBucket, 15);
            checkDataInHudiMORTable(pkTablePath, "", rows, 0);
            checkFlussOffsetsInSnapshot(pkTablePath, Collections.singletonMap(pkTableBucket, 15L));
            checkHudiCompactionCommitted(pkTablePath);
        } finally {
            jobClient.cancel().get();
        }
    }

    private void testLogTableTiering() throws Exception {
        TablePath logTablePath = TablePath.of(DEFAULT_DB, "logTable");
        long logTableId = createLogTable(logTablePath, 1, false, LOG_SCHEMA);
        TableBucket logTableBucket = new TableBucket(logTableId, 0);

        List<InternalRow> flussRows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            List<InternalRow> rows =
                    Arrays.asList(
                            row(i + 1, "v" + (i + 1)),
                            row(i + 11, "v" + (i + 11)),
                            row(i + 21, "v" + (i + 21)));
            flussRows.addAll(rows);
            writeRows(logTablePath, rows, true);
        }

        assertReplicaStatus(logTableBucket, 30);
        assertThat(getLeaderReplica(logTableBucket).getLogTablet().getLakeMaxTimestamp())
                .isGreaterThan(-1);
        checkDataInHudiCOWTable(logTablePath, "", flussRows, 0, 0);
        checkFlussOffsetsInSnapshot(logTablePath, Collections.singletonMap(logTableBucket, 30L));
    }

    private void testPartitionedTableTiering() throws Exception {
        TablePath partitionedTablePath = TablePath.of(DEFAULT_DB, "partitionedTable");
        Tuple2<Long, TableDescriptor> tableIdAndDescriptor =
                createPartitionedTable(partitionedTablePath);
        Map<Long, String> partitionNameByIds = waitUntilPartitions(partitionedTablePath);

        TableDescriptor partitionedTableDescriptor = tableIdAndDescriptor.f1;
        Map<String, List<InternalRow>> writtenRowsByPartition =
                writeRowsIntoPartitionedTable(
                        partitionedTablePath, partitionedTableDescriptor, partitionNameByIds);
        long tableId = tableIdAndDescriptor.f0;

        Map<TableBucket, Long> expectedOffsets = new HashMap<>();
        for (Long partitionId : partitionNameByIds.keySet()) {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, 0);
            assertReplicaStatus(tableBucket, 3);
            expectedOffsets.put(tableBucket, 3L);
        }

        for (String partitionName : partitionNameByIds.values()) {
            checkDataInHudiMORPartitionTable(
                    partitionedTablePath,
                    partitionName,
                    writtenRowsByPartition.get(partitionName),
                    0);
        }
        checkFlussOffsetsInSnapshot(partitionedTablePath, expectedOffsets);
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
                        .distributedBy(1, "id")
                        .partitionedBy("date")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.YEAR)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .customProperty(HUDI_CONF_PREFIX + "precombine.field", "date")
                        .customProperty(
                                HUDI_CONF_PREFIX + "hoodie.datasource.write.recordkey.field", "id")
                        .build();
        return Tuple2.of(
                createTable(partitionedTablePath, partitionedTableDescriptor),
                partitionedTableDescriptor);
    }

    private static InternalRow pkRow(int id, String value) {
        return row(
                true,
                id,
                id + 400L,
                500.1f,
                600.0d,
                value,
                Decimal.fromUnscaledLong(900, 5, 2),
                Decimal.fromBigDecimal(new BigDecimal("1000"), 20, 0),
                TimestampLtz.fromEpochMillis(1698235273400L),
                TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                TimestampNtz.fromMillis(1698235273501L),
                TimestampNtz.fromMillis(1698235273501L, 8000),
                new byte[] {5, 6, 7, 8},
                TypeUtils.castFromString("2026-02-25", DataTypes.DATE()),
                TypeUtils.castFromString("09:30:00.0", DataTypes.TIME()),
                BinaryString.fromString("abc"),
                new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    }

    @Override
    protected FlussClusterExtension getFlussClusterExtension() {
        return FLUSS_CLUSTER_EXTENSION;
    }
}

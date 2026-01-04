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

package org.apache.fluss.flink.tiering;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.TableWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.ExceptionUtils;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.config.ConfigOptions.LAKE_TIERING_TABLE_DURATION_DETECT_INTERVAL;
import static org.apache.fluss.config.ConfigOptions.LAKE_TIERING_TABLE_DURATION_MAX;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/** The IT case for tiering. */
class TieringITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(initConfig())
                    .setNumOfTabletServers(3)
                    .build();

    protected static String warehousePath;
    protected static Connection conn;
    protected static Admin admin;
    protected static StreamExecutionEnvironment execEnv;

    protected static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1))
                // not to clean snapshots for test purpose
                .set(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, Integer.MAX_VALUE);
        conf.setString("datalake.format", "paimon");
        conf.setString("datalake.paimon.metastore", "filesystem");
        try {
            warehousePath =
                    Files.createTempDirectory("fluss-testing-datalake-tiered")
                            .resolve("warehouse")
                            .toString();
        } catch (Exception e) {
            throw new FlussRuntimeException("Failed to create warehouse path");
        }
        conf.setString("datalake.paimon.warehouse", warehousePath);
        return conf;
    }

    @BeforeAll
    static void beforeAll() {
        conn = ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig());
        admin = conn.getAdmin();
        execEnv =
                StreamExecutionEnvironment.getExecutionEnvironment()
                        .setParallelism(1)
                        .setRuntimeMode(RuntimeExecutionMode.STREAMING);
    }

    @Test
    void testTieringReachMaxDuration() throws Exception {
        TablePath logTablePath = TablePath.of("fluss", "logtable");
        createTable(logTablePath, false);
        TablePath pkTablePath = TablePath.of("fluss", "pktable");
        long pkTableId = createTable(pkTablePath, true);

        // write some records to log table
        List<InternalRow> rows = new ArrayList<>();
        int recordCount = 6;
        for (int i = 0; i < recordCount; i++) {
            rows.add(GenericRow.of(i, BinaryString.fromString("v" + i)));
        }
        writeRows(logTablePath, rows, true);

        rows = new ArrayList<>();
        //  write 6 records to primary key table, each bucket should only contain few record
        for (int i = 0; i < recordCount; i++) {
            rows.add(GenericRow.of(i, BinaryString.fromString("v" + i)));
        }
        writeRows(pkTablePath, rows, false);

        waitUntilSnapshot(pkTableId, 3, 0);

        JobClient jobClient = buildTieringJob(execEnv);

        try {
            // verify the tiered records is less than the table total record to
            // make sure tiering is forced to complete when reach max duration
            LakeSnapshot logTableLakeSnapshot = waitLakeSnapshot(logTablePath);
            long tieredRecords = countTieredRecords(logTableLakeSnapshot);
            assertThat(tieredRecords).isLessThan(recordCount);

            // verify the tiered records is less than the table total record to
            // make sure tiering is forced to complete when reach max duration
            LakeSnapshot pkTableLakeSnapshot = waitLakeSnapshot(pkTablePath);
            tieredRecords = countTieredRecords(pkTableLakeSnapshot);
            assertThat(tieredRecords).isLessThan(recordCount);
        } finally {
            jobClient.cancel();
        }
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (admin != null) {
            admin.close();
            admin = null;
        }
        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    private long countTieredRecords(LakeSnapshot lakeSnapshot) throws Exception {
        return lakeSnapshot.getTableBucketsOffset().values().stream()
                .mapToLong(Long::longValue)
                .sum();
    }

    private LakeSnapshot waitLakeSnapshot(TablePath tablePath) {
        return waitValue(
                () -> {
                    try {
                        return Optional.of(admin.getLatestLakeSnapshot(tablePath).get());
                    } catch (Exception e) {
                        if (ExceptionUtils.stripExecutionException(e)
                                instanceof LakeTableSnapshotNotExistException) {
                            return Optional.empty();
                        }
                        throw e;
                    }
                },
                Duration.ofSeconds(30),
                "Fail to wait for one round of tiering finish for table " + tablePath);
    }

    private long createTable(TablePath tablePath, boolean isPrimaryKeyTable) throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder().column("a", DataTypes.INT()).column("b", DataTypes.STRING());
        if (isPrimaryKeyTable) {
            schemaBuilder.primaryKey("a");
        }
        TableDescriptor.Builder tableDescriptorBuilder =
                TableDescriptor.builder()
                        .schema(schemaBuilder.build())
                        .distributedBy(3, "a")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500));

        // see TestingPaimonStoragePlugin#TestingPaimonWriter, we set write-pause
        // to 1s to make it easy to mock tiering reach max duration
        Map<String, String> customProperties = Collections.singletonMap("write-pause", "1s");
        tableDescriptorBuilder.customProperties(customProperties);
        return createTable(tablePath, tableDescriptorBuilder.build());
    }

    protected long createTable(TablePath tablePath, TableDescriptor tableDescriptor)
            throws Exception {
        admin.createTable(tablePath, tableDescriptor, true).get();
        return admin.getTableInfo(tablePath).get().getTableId();
    }

    private void writeRows(TablePath tablePath, List<InternalRow> rows, boolean append)
            throws Exception {
        try (Table table = conn.getTable(tablePath)) {
            TableWriter tableWriter;
            if (append) {
                tableWriter = table.newAppend().createWriter();
            } else {
                tableWriter = table.newUpsert().createWriter();
            }
            for (InternalRow row : rows) {
                if (tableWriter instanceof AppendWriter) {
                    ((AppendWriter) tableWriter).append(row);
                } else {
                    ((UpsertWriter) tableWriter).upsert(row);
                }
            }
            tableWriter.flush();
        }
    }

    private JobClient buildTieringJob(StreamExecutionEnvironment execEnv) throws Exception {
        Configuration lakeTieringConfig = new Configuration();
        lakeTieringConfig.set(LAKE_TIERING_TABLE_DURATION_MAX, Duration.ofSeconds(1));
        lakeTieringConfig.set(LAKE_TIERING_TABLE_DURATION_DETECT_INTERVAL, Duration.ofMillis(100));

        Configuration flussConfig = new Configuration();
        flussConfig.setString(
                ConfigOptions.BOOTSTRAP_SERVERS.key(),
                FLUSS_CLUSTER_EXTENSION.getBootstrapServers());
        return LakeTieringJobBuilder.newBuilder(
                        execEnv,
                        flussConfig,
                        new Configuration(),
                        lakeTieringConfig,
                        DataLakeFormat.PAIMON.toString())
                .build();
    }

    protected void waitUntilSnapshot(long tableId, int bucketNum, long snapshotId) {
        for (int i = 0; i < bucketNum; i++) {
            TableBucket tableBucket = new TableBucket(tableId, i);
            FLUSS_CLUSTER_EXTENSION.waitUntilSnapshotFinished(tableBucket, snapshotId);
        }
    }
}

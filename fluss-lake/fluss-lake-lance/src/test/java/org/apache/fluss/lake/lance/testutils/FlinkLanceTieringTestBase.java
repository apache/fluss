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

package org.apache.fluss.lake.lance.testutils;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.TableWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.flink.tiering.LakeTieringJobBuilder;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Files;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.flink.tiering.source.TieringSourceOptions.POLL_TIERING_TABLE_INTERVAL;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test base for sync to lance by Flink. */
public class FlinkLanceTieringTestBase {

    protected static final String DEFAULT_DB = "fluss";

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(initConfig())
                    .setNumOfTabletServers(3)
                    .build();

    protected StreamExecutionEnvironment execEnv;

    protected static Connection conn;
    protected static Admin admin;
    protected static Configuration clientConf;
    private static String warehousePath;

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1))
                // not to clean snapshots for test purpose
                .set(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, Integer.MAX_VALUE);
        conf.setString("datalake.format", "lance");
        try {
            warehousePath =
                    Files.createTempDirectory("fluss-testing-datalake-tiered")
                            .resolve("warehouse")
                            .toString();
        } catch (Exception e) {
            throw new FlussRuntimeException("Failed to create warehouse path");
        }
        conf.setString("datalake.lance.warehouse", warehousePath);
        return conf;
    }

    @BeforeAll
    protected static void beforeAll() {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    @BeforeEach
    public void beforeEach() {
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        execEnv.setParallelism(2);
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
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

    protected static Map<String, String> getLanceCatalogConf() {
        Map<String, String> lanceConf = new HashMap<>();
        lanceConf.put("warehouse", warehousePath);
        return lanceConf;
    }

    protected long createTable(TablePath tablePath, TableDescriptor tableDescriptor)
            throws Exception {
        admin.createTable(tablePath, tableDescriptor, true).get();
        return admin.getTableInfo(tablePath).get().getTableId();
    }

    protected long createLogTable(TablePath tablePath) throws Exception {
        return createLogTable(tablePath, 1);
    }

    protected long createLogTable(TablePath tablePath, int bucketNum) throws Exception {
        return createLogTable(tablePath, bucketNum, false);
    }

    protected Replica getLeaderReplica(TableBucket tableBucket) {
        return FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tableBucket);
    }

    protected void assertReplicaStatus(TableBucket tb, long expectedLogEndOffset) {
        retry(
                Duration.ofMinutes(1),
                () -> {
                    Replica replica = getLeaderReplica(tb);
                    // datalake snapshot id should be updated
                    assertThat(replica.getLogTablet().getLakeTableSnapshotId())
                            .isGreaterThanOrEqualTo(0);
                    assertThat(replica.getLakeLogEndOffset()).isEqualTo(expectedLogEndOffset);
                });
    }

    protected long createLogTable(TablePath tablePath, int bucketNum, boolean isPartitioned)
            throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder().column("a", DataTypes.INT()).column("b", DataTypes.STRING());

        TableDescriptor.Builder tableBuilder =
                TableDescriptor.builder()
                        .distributedBy(bucketNum, "a")
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

    protected void writeRows(TablePath tablePath, List<InternalRow> rows, boolean append)
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

    protected JobClient buildTieringJob(StreamExecutionEnvironment execEnv) throws Exception {
        Configuration flussConfig = new Configuration(clientConf);
        flussConfig.set(POLL_TIERING_TABLE_INTERVAL, Duration.ofMillis(500L));
        return LakeTieringJobBuilder.newBuilder(
                        execEnv,
                        flussConfig,
                        Configuration.fromMap(getLanceCatalogConf()),
                        new Configuration(),
                        DataLakeFormat.LANCE.toString())
                .build();
    }

    protected void waitUntilSnapshot(long tableId, int bucketNum, long snapshotId) {
        for (int i = 0; i < bucketNum; i++) {
            TableBucket tableBucket = new TableBucket(tableId, i);
            FLUSS_CLUSTER_EXTENSION.waitUntilSnapshotFinished(tableBucket, snapshotId);
        }
    }
}

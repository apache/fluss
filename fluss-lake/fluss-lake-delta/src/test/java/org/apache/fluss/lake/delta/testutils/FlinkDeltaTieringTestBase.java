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

package org.apache.fluss.lake.delta.testutils;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.flink.tiering.LakeTieringJobBuilder;
import org.apache.fluss.lake.delta.DeltaConfig;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Files;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.fluss.flink.tiering.source.TieringSourceOptions.POLL_TIERING_TABLE_INTERVAL;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test base for tiering to Delta Lake by Flink. */
public class FlinkDeltaTieringTestBase {

    protected static final String DEFAULT_DB = "fluss";

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(initConfig())
                    .setNumOfTabletServers(3)
                    .build();

    protected static Connection conn;
    protected static Admin admin;
    protected static Configuration clientConf;
    private static String warehousePath;

    /**
     * Set {@code -Ddelta.it.warehouse=s3a://bucket/path} to tier into another file system, and
     * {@code -Ddelta.it.hadoop.<key>=<value>} to pass Hadoop options such as {@code
     * fs.s3a.endpoint.region}. Each run writes under a fresh sub directory.
     */
    private static final String WAREHOUSE_PROPERTY = "delta.it.warehouse";

    private static final String HADOOP_PROPERTY_PREFIX = "delta.it.hadoop.";

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.DELTA);
        String externalWarehouse = System.getProperty(WAREHOUSE_PROPERTY);
        if (externalWarehouse != null) {
            warehousePath =
                    externalWarehouse.replaceAll("/+$", "")
                            + "/fluss-testing-delta-tiered-"
                            + UUID.randomUUID();
        } else {
            try {
                warehousePath =
                        Files.createTempDirectory("fluss-testing-delta-tiered")
                                .resolve("warehouse")
                                .toString();
            } catch (Exception e) {
                throw new FlussRuntimeException("Failed to create warehouse path");
            }
        }
        for (Map.Entry<String, String> entry : deltaConf().toMap().entrySet()) {
            conf.setString("datalake.delta." + entry.getKey(), entry.getValue());
        }
        return conf;
    }

    @BeforeAll
    protected static void beforeAll() {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
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

    protected static Configuration deltaConf() {
        Map<String, String> deltaConf = new HashMap<>();
        deltaConf.put(DeltaConfig.WAREHOUSE, warehousePath);
        for (String name : System.getProperties().stringPropertyNames()) {
            if (name.startsWith(HADOOP_PROPERTY_PREFIX)) {
                deltaConf.put(
                        name.substring(HADOOP_PROPERTY_PREFIX.length()), System.getProperty(name));
            }
        }
        return Configuration.fromMap(deltaConf);
    }

    protected static String deltaTablePath(TablePath tablePath) {
        return DeltaConfig.tablePath(deltaConf(), tablePath);
    }

    protected long createLogTable(TablePath tablePath, int bucketNum, boolean isPartitioned)
            throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("tags", DataTypes.ARRAY(DataTypes.STRING()))
                        .column(
                                "address",
                                DataTypes.ROW(
                                        DataTypes.FIELD("city", DataTypes.STRING()),
                                        DataTypes.FIELD("zip", DataTypes.INT())))
                        .column("ts", DataTypes.TIMESTAMP_LTZ(3))
                        .column("ts_ntz", DataTypes.TIMESTAMP(6));

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
        admin.createTable(tablePath, tableBuilder.build(), true).get();
        return admin.getTableInfo(tablePath).get().getTableId();
    }

    protected void writeRows(TablePath tablePath, List<InternalRow> rows) throws Exception {
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (InternalRow row : rows) {
                writer.append(row);
            }
            writer.flush();
        }
    }

    protected void assertReplicaStatus(TableBucket tb, long expectedLogEndOffset) {
        retry(
                Duration.ofMinutes(1),
                () -> {
                    Replica replica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tb);
                    assertThat(replica.getLogTablet().getLakeTableSnapshotId())
                            .isGreaterThanOrEqualTo(0);
                    assertThat(replica.getLakeLogEndOffset()).isEqualTo(expectedLogEndOffset);
                });
    }

    protected JobClient buildTieringJob(StreamExecutionEnvironment execEnv) throws Exception {
        Configuration flussConfig = new Configuration(clientConf);
        flussConfig.set(POLL_TIERING_TABLE_INTERVAL, Duration.ofMillis(500L));
        return LakeTieringJobBuilder.newBuilder(
                        execEnv,
                        flussConfig,
                        deltaConf(),
                        new Configuration(),
                        DataLakeFormat.DELTA.toString())
                .build();
    }
}

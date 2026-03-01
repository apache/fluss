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

package org.apache.fluss.lake.testutils;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.TableWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.flink.tiering.LakeTieringJobBuilder;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;

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
import java.util.Optional;

import static org.apache.fluss.flink.tiering.source.TieringSourceOptions.POLL_TIERING_TABLE_INTERVAL;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/** Test base for tiering to Iceberg by Flink. */
public class FlinkLakeTieringTestBase {

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
    protected static String warehousePath;

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1))
                .set(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, Integer.MAX_VALUE);

        // Configure the tiering sink to be Iceberg
        conf.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.ICEBERG);
        conf.setString("datalake.iceberg.type", "hadoop");
        try {
            warehousePath =
                    Files.createTempDirectory("fluss-testing-iceberg-tiered")
                            .resolve("warehouse")
                            .toString();
        } catch (Exception e) {
            throw new FlussRuntimeException("Failed to create Iceberg warehouse path", e);
        }
        conf.setString("datalake.iceberg.warehouse", warehousePath);
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

    @BeforeEach
    public void beforeEach() {
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        execEnv.setParallelism(2);
    }

    protected JobClient buildTieringJob(StreamExecutionEnvironment execEnv) throws Exception {
        Configuration flussConfig = new Configuration(clientConf);
        flussConfig.set(POLL_TIERING_TABLE_INTERVAL, Duration.ofMillis(500L));
        return LakeTieringJobBuilder.newBuilder(
                        execEnv,
                        flussConfig,
                        Configuration.fromMap(getIcebergCatalogConf()),
                        DataLakeFormat.ICEBERG.toString())
                .build();
    }

    protected static Map<String, String> getIcebergCatalogConf() {
        Map<String, String> icebergConf = new HashMap<>();
        icebergConf.put("type", "hadoop");
        icebergConf.put("warehouse", warehousePath);
        return icebergConf;
    }

    protected long createTable(TablePath tablePath, TableDescriptor tableDescriptor)
            throws Exception {
        admin.createTable(tablePath, tableDescriptor, true).get();
        return admin.getTableInfo(tablePath).get().getTableId();
    }

    protected void assertReplicaStatus(
            TablePath tablePath,
            long tableId,
            int bucketCount,
            boolean isPartitioned,
            Map<TableBucket, Long> expectedLogEndOffset) {
        if (isPartitioned) {
            Map<Long, String> partitionById =
                    waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), tablePath);
            for (Long partitionId : partitionById.keySet()) {
                for (int i = 0; i < bucketCount; i++) {
                    TableBucket tableBucket = new TableBucket(tableId, partitionId, i);
                    assertReplicaStatus(tableBucket, expectedLogEndOffset.get(tableBucket));
                }
            }
        } else {
            for (int i = 0; i < bucketCount; i++) {
                TableBucket tableBucket = new TableBucket(tableId, i);
                assertReplicaStatus(tableBucket, expectedLogEndOffset.get(tableBucket));
            }
        }
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

    /**
     * Wait until the default number of partitions is created. Return the map from partition id to
     * partition name.
     */
    public static Map<Long, String> waitUntilPartitions(
            ZooKeeperClient zooKeeperClient, TablePath tablePath) {
        return waitUntilPartitions(
                zooKeeperClient,
                tablePath,
                ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE.defaultValue());
    }

    public static Map<Long, String> waitUntilPartitions(TablePath tablePath) {
        return waitUntilPartitions(
                FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(),
                tablePath,
                ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE.defaultValue());
    }

    /**
     * Wait until the given number of partitions is created. Return the map from partition id to
     * partition name.
     */
    public static Map<Long, String> waitUntilPartitions(
            ZooKeeperClient zooKeeperClient, TablePath tablePath, int expectPartitions) {
        return waitValue(
                () -> {
                    Map<Long, String> gotPartitions =
                            zooKeeperClient.getPartitionIdAndNames(tablePath);
                    return expectPartitions == gotPartitions.size()
                            ? Optional.of(gotPartitions)
                            : Optional.empty();
                },
                Duration.ofMinutes(1),
                String.format("expect %d table partition has not been created", expectPartitions));
    }

    protected Replica getLeaderReplica(TableBucket tableBucket) {
        return FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tableBucket);
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

    protected void waitUntilBucketSynced(
            TablePath tablePath, long tableId, int bucketCount, boolean isPartition) {
        if (isPartition) {
            Map<Long, String> partitionById = waitUntilPartitions(tablePath);
            for (Long partitionId : partitionById.keySet()) {
                for (int i = 0; i < bucketCount; i++) {
                    TableBucket tableBucket = new TableBucket(tableId, partitionId, i);
                    waitUntilBucketSynced(tableBucket);
                }
            }
        } else {
            for (int i = 0; i < bucketCount; i++) {
                TableBucket tableBucket = new TableBucket(tableId, i);
                waitUntilBucketSynced(tableBucket);
            }
        }
    }

    protected void waitUntilBucketSynced(TableBucket tb) {
        waitUntil(
                () -> {
                    Replica replica = getLeaderReplica(tb);
                    return replica.getLogTablet().getLakeTableSnapshotId() >= 0;
                },
                Duration.ofMinutes(2),
                "bucket " + tb + " not synced");
    }
}

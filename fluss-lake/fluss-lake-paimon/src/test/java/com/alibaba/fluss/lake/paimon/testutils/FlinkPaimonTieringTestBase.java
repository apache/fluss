/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.paimon.testutils;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.TableWriter;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.flink.tiering.LakeTieringJobBuilder;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.server.replica.Replica;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.types.DataTypes;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.alibaba.fluss.flink.tiering.source.TieringSourceOptions.POLL_TIERING_TABLE_INTERVAL;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.waitUtil;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/** Test base for sync to paimon by Flink. */
public class FlinkPaimonTieringTestBase {
    protected static final String DEFAULT_DB = "fluss";

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(initConfig())
                    .setNumOfTabletServers(3)
                    .build();

    protected static final String CATALOG_NAME = "testcatalog";
    protected StreamExecutionEnvironment execEnv;

    protected static Connection conn;
    protected static Admin admin;
    protected static Configuration clientConf;
    private static String warehousePath;
    protected static Catalog paimonCatalog;

    private static Configuration initConfig() {
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
    protected static void beforeAll() {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
        paimonCatalog = getPaimonCatalog();
    }

    @BeforeEach
    public void beforeEach() {
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        execEnv.setParallelism(2);
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    protected JobClient buildTieringJob(StreamExecutionEnvironment execEnv) throws Exception {
        Configuration flussConfig = new Configuration(clientConf);
        flussConfig.set(POLL_TIERING_TABLE_INTERVAL, Duration.ofMillis(500L));
        return LakeTieringJobBuilder.newBuilder(
                        execEnv,
                        flussConfig,
                        Configuration.fromMap(getPaimonCatalogConf()),
                        DataLakeFormat.PAIMON.toString())
                .build();
    }

    protected static Map<String, String> getPaimonCatalogConf() {
        Map<String, String> paimonConf = new HashMap<>();
        paimonConf.put("metastore", "filesystem");
        paimonConf.put("warehouse", warehousePath);
        return paimonConf;
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

    protected long createTable(TablePath tablePath, TableDescriptor tableDescriptor)
            throws Exception {
        admin.createTable(tablePath, tableDescriptor, true).get();
        return admin.getTableInfo(tablePath).get().getTableId();
    }

    protected void waitUntilSnapshot(long tableId, int bucketNum, long snapshotId) {
        for (int i = 0; i < bucketNum; i++) {
            TableBucket tableBucket = new TableBucket(tableId, i);
            FLUSS_CLUSTER_EXTENSION.waitUtilSnapshotFinished(tableBucket, snapshotId);
        }
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

    protected Map<String, List<InternalRow>> writeRowsIntoPartitionedTable(
            TablePath tablePath,
            TableDescriptor tableDescriptor,
            Map<Long, String> partitionNameByIds)
            throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        Map<String, List<InternalRow>> writtenRowsByPartition = new HashMap<>();
        for (String partitionName : partitionNameByIds.values()) {
            List<InternalRow> partitionRows =
                    Arrays.asList(
                            row(11, "v1", partitionName),
                            row(12, "v2", partitionName),
                            row(13, "v3", partitionName));
            rows.addAll(partitionRows);
            writtenRowsByPartition.put(partitionName, partitionRows);
        }

        writeRows(tablePath, rows, !tableDescriptor.hasPrimaryKey());
        return writtenRowsByPartition;
    }

    /**
     * Wait until the default number of partitions is created. Return the map from partition id to
     * partition name. .
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

    protected static Catalog getPaimonCatalog() {
        Map<String, String> catalogOptions = getPaimonCatalogConf();
        return CatalogFactory.createCatalog(CatalogContext.create(Options.fromMap(catalogOptions)));
    }

    protected Replica getLeaderReplica(TableBucket tableBucket) {
        return FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tableBucket);
    }

    protected long createLogTable(TablePath tablePath) throws Exception {
        return createLogTable(tablePath, 1);
    }

    protected long createLogTable(TablePath tablePath, int bucketNum) throws Exception {
        return createLogTable(tablePath, bucketNum, false);
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

    protected long createPkTable(TablePath tablePath) throws Exception {
        return createPkTable(tablePath, 1);
    }

    protected long createPkTable(TablePath tablePath, int bucketNum) throws Exception {
        TableDescriptor table1Descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("b", DataTypes.STRING())
                                        .primaryKey("a")
                                        .build())
                        .distributedBy(bucketNum)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .build();
        return createTable(tablePath, table1Descriptor);
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

    protected void waitUtilBucketSynced(
            TablePath tablePath, long tableId, int bucketCount, boolean isPartition) {
        if (isPartition) {
            Map<Long, String> partitionById = waitUntilPartitions(tablePath);
            for (Long partitionId : partitionById.keySet()) {
                for (int i = 0; i < bucketCount; i++) {
                    TableBucket tableBucket = new TableBucket(tableId, partitionId, i);
                    waitUtilBucketSynced(tableBucket);
                }
            }
        } else {
            for (int i = 0; i < bucketCount; i++) {
                TableBucket tableBucket = new TableBucket(tableId, i);
                waitUtilBucketSynced(tableBucket);
            }
        }
    }

    protected void waitUtilBucketSynced(TableBucket tb) {
        waitUtil(
                () -> {
                    Replica replica = getLeaderReplica(tb);
                    return replica.getLogTablet().getLakeTableSnapshotId() >= 0;
                },
                Duration.ofMinutes(2),
                "bucket " + tb + "not synced");
    }

    protected Object[] rowValues(Object[] values, @Nullable String partition) {
        if (partition == null) {
            return values;
        } else {
            Object[] newValues = new Object[values.length + 1];
            System.arraycopy(values, 0, newValues, 0, values.length);
            newValues[values.length] = partition;
            return newValues;
        }
    }
}

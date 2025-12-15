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

package org.apache.fluss.lake.paimon.testutils;

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
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.CloseableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;

import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.fluss.flink.tiering.source.TieringSourceOptions.POLL_TIERING_TABLE_INTERVAL;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/** Test base for sync to paimon by Flink. */
public abstract class FlinkPaimonTieringTestBase {
    protected static final String DEFAULT_DB = "fluss";

    protected static final String CATALOG_NAME = "testcatalog";
    protected StreamExecutionEnvironment execEnv;

    protected static Connection conn;
    protected static Admin admin;
    protected static Configuration clientConf;
    protected static String warehousePath;
    protected static Catalog paimonCatalog;

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

    public static void beforeAll(Configuration conf) {
        clientConf = conf;
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
        paimonCatalog = getPaimonCatalog();
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

    protected abstract FlussClusterExtension getFlussClusterExtension();

    protected long createTable(TablePath tablePath, TableDescriptor tableDescriptor)
            throws Exception {
        admin.createTable(tablePath, tableDescriptor, true).get();
        return admin.getTableInfo(tablePath).get().getTableId();
    }

    protected void waitUntilSnapshot(long tableId, int bucketNum, long snapshotId) {
        for (int i = 0; i < bucketNum; i++) {
            TableBucket tableBucket = new TableBucket(tableId, i);
            getFlussClusterExtension().waitUntilSnapshotFinished(tableBucket, snapshotId);
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
     * partition name.
     */
    public static Map<Long, String> waitUntilPartitions(
            ZooKeeperClient zooKeeperClient, TablePath tablePath) {
        return waitUntilPartitions(
                zooKeeperClient,
                tablePath,
                ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE.defaultValue());
    }

    public Map<Long, String> waitUntilPartitions(TablePath tablePath) {
        return waitUntilPartitions(
                getFlussClusterExtension().getZooKeeperClient(),
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
        return getFlussClusterExtension().waitAndGetLeaderReplica(tableBucket);
    }

    protected long createLogTable(TablePath tablePath) throws Exception {
        return createLogTable(tablePath, 1);
    }

    protected long createLogTable(TablePath tablePath, int bucketNum) throws Exception {
        return createLogTable(
                tablePath, bucketNum, false, Collections.emptyMap(), Collections.emptyMap());
    }

    protected long createLogTable(
            TablePath tablePath,
            int bucketNum,
            boolean isPartitioned,
            Map<String, String> properties,
            Map<String, String> customProperties)
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
        tableBuilder.properties(properties);
        tableBuilder.customProperties(customProperties);
        tableBuilder.schema(schemaBuilder.build());
        return createTable(tablePath, tableBuilder.build());
    }

    protected long createFullTypeLogTable(TablePath tablePath, int bucketNum, boolean isPartitioned)
            throws Exception {
        return createFullTypeLogTable(tablePath, bucketNum, isPartitioned, true);
    }

    protected long createFullTypeLogTable(
            TablePath tablePath, int bucketNum, boolean isPartitioned, boolean lakeEnabled)
            throws Exception {
        Schema.Builder schemaBuilder =
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
                        .column("f_binary", DataTypes.BINARY(4));

        TableDescriptor.Builder tableBuilder =
                TableDescriptor.builder().distributedBy(bucketNum, "f_int");
        if (lakeEnabled) {
            tableBuilder
                    .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                    .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500));
        }

        if (isPartitioned) {
            schemaBuilder.column("p", DataTypes.STRING());
            tableBuilder.property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true);
            tableBuilder.partitionedBy("p");
            tableBuilder.property(
                    ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, AutoPartitionTimeUnit.YEAR);
        }
        tableBuilder.schema(schemaBuilder.build());
        return createTable(tablePath, tableBuilder.build());
    }

    protected long createPkTable(TablePath tablePath) throws Exception {
        return createPkTable(tablePath, 1);
    }

    protected long createPkTable(
            TablePath tablePath,
            Map<String, String> tableProperties,
            Map<String, String> tableCustomProperties)
            throws Exception {
        return createPkTable(tablePath, 1, tableProperties, tableCustomProperties);
    }

    protected long createPkTable(TablePath tablePath, int bucketNum) throws Exception {
        return createPkTable(tablePath, bucketNum, Collections.emptyMap(), Collections.emptyMap());
    }

    protected long createPkTable(
            TablePath tablePath,
            int bucketNum,
            Map<String, String> tableProperties,
            Map<String, String> tableCustomProperties)
            throws Exception {
        TableDescriptor.Builder tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("b", DataTypes.STRING())
                                        .primaryKey("a")
                                        .build())
                        .distributedBy(bucketNum)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500));
        tableDescriptor.customProperties(tableCustomProperties);
        tableDescriptor.properties(tableProperties);
        return createTable(tablePath, tableDescriptor.build());
    }

    protected void dropTable(TablePath tablePath) throws Exception {
        admin.dropTable(tablePath, false).get();
        Identifier tableIdentifier = toPaimonIdentifier(tablePath);
        try {
            paimonCatalog.dropTable(tableIdentifier, false);
        } catch (Catalog.TableNotExistException e) {
            // do nothing, table not exists
        }
    }

    private Identifier toPaimonIdentifier(TablePath tablePath) {
        return Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    protected void assertReplicaStatus(Map<TableBucket, Long> expectedLogEndOffset) {
        for (Map.Entry<TableBucket, Long> expectedLogEndOffsetEntry :
                expectedLogEndOffset.entrySet()) {
            assertReplicaStatus(
                    expectedLogEndOffsetEntry.getKey(), expectedLogEndOffsetEntry.getValue());
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

    protected void waitUntilBucketSynced(
            TablePath tablePath, long tableId, int bucketCount, boolean isPartition) {
        Set<TableBucket> tableBuckets = new HashSet<>();
        if (isPartition) {
            Map<Long, String> partitionById = waitUntilPartitions(tablePath);
            for (Long partitionId : partitionById.keySet()) {
                for (int i = 0; i < bucketCount; i++) {
                    TableBucket tableBucket = new TableBucket(tableId, partitionId, i);
                    tableBuckets.add(tableBucket);
                }
            }
        } else {
            for (int i = 0; i < bucketCount; i++) {
                TableBucket tableBucket = new TableBucket(tableId, i);
                tableBuckets.add(tableBucket);
            }
        }
        waitUntilBucketsSynced(tableBuckets);
    }

    protected void waitUntilBucketsSynced(Set<TableBucket> tableBuckets) {
        for (TableBucket tableBucket : tableBuckets) {
            waitUntilBucketSynced(tableBucket);
        }
    }

    protected void waitUntilBucketSynced(TableBucket tb) {
        waitUntil(
                () -> {
                    Replica replica = getLeaderReplica(tb);
                    return replica.getLogTablet().getLakeTableSnapshotId() >= 0;
                },
                Duration.ofMinutes(2),
                "bucket " + tb + "not synced");
    }

    protected void checkDataInPaimonPrimaryKeyTable(
            TablePath tablePath, List<InternalRow> expectedRows) throws Exception {
        Iterator<org.apache.paimon.data.InternalRow> paimonRowIterator =
                getPaimonRowCloseableIterator(tablePath);
        for (InternalRow expectedRow : expectedRows) {
            org.apache.paimon.data.InternalRow row = paimonRowIterator.next();
            assertThat(row.getInt(0)).isEqualTo(expectedRow.getInt(0));
            assertThat(row.getString(1).toString()).isEqualTo(expectedRow.getString(1).toString());
        }
    }

    protected CloseableIterator<org.apache.paimon.data.InternalRow> getPaimonRowCloseableIterator(
            TablePath tablePath) throws Exception {
        Identifier tableIdentifier =
                Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName());

        paimonCatalog = getPaimonCatalog();

        FileStoreTable table = (FileStoreTable) paimonCatalog.getTable(tableIdentifier);

        RecordReader<org.apache.paimon.data.InternalRow> reader =
                table.newRead().createReader(table.newReadBuilder().newScan().plan());
        return reader.toCloseableIterator();
    }

    protected void checkSnapshotPropertyInPaimon(
            TablePath tablePath, Map<String, String> expectedProperties) throws Exception {
        FileStoreTable table =
                (FileStoreTable)
                        getPaimonCatalog()
                                .getTable(
                                        Identifier.create(
                                                tablePath.getDatabaseName(),
                                                tablePath.getTableName()));
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        assertThat(snapshot).isNotNull();
        assertThat(snapshot.properties()).isEqualTo(expectedProperties);
    }
}

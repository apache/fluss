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

package org.apache.fluss.lake.iceberg.testutils;

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
import org.apache.fluss.metadata.Schema;
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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.parquet.Parquet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.fluss.flink.tiering.source.TieringSourceOptions.POLL_TIERING_TABLE_INTERVAL;
import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.assertj.core.api.Assertions.assertThat;

/** Test base for tiering to Iceberg by Flink. */
public class FlinkIcebergTieringTestBase {

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
    protected static Catalog icebergCatalog;

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
        icebergCatalog = getIcebergCatalog();
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
        if (icebergCatalog instanceof Closeable) {
            ((Closeable) icebergCatalog).close();
            icebergCatalog = null;
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

    protected static Catalog getIcebergCatalog() {
        HadoopCatalog catalog = new HadoopCatalog();
        catalog.setConf(new org.apache.hadoop.conf.Configuration());
        Map<String, String> properties = new HashMap<>();
        properties.put("warehouse", warehousePath);
        catalog.initialize("hadoop", properties);
        return catalog;
    }

    protected long createPkTable(
            TablePath tablePath, int bucketNum, boolean enableAutoCompaction, Schema schema)
            throws Exception {
        TableDescriptor.Builder pkTableBuilder =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(bucketNum)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500));

        if (enableAutoCompaction) {
            pkTableBuilder.property(ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION.key(), "true");
        }
        return createTable(tablePath, pkTableBuilder.build());
    }

    protected long createLogTable(
            TablePath tablePath, int bucketNum, boolean enableAutoCompaction, Schema schema)
            throws Exception {
        TableDescriptor.Builder tableBuilder =
                TableDescriptor.builder()
                        .distributedBy(bucketNum, "f_int")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500));

        if (enableAutoCompaction) {
            tableBuilder.property(ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION.key(), "true");
        }
        tableBuilder.schema(schema);
        return createTable(tablePath, tableBuilder.build());
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

    protected void waitUntilSnapshot(long tableId, int bucketNum, long snapshotId) {
        for (int i = 0; i < bucketNum; i++) {
            TableBucket tableBucket = new TableBucket(tableId, i);
            FLUSS_CLUSTER_EXTENSION.waitUntilSnapshotFinished(tableBucket, snapshotId);
        }
    }

    protected List<Record> getIcebergRecords(TablePath tablePath) throws IOException {
        List<Record> icebergRecords = new ArrayList<>();
        try (CloseableIterator<Record> records = getIcebergRows(tablePath)) {
            while (records.hasNext()) {
                icebergRecords.add(records.next());
            }
        }
        return icebergRecords;
    }

    protected void checkDataInIcebergAppendOnlyTable(
            TablePath tablePath, List<InternalRow> expectedRows, long startingOffset)
            throws Exception {
        try (CloseableIterator<Record> records = getIcebergRows(tablePath)) {
            Iterator<InternalRow> flussRowIterator = expectedRows.iterator();
            while (records.hasNext()) {
                Record actualRecord = records.next();
                InternalRow flussRow = flussRowIterator.next();
                assertThat(actualRecord.get(0)).isEqualTo(flussRow.getInt(0));
                assertThat(actualRecord.get(1)).isEqualTo(flussRow.getString(1).toString());
                // the idx 2 is __bucket, so use 3
                assertThat(actualRecord.get(3)).isEqualTo(startingOffset++);
            }
            assertThat(flussRowIterator.hasNext()).isFalse();
        }
    }

    protected void checkFileStatusInIcebergTable(
            TablePath tablePath, int expectedFileCount, boolean shouldDeleteFileExist)
            throws IOException {
        org.apache.iceberg.Table table = icebergCatalog.loadTable(toIceberg(tablePath));
        int count = 0;
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask ignored : tasks) {
                if (shouldDeleteFileExist) {
                    assertThat(ignored.deletes()).isNotEmpty();
                } else {
                    assertThat(ignored.deletes()).isEmpty();
                }
                count++;
            }
        }
        assertThat(count).isEqualTo(expectedFileCount);
    }

    protected void checkDataInIcebergAppendOnlyPartitionedTable(
            TablePath tablePath,
            Map<String, String> partitionSpec,
            List<InternalRow> expectedRows,
            long startingOffset)
            throws Exception {
        try (CloseableIterator<Record> records = getIcebergRows(tablePath, partitionSpec)) {
            Iterator<InternalRow> flussRowIterator = expectedRows.iterator();
            while (records.hasNext()) {
                Record actualRecord = records.next();
                InternalRow flussRow = flussRowIterator.next();
                assertThat(actualRecord.get(0)).isEqualTo(flussRow.getInt(0));
                assertThat(actualRecord.get(1)).isEqualTo(flussRow.getString(1).toString());
                assertThat(actualRecord.get(2)).isEqualTo(flussRow.getString(2).toString());
                // the idx 3 is __bucket, so use 4
                assertThat(actualRecord.get(4)).isEqualTo(startingOffset++);
            }
            assertThat(flussRowIterator.hasNext()).isFalse();
        }
    }

    private CloseableIterator<Record> getIcebergRows(TablePath tablePath) {
        return getIcebergRows(tablePath, Collections.emptyMap());
    }

    @SuppressWarnings("resource")
    private CloseableIterator<Record> getIcebergRows(
            TablePath tablePath, Map<String, String> partitionSpec) {
        org.apache.iceberg.Table table = icebergCatalog.loadTable(toIceberg(tablePath));
        // is primary key, we don't care about records order,
        // use iceberg read api directly
        if (!table.schema().identifierFieldIds().isEmpty()) {
            IcebergGenerics.ScanBuilder scanBuilder =
                    filterByPartition(IcebergGenerics.read(table), partitionSpec);
            return scanBuilder.build().iterator();
        } else {
            // is log table, we want to compare __offset column
            // so sort data files by __offset according to the column stats
            List<Record> records = new ArrayList<>();
            int fieldId = table.schema().findField(OFFSET_COLUMN_NAME).fieldId();
            SortedSet<DataFile> files =
                    new TreeSet<>(
                            (f1, f2) -> {
                                ByteBuffer buffer1 =
                                        (ByteBuffer)
                                                f1.lowerBounds()
                                                        .get(fieldId)
                                                        .order(ByteOrder.LITTLE_ENDIAN)
                                                        .rewind();
                                long offset1 = buffer1.getLong();
                                ByteBuffer buffer2 =
                                        (ByteBuffer)
                                                f2.lowerBounds()
                                                        .get(fieldId)
                                                        .order(ByteOrder.LITTLE_ENDIAN)
                                                        .rewind();
                                long offset2 = buffer2.getLong();
                                return Long.compare(offset1, offset2);
                            });

            table.refresh();
            TableScan tableScan = filterByPartition(table.newScan(), partitionSpec);
            tableScan
                    .includeColumnStats()
                    .planFiles()
                    .iterator()
                    .forEachRemaining(fileScanTask -> files.add(fileScanTask.file()));

            for (DataFile file : files) {
                Iterable<Record> iterable =
                        Parquet.read(table.io().newInputFile(file.location()))
                                .project(table.schema())
                                .createReaderFunc(
                                        fileSchema ->
                                                GenericParquetReaders.buildReader(
                                                        table.schema(), fileSchema))
                                .build();
                iterable.forEach(records::add);
            }

            return CloseableIterator.withClose(records.iterator());
        }
    }

    private IcebergGenerics.ScanBuilder filterByPartition(
            IcebergGenerics.ScanBuilder scanBuilder, Map<String, String> partitionSpec) {
        for (Map.Entry<String, String> partitionKeyAndValue : partitionSpec.entrySet()) {
            String partitionCol = partitionKeyAndValue.getKey();
            String partitionValue = partitionKeyAndValue.getValue();
            scanBuilder = scanBuilder.where(equal(partitionCol, partitionValue));
        }
        return scanBuilder;
    }

    private TableScan filterByPartition(TableScan tableScan, Map<String, String> partitionSpec) {
        for (Map.Entry<String, String> partitionKeyAndValue : partitionSpec.entrySet()) {
            String partitionCol = partitionKeyAndValue.getKey();
            String partitionValue = partitionKeyAndValue.getValue();
            tableScan = tableScan.filter(equal(partitionCol, partitionValue));
        }
        return tableScan;
    }

    protected void checkSnapshotPropertyInIceberg(
            TablePath tablePath, Map<String, String> expectedProperties) {
        org.apache.iceberg.Table table = icebergCatalog.loadTable(toIceberg(tablePath));
        Snapshot snapshot = table.currentSnapshot();
        assertThat(snapshot.summary()).containsAllEntriesOf(expectedProperties);
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

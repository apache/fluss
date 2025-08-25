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

package com.alibaba.fluss.lake.iceberg.testutils;

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
import com.alibaba.fluss.utils.DateTimeUtils;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.parquet.Parquet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
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
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.alibaba.fluss.flink.tiering.source.TieringSourceOptions.POLL_TIERING_TABLE_INTERVAL;
import static com.alibaba.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static com.alibaba.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.waitValue;
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

    protected long createPkTable(TablePath tablePath) throws Exception {
        return createPkTable(tablePath, 1);
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

    protected long createPkTable(TablePath tablePath, int bucketNum) throws Exception {
        TableDescriptor table1Descriptor =
                TableDescriptor.builder()
                        .schema(
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
                                        .build())
                        .distributedBy(bucketNum)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .build();
        return createTable(tablePath, table1Descriptor);
    }

    protected long createTable(TablePath tablePath, TableDescriptor tableDescriptor)
            throws Exception {
        admin.createTable(tablePath, tableDescriptor, true).get();
        return admin.getTableInfo(tablePath).get().getTableId();
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

    protected void checkDataInIcebergPrimaryKeyTable(
            TablePath tablePath, List<InternalRow> expectedRows) throws Exception {
        try (CloseableIterator<Record> records = getIcebergRows(tablePath)) {
            for (InternalRow row : expectedRows) {
                Record record = records.next();
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
            assertThat(records.hasNext()).isFalse();
        }
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
                        Parquet.read(table.io().newInputFile(file.path().toString()))
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
            TablePath tablePath, Map<String, String> expectedProperties) throws Exception {
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
}

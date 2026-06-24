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

package org.apache.fluss.lake.hudi.testutils;

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
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.hudi.source.UnifiedHudiTableReader;
import org.apache.fluss.lake.hudi.utils.HudiTableInfo;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.lake.LakeTable;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.data.RowData;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.catalog.HoodieCatalog;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.util.StreamerUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;

import java.io.Closeable;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.fluss.flink.tiering.source.TieringSourceOptions.POLL_TIERING_TABLE_INTERVAL;
import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/** Test base for tiering to Hudi by Flink. */
public abstract class FlinkHudiTieringTestBase {

    protected static final String DEFAULT_DB = "fluss";
    protected static final String CATALOG_NAME = "testcatalog";

    protected StreamExecutionEnvironment execEnv;

    protected static Connection conn;
    protected static Admin admin;
    protected static Configuration clientConf;
    protected static String warehousePath;
    protected static Catalog hudiCatalog;

    protected static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1))
                .set(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, Integer.MAX_VALUE);
        conf.set(ConfigOptions.SERVER_DATA_DISK_WRITE_LIMIT_RATIO, 1.0);
        conf.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.HUDI);
        conf.setString("datalake.hudi.mode", "dfs");

        try {
            warehousePath = Files.createTempDirectory("fluss-testing-hudi-tiered").toString();
        } catch (Exception e) {
            throw new FlussRuntimeException("Failed to create Hudi warehouse path", e);
        }
        conf.setString("datalake.hudi.catalog.path", warehousePath);
        return conf;
    }

    protected static void beforeAll(Configuration conf) {
        clientConf = conf;
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
        hudiCatalog = getHudiCatalog();
        hudiCatalog.open();
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
        if (hudiCatalog instanceof Closeable) {
            ((Closeable) hudiCatalog).close();
            hudiCatalog = null;
        }
    }

    @BeforeEach
    public void beforeEach() {
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        execEnv.setParallelism(2);
    }

    protected abstract FlussClusterExtension getFlussClusterExtension();

    protected JobClient buildTieringJob(StreamExecutionEnvironment execEnv) throws Exception {
        Configuration flussConfig = new Configuration(clientConf);
        flussConfig.set(POLL_TIERING_TABLE_INTERVAL, Duration.ofMillis(500L));
        return LakeTieringJobBuilder.newBuilder(
                        execEnv,
                        flussConfig,
                        Configuration.fromMap(getHudiCatalogConf()),
                        new Configuration(),
                        DataLakeFormat.HUDI.toString())
                .build();
    }

    protected static Map<String, String> getHudiCatalogConf() {
        Map<String, String> hudiConf = new HashMap<>();
        hudiConf.put("mode", "dfs");
        hudiConf.put("catalog.path", warehousePath);
        return hudiConf;
    }

    protected static Catalog getHudiCatalog() {
        return new HoodieCatalog(
                CATALOG_NAME,
                org.apache.flink.configuration.Configuration.fromMap(getHudiCatalogConf()));
    }

    protected long createPkTable(
            TablePath tablePath, int bucketNum, boolean enableAutoCompaction, Schema schema)
            throws Exception {
        TableDescriptor.Builder tableBuilder =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(bucketNum)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .customProperty("hudi.precombine.field", "f_time");

        if (enableAutoCompaction) {
            tableBuilder.property(ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION.key(), "true");
            tableBuilder.customProperty("hudi." + FlinkOptions.COMPACTION_DELTA_COMMITS.key(), "1");
        }
        return createTable(tablePath, tableBuilder.build());
    }

    protected long createLogTable(
            TablePath tablePath, int bucketNum, boolean enableAutoCompaction, Schema schema)
            throws Exception {
        TableDescriptor.Builder tableBuilder =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(bucketNum, "f_int")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .customProperty("hudi.precombine.field", "f_str")
                        .customProperty("hudi." + FlinkOptions.RECORD_KEY_FIELD.key(), "f_int");

        if (enableAutoCompaction) {
            tableBuilder.property(ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION.key(), "true");
            tableBuilder.customProperty("hudi." + FlinkOptions.COMPACTION_DELTA_COMMITS.key(), "1");
        }
        return createTable(tablePath, tableBuilder.build());
    }

    protected long createTable(TablePath tablePath, TableDescriptor tableDescriptor)
            throws Exception {
        admin.createTable(tablePath, tableDescriptor, true).get();
        return admin.getTableInfo(tablePath).get().getTableId();
    }

    protected void waitUntilSnapshot(long tableId, int bucketNum) {
        List<TableBucket> buckets = new ArrayList<>();
        for (int i = 0; i < bucketNum; i++) {
            buckets.add(new TableBucket(tableId, i));
        }
        getFlussClusterExtension().triggerAndWaitSnapshots(buckets);
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
     * Waits until the default number of partitions is created, and returns the partition id to
     * partition name mapping.
     */
    public Map<Long, String> waitUntilPartitions(TablePath tablePath) {
        return waitUntilPartitions(
                getFlussClusterExtension().getZooKeeperClient(),
                tablePath,
                ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE.defaultValue());
    }

    /**
     * Waits until the default number of partitions is created, and returns the partition id to
     * partition name mapping.
     */
    public static Map<Long, String> waitUntilPartitions(
            ZooKeeperClient zooKeeperClient, TablePath tablePath) {
        return waitUntilPartitions(
                zooKeeperClient,
                tablePath,
                ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE.defaultValue());
    }

    /**
     * Waits until the expected number of partitions is created, and returns the partition id to
     * partition name mapping.
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
        return getFlussClusterExtension().waitAndGetLeaderReplica(tableBucket);
    }

    protected void assertReplicaStatus(TableBucket tb, long expectedLogEndOffset) {
        retry(
                Duration.ofMinutes(1),
                () -> {
                    Replica replica = getLeaderReplica(tb);
                    assertThat(replica.getLogTablet().getLakeTableSnapshotId())
                            .isGreaterThanOrEqualTo(0);
                    assertThat(replica.getLakeLogEndOffset()).isEqualTo(expectedLogEndOffset);
                });
    }

    protected void waitUntilBucketSynced(TableBucket tb) {
        waitUntil(
                () -> getLeaderReplica(tb).getLogTablet().getLakeTableSnapshotId() >= 0,
                Duration.ofMinutes(2),
                "bucket " + tb + " not synced");
    }

    protected void checkDataInHudiMORTable(
            TablePath tablePath, String partition, List<InternalRow> expectedRows, int bucket)
            throws Exception {
        List<String> expectedRecords = new ArrayList<>();
        for (InternalRow row : expectedRows) {
            expectedRecords.add(formatMORRow(row));
        }

        List<String> actualRecords =
                collectHudiRows(
                        tablePath,
                        partition,
                        bucket,
                        record ->
                                record.getBoolean(5)
                                        + ","
                                        + record.getInt(6)
                                        + ","
                                        + record.getLong(7)
                                        + ","
                                        + record.getFloat(8)
                                        + ","
                                        + record.getDouble(9)
                                        + ","
                                        + record.getString(10).toString()
                                        + ","
                                        + record.getDecimal(11, 5, 2).toBigDecimal().toPlainString()
                                        + ","
                                        + record.getDecimal(12, 20, 0)
                                                .toBigDecimal()
                                                .toPlainString());

        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(expectedRecords);
    }

    protected void checkDataInHudiMORPartitionTable(
            TablePath tablePath, String partition, List<InternalRow> expectedRows, int bucket)
            throws Exception {
        List<String> expectedRecords = new ArrayList<>();
        for (InternalRow row : expectedRows) {
            expectedRecords.add(
                    row.getInt(0)
                            + ","
                            + row.getString(1).toString()
                            + ","
                            + row.getString(2).toString());
        }

        List<String> actualRecords =
                collectHudiRows(
                        tablePath,
                        partition,
                        bucket,
                        record ->
                                record.getInt(5)
                                        + ","
                                        + record.getString(6).toString()
                                        + ","
                                        + record.getString(7).toString());

        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(expectedRecords);
    }

    protected void checkDataInHudiCOWTable(
            TablePath tablePath,
            String partition,
            List<InternalRow> expectedRows,
            long startingOffset,
            int bucket)
            throws Exception {
        List<String> expectedRecords = new ArrayList<>();
        Iterator<InternalRow> flussRowIterator = expectedRows.iterator();
        while (flussRowIterator.hasNext()) {
            InternalRow flussRow = flussRowIterator.next();
            expectedRecords.add(
                    flussRow.getInt(0)
                            + ","
                            + flussRow.getString(1).toString()
                            + ","
                            + startingOffset++);
        }

        List<String> actualRecords =
                collectHudiRows(
                        tablePath,
                        partition,
                        bucket,
                        record ->
                                record.getInt(5)
                                        + ","
                                        + record.getString(6).toString()
                                        + ","
                                        + record.getLong(8));

        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(expectedRecords);
        assertThat(flussRowIterator.hasNext()).isFalse();
    }

    protected void checkFlussOffsetsInSnapshot(
            TablePath tablePath, Map<TableBucket, Long> expectedOffsets) throws Exception {
        try (HudiTableInfo hudiTableInfo =
                HudiTableInfo.create(tablePath, Configuration.fromMap(getHudiCatalogConf()))) {
            HoodieTableMetaClient metaClient = hudiTableInfo.getMetaClient();
            metaClient.reloadActiveTimeline();
            HoodieTimeline timeline =
                    metaClient
                            .getActiveTimeline()
                            .getCommitsAndCompactionTimeline()
                            .filterCompletedInstants();
            Optional<HoodieInstant> latestInstant =
                    timeline.getReverseOrderedInstantsByCompletionTime().findFirst();
            assertThat(latestInstant).isPresent();

            HoodieCommitMetadata metadata = timeline.readCommitMetadata(latestInstant.get());
            Map<String, String> extraMetadata = metadata.getExtraMetadata();
            assertThat(extraMetadata).isNotNull();
            String offsetFile = extraMetadata.get(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY);
            assertThat(offsetFile).isNotNull();

            Map<TableBucket, Long> recordedOffsets =
                    new LakeTable(
                                    new LakeTable.LakeSnapshotMetadata(
                                            -1, new FsPath(offsetFile), null))
                            .getOrReadLatestTableSnapshot()
                            .getBucketLogEndOffset();
            assertThat(recordedOffsets).isEqualTo(expectedOffsets);
        }
    }

    protected void checkHudiCompactionCommitted(TablePath tablePath) {
        retry(
                Duration.ofMinutes(1),
                () -> {
                    try (HudiTableInfo hudiTableInfo =
                            HudiTableInfo.create(
                                    tablePath, Configuration.fromMap(getHudiCatalogConf()))) {
                        HoodieTimeline timeline =
                                hudiTableInfo
                                        .getMetaClient()
                                        .getActiveTimeline()
                                        .getCommitsAndCompactionTimeline()
                                        .filterCompletedInstants();
                        assertThat(
                                        timeline.getInstantsAsStream()
                                                .anyMatch(
                                                        instant ->
                                                                isCompactionInstant(
                                                                        timeline, instant)))
                                .isTrue();
                    }
                });
    }

    private static String formatMORRow(InternalRow row) {
        return row.getBoolean(0)
                + ","
                + row.getInt(1)
                + ","
                + row.getLong(2)
                + ","
                + row.getFloat(3)
                + ","
                + row.getDouble(4)
                + ","
                + row.getString(5).toString()
                + ","
                + row.getDecimal(6, 5, 2).toBigDecimal().toPlainString()
                + ","
                + row.getDecimal(7, 20, 0).toBigDecimal().toPlainString();
    }

    private List<String> collectHudiRows(
            TablePath tablePath, String partition, int bucket, HudiRowFormatter formatter)
            throws Exception {
        try (HudiTableInfo hudiTableInfo =
                HudiTableInfo.create(tablePath, Configuration.fromMap(getHudiCatalogConf()))) {
            org.apache.hudi.org.apache.avro.Schema avroSchema =
                    StreamerUtil.getTableAvroSchema(hudiTableInfo.getMetaClient(), true);
            org.apache.flink.configuration.Configuration flinkHudiOptions =
                    buildFlinkHudiOptions(tablePath, hudiTableInfo, avroSchema);

            StorageConfiguration<org.apache.hadoop.conf.Configuration> hadoopConf =
                    new HadoopStorageConfiguration(
                            HadoopConfigurations.getHadoopConf(flinkHudiOptions));
            InternalSchemaManager internalSchemaManager =
                    InternalSchemaManager.get(hadoopConf, hudiTableInfo.getMetaClient());
            int columnCount = avroSchema.getFields().size();

            List<FileSlice> fileSlices =
                    getLatestFileSlicesAtCompletedInstant(hudiTableInfo, partition);
            List<String> records = new ArrayList<>();
            for (FileSlice fileSlice : fileSlices) {
                if (fileSlice.isEmpty()) {
                    continue;
                }
                if (!fileSlice.getFileId().contains(BucketIdentifier.bucketIdStr(bucket))) {
                    continue;
                }
                try (UnifiedHudiTableReader reader =
                                UnifiedHudiTableReader.newBuilder()
                                        .withMetaClient(hudiTableInfo.getMetaClient())
                                        .withInternalSchemaManager(internalSchemaManager)
                                        .withProps(flinkHudiOptions)
                                        .withTableSchema(avroSchema)
                                        .withSelectedFields(
                                                IntStream.range(0, columnCount).toArray())
                                        .withLatestCommitTime(fileSlice.getLatestInstantTime())
                                        .build();
                        ClosableIterator<RowData> iterator = reader.readFileSlice(fileSlice)) {
                    while (iterator.hasNext()) {
                        records.add(formatter.format(iterator.next()));
                    }
                }
            }
            return records;
        }
    }

    private static List<FileSlice> getLatestFileSlicesAtCompletedInstant(
            HudiTableInfo hudiTableInfo, String partition) {
        HoodieTimeline completedTimeline = hudiTableInfo.getCompletedTimeline();
        HoodieInstant latestInstant =
                completedTimeline
                        .lastInstant()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "No completed Hudi instant found for table "
                                                        + hudiTableInfo.getTablePath()
                                                        + "."));
        String latestInstantTime = latestInstant.requestedTime();
        if (hudiTableInfo.getTableType() == HoodieTableType.MERGE_ON_READ) {
            return hudiTableInfo
                    .getFileSystemView()
                    .getLatestMergedFileSlicesBeforeOrOn(partition, latestInstantTime)
                    .collect(Collectors.toList());
        }
        return hudiTableInfo
                .getFileSystemView()
                .getLatestFileSlicesBeforeOrOn(partition, latestInstantTime, true)
                .collect(Collectors.toList());
    }

    private org.apache.flink.configuration.Configuration buildFlinkHudiOptions(
            TablePath tablePath,
            HudiTableInfo hudiTableInfo,
            org.apache.hudi.org.apache.avro.Schema avroSchema) {
        Map<String, String> hudiOptions = new HashMap<>(hudiTableInfo.getTableOptions());
        hudiOptions.putAll(getHudiCatalogConf());
        hudiOptions.put(FlinkOptions.PATH.key(), hudiTableInfo.getBasePath());
        hudiOptions.put(FlinkOptions.TABLE_NAME.key(), tablePath.getTableName());
        hudiOptions.put(FlinkOptions.SOURCE_AVRO_SCHEMA.key(), avroSchema.toString());
        return org.apache.flink.configuration.Configuration.fromMap(hudiOptions);
    }

    private static boolean isCompactionInstant(HoodieTimeline timeline, HoodieInstant instant) {
        try {
            HoodieCommitMetadata metadata = timeline.readCommitMetadata(instant);
            return metadata != null && metadata.getOperationType() == WriteOperationType.COMPACT;
        } catch (Exception e) {
            throw new RuntimeException("Failed to read Hudi instant metadata " + instant + ".", e);
        }
    }

    private interface HudiRowFormatter {
        String format(RowData row);
    }
}

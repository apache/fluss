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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.lake.paimon.testutils.FlinkPaimonTieringTestBase;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.types.Tuple2;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.CloseableIterator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.lake.committer.BucketOffset.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** IT case for tiering tables to paimon. */
class PaimonTieringITCase extends FlinkPaimonTieringTestBase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(initConfig())
                    .setNumOfTabletServers(3)
                    .build();

    protected static final String DEFAULT_DB = "fluss";

    private static StreamExecutionEnvironment execEnv;
    private static Catalog paimonCatalog;

    @BeforeAll
    protected static void beforeAll() {
        FlinkPaimonTieringTestBase.beforeAll(FLUSS_CLUSTER_EXTENSION.getClientConfig());

        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(2);
        execEnv.enableCheckpointing(1000);
        paimonCatalog = getPaimonCatalog();
    }

    @Test
    void testTiering() throws Exception {
        // create a pk table, write some records and wait until snapshot finished
        TablePath t1 = TablePath.of(DEFAULT_DB, "pkTable");
        long t1Id = createPkTable(t1);
        TableBucket t1Bucket = new TableBucket(t1Id, 0);
        // write records
        List<InternalRow> rows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));
        writeRows(t1, rows, false);
        waitUntilSnapshot(t1Id, 1, 0);

        // then start tiering job
        JobClient jobClient = buildTieringJob(execEnv);

        try {
            // check the status of replica after synced
            assertReplicaStatus(t1Bucket, 3);
            // check data in paimon
            checkDataInPaimonPrimaryKeyTable(t1, rows);
            // check snapshot property in paimon
            Map<String, String> properties =
                    new HashMap<String, String>() {
                        {
                            put(
                                    FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY,
                                    "[{\"bucket\":0,\"offset\":3}]");
                        }
                    };
            checkSnapshotPropertyInPaimon(t1, properties);

            // then, create another log table
            TablePath t2 = TablePath.of(DEFAULT_DB, "logTable");
            long t2Id = createLogTable(t2);
            TableBucket t2Bucket = new TableBucket(t2Id, 0);
            List<InternalRow> flussRows = new ArrayList<>();
            // write records
            for (int i = 0; i < 10; i++) {
                rows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));
                flussRows.addAll(rows);
                // write records
                writeRows(t2, rows, true);
            }
            // check the status of replica after synced;
            // note: we can't update log start offset for unaware bucket mode log table
            assertReplicaStatus(t2Bucket, 30);

            // check data in paimon
            checkDataInPaimonAppendOnlyTable(t2, flussRows, 0);

            // then write data to the pk tables
            // write records
            rows = Arrays.asList(row(1, "v111"), row(2, "v222"), row(3, "v333"));
            // write records
            writeRows(t1, rows, false);

            // check the status of replica of t2 after synced
            // not check start offset since we won't
            // update start log offset for primary key table
            assertReplicaStatus(t1Bucket, 9);

            checkDataInPaimonPrimaryKeyTable(t1, rows);

            // then create partitioned table and wait partitions are ready
            TablePath partitionedTablePath = TablePath.of(DEFAULT_DB, "partitionedTable");
            Tuple2<Long, TableDescriptor> tableIdAndDescriptor =
                    createPartitionedTable(partitionedTablePath);
            Map<Long, String> partitionNameByIds = waitUntilPartitions(partitionedTablePath);

            // now, write rows into partitioned table
            TableDescriptor partitionedTableDescriptor = tableIdAndDescriptor.f1;
            Map<String, List<InternalRow>> writtenRowsByPartition =
                    writeRowsIntoPartitionedTable(
                            partitionedTablePath, partitionedTableDescriptor, partitionNameByIds);
            long tableId = tableIdAndDescriptor.f0;

            // wait until synced to paimon
            for (Long partitionId : partitionNameByIds.keySet()) {
                TableBucket tableBucket = new TableBucket(tableId, partitionId, 0);
                assertReplicaStatus(tableBucket, 3);
            }

            // now, let's check data in paimon per partition
            // check data in paimon
            String partitionCol = partitionedTableDescriptor.getPartitionKeys().get(0);
            for (String partitionName : partitionNameByIds.values()) {
                checkDataInPaimonAppendOnlyPartitionedTable(
                        partitionedTablePath,
                        Collections.singletonMap(partitionCol, partitionName),
                        writtenRowsByPartition.get(partitionName),
                        0);
            }

            properties =
                    new HashMap<String, String>() {
                        {
                            put(
                                    FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY,
                                    getPartitionOffsetStr(partitionNameByIds));
                        }
                    };
            checkSnapshotPropertyInPaimon(partitionedTablePath, properties);
        } finally {
            jobClient.cancel().get();
        }
    }

    @Test
    void testTieringForAlterTable() throws Exception {
        TablePath t1 = TablePath.of(DEFAULT_DB, "pkTableAlter");
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "false");

        long t1Id = createPkTable(t1, 1, tableProperties, Collections.emptyMap());

        TableChange.SetOption setOption =
                TableChange.set(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true");
        List<TableChange> changes = Collections.singletonList(setOption);
        admin.alterTable(t1, changes, false).get();

        TableBucket t1Bucket = new TableBucket(t1Id, 0);

        // write records
        List<InternalRow> rows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));
        writeRows(t1, rows, false);
        waitUntilSnapshot(t1Id, 1, 0);

        // then start tiering job
        JobClient jobClient = buildTieringJob(execEnv);

        try {
            // check the status of replica after synced
            assertReplicaStatus(t1Bucket, 3);
            // check data in paimon
            checkDataInPaimonPrimaryKeyTable(t1, rows);
            // check snapshot property in paimon
            Map<String, String> properties =
                    new HashMap<String, String>() {
                        {
                            put(
                                    FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY,
                                    "[{\"bucket\":0,\"offset\":3}]");
                        }
                    };
            checkSnapshotPropertyInPaimon(t1, properties);

            // then, create another log table
            TablePath t2 = TablePath.of(DEFAULT_DB, "logTableAlter");

            Map<String, String> logTableProperties = new HashMap<>();
            logTableProperties.put(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "false");
            long t2Id = createLogTable(t2, 1, false, logTableProperties, Collections.emptyMap());
            // enable lake
            admin.alterTable(t2, changes, false).get();

            TableBucket t2Bucket = new TableBucket(t2Id, 0);
            List<InternalRow> flussRows = new ArrayList<>();
            // write records
            for (int i = 0; i < 10; i++) {
                rows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));
                flussRows.addAll(rows);
                // write records
                writeRows(t2, rows, true);
            }
            // check the status of replica after synced;
            // note: we can't update log start offset for unaware bucket mode log table
            assertReplicaStatus(t2Bucket, 30);

            // check data in paimon
            checkDataInPaimonAppendOnlyTable(t2, flussRows, 0);

            // then write data to the pk tables
            // write records
            rows = Arrays.asList(row(1, "v111"), row(2, "v222"), row(3, "v333"));
            // write records
            writeRows(t1, rows, false);

            // check the status of replica of t2 after synced
            // not check start offset since we won't
            // update start log offset for primary key table
            assertReplicaStatus(t1Bucket, 9);

            checkDataInPaimonPrimaryKeyTable(t1, rows);

            // then create partitioned table and wait partitions are ready
            TablePath partitionedTablePath = TablePath.of(DEFAULT_DB, "partitionedTableAlter");
            Map<String, String> partitionTableProperties = new HashMap<>();
            partitionTableProperties.put(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "false");

            Tuple2<Long, TableDescriptor> tableIdAndDescriptor =
                    createPartitionedTable(
                            partitionedTablePath, partitionTableProperties, Collections.emptyMap());

            admin.alterTable(partitionedTablePath, changes, false).get();

            Map<Long, String> partitionNameByIds = waitUntilPartitions(partitionedTablePath);

            // now, write rows into partitioned table
            TableDescriptor partitionedTableDescriptor = tableIdAndDescriptor.f1;
            Map<String, List<InternalRow>> writtenRowsByPartition =
                    writeRowsIntoPartitionedTable(
                            partitionedTablePath, partitionedTableDescriptor, partitionNameByIds);
            long tableId = tableIdAndDescriptor.f0;

            // wait until synced to paimon
            for (Long partitionId : partitionNameByIds.keySet()) {
                TableBucket tableBucket = new TableBucket(tableId, partitionId, 0);
                assertReplicaStatus(tableBucket, 3);
            }

            // now, let's check data in paimon per partition
            // check data in paimon
            String partitionCol = partitionedTableDescriptor.getPartitionKeys().get(0);
            for (String partitionName : partitionNameByIds.values()) {
                checkDataInPaimonAppendOnlyPartitionedTable(
                        partitionedTablePath,
                        Collections.singletonMap(partitionCol, partitionName),
                        writtenRowsByPartition.get(partitionName),
                        0);
            }

            properties =
                    new HashMap<String, String>() {
                        {
                            put(
                                    FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY,
                                    getPartitionOffsetStr(partitionNameByIds));
                        }
                    };
            checkSnapshotPropertyInPaimon(partitionedTablePath, properties);
        } finally {
            jobClient.cancel().get();
        }
    }

    private String getPartitionOffsetStr(Map<Long, String> partitionNameByIds) {
        String raw =
                "{\"partition_id\":%s,\"bucket\":0,\"partition_name\":\"date=%s\",\"offset\":3}";
        List<Long> partitionIds = new ArrayList<>(partitionNameByIds.keySet());
        Collections.sort(partitionIds);
        List<String> partitionOffsetStrs = new ArrayList<>();

        for (Long partitionId : partitionIds) {
            String partitionName = partitionNameByIds.get(partitionId);
            String partitionOffsetStr = String.format(raw, partitionId, partitionName);
            partitionOffsetStrs.add(partitionOffsetStr);
        }

        return "[" + String.join(",", partitionOffsetStrs) + "]";
    }

    @Test
    void testTieringToDvEnabledTable() throws Exception {
        TablePath t1 = TablePath.of(DEFAULT_DB, "pkTableWithDv");
        long t1Id =
                createPkTable(
                        t1,
                        Collections.singletonMap("table.datalake.auto-compaction", "true"),
                        Collections.singletonMap("paimon.deletion-vectors.enabled", "true"));
        // write records
        List<InternalRow> rows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));
        writeRows(t1, rows, false);
        waitUntilSnapshot(t1Id, 1, 0);

        // then start tiering job
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            // check the status of replica after synced
            assertReplicaStatus(new TableBucket(t1Id, 0), 3);
            // check data in paimon
            checkDataInPaimonPrimaryKeyTable(t1, rows);
        } finally {
            jobClient.cancel().get();
        }
    }

    private Tuple2<Long, TableDescriptor> createPartitionedTable(TablePath partitionedTablePath)
            throws Exception {
        return createPartitionedTable(
                partitionedTablePath, Collections.emptyMap(), Collections.emptyMap());
    }

    private Tuple2<Long, TableDescriptor> createPartitionedTable(
            TablePath partitionedTablePath,
            Map<String, String> properties,
            Map<String, String> customProperties)
            throws Exception {
        TableDescriptor partitionedTableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("name", DataTypes.STRING())
                                        .column("date", DataTypes.STRING())
                                        .build())
                        .partitionedBy("date")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.YEAR)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .properties(properties)
                        .customProperties(customProperties)
                        .build();
        return Tuple2.of(
                createTable(partitionedTablePath, partitionedTableDescriptor),
                partitionedTableDescriptor);
    }

    private void checkDataInPaimonAppendOnlyTable(
            TablePath tablePath, List<InternalRow> expectedRows, long startingOffset)
            throws Exception {
        Iterator<org.apache.paimon.data.InternalRow> paimonRowIterator =
                getPaimonRowCloseableIterator(tablePath);
        Iterator<InternalRow> flussRowIterator = expectedRows.iterator();
        while (paimonRowIterator.hasNext()) {
            org.apache.paimon.data.InternalRow row = paimonRowIterator.next();
            InternalRow flussRow = flussRowIterator.next();
            assertThat(row.getInt(0)).isEqualTo(flussRow.getInt(0));
            assertThat(row.getString(1).toString()).isEqualTo(flussRow.getString(1).toString());
            // the idx 2 is __bucket, so use 3
            assertThat(row.getLong(3)).isEqualTo(startingOffset++);
        }
        assertThat(flussRowIterator.hasNext()).isFalse();
    }

    private void checkDataInPaimonAppendOnlyPartitionedTable(
            TablePath tablePath,
            Map<String, String> partitionSpec,
            List<InternalRow> expectedRows,
            long startingOffset)
            throws Exception {
        Iterator<org.apache.paimon.data.InternalRow> paimonRowIterator =
                getPaimonRowCloseableIterator(tablePath, partitionSpec);
        Iterator<InternalRow> flussRowIterator = expectedRows.iterator();
        while (paimonRowIterator.hasNext()) {
            org.apache.paimon.data.InternalRow row = paimonRowIterator.next();
            InternalRow flussRow = flussRowIterator.next();
            assertThat(row.getInt(0)).isEqualTo(flussRow.getInt(0));
            assertThat(row.getString(1).toString()).isEqualTo(flussRow.getString(1).toString());
            assertThat(row.getString(2).toString()).isEqualTo(flussRow.getString(2).toString());
            // the idx 3 is __bucket, so use 4
            assertThat(row.getLong(4)).isEqualTo(startingOffset++);
        }
        assertThat(flussRowIterator.hasNext()).isFalse();
    }

    private CloseableIterator<org.apache.paimon.data.InternalRow> getPaimonRowCloseableIterator(
            TablePath tablePath, Map<String, String> partitionSpec) throws Exception {
        Identifier tableIdentifier =
                Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName());

        FileStoreTable table = (FileStoreTable) paimonCatalog.getTable(tableIdentifier);

        RecordReader<org.apache.paimon.data.InternalRow> reader =
                table.newRead()
                        .createReader(
                                table.newReadBuilder()
                                        .withPartitionFilter(partitionSpec)
                                        .newScan()
                                        .plan());
        return reader.toCloseableIterator();
    }

    @Override
    protected FlussClusterExtension getFlussClusterExtension() {
        return FLUSS_CLUSTER_EXTENSION;
    }
}

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

package com.alibaba.fluss.lake.paimon.tiering;

import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.lake.paimon.testutils.FlinkPaimonTieringTestBase;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.utils.types.Tuple2;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.CloseableIterator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** IT case for tiering tables to paimon. */
class PaimonTieringITCase extends FlinkPaimonTieringTestBase {

    protected static final String DEFAULT_DB = "fluss";

    private static StreamExecutionEnvironment execEnv;
    private static Catalog paimonCatalog;

    @BeforeAll
    protected static void beforeAll() {
        FlinkPaimonTieringTestBase.beforeAll();
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

        // check the status of replica after synced
        assertReplicaStatus(t1Bucket, 3);
        // check data in paimon
        checkDataInPaimonPrimayKeyTable(t1, rows);

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

        checkDataInPaimonPrimayKeyTable(t1, rows);

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

        // wait util synced to paimon
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
        jobClient.cancel().get();
    }

    private Tuple2<Long, TableDescriptor> createPartitionedTable(TablePath partitionedTablePath)
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
}

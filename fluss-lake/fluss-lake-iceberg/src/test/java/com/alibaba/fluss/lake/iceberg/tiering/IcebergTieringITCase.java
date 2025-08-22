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

package com.alibaba.fluss.lake.iceberg.tiering;

import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.lake.iceberg.testutils.FlinkIcebergTieringTestBase;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.utils.types.Tuple2;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.lake.committer.BucketOffset.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static com.alibaba.fluss.testutils.DataTestUtils.row;

/** The ITCase for tiering into iceberg. */
class IcebergTieringITCase extends FlinkIcebergTieringTestBase {

    protected static final String DEFAULT_DB = "fluss";

    private static StreamExecutionEnvironment execEnv;

    @BeforeAll
    protected static void beforeAll() {
        FlinkIcebergTieringTestBase.beforeAll();
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(2);
        execEnv.enableCheckpointing(1000);
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

            checkDataInIcebergPrimaryKeyTable(t1, rows);
            // check snapshot property in iceberg
            Map<String, String> properties =
                    new HashMap<String, String>() {
                        {
                            put(
                                    FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY,
                                    "[{\"bucket_id\":0,\"log_offset\":3}]");
                        }
                    };
            checkSnapshotPropertyInIceberg(t1, properties);

            // test log table
            testLogTableTiering();

            // then write data to the pk tables
            // write records
            rows = Arrays.asList(row(1, "v111"), row(2, "v222"), row(3, "v333"));
            // write records
            writeRows(t1, rows, false);

            // check the status of replica of t1 after synced
            // not check start offset since we won't
            // update start log offset for primary key table
            // 3 initial + (3 deletes + 3 inserts) = 9
            assertReplicaStatus(t1Bucket, 9);

            checkDataInIcebergPrimaryKeyTable(t1, rows);

            // then create partitioned table and wait partitions are ready
            testPartitionedTableTiering();
        } finally {
            jobClient.cancel().get();
        }
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

    private void testLogTableTiering() throws Exception {
        // then, create another log table
        TablePath t2 = TablePath.of(DEFAULT_DB, "logTable");
        long t2Id = createLogTable(t2);
        TableBucket t2Bucket = new TableBucket(t2Id, 0);
        List<InternalRow> flussRows = new ArrayList<>();
        List<InternalRow> rows;
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

        // check data in iceberg
        checkDataInIcebergAppendOnlyTable(t2, flussRows, 0);
    }

    private void testPartitionedTableTiering() throws Exception {
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

        // wait until synced to iceberg
        for (Long partitionId : partitionNameByIds.keySet()) {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, 0);
            assertReplicaStatus(tableBucket, 3);
        }

        // now, let's check data in iceberg per partition
        // check data in iceberg
        String partitionCol = partitionedTableDescriptor.getPartitionKeys().get(0);
        for (String partitionName : partitionNameByIds.values()) {
            checkDataInIcebergAppendOnlyPartitionedTable(
                    partitionedTablePath,
                    Collections.singletonMap(partitionCol, partitionName),
                    writtenRowsByPartition.get(partitionName),
                    0);
        }

        Map<String, String> properties =
                new HashMap<String, String>() {
                    {
                        put(
                                FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY,
                                "["
                                        + "{\"partition_id\":0,\"bucket_id\":0,\"partition_name\":\"date=2025\",\"log_offset\":3},"
                                        + "{\"partition_id\":1,\"bucket_id\":0,\"partition_name\":\"date=2026\",\"log_offset\":3}"
                                        + "]");
                    }
                };

        checkSnapshotPropertyInIceberg(partitionedTablePath, properties);
    }
}

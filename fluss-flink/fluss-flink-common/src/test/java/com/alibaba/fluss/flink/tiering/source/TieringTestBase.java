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

package com.alibaba.fluss.flink.tiering.source;

import com.alibaba.fluss.bucketing.BucketingFunction;
import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.client.write.HashBucketAssigner;
import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.encode.KeyEncoder;
import com.alibaba.fluss.rpc.gateway.CoordinatorGateway;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.test.util.AbstractTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.nio.file.Files;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.fluss.testutils.DataTestUtils.row;

/** A base class for testing {@link TieringSource} with Fluss cluster prepared. */
public class TieringTestBase extends AbstractTestBase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(flussClusterConfig())
                    .setNumOfTabletServers(3)
                    .build();

    protected static final int DEFAULT_BUCKET_NUM = 3;

    protected static final Schema DEFAULT_PK_TABLE_SCHEMA =
            Schema.newBuilder()
                    .primaryKey("id")
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .build();

    protected static final Schema DEFAULT_LOG_TABLE_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .build();

    protected static final TableDescriptor DEFAULT_PK_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(DEFAULT_PK_TABLE_SCHEMA)
                    .distributedBy(DEFAULT_BUCKET_NUM, "id")
                    .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                    .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(200))
                    .build();

    protected static final TableDescriptor DEFAULT_LOG_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(DEFAULT_LOG_TABLE_SCHEMA)
                    .distributedBy(DEFAULT_BUCKET_NUM, "id")
                    .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                    .property(ConfigOptions.TABLE_DATALAKE_FORMAT, DataLakeFormat.PAIMON)
                    .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(200))
                    .build();

    protected static final TableDescriptor DEFAULT_AUTO_PARTITIONED_LOG_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(
                            Schema.newBuilder()
                                    .column("id", DataTypes.INT())
                                    .column("name", DataTypes.STRING())
                                    .column("date", DataTypes.STRING())
                                    .build())
                    .distributedBy(DEFAULT_BUCKET_NUM, "id")
                    .partitionedBy("date")
                    .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                    .property(
                            ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                            AutoPartitionTimeUnit.YEAR)
                    .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                    .property(ConfigOptions.TABLE_DATALAKE_FORMAT, DataLakeFormat.PAIMON)
                    .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                    .build();

    protected static final TableDescriptor DEFAULT_AUTO_PARTITIONED_PK_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(
                            Schema.newBuilder()
                                    .column("id", DataTypes.INT())
                                    .column("name", DataTypes.STRING())
                                    .column("date", DataTypes.STRING())
                                    .primaryKey("id", "date")
                                    .build())
                    .distributedBy(DEFAULT_BUCKET_NUM)
                    .partitionedBy("date")
                    .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                    .property(
                            ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                            AutoPartitionTimeUnit.YEAR)
                    .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                    .property(ConfigOptions.TABLE_DATALAKE_FORMAT, DataLakeFormat.PAIMON)
                    .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                    .build();

    protected static final String DEFAULT_DB = "test-tiering-db";

    protected static final TablePath DEFAULT_TABLE_PATH =
            TablePath.of(DEFAULT_DB, "tiering-test-table");

    protected static Connection conn;
    protected static Admin admin;
    protected static CoordinatorGateway coordinatorGateway;

    protected static Configuration clientConf;
    protected static String bootstrapServers;

    @BeforeAll
    protected static void beforeAll() {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        bootstrapServers = FLUSS_CLUSTER_EXTENSION.getBootstrapServers();
        coordinatorGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    @BeforeEach
    protected void beforeEach() throws Exception {
        admin.createDatabase(DEFAULT_DB, DatabaseDescriptor.EMPTY, true).get();
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

    private static Configuration flussClusterConfig() {
        Configuration conf = new Configuration();
        // set snapshot interval to 1s for testing purposes
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1));
        // not to clean snapshots for test purpose
        conf.set(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, Integer.MAX_VALUE);

        // enable lake tiering
        conf.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);
        conf.setString("datalake.paimon.metastore", "filesystem");
        String warehousePath;
        try {
            warehousePath =
                    Files.createTempDirectory("fluss-testing-datalake-enabled")
                            .resolve("warehouse")
                            .toString();
        } catch (Exception e) {
            throw new FlussRuntimeException("Failed to create warehouse path");
        }
        conf.setString("datalake.paimon.warehouse", warehousePath);

        return conf;
    }

    protected long createTable(TablePath tablePath, TableDescriptor tableDescriptor)
            throws Exception {
        admin.createTable(tablePath, tableDescriptor, true).get();
        long tableId = admin.getTableInfo(tablePath).get().getTableId();
        FLUSS_CLUSTER_EXTENSION.waitUtilTableReady(tableId);
        return tableId;
    }

    protected void dropTable(TablePath tablePath) throws Exception {
        admin.dropTable(tablePath, true).get();
    }

    protected long createPartitionedTable(TablePath tablePath, TableDescriptor tableDescriptor)
            throws Exception {
        admin.createTable(tablePath, tableDescriptor, true).get();
        return admin.getTableInfo(tablePath).get().getTableId();
    }

    protected void waitUntilSnapshot(long tableId, long snapshotId) {
        waitUntilSnapshot(tableId, null, snapshotId);
    }

    protected void waitUntilPartitionTableSnapshot(
            long tableId, Map<String, Long> partitionNameByIds, long snapshotId) {
        for (Map.Entry<String, Long> entry : partitionNameByIds.entrySet()) {
            for (int i = 0; i < DEFAULT_BUCKET_NUM; i++) {
                TableBucket tableBucket = new TableBucket(tableId, entry.getValue(), i);
                FLUSS_CLUSTER_EXTENSION.waitUtilSnapshotFinished(tableBucket, snapshotId);
            }
        }
    }

    protected void waitUntilSnapshot(long tableId, @Nullable Long partitionId, long snapshotId) {
        for (int i = 0; i < DEFAULT_BUCKET_NUM; i++) {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, i);
            FLUSS_CLUSTER_EXTENSION.waitUtilSnapshotFinished(tableBucket, snapshotId);
        }
    }

    protected static Map<Long, Map<Integer, Long>> upsertRowForPartitionedTable(
            TablePath tablePath,
            TableDescriptor tableDescriptor,
            Map<String, Long> partitionNameByIds,
            int pkStart,
            int rowsNum)
            throws Exception {
        Map<Long, Map<Integer, Long>> bucketRows = new HashMap<>();
        for (Map.Entry<String, Long> partitionEntry : partitionNameByIds.entrySet()) {
            Map<Integer, Long> bucketRowsForPartition =
                    upsertRow(
                            tablePath, tableDescriptor, pkStart, rowsNum, partitionEntry.getKey());
            bucketRows.put(partitionEntry.getValue(), bucketRowsForPartition);
        }
        return bucketRows;
    }

    protected static Map<Integer, Long> upsertRow(
            TablePath tablePath, TableDescriptor tableDescriptor, int pkStart, int rowsNum)
            throws Exception {
        return upsertRow(tablePath, tableDescriptor, pkStart, rowsNum, null);
    }

    protected static Map<Integer, Long> upsertRow(
            TablePath tablePath,
            TableDescriptor tableDescriptor,
            int pkStart,
            int rowsNum,
            @Nullable String partitionName)
            throws Exception {
        RowType rowType = tableDescriptor.getSchema().getRowType();
        // use lake keyEncoder and bucketAssigner fot tiering tests
        KeyEncoder keyEncoder =
                KeyEncoder.of(rowType, tableDescriptor.getBucketKeys(), DataLakeFormat.PAIMON);
        HashBucketAssigner hashBucketAssigner =
                new HashBucketAssigner(
                        DEFAULT_BUCKET_NUM, BucketingFunction.of(DataLakeFormat.PAIMON));
        Map<Integer, Long> bucketRows = new HashMap<>();
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            for (int i = pkStart; i < pkStart + rowsNum; i++) {
                InternalRow row =
                        partitionName == null ? row(i, "v" + i) : row(i, "v" + i, partitionName);
                upsertWriter.upsert(row);
                // bucket statistics
                byte[] key = keyEncoder.encodeKey(row);
                int bucketId = hashBucketAssigner.assignBucket(key);
                bucketRows.merge(bucketId, 1L, Long::sum);
            }
            upsertWriter.flush();
        }
        return bucketRows;
    }

    protected static Map<Long, Map<Integer, Long>> appendRowForPartitionedTable(
            TablePath tablePath,
            TableDescriptor tableDescriptor,
            Map<String, Long> partitionNameByIds,
            int pkStart,
            int rowsNum)
            throws Exception {
        Map<Long, Map<Integer, Long>> bucketRows = new HashMap<>();
        for (Map.Entry<String, Long> partitionEntry : partitionNameByIds.entrySet()) {
            Map<Integer, Long> bucketRowsForPartition =
                    appendRow(
                            tablePath, tableDescriptor, pkStart, rowsNum, partitionEntry.getKey());
            bucketRows.put(partitionEntry.getValue(), bucketRowsForPartition);
        }
        return bucketRows;
    }

    protected static Map<Integer, Long> appendRow(
            TablePath tablePath, TableDescriptor tableDescriptor, int idStart, int rowsNum)
            throws Exception {
        return appendRow(tablePath, tableDescriptor, idStart, rowsNum, null);
    }

    protected static Map<Integer, Long> appendRow(
            TablePath tablePath,
            TableDescriptor tableDescriptor,
            int idStart,
            int rowsNum,
            @Nullable String partitionName)
            throws Exception {
        RowType rowType = tableDescriptor.getSchema().getRowType();
        // use lake keyEncoder and bucketAssigner fot tiering tests
        KeyEncoder keyEncoder =
                KeyEncoder.of(rowType, tableDescriptor.getBucketKeys(), DataLakeFormat.PAIMON);
        HashBucketAssigner hashBucketAssigner =
                new HashBucketAssigner(
                        DEFAULT_BUCKET_NUM, BucketingFunction.of(DataLakeFormat.PAIMON));
        Map<Integer, Long> bucketRows = new HashMap<>();
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            for (int i = idStart; i < idStart + rowsNum; i++) {
                InternalRow row =
                        partitionName == null ? row(i, "v" + i) : row(i, "v" + i, partitionName);
                appendWriter.append(row);
                byte[] key = keyEncoder.encodeKey(row);
                int bucketId = hashBucketAssigner.assignBucket(key);

                bucketRows.merge(bucketId, 1L, Long::sum);
            }
            appendWriter.flush();
        }
        return bucketRows;
    }
}

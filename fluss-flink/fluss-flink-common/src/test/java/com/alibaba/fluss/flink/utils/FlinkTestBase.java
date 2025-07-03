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

package com.alibaba.fluss.flink.utils;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.TableWriter;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.cluster.TabletServerInfo;
import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.server.coordinator.MetadataManager;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.PartitionAssignment;
import com.alibaba.fluss.server.zk.data.TableAssignment;
import com.alibaba.fluss.types.DataTypes;

import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.alibaba.fluss.server.utils.TableAssignmentUtils.generateAssignment;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/** A base class for testing with Fluss cluster prepared. */
public class FlinkTestBase extends AbstractTestBase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(
                            new Configuration()
                                    // set snapshot interval to 1s for testing purposes
                                    .set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1))
                                    // not to clean snapshots for test purpose
                                    .set(
                                            ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS,
                                            Integer.MAX_VALUE))
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
                    .build();

    protected static final TableDescriptor DEFAULT_LOG_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(DEFAULT_LOG_TABLE_SCHEMA)
                    .distributedBy(DEFAULT_BUCKET_NUM, "id")
                    .build();

    protected static final TableDescriptor DEFAULT_AUTO_PARTITIONED_LOG_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(
                            Schema.newBuilder()
                                    .column("id", DataTypes.INT())
                                    .column("name", DataTypes.STRING())
                                    .build())
                    .distributedBy(DEFAULT_BUCKET_NUM)
                    .partitionedBy("name")
                    .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                    .property(
                            ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                            AutoPartitionTimeUnit.YEAR)
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
                    .build();

    protected static final String DEFAULT_DB = "test-flink-db";

    protected static final TablePath DEFAULT_TABLE_PATH =
            TablePath.of(DEFAULT_DB, "test-flink-table");

    protected static Connection conn;
    protected static Admin admin;

    protected static Configuration clientConf;
    protected static String bootstrapServers;

    @BeforeAll
    protected static void beforeAll() {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        bootstrapServers = FLUSS_CLUSTER_EXTENSION.getBootstrapServers();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    @BeforeEach
    void beforeEach() throws Exception {
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

    protected long createTable(TablePath tablePath, TableDescriptor tableDescriptor)
            throws Exception {
        admin.createTable(tablePath, tableDescriptor, true).get();
        return admin.getTableInfo(tablePath).get().getTableId();
    }

    public static List<String> assertAndCollectRecords(
            org.apache.flink.util.CloseableIterator<Row> iterator, int expectedNum)
            throws Exception {
        List<String> actual = new ArrayList<>(expectedNum);
        for (int i = 0; i < expectedNum; i++) {
            actual.add(iterator.next().toString());
        }
        assertThat(iterator.hasNext()).isFalse();
        iterator.close();
        return actual;
    }

    protected void waitUntilSnapshot(long tableId, long snapshotId) {
        for (int i = 0; i < DEFAULT_BUCKET_NUM; i++) {
            TableBucket tableBucket = new TableBucket(tableId, i);
            FLUSS_CLUSTER_EXTENSION.waitUtilSnapshotFinished(tableBucket, snapshotId);
        }
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

    public static Map<Long, String> createPartitions(
            ZooKeeperClient zkClient, TablePath tablePath, List<String> partitionsToCreate)
            throws Exception {
        MetadataManager metadataManager = new MetadataManager(zkClient, new Configuration());
        TableInfo tableInfo = metadataManager.getTable(tablePath);
        Map<Long, String> newPartitionIds = new HashMap<>();
        for (String partition : partitionsToCreate) {
            long partitionId = zkClient.getPartitionIdAndIncrement();
            newPartitionIds.put(partitionId, partition);
            TableAssignment assignment =
                    generateAssignment(
                            tableInfo.getNumBuckets(),
                            tableInfo.getTableConfig().getReplicationFactor(),
                            new TabletServerInfo[] {
                                new TabletServerInfo(0, "rack0"),
                                new TabletServerInfo(1, "rack1"),
                                new TabletServerInfo(2, "rack2")
                            });

            // register partition assignments and metadata
            zkClient.registerPartitionAssignmentAndMetadata(
                    partitionId,
                    partition,
                    new PartitionAssignment(
                            tableInfo.getTableId(), assignment.getBucketAssignments()),
                    tablePath,
                    tableInfo.getTableId());
        }
        return newPartitionIds;
    }

    public static void dropPartitions(
            ZooKeeperClient zkClient, TablePath tablePath, Set<String> droppedPartitions)
            throws Exception {
        for (String partition : droppedPartitions) {
            zkClient.deletePartition(tablePath, partition);
        }
    }

    public static List<String> writeRowsToPartition(
            Connection connection, TablePath tablePath, Collection<String> partitions)
            throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        List<String> expectedRowValues = new ArrayList<>();
        for (String partition : partitions) {
            for (int i = 0; i < 10; i++) {
                rows.add(row(i, "v1", partition));
                expectedRowValues.add(String.format("+I[%d, v1, %s]", i, partition));
            }
        }
        // write records
        writeRows(connection, tablePath, rows, false);
        return expectedRowValues;
    }

    public static void writeRows(
            Connection connection, TablePath tablePath, List<InternalRow> rows, boolean append)
            throws Exception {
        try (Table table = connection.getTable(tablePath)) {
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
}

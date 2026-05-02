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

package org.apache.fluss.server.coordinator.system;

import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor.TableDistribution;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.And;
import org.apache.fluss.predicate.CompoundPredicate;
import org.apache.fluss.predicate.Equal;
import org.apache.fluss.predicate.LeafPredicate;
import org.apache.fluss.record.DefaultValueRecordBatch;
import org.apache.fluss.record.ValueRecord;
import org.apache.fluss.record.ValueRecordBatch;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.DatabaseRegistration;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.ZkVersion;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TableBucketsViewProvider}. */
class TableBucketsViewProviderTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zookeeperClient;
    private static TableBucketsViewProvider provider;

    @BeforeAll
    static void beforeAll() {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
        provider = new TableBucketsViewProvider(zookeeperClient);
    }

    @AfterEach
    void afterEach() {
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupRoot();
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (zookeeperClient != null) {
            zookeeperClient.close();
        }
    }

    @Test
    void testViewPath() {
        TablePath path = provider.viewPath();
        assertThat(path.getDatabaseName()).isEqualTo("sys");
        assertThat(path.getTableName()).isEqualTo("table_buckets");
    }

    @Test
    void testSchemaColumns() {
        Schema schema = provider.schema();
        List<Schema.Column> columns = schema.getColumns();
        assertThat(columns).hasSize(9);
        assertThat(columns.get(0).getName()).isEqualTo("database_name");
        assertThat(columns.get(1).getName()).isEqualTo("table_name");
        assertThat(columns.get(2).getName()).isEqualTo("partition_name");
        assertThat(columns.get(3).getName()).isEqualTo("bucket_id");
        assertThat(columns.get(4).getName()).isEqualTo("leader_id");
        assertThat(columns.get(5).getName()).isEqualTo("replicas");
        assertThat(columns.get(6).getName()).isEqualTo("isr");
        assertThat(columns.get(7).getName()).isEqualTo("replica_count");
        assertThat(columns.get(8).getName()).isEqualTo("isr_count");
    }

    @Test
    void testSchemaId() {
        assertThat(provider.schemaId()).isEqualTo(2);
    }

    @Test
    void testScanRowsWithNoTables() throws Exception {
        byte[] result = provider.scanRows(null, null);
        List<InternalRow> rows = deserializeRows(result);
        assertThat(rows).isEmpty();
    }

    @Test
    void testScanRowsNonPartitionedTable() throws Exception {
        long tableId = 1L;
        String dbName = "testdb";
        String tableName = "orders";
        TablePath tablePath = TablePath.of(dbName, tableName);

        registerDatabase(dbName);
        registerTable(tablePath, tableId, Collections.emptyList(), 2);

        // Register assignment with 2 buckets, replicas on servers 0 and 1.
        Map<Integer, BucketAssignment> assignments = new HashMap<>();
        assignments.put(0, BucketAssignment.of(0, 1));
        assignments.put(1, BucketAssignment.of(1, 0));
        zookeeperClient.registerTableAssignment(tableId, new TableAssignment(assignments));

        // Register leader/ISR for bucket 0 only.
        LeaderAndIsr leaderAndIsr = new LeaderAndIsr(0, 5, Arrays.asList(0, 1), 1, 10);
        zookeeperClient.registerLeaderAndIsr(
                new TableBucket(tableId, 0),
                leaderAndIsr,
                ZkVersion.MATCH_ANY_VERSION.getVersion());

        byte[] result = provider.scanRows(null, null);
        List<InternalRow> rows = deserializeRows(result);
        assertThat(rows).hasSize(2);

        // Verify bucket 0 with leader/ISR.
        InternalRow row0 = findRowByBucketId(rows, 0);
        assertThat(row0.getString(0).toString()).isEqualTo(dbName);
        assertThat(row0.getString(1).toString()).isEqualTo(tableName);
        assertThat(row0.isNullAt(2)).isTrue(); // partition_name
        assertThat(row0.getInt(3)).isEqualTo(0); // bucket_id
        assertThat(row0.getInt(4)).isEqualTo(0); // leader_id
        assertThat(row0.getString(5).toString()).isEqualTo("0,1"); // replicas
        assertThat(row0.getString(6).toString()).isEqualTo("0,1"); // isr
        assertThat(row0.getInt(7)).isEqualTo(2); // replica_count
        assertThat(row0.getInt(8)).isEqualTo(2); // isr_count

        // Verify bucket 1 without leader/ISR has nulls for leader fields.
        InternalRow row1 = findRowByBucketId(rows, 1);
        assertThat(row1.isNullAt(4)).isTrue(); // leader_id null
        assertThat(row1.isNullAt(6)).isTrue(); // isr null
        assertThat(row1.isNullAt(8)).isTrue(); // isr_count null
    }

    @Test
    void testScanRowsPartitionedTable() throws Exception {
        long tableId = 2L;
        long partitionId = 100L;
        String dbName = "testdb";
        String tableName = "events";
        String partitionName = "2024-01-01";
        TablePath tablePath = TablePath.of(dbName, tableName);

        registerDatabase(dbName);
        registerTable(tablePath, tableId, Collections.singletonList("dt"), 1);

        // Register partition assignment.
        Map<Integer, BucketAssignment> bucketMap = new HashMap<>();
        bucketMap.put(0, BucketAssignment.of(0));
        PartitionAssignment partAssignment = new PartitionAssignment(tableId, bucketMap);
        zookeeperClient.registerPartitionAssignmentAndMetadata(
                partitionId, partitionName, partAssignment, "/tmp/remote", tablePath, tableId);

        // Register leader/ISR for the partition bucket.
        LeaderAndIsr leaderAndIsr = new LeaderAndIsr(0, 1, Collections.singletonList(0), 0, 3);
        zookeeperClient.registerLeaderAndIsr(
                new TableBucket(tableId, partitionId, 0),
                leaderAndIsr,
                ZkVersion.MATCH_ANY_VERSION.getVersion());

        byte[] result = provider.scanRows(null, null);
        List<InternalRow> rows = deserializeRows(result);
        assertThat(rows).hasSize(1);

        InternalRow row = rows.get(0);
        assertThat(row.getString(0).toString()).isEqualTo(dbName);
        assertThat(row.getString(1).toString()).isEqualTo(tableName);
        assertThat(row.getString(2).toString()).isEqualTo(partitionName);
        assertThat(row.getInt(3)).isEqualTo(0); // bucket_id
    }

    @Test
    void testFilterPushdownByTableName() throws Exception {
        String dbName = "mydb";
        registerDatabase(dbName);

        registerTable(TablePath.of(dbName, "orders"), 10L, Collections.emptyList(), 1);
        registerTable(TablePath.of(dbName, "users"), 11L, Collections.emptyList(), 1);

        Map<Integer, BucketAssignment> a1 = new HashMap<>();
        a1.put(0, BucketAssignment.of(0));
        zookeeperClient.registerTableAssignment(10L, new TableAssignment(a1));

        Map<Integer, BucketAssignment> a2 = new HashMap<>();
        a2.put(0, BucketAssignment.of(0));
        zookeeperClient.registerTableAssignment(11L, new TableAssignment(a2));

        // Filter: table_name = 'orders'
        LeafPredicate predicate =
                new LeafPredicate(
                        Equal.INSTANCE,
                        DataTypes.STRING(),
                        1,
                        "table_name",
                        Collections.singletonList(BinaryString.fromString("orders")));

        byte[] result = provider.scanRows(null, predicate);
        List<InternalRow> rows = deserializeRows(result);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString(1).toString()).isEqualTo("orders");
    }

    @Test
    void testFilterPushdownByDatabaseAndTableName() throws Exception {
        registerDatabase("db1");
        registerDatabase("db2");

        registerTable(TablePath.of("db1", "t1"), 20L, Collections.emptyList(), 1);
        registerTable(TablePath.of("db2", "t1"), 21L, Collections.emptyList(), 1);

        Map<Integer, BucketAssignment> a1 = new HashMap<>();
        a1.put(0, BucketAssignment.of(0));
        zookeeperClient.registerTableAssignment(20L, new TableAssignment(a1));

        Map<Integer, BucketAssignment> a2 = new HashMap<>();
        a2.put(0, BucketAssignment.of(0));
        zookeeperClient.registerTableAssignment(21L, new TableAssignment(a2));

        // Filter: database_name = 'db1' AND table_name = 't1'
        LeafPredicate dbPredicate =
                new LeafPredicate(
                        Equal.INSTANCE,
                        DataTypes.STRING(),
                        0,
                        "database_name",
                        Collections.singletonList(BinaryString.fromString("db1")));
        LeafPredicate tablePredicate =
                new LeafPredicate(
                        Equal.INSTANCE,
                        DataTypes.STRING(),
                        1,
                        "table_name",
                        Collections.singletonList(BinaryString.fromString("t1")));
        CompoundPredicate predicate =
                new CompoundPredicate(And.INSTANCE, Arrays.asList(dbPredicate, tablePredicate));

        byte[] result = provider.scanRows(null, predicate);
        List<InternalRow> rows = deserializeRows(result);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString(0).toString()).isEqualTo("db1");
        assertThat(rows.get(0).getString(1).toString()).isEqualTo("t1");
    }

    @Test
    void testColumnProjection() throws Exception {
        String dbName = "projdb";
        registerDatabase(dbName);
        registerTable(TablePath.of(dbName, "mytable"), 30L, Collections.emptyList(), 1);

        Map<Integer, BucketAssignment> assignments = new HashMap<>();
        assignments.put(0, BucketAssignment.of(0, 1));
        zookeeperClient.registerTableAssignment(30L, new TableAssignment(assignments));

        // Project columns: table_name (1), bucket_id (3), replica_count (7)
        int[] projectedFields = new int[] {1, 3, 7};
        byte[] result = provider.scanRows(projectedFields, null);

        DataType[] projectedTypes =
                new DataType[] {DataTypes.STRING(), DataTypes.INT(), DataTypes.INT()};
        RowDecoder decoder = RowDecoder.create(KvFormat.INDEXED, projectedTypes);
        ValueRecordBatch.ReadContext readContext = schemaId -> decoder;

        DefaultValueRecordBatch batch = DefaultValueRecordBatch.pointToBytes(result);
        List<InternalRow> rows = new ArrayList<>();
        for (ValueRecord record : batch.records(readContext)) {
            InternalRow row = record.getRow();
            if (row != null) {
                rows.add(row);
            }
        }

        assertThat(rows).hasSize(1);
        InternalRow row = rows.get(0);
        assertThat(row.getString(0).toString()).isEqualTo("mytable");
        assertThat(row.getInt(1)).isEqualTo(0);
        assertThat(row.getInt(2)).isEqualTo(2);
    }

    @Test
    void testSysDatabaseIsExcluded() throws Exception {
        // With no user databases registered, only the sys database exists implicitly.
        // The provider should exclude it and return empty results.
        byte[] result = provider.scanRows(null, null);
        List<InternalRow> rows = deserializeRows(result);
        assertThat(rows).isEmpty();
    }

    // ---- Helper methods ----

    private void registerDatabase(String dbName) throws Exception {
        zookeeperClient.registerDatabase(
                dbName, DatabaseRegistration.of(DatabaseDescriptor.builder().build()));
    }

    private void registerTable(
            TablePath tablePath, long tableId, List<String> partitionKeys, int bucketCount)
            throws Exception {
        TableDistribution distribution =
                new TableDistribution(bucketCount, Collections.emptyList());
        TableRegistration registration =
                new TableRegistration(
                        tableId,
                        null,
                        partitionKeys,
                        distribution,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        "/tmp/remote",
                        System.currentTimeMillis(),
                        System.currentTimeMillis());
        zookeeperClient.registerTable(tablePath, registration);
    }

    private InternalRow findRowByBucketId(List<InternalRow> rows, int bucketId) {
        for (InternalRow row : rows) {
            if (row.getInt(3) == bucketId) {
                return row;
            }
        }
        throw new AssertionError("No row found with bucket_id=" + bucketId);
    }

    /** Deserializes a byte array produced by {@link SystemViewProvider#scanRows} into rows. */
    private List<InternalRow> deserializeRows(byte[] bytes) {
        DataType[] fieldTypes =
                provider.schema().getRowType().getFieldTypes().toArray(new DataType[0]);
        RowDecoder decoder = RowDecoder.create(KvFormat.INDEXED, fieldTypes);
        ValueRecordBatch.ReadContext readContext = schemaId -> decoder;

        DefaultValueRecordBatch batch = DefaultValueRecordBatch.pointToBytes(bytes);
        List<InternalRow> rows = new ArrayList<>();
        for (ValueRecord record : batch.records(readContext)) {
            InternalRow row = record.getRow();
            if (row != null) {
                rows.add(row);
            }
        }
        return rows;
    }
}

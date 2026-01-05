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

package org.apache.fluss.lake.paimon.flink;

import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.lake.paimon.utils.PaimonTestUtils.CompactCommitter;
import static org.apache.fluss.lake.paimon.utils.PaimonTestUtils.CompactHelper;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test case for Flink union read functionality on Paimon tables with deletion vectors
 * enabled.
 */
class FlinkUnionReadDvTableITCase extends FlinkUnionReadTestBase {

    @TempDir private File tempDir;

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
    }

    /**
     * Test union read on a non-partitioned deletion-vectors enabled table.
     *
     * <p>Test flow:
     *
     * <ol>
     *   <li>Create a non-partitioned DV table with 3 buckets
     *   <li>Write initial data (0-10) - all buckets have L0 files, no readable snapshot yet
     *   <li>Compact bucket 0 and 1 - still no readable snapshot (bucket 2 has L0 files)
     *   <li>Write more data (10-20)
     *   <li>Compact bucket 2 - now all buckets are compacted, readable snapshot should be created
     *   <li>Write more data (20-25) - verify union read works: reads from readable snapshot + log
     *   <li>Verify the readable snapshot contains correct bucket offsets
     * </ol>
     *
     * <p>Key assertions:
     *
     * <ul>
     *   <li>Before all buckets are compacted: readable snapshot should not exist
     *   <li>After all buckets are compacted: readable snapshot should exist with correct offsets
     *   <li>Union read should return all written data correctly
     * </ul>
     */
    @Test
    void testUnionReadDvTable() throws Exception {
        // Start the tiering job that syncs data from Fluss to Paimon
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName = "testUnionReadDvTable";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            int bucketNum = 3;
            long tableId = createDvEnabledTable(tablePath, bucketNum, false);

            // Step 1: Write initial data (rows 0-9)
            List<Row> writtenRows = writeRows(tablePath, 0, 10);
            Map<TableBucket, Long> bucketLogEndOffset =
                    getBucketLogEndOffset(tableId, bucketNum, null);
            waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, false);
            assertReplicaStatus(bucketLogEndOffset);

            // At this point, all buckets have L0 files, so no readable snapshot exists yet
            assertThatThrownBy(() -> admin.getReadableLakeSnapshot(tablePath).get())
                    .rootCause()
                    .isInstanceOf(LakeTableSnapshotNotExistException.class);

            // Step 2: Compact bucket 0 and bucket 1
            // This flushes their L0 files to L1, but bucket 2 still has L0 files
            // So still no readable snapshot
            FileStoreTable fileStoreTable = getPaimonTable(tablePath);
            CompactHelper compactHelper = new CompactHelper(fileStoreTable, tempDir);
            CompactCommitter compactBucket0 = compactHelper.compactBucket(0);
            CompactCommitter compactBucket1 = compactHelper.compactBucket(1);
            compactBucket0.commit();
            compactBucket1.commit();

            // Step 3: Write more data (rows 10-19)
            // This adds new L0 files to all buckets
            writtenRows.addAll(writeRows(tablePath, 10, 20));

            // Step 4: Wait until data is synced and verify
            bucketLogEndOffset = getBucketLogEndOffset(tableId, bucketNum, null);
            waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, false);
            assertReplicaStatus(bucketLogEndOffset);

            // Still no readable snapshot because bucket 2 still has L0 files from step 1
            assertThatThrownBy(() -> admin.getReadableLakeSnapshot(tablePath).get())
                    .rootCause()
                    .isInstanceOf(LakeTableSnapshotNotExistException.class);

            // Step 5: Compact bucket 2
            // Now all buckets have been compacted at least once (no L0 files from initial write)
            // This creates a readable snapshot that can be used for union reads
            CompactCommitter compactBucket2 = compactHelper.compactBucket(2);
            // note: the data written to bucket 1 still invisible in step 3
            compactBucket2.commit();
            long expectedReadableSnapshot =
                    checkNotNull(fileStoreTable.snapshotManager().latestSnapshotId());

            // Step 6: Write more data (rows 20-24)
            // This data will be read via union read: from readable snapshot + new log data
            writtenRows.addAll(writeRows(tablePath, 20, 25));

            // Step 7: Verify union read works correctly
            bucketLogEndOffset = getBucketLogEndOffset(tableId, bucketNum, null);
            waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, false);
            assertReplicaStatus(bucketLogEndOffset);

            // Verify readable snapshot exists and has correct bucket offsets
            // The offsets indicate where to start reading from Fluss log for each bucket
            LakeSnapshot lakeSnapshot = admin.getReadableLakeSnapshot(tablePath).get();
            assertThat(lakeSnapshot.getSnapshotId()).isEqualTo(expectedReadableSnapshot);
            Map<TableBucket, Long> expectedEndOffset = new HashMap<>();
            // Expected offsets are based on how data was distributed across buckets
            // These values depend on the hash distribution of primary keys
            expectedEndOffset.put(new TableBucket(tableId, 0), 4L);
            expectedEndOffset.put(new TableBucket(tableId, 1), 3L);
            expectedEndOffset.put(new TableBucket(tableId, 2), 7L);
            assertThat(lakeSnapshot.getTableBucketsOffset()).isEqualTo(expectedEndOffset);

            // Verify union read returns all data correctly
            // It should read from readable snapshot (rows 0-13) + log (rows 14-24)
            CloseableIterator<Row> rowIter =
                    streamTEnv.executeSql("select * from " + tableName).collect();
            assertResultsIgnoreOrder(rowIter, toString(writtenRows), true);
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * Test union read on a partitioned deletion-vectors enabled table.
     *
     * <p>Test setup: 2 partitions ("2024", "2025") × 2 buckets = 4 partition-bucket combinations
     *
     * <p>Test flow:
     *
     * <ol>
     *   <li>Create partitioned DV table with 2 buckets
     *   <li>Write initial data to both partitions (0-9) - creates partitions, all have L0 files
     *   <li>Compact: partition0-bucket0, partition1-bucket0, partition1-bucket1
     *   <li>Write more data to partition0 (10-19) - partition0-bucket1 still has L0, no readable
     *       snapshot
     *   <li>Compact partition0-bucket1 - now all partition-buckets are compacted at least onece,
     *       readable snapshot created
     *   <li>Write more data to partition1 (10-19) - verify union read works
     *   <li>Verify readable snapshot has correct offsets for all partition-bucket combinations
     * </ol>
     */
    @Test
    void testUnionReadDvPartitionedTable() throws Exception {
        // Start the tiering job that syncs data from Fluss to Paimon
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName = "testUnionReadDvPartitionedTable";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            int bucketNum = 2;
            long tableId = createDvEnabledTable(tablePath, bucketNum, true);

            // Use fixed partition values for testing
            String partition0 = "2024";
            String partition1 = "2025";

            // Step 1: Write initial data to both partitions (rows 0-9 each)
            // This creates the partitions and writes data to L0 files
            // Data is distributed across 2 buckets per partition based on PK hash
            List<Row> writtenRows = new ArrayList<>();
            writtenRows.addAll(writeRows(tablePath, 0, 10, partition0));
            writtenRows.addAll(writeRows(tablePath, 0, 10, partition1));

            // Get partition IDs after partitions are created
            Map<Long, String> partitionNameById =
                    waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), tablePath, 2);
            Map<TableBucket, Long> bucketLogEndOffset = new HashMap<>();
            for (Long partitionId : partitionNameById.keySet()) {
                bucketLogEndOffset.putAll(getBucketLogEndOffset(tableId, bucketNum, partitionId));
            }
            waitUntilBucketSynced(tablePath, tableId, bucketNum, true);
            assertReplicaStatus(bucketLogEndOffset);

            // At this point, all partition-bucket combinations have L0 files
            // No readable snapshot exists yet
            assertThatThrownBy(() -> admin.getReadableLakeSnapshot(tablePath).get())
                    .rootCause()
                    .isInstanceOf(LakeTableSnapshotNotExistException.class);

            // Step 2: Compact some buckets across partitions
            // Compact: partition0-bucket0, partition1-bucket0, partition1-bucket1
            // This leaves partition0-bucket1 with L0 files, so still no readable snapshot
            FileStoreTable fileStoreTable = getPaimonTable(tablePath);
            CompactHelper compactHelper = new CompactHelper(fileStoreTable, tempDir);
            BinaryRow partition0BinaryRow = BinaryRow.singleColumn(partition0);
            BinaryRow partition1BinaryRow = BinaryRow.singleColumn(partition1);
            CompactCommitter compactP0B0 = compactHelper.compactBucket(partition0BinaryRow, 0);
            CompactCommitter compactP1B0 = compactHelper.compactBucket(partition1BinaryRow, 0);
            CompactCommitter compactP1B1 = compactHelper.compactBucket(partition1BinaryRow, 1);
            compactP0B0.commit();
            compactP1B0.commit();
            compactP1B1.commit();

            // Step 3: Write more data to partition0 (rows 10-19)
            // This adds new L0 files to partition0 buckets
            writtenRows.addAll(writeRows(tablePath, 10, 20, partition0));

            // Step 4: Wait until data is synced and verify
            bucketLogEndOffset = new HashMap<>();
            for (Long partitionId : partitionNameById.keySet()) {
                bucketLogEndOffset.putAll(getBucketLogEndOffset(tableId, bucketNum, partitionId));
            }
            waitUntilBucketSynced(tablePath, tableId, bucketNum, true);
            assertReplicaStatus(bucketLogEndOffset);

            // Still no readable snapshot because partition0-bucket1 still has L0 files from step 1
            assertThatThrownBy(() -> admin.getReadableLakeSnapshot(tablePath).get())
                    .rootCause()
                    .isInstanceOf(LakeTableSnapshotNotExistException.class);

            // Step 5: Compact partition0-bucket1
            // Now all partition-bucket combinations have been compacted at least once
            // This creates a readable snapshot that can be used for union reads
            // Note the data written into bucket 0 in step3 is invisible
            CompactCommitter compactP0B1 = compactHelper.compactBucket(partition0BinaryRow, 1);
            compactP0B1.commit();
            long expectedReadableSnapshot =
                    checkNotNull(fileStoreTable.snapshotManager().latestSnapshotId());

            // Step 6: Write more data to partition1 (rows 10-19)
            // This data will be read via union read: from readable snapshot + new log data
            writtenRows.addAll(writeRows(tablePath, 10, 20, partition1));

            // Step 7: Verify union read works correctly
            bucketLogEndOffset = new HashMap<>();
            for (Long partitionId : partitionNameById.keySet()) {
                bucketLogEndOffset.putAll(getBucketLogEndOffset(tableId, bucketNum, partitionId));
            }
            waitUntilBucketSynced(tablePath, tableId, bucketNum, true);
            assertReplicaStatus(bucketLogEndOffset);

            // Verify readable snapshot exists and has correct offsets for all partition-buckets
            // The offsets indicate where to start reading from Fluss log for each partition-bucket
            LakeSnapshot lakeSnapshot = admin.getReadableLakeSnapshot(tablePath).get();
            assertThat(lakeSnapshot.getSnapshotId()).isEqualTo(expectedReadableSnapshot);
            Map<TableBucket, Long> expectedEndOffset = new HashMap<>();
            Long partitionId0 = getPartitionIdByName(partitionNameById, partition0);
            Long partitionId1 = getPartitionIdByName(partitionNameById, partition1);
            // Expected offsets based on data distribution:
            // - partition0-bucket0: 6 rows (from initial 0-9 write, distributed by hash)
            // - partition0-bucket1: 8 rows (from initial 0-9 + 10-19 writes)
            // - partition1-bucket0: 6 rows (from initial 0-9 write)
            // - partition1-bucket1: 4 rows (from initial 0-9 write)
            expectedEndOffset.put(new TableBucket(tableId, partitionId0, 0), 6L);
            expectedEndOffset.put(new TableBucket(tableId, partitionId0, 1), 8L);
            expectedEndOffset.put(new TableBucket(tableId, partitionId1, 0), 6L);
            expectedEndOffset.put(new TableBucket(tableId, partitionId1, 1), 4L);

            assertThat(lakeSnapshot.getTableBucketsOffset()).isEqualTo(expectedEndOffset);

            // Verify union read returns all data correctly
            // It should read from readable snapshot + log for all partitions
            CloseableIterator<Row> rowIter =
                    streamTEnv.executeSql("select * from " + tableName).collect();
            assertResultsIgnoreOrder(rowIter, toString(writtenRows), true);
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * Get the Paimon FileStoreTable instance for the given table path.
     *
     * @param tablePath the Fluss table path
     * @return the Paimon FileStoreTable instance
     */
    private FileStoreTable getPaimonTable(TablePath tablePath) throws Exception {
        Identifier identifier =
                Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName());
        return (FileStoreTable) paimonCatalog.getTable(identifier);
    }

    /**
     * Write rows to the table through Fluss API.
     *
     * <p>This method writes data through Fluss, which will then be synced to Paimon via tiering
     * job. The data will be distributed to buckets based on the primary key hash.
     *
     * @param tablePath the table path
     * @param from the starting value (inclusive) for the primary key
     * @param to the ending value (exclusive) for the primary key
     * @return the list of Flink Row objects that were written
     */
    private List<Row> writeRows(TablePath tablePath, int from, int to) throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        List<Row> flinkRows = new ArrayList<>();
        for (int i = from; i < to; i++) {
            rows.add(row(i, "value1_" + i, "value2_" + i));
            flinkRows.add(Row.of(i, "value1_" + i, "value2_" + i));
        }
        writeRows(tablePath, rows, false);
        return flinkRows;
    }

    /**
     * Write rows to a partitioned table through Fluss API.
     *
     * <p>This method writes data through Fluss, which will then be synced to Paimon via tiering
     * job. The data will be distributed to buckets based on the primary key hash.
     *
     * @param tablePath the table path
     * @param from the starting value (inclusive) for the primary key
     * @param to the ending value (exclusive) for the primary key
     * @param partition the partition value
     * @return the list of Flink Row objects that were written
     */
    private List<Row> writeRows(TablePath tablePath, int from, int to, String partition)
            throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        List<Row> flinkRows = new ArrayList<>();
        for (int i = from; i < to; i++) {
            rows.add(row(i, "value1_" + i, partition));
            flinkRows.add(Row.of(i, "value1_" + i, partition));
        }
        writeRows(tablePath, rows, false);
        return flinkRows;
    }

    /**
     * Create a deletion-vectors enabled table for testing.
     *
     * <p>Table schema:
     *
     * <ul>
     *   <li>c1: INT (primary key, or part of composite PK for partitioned tables)
     *   <li>c2: STRING
     *   <li>c3: STRING (partition column for partitioned tables)
     * </ul>
     *
     * @param tablePath the table path
     * @param bucketNum number of buckets for data distribution
     * @param isPartitioned whether the table is partitioned (by c3)
     * @return the created table ID
     */
    private long createDvEnabledTable(TablePath tablePath, int bucketNum, boolean isPartitioned)
            throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .column("c3", DataTypes.STRING());

        TableDescriptor.Builder tableBuilder = TableDescriptor.builder().distributedBy(bucketNum);
        // Enable lake storage (Paimon) integration
        tableBuilder
                .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500));

        // Enable deletion vectors feature in Paimon
        tableBuilder.customProperty("paimon.deletion-vectors.enabled", "true");

        if (isPartitioned) {
            // For partitioned tables: partition by c3, primary key is (c1, c3)
            tableBuilder.partitionedBy("c3");
            schemaBuilder.primaryKey("c1", "c3");
        } else {
            // For non-partitioned tables: primary key is c1
            schemaBuilder.primaryKey("c1");
        }
        tableBuilder.schema(schemaBuilder.build());
        return createTable(tablePath, tableBuilder.build());
    }

    private List<Row> writeRows(TablePath tablePath, int rowCount, @Nullable String partition)
            throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        List<Row> flinkRows = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            if (partition == null) {
                rows.add(row(i, "value1_" + i, "value2_" + i));
                flinkRows.add(Row.of(i, "value1_" + i, "value2_" + i));
            } else {
                rows.add(row(i, "value1_" + i, partition));
                flinkRows.add(Row.of(i, "value1_" + i, partition));
            }
        }
        writeRows(tablePath, rows, false);
        return flinkRows;
    }

    /**
     * Get the log end offset for all buckets of a table (or a specific partition).
     *
     * <p>The log end offset represents the position in the Fluss log where data has been written.
     * This is used to track how much data has been synced to Paimon.
     *
     * @param tableId the table ID
     * @param bucketNum number of buckets
     * @param partitionId partition ID (null for non-partitioned tables)
     * @return map from TableBucket to its log end offset
     */
    private Map<TableBucket, Long> getBucketLogEndOffset(
            long tableId, int bucketNum, Long partitionId) {
        Map<TableBucket, Long> bucketLogEndOffsets = new HashMap<>();
        for (int i = 0; i < bucketNum; i++) {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, i);
            Replica replica = getLeaderReplica(tableBucket);
            bucketLogEndOffsets.put(tableBucket, replica.getLocalLogEndOffset());
        }
        return bucketLogEndOffsets;
    }

    /**
     * Convert a list of Flink Row objects to a list of their string representations.
     *
     * @param rows the rows to convert
     * @return list of string representations
     */
    private List<String> toString(List<Row> rows) {
        return rows.stream().map(Row::toString).collect(Collectors.toList());
    }

    /**
     * Get partition ID by partition name.
     *
     * @param partitionNameById map from partition ID to partition name
     * @param partitionName the partition name to look up
     * @return the partition ID
     */
    private Long getPartitionIdByName(Map<Long, String> partitionNameById, String partitionName) {
        return partitionNameById.entrySet().stream()
                .filter(entry -> entry.getValue().equals(partitionName))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "Partition name " + partitionName + " not found"));
    }
}

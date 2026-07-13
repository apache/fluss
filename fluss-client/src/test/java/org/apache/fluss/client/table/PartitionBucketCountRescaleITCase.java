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

package org.apache.fluss.client.table;

import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end IT case verifying that reads and writes route by the per-partition bucket count
 * (bucket.num.actual) after an ALTER TABLE ... SET ('bucket.num' = N): old partitions keep their
 * original bucket count while partitions created after the ALTER use the new count, and data
 * written to each partition is read back correctly through its own bucket range.
 */
class PartitionBucketCountRescaleITCase extends ClientToServerITCaseBase {

    private static final int OLD_BUCKET_NUM = 2;
    private static final int NEW_BUCKET_NUM = 4;
    private static final int RECORDS_PER_PARTITION = 12;
    private static final List<String> OLD_NEW_PARTITIONS = Arrays.asList("old", "new");

    @Test
    void testLogTableReadWriteAndPerBucketDistributionAcrossRescale() throws Exception {
        // Covers both write-routing paths across a rescale: part 1 (no bucket key) verifies row
        // content, part 2 (bucket key "a") verifies per-bucket distribution. Routing by the
        // wrong (table-level) count would miss rows in part 1 and leave buckets empty in part 2.

        // === Part 1: no-bucket-key table — content round trip ===
        TablePath tablePath = TablePath.of("test_db_1", "test_rescale_log_table");
        Schema schema = logSchema();
        createPartitionedTable(tablePath, schema);
        List<PartitionInfo> partitionInfos = setupOldNewPartitions(tablePath);
        Map<String, Long> idByName = partitionIdByName(partitionInfos);

        // append RECORDS_PER_PARTITION rows to each partition
        Table table = conn.getTable(tablePath);
        AppendWriter appendWriter = table.newAppend().createWriter();
        Map<Long, List<InternalRow>> expectedByPartitionId = new HashMap<>();
        for (String partitionName : OLD_NEW_PARTITIONS) {
            long partitionId = idByName.get(partitionName);
            for (int j = 0; j < RECORDS_PER_PARTITION; j++) {
                InternalRow r = row(j, "v" + j, partitionName);
                appendWriter.append(r);
                expectedByPartitionId.computeIfAbsent(partitionId, k -> new ArrayList<>()).add(r);
            }
        }
        appendWriter.flush();

        // read back by subscribing EACH partition's own bucket range [0, bucketCount)
        Map<Long, List<InternalRow>> actualByPartitionId =
                scanAllBucketsPerPartition(table, partitionInfos);

        assertRowsPerPartition(schema.getRowType(), actualByPartitionId, expectedByPartitionId);

        // === Part 2: bucket-keyed table — per-bucket distribution ===
        TablePath keyedTablePath = TablePath.of("test_db_1", "test_rescale_log_per_bucket");
        createPartitionedTable(keyedTablePath, schema, "a");
        List<PartitionInfo> keyedPartitionInfos = setupOldNewPartitions(keyedTablePath);
        Map<String, Long> keyedIdByName = partitionIdByName(keyedPartitionInfos);
        Map<String, Integer> keyedBucketCountByName = bucketCountByName(keyedPartitionInfos);

        // Use a broad key range so hash distribution reaches every bucket with high probability.
        int recordsPerPartition = 64;
        Table keyedTable = conn.getTable(keyedTablePath);
        AppendWriter keyedAppendWriter = keyedTable.newAppend().createWriter();
        for (String partitionName : OLD_NEW_PARTITIONS) {
            for (int j = 0; j < recordsPerPartition; j++) {
                keyedAppendWriter.append(row(j, "v" + j, partitionName));
            }
        }
        keyedAppendWriter.flush();

        Map<TableBucket, Integer> perBucketCount =
                pollRecordCountPerBucket(keyedTable, keyedPartitionInfos, 2 * recordsPerPartition);

        for (String partitionName : OLD_NEW_PARTITIONS) {
            long partitionId = keyedIdByName.get(partitionName);
            int bucketCount = keyedBucketCountByName.get(partitionName);
            int partitionSum = 0;
            for (int bucketId = 0; bucketId < bucketCount; bucketId++) {
                TableBucket tb =
                        new TableBucket(
                                keyedTable.getTableInfo().getTableId(), partitionId, bucketId);
                int c = perBucketCount.getOrDefault(tb, 0);
                partitionSum += c;
                // Every bucket must have been landed on at least once given 64 records over N
                // buckets.
                assertThat(c)
                        .as(
                                "partition %s bucket %d must receive at least one record",
                                partitionName, bucketId)
                        .isGreaterThan(0);
            }
            assertThat(partitionSum)
                    .as("partition %s per-bucket sum", partitionName)
                    .isEqualTo(recordsPerPartition);
        }
    }

    @Test
    void testPkTableReadPathsAcrossRescale() throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "test_rescale_pk_read_paths");
        Schema schema = pkSchema("a", "c");
        createPartitionedTable(tablePath, schema);
        List<PartitionInfo> partitionInfos = setupOldNewPartitions(tablePath);
        Map<String, Long> idByName = partitionIdByName(partitionInfos);
        Map<String, Integer> bucketCountByName = bucketCountByName(partitionInfos);

        Table table = conn.getTable(tablePath);
        long tableId = table.getTableInfo().getTableId();
        upsertRowsToOldAndNew(table);

        // 1. Lookup: write routing N must equal lookup routing N per partition, otherwise the
        // lookup would hit the wrong bucket and miss the row.
        Lookuper lookuper = table.newLookup().createLookuper();
        for (String partitionName : OLD_NEW_PARTITIONS) {
            for (int j = 0; j < RECORDS_PER_PARTITION; j++) {
                InternalRow expected = row(j, "v" + j, partitionName);
                InternalRow looked = lookuper.lookup(row(j, partitionName)).get().getSingletonRow();
                assertThatRow(looked).withSchema(schema.getRowType()).isEqualTo(expected);
            }
        }

        // 2. PK stream read: LogScanner subscribes by per-partition bucket count. Each partition's
        // total polled records must equal the writes; if routing used the wrong count, the
        // subscribed bucket range would miss rows.
        Map<TableBucket, Integer> perBucketCount =
                pollRecordCountPerBucket(table, partitionInfos, 2 * RECORDS_PER_PARTITION);
        Map<Long, Integer> streamCountsPerPartition = new HashMap<>();
        perBucketCount.forEach(
                (tb, c) -> streamCountsPerPartition.merge(tb.getPartitionId(), c, Integer::sum));
        for (String partitionName : OLD_NEW_PARTITIONS) {
            assertThat(streamCountsPerPartition.get(idByName.get(partitionName)))
                    .as("PK stream count for partition %s", partitionName)
                    .isEqualTo(RECORDS_PER_PARTITION);
        }

        // 3. Batch read: for each partition, trigger a KV snapshot on every bucket, then scan back
        // per (partition, bucket) with BatchScanner using the partition's own bucket count.
        for (String partitionName : OLD_NEW_PARTITIONS) {
            long partitionId = idByName.get(partitionName);
            int bucketCount = bucketCountByName.get(partitionName);
            int partitionSum = 0;
            for (int bucketId = 0; bucketId < bucketCount; bucketId++) {
                TableBucket tb = new TableBucket(tableId, partitionId, bucketId);
                long snapshotId =
                        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tb).getSnapshotID();
                try (BatchScanner batchScanner =
                        table.newScan().createBatchScanner(tb, snapshotId)) {
                    while (true) {
                        CloseableIterator<InternalRow> it =
                                batchScanner.pollBatch(Duration.ofSeconds(10));
                        if (it == null) {
                            break;
                        }
                        try {
                            while (it.hasNext()) {
                                it.next();
                                partitionSum++;
                            }
                        } finally {
                            it.close();
                        }
                    }
                }
            }
            assertThat(partitionSum)
                    .as("PK batch count for partition %s", partitionName)
                    .isEqualTo(RECORDS_PER_PARTITION);
        }

        // 4. count(*) must use each partition's own bucket count, not the table-level count
        // (which would enumerate out-of-range buckets for old partitions and skew the total).
        assertThat(admin.getTableStats(tablePath).get().getRowCount())
                .isEqualTo(2L * RECORDS_PER_PARTITION);
    }

    @Test
    void testDynamicallyCreatedPartitionUsesPostAlterBucketCount() throws Exception {
        // A partition created dynamically by the WRITER after an ALTER must use the new bucket
        // count and be readable through that range.
        clientConf.set(ConfigOptions.CLIENT_WRITER_DYNAMIC_CREATE_PARTITION_ENABLED, true);
        conn.close();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();

        TablePath tablePath = TablePath.of("test_db_1", "test_rescale_dynamic_create");
        Schema schema = logSchema();
        createPartitionedTable(tablePath, schema);
        alterBucketNum(tablePath, NEW_BUCKET_NUM);

        // write to a partition that does not exist yet; the writer creates it dynamically
        Table table = conn.getTable(tablePath);
        AppendWriter appendWriter = table.newAppend().createWriter();
        Map<Long, List<InternalRow>> expectedByPartitionId = new HashMap<>();
        List<InternalRow> expectedRows = new ArrayList<>();
        for (int j = 0; j < RECORDS_PER_PARTITION; j++) {
            InternalRow r = row(j, "v" + j, "auto");
            appendWriter.append(r);
            expectedRows.add(r);
        }
        appendWriter.flush();

        // the dynamically created partition carries the post-ALTER bucket count
        List<PartitionInfo> partitionInfos = admin.listPartitionInfos(tablePath).get();
        PartitionInfo autoPartition =
                partitionInfos.stream()
                        .filter(p -> "auto".equals(p.getPartitionName()))
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("dynamic partition was not created"));
        assertThat(autoPartition.getBucketCount()).isEqualTo(NEW_BUCKET_NUM);
        expectedByPartitionId.put(autoPartition.getPartitionId(), expectedRows);

        // all rows are readable through the partition's own bucket range
        Map<Long, List<InternalRow>> actualByPartitionId =
                scanAllBucketsPerPartition(table, Collections.singletonList(autoPartition));
        assertRowsPerPartition(schema.getRowType(), actualByPartitionId, expectedByPartitionId);
    }

    @Test
    void testStaleTableHandleWritesToDynamicallyCreatedPartitionAfterAlter() throws Exception {
        // Old writers hold a stale table-level bucket count; new partitions must route by
        // their actual (post-ALTER) count, otherwise lookups miss.
        clientConf.set(ConfigOptions.CLIENT_WRITER_DYNAMIC_CREATE_PARTITION_ENABLED, true);
        conn.close();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();

        TablePath tablePath = TablePath.of("test_db_1", "test_rescale_stale_handle");
        Schema schema = pkSchema("a", "c");
        createPartitionedTable(tablePath, schema);

        // open the handle BEFORE the ALTER, then rescale on the server side
        Table staleTable = conn.getTable(tablePath);
        UpsertWriter upsertWriter = staleTable.newUpsert().createWriter();
        alterBucketNum(tablePath, NEW_BUCKET_NUM);

        // write through the stale handle into a partition that does not exist yet
        for (int j = 0; j < RECORDS_PER_PARTITION; j++) {
            upsertWriter.upsert(row(j, "v" + j, "auto"));
        }
        upsertWriter.flush();

        // the dynamically created partition carries the post-ALTER bucket count
        List<PartitionInfo> partitionInfos = admin.listPartitionInfos(tablePath).get();
        assertThat(bucketCountByName(partitionInfos)).containsEntry("auto", NEW_BUCKET_NUM);

        // every key must be found: write routing and lookup routing must agree on the
        // partition's actual bucket count
        Lookuper lookuper = staleTable.newLookup().createLookuper();
        for (int j = 0; j < RECORDS_PER_PARTITION; j++) {
            InternalRow expected = row(j, "v" + j, "auto");
            InternalRow looked = lookuper.lookup(row(j, "auto")).get().getSingletonRow();
            assertThatRow(looked).withSchema(schema.getRowType()).isEqualTo(expected);
        }
    }

    @Test
    void testPrefixLookupAcrossPartitionsWithDifferentBucketCounts() throws Exception {
        // Prefix lookup must resolve the bucket with the correct per-partition count; a mismatch
        // would query the wrong bucket and miss rows.
        TablePath tablePath = TablePath.of("test_db_1", "test_rescale_prefix_lookup");
        Schema schema = pkSchema("a", "b", "c");
        createPartitionedTable(tablePath, schema, "a");
        setupOldNewPartitions(tablePath);

        int aCardinality = 8;
        int bPerA = 3;
        Table table = conn.getTable(tablePath);
        UpsertWriter upsertWriter = table.newUpsert().createWriter();
        for (String partitionName : OLD_NEW_PARTITIONS) {
            for (int a = 0; a < aCardinality; a++) {
                for (int k = 0; k < bPerA; k++) {
                    upsertWriter.upsert(row(a, "b" + k, partitionName));
                }
            }
        }
        upsertWriter.flush();

        Lookuper prefixLookuper =
                table.newLookup().lookupBy(Arrays.asList("a", "c")).createLookuper();
        for (String partitionName : OLD_NEW_PARTITIONS) {
            for (int a = 0; a < aCardinality; a++) {
                List<InternalRow> rows =
                        prefixLookuper.lookup(row(a, partitionName)).get().getRowList();
                assertThat(rows)
                        .as("prefix (a=%d, c=%s) should return %d rows", a, partitionName, bPerA)
                        .hasSize(bPerA);
            }
        }
    }

    // ==================== helpers ====================

    private static Schema logSchema() {
        return Schema.newBuilder()
                .column("a", DataTypes.INT())
                .column("b", DataTypes.STRING())
                .column("c", DataTypes.STRING())
                .build();
    }

    private static Schema pkSchema(String... primaryKeys) {
        return Schema.newBuilder()
                .column("a", DataTypes.INT())
                .column("b", DataTypes.STRING())
                .column("c", DataTypes.STRING())
                .primaryKey(primaryKeys)
                .build();
    }

    /** Creates a table partitioned by "c" with OLD_BUCKET_NUM buckets and given bucket keys. */
    private void createPartitionedTable(TablePath tablePath, Schema schema, String... bucketKeys)
            throws Exception {
        createTable(
                tablePath,
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(OLD_BUCKET_NUM, bucketKeys)
                        .partitionedBy("c")
                        .build(),
                true);
    }

    /**
     * Creates the "old" partition, ALTERs bucket.num to NEW_BUCKET_NUM, creates the "new"
     * partition, and asserts the reported per-partition bucket counts.
     */
    private List<PartitionInfo> setupOldNewPartitions(TablePath tablePath) throws Exception {
        // old partition (created before ALTER -> OLD_BUCKET_NUM buckets)
        admin.createPartition(tablePath, newPartitionSpec("c", "old"), false).get();
        alterBucketNum(tablePath, NEW_BUCKET_NUM);
        // new partition (created after ALTER -> NEW_BUCKET_NUM buckets)
        admin.createPartition(tablePath, newPartitionSpec("c", "new"), false).get();

        List<PartitionInfo> partitionInfos = admin.listPartitionInfos(tablePath).get();
        assertThat(bucketCountByName(partitionInfos))
                .containsEntry("old", OLD_BUCKET_NUM)
                .containsEntry("new", NEW_BUCKET_NUM);
        return partitionInfos;
    }

    /** Upserts RECORDS_PER_PARTITION rows (j, "v"+j, partition) into "old" and "new". */
    private static void upsertRowsToOldAndNew(Table table) throws Exception {
        UpsertWriter upsertWriter = table.newUpsert().createWriter();
        for (String partitionName : OLD_NEW_PARTITIONS) {
            for (int j = 0; j < RECORDS_PER_PARTITION; j++) {
                upsertWriter.upsert(row(j, "v" + j, partitionName));
            }
        }
        upsertWriter.flush();
    }

    private void alterBucketNum(TablePath tablePath, int newBucketNum) throws Exception {
        admin.alterTable(
                        tablePath,
                        Collections.singletonList(
                                TableChange.set("bucket.num", String.valueOf(newBucketNum))),
                        false)
                .get();
    }

    private static Map<String, Integer> bucketCountByName(List<PartitionInfo> partitionInfos) {
        Map<String, Integer> map = new HashMap<>();
        for (PartitionInfo p : partitionInfos) {
            map.put(p.getPartitionName(), p.getBucketCount());
        }
        return map;
    }

    private static Map<String, Long> partitionIdByName(List<PartitionInfo> partitionInfos) {
        Map<String, Long> map = new HashMap<>();
        for (PartitionInfo p : partitionInfos) {
            map.put(p.getPartitionName(), p.getPartitionId());
        }
        return map;
    }

    private static void subscribeAllBuckets(
            LogScanner logScanner, List<PartitionInfo> partitionInfos) {
        for (PartitionInfo partitionInfo : partitionInfos) {
            for (int bucketId = 0; bucketId < partitionInfo.getBucketCount(); bucketId++) {
                logScanner.subscribeFromBeginning(partitionInfo.getPartitionId(), bucketId);
            }
        }
    }

    /**
     * Subscribes every bucket of every partition using that partition's own bucket count and polls
     * until {@code expectedTotal} records arrive, returning the record count per bucket. If write
     * routing used the wrong (table-level) bucket count for a partition, the scan of its real
     * bucket range would miss rows and the expected count would never be reached.
     */
    private static Map<TableBucket, Integer> pollRecordCountPerBucket(
            Table table, List<PartitionInfo> partitionInfos, int expectedTotal) throws Exception {
        Map<TableBucket, Integer> perBucketCount = new HashMap<>();
        int scanned = 0;
        try (LogScanner logScanner = table.newScan().createLogScanner()) {
            subscribeAllBuckets(logScanner, partitionInfos);
            long deadline = System.currentTimeMillis() + Duration.ofMinutes(1).toMillis();
            while (scanned < expectedTotal && System.currentTimeMillis() < deadline) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (TableBucket scanBucket : scanRecords.buckets()) {
                    int c = 0;
                    for (ScanRecord ignored : scanRecords.records(scanBucket)) {
                        c++;
                    }
                    perBucketCount.merge(scanBucket, c, Integer::sum);
                    scanned += c;
                }
            }
        }
        assertThat(scanned).isEqualTo(expectedTotal);
        return perBucketCount;
    }

    /**
     * Subscribes every bucket of every partition using that partition's own bucket count and
     * collects all rows grouped by partition id. If write routing used the wrong (table-level)
     * bucket count for a partition, the scan of its real bucket range would miss rows and the
     * expected count would never be reached.
     */
    private static Map<Long, List<InternalRow>> scanAllBucketsPerPartition(
            Table table, List<PartitionInfo> partitionInfos) throws Exception {
        int totalExpected = partitionInfos.size() * RECORDS_PER_PARTITION;
        Map<Long, List<InternalRow>> actual = new HashMap<>();
        int scanned = 0;
        try (LogScanner logScanner = table.newScan().createLogScanner()) {
            subscribeAllBuckets(logScanner, partitionInfos);
            long deadline = System.currentTimeMillis() + Duration.ofMinutes(1).toMillis();
            while (scanned < totalExpected && System.currentTimeMillis() < deadline) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (TableBucket scanBucket : scanRecords.buckets()) {
                    for (ScanRecord record : scanRecords.records(scanBucket)) {
                        actual.computeIfAbsent(scanBucket.getPartitionId(), k -> new ArrayList<>())
                                .add(record.getRow());
                    }
                }
                scanned += scanRecords.count();
            }
        }
        assertThat(scanned).isEqualTo(totalExpected);
        return actual;
    }

    private static void assertRowsPerPartition(
            RowType rowType,
            Map<Long, List<InternalRow>> actual,
            Map<Long, List<InternalRow>> expected) {
        assertThat(actual.keySet()).isEqualTo(expected.keySet());
        for (Map.Entry<Long, List<InternalRow>> entry : expected.entrySet()) {
            List<InternalRow> actualRows = actual.get(entry.getKey());
            List<InternalRow> expectedRows = entry.getValue();
            // rows from different buckets of the same partition may interleave, so compare as a
            // multiset: same size and same elements regardless of order.
            assertThat(actualRows).hasSameSizeAs(expectedRows);
            for (InternalRow expectedRow : expectedRows) {
                boolean found =
                        actualRows.stream()
                                .anyMatch(
                                        a -> {
                                            try {
                                                assertThatRow(a)
                                                        .withSchema(rowType)
                                                        .isEqualTo(expectedRow);
                                                return true;
                                            } catch (AssertionError e) {
                                                return false;
                                            }
                                        });
                assertThat(found)
                        .as("expected row %s present in partition rows", expectedRow)
                        .isTrue();
            }
        }
    }
}

/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.admin;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.OffsetSpec.LatestSpec;
import com.alibaba.fluss.client.admin.OffsetSpec.TimestampSpec;
import com.alibaba.fluss.client.lookup.Lookuper;
import com.alibaba.fluss.client.scanner.ScanRecord;
import com.alibaba.fluss.client.scanner.log.LogScan;
import com.alibaba.fluss.client.scanner.log.LogScanner;
import com.alibaba.fluss.client.scanner.log.ScanRecords;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.MemorySize;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.types.RowType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.testutils.DataTestUtils.compactedRow;
import static com.alibaba.fluss.testutils.DataTestUtils.keyRow;
import static com.alibaba.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The base test class for client to server request and response. The server include
 * CoordinatorServer and TabletServer.
 */
public abstract class ClientToServerITCaseBase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    protected Connection conn;
    protected Admin admin;
    protected Configuration clientConf;

    @BeforeEach
    protected void setup() throws Exception {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    @AfterEach
    protected void teardown() throws Exception {
        if (admin != null) {
            admin.close();
            admin = null;
        }

        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    protected long createTable(
            TablePath tablePath, TableDescriptor tableDescriptor, boolean ignoreIfExists)
            throws Exception {
        admin.createDatabase(tablePath.getDatabaseName(), ignoreIfExists).get();
        admin.createTable(tablePath, tableDescriptor, ignoreIfExists).get();
        return admin.getTable(tablePath).get().getTableId();
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // set a shorter interval for testing purpose
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1));
        // set a shorter max lag time to make tests in FlussFailServerTableITCase faster
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(10));

        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("1kb"));
        return conf;
    }

    protected static LogScanner createLogScanner(Table table) {
        return table.getLogScanner(new LogScan());
    }

    protected static LogScanner createLogScanner(Table table, int[] projectFields) {
        return table.getLogScanner(new LogScan().withProjectedFields(projectFields));
    }

    protected static void subscribeFromBeginning(LogScanner logScanner, Table table) {
        int bucketCount = getBucketCount(table);
        for (int i = 0; i < bucketCount; i++) {
            logScanner.subscribeFromBeginning(i);
        }
    }

    protected static void subscribeFromTimestamp(
            PhysicalTablePath physicalTablePath,
            @Nullable Long partitionId,
            Table table,
            LogScanner logScanner,
            Admin admin,
            long timestamp)
            throws Exception {
        Map<Integer, Long> offsetsMap =
                admin.listOffsets(
                                physicalTablePath,
                                getAllBuckets(table),
                                new TimestampSpec(timestamp))
                        .all()
                        .get();
        if (partitionId != null) {
            offsetsMap.forEach(
                    (bucketId, offset) -> logScanner.subscribe(partitionId, bucketId, offset));
        } else {
            offsetsMap.forEach(logScanner::subscribe);
        }
    }

    protected static void subscribeFromLatestOffset(
            PhysicalTablePath physicalTablePath,
            @Nullable Long partitionId,
            Table table,
            LogScanner logScanner,
            Admin admin)
            throws Exception {
        Map<Integer, Long> offsetsMap =
                admin.listOffsets(physicalTablePath, getAllBuckets(table), new LatestSpec())
                        .all()
                        .get();
        if (partitionId != null) {
            offsetsMap.forEach(
                    (bucketId, offset) -> logScanner.subscribe(partitionId, bucketId, offset));
        } else {
            offsetsMap.forEach(logScanner::subscribe);
        }
    }

    protected static List<Integer> getAllBuckets(Table table) {
        List<Integer> buckets = new ArrayList<>();
        int bucketCount = getBucketCount(table);
        for (int i = 0; i < bucketCount; i++) {
            buckets.add(i);
        }
        return buckets;
    }

    private static int getBucketCount(Table table) {
        return table.getDescriptor()
                .getTableDistribution()
                .flatMap(TableDescriptor.TableDistribution::getBucketCount)
                .orElse(ConfigOptions.DEFAULT_BUCKET_NUMBER.defaultValue());
    }

    public static void verifyPartitionLogs(
            Table table, RowType rowType, Map<Long, List<InternalRow>> expectPartitionsRows)
            throws Exception {
        int totalRecords =
                expectPartitionsRows.values().stream().map(List::size).reduce(0, Integer::sum);
        int scanRecordCount = 0;
        Map<Long, List<InternalRow>> actualRows = new HashMap<>();
        try (LogScanner logScanner = table.getLogScanner(new LogScan())) {
            for (Long partitionId : expectPartitionsRows.keySet()) {
                logScanner.subscribeFromBeginning(partitionId, 0);
            }
            while (scanRecordCount < totalRecords) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (TableBucket scanBucket : scanRecords.buckets()) {
                    List<ScanRecord> records = scanRecords.records(scanBucket);
                    for (ScanRecord scanRecord : records) {
                        actualRows
                                .computeIfAbsent(
                                        scanBucket.getPartitionId(), k -> new ArrayList<>())
                                .add(scanRecord.getRow());
                    }
                }
                scanRecordCount += scanRecords.count();
            }
        }
        assertThat(scanRecordCount).isEqualTo(totalRecords);
        verifyRows(rowType, actualRows, expectPartitionsRows);
    }

    public static void waitAllReplicasReady(long tableId, TableDescriptor tableDescriptor) {
        // retry until all replica ready.
        int expectBucketCount = tableDescriptor.getTableDistribution().get().getBucketCount().get();
        for (int i = 0; i < expectBucketCount; i++) {
            FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(new TableBucket(tableId, i));
        }
    }

    protected static void verifyRows(
            RowType rowType,
            Map<Long, List<InternalRow>> actualRows,
            Map<Long, List<InternalRow>> expectedRows) {
        // verify rows size
        assertThat(actualRows.size()).isEqualTo(expectedRows.size());
        // verify each partition -> rows
        for (Map.Entry<Long, List<InternalRow>> entry : actualRows.entrySet()) {
            List<InternalRow> actual = entry.getValue();
            List<InternalRow> expected = expectedRows.get(entry.getKey());
            // verify size
            assertThat(actual.size()).isEqualTo(expected.size());
            // verify each row
            for (int i = 0; i < actual.size(); i++) {
                assertThatRow(actual.get(i)).withSchema(rowType).isEqualTo(expected.get(i));
            }
        }
    }

    protected static void verifyPutAndLookup(Table table, Schema tableSchema, Object[] fields)
            throws Exception {
        // put data.
        InternalRow row = compactedRow(tableSchema.toRowType(), fields);
        UpsertWriter upsertWriter = table.getUpsertWriter();
        // put data.
        upsertWriter.upsert(row);
        upsertWriter.flush();
        // lookup this key.
        Lookuper lookuper = table.getLookuper();
        IndexedRow keyRow = keyRow(tableSchema, fields);
        assertThat(lookupRow(lookuper, keyRow)).isEqualTo(row);
    }

    protected static InternalRow lookupRow(Lookuper lookuper, IndexedRow keyRow) throws Exception {
        // lookup this key.
        return lookuper.lookup(keyRow).get().getRow();
    }
}

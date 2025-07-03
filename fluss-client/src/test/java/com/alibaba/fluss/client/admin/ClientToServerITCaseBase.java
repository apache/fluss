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

package com.alibaba.fluss.client.admin;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.OffsetSpec.LatestSpec;
import com.alibaba.fluss.client.admin.OffsetSpec.TimestampSpec;
import com.alibaba.fluss.client.lookup.Lookuper;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.MemorySize;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.PartitionSpec;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.ProjectedRow;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.types.RowType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static com.alibaba.fluss.testutils.InternalRowAssert.assertThatRow;
import static com.alibaba.fluss.utils.Preconditions.checkArgument;
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
        admin.createDatabase(tablePath.getDatabaseName(), DatabaseDescriptor.EMPTY, ignoreIfExists)
                .get();
        admin.createTable(tablePath, tableDescriptor, ignoreIfExists).get();
        return admin.getTableInfo(tablePath).get().getTableId();
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // set a shorter interval for testing purpose
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1));
        // set default datalake format for the cluster and enable datalake tables
        conf.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);

        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("1kb"));
        conf.set(ConfigOptions.MAX_PARTITION_NUM, 10);
        conf.set(ConfigOptions.MAX_BUCKET_NUM, 30);
        return conf;
    }

    protected static LogScanner createLogScanner(Table table) {
        return table.newScan().createLogScanner();
    }

    protected static LogScanner createLogScanner(Table table, int[] projectFields) {
        return table.newScan().project(projectFields).createLogScanner();
    }

    protected static void subscribeFromBeginning(LogScanner logScanner, Table table) {
        int bucketCount = table.getTableInfo().getNumBuckets();
        for (int i = 0; i < bucketCount; i++) {
            logScanner.subscribeFromBeginning(i);
        }
    }

    protected static void subscribeFromTimestamp(
            TablePath tablePath,
            @Nullable String partitionName,
            @Nullable Long partitionId,
            Table table,
            LogScanner logScanner,
            Admin admin,
            long timestamp)
            throws Exception {
        Map<Integer, Long> offsetsMap =
                listOffsets(tablePath, partitionName, table, admin, new TimestampSpec(timestamp));
        if (partitionId != null) {
            offsetsMap.forEach(
                    (bucketId, offset) -> logScanner.subscribe(partitionId, bucketId, offset));
        } else {
            offsetsMap.forEach(logScanner::subscribe);
        }
    }

    private static Map<Integer, Long> listOffsets(
            TablePath tablePath,
            String partitionName,
            Table table,
            Admin admin,
            OffsetSpec offsetSpec)
            throws InterruptedException, ExecutionException {
        return partitionName == null
                ? admin.listOffsets(tablePath, getAllBuckets(table), offsetSpec).all().get()
                : admin.listOffsets(tablePath, partitionName, getAllBuckets(table), offsetSpec)
                        .all()
                        .get();
    }

    protected static void subscribeFromLatestOffset(
            TablePath tablePath,
            @Nullable String partitionName,
            @Nullable Long partitionId,
            Table table,
            LogScanner logScanner,
            Admin admin)
            throws Exception {
        Map<Integer, Long> offsetsMap =
                listOffsets(tablePath, partitionName, table, admin, new LatestSpec());
        if (partitionId != null) {
            offsetsMap.forEach(
                    (bucketId, offset) -> logScanner.subscribe(partitionId, bucketId, offset));
        } else {
            offsetsMap.forEach(logScanner::subscribe);
        }
    }

    protected static List<Integer> getAllBuckets(Table table) {
        List<Integer> buckets = new ArrayList<>();
        int bucketCount = table.getTableInfo().getNumBuckets();
        for (int i = 0; i < bucketCount; i++) {
            buckets.add(i);
        }
        return buckets;
    }

    public static void verifyPartitionLogs(
            Table table, RowType rowType, Map<Long, List<InternalRow>> expectPartitionsRows)
            throws Exception {
        int totalRecords =
                expectPartitionsRows.values().stream().map(List::size).reduce(0, Integer::sum);
        int scanRecordCount = 0;
        Map<Long, List<InternalRow>> actualRows = new HashMap<>();
        try (LogScanner logScanner = table.newScan().createLogScanner()) {
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

    public static void waitAllReplicasReady(long tableId, int expectBucketCount) {
        // retry until all replica ready.
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

    protected static void verifyPutAndLookup(Table table, Object[] fields) throws Exception {
        Schema schema = table.getTableInfo().getSchema();
        // put data.
        InternalRow row = row(fields);
        UpsertWriter upsertWriter = table.newUpsert().createWriter();
        // put data.
        upsertWriter.upsert(row);
        upsertWriter.flush();
        // lookup this key.
        Lookuper lookuper = table.newLookup().createLookuper();
        ProjectedRow keyRow = ProjectedRow.from(schema.getPrimaryKeyIndexes());
        keyRow.replaceRow(row);
        assertThatRow(lookupRow(lookuper, keyRow)).withSchema(schema.getRowType()).isEqualTo(row);
    }

    protected static InternalRow lookupRow(Lookuper lookuper, InternalRow keyRow) throws Exception {
        // lookup this key.
        return lookuper.lookup(keyRow).get().getSingletonRow();
    }

    protected static PartitionSpec newPartitionSpec(String partitionKey, String partitionValue) {
        return new PartitionSpec(Collections.singletonMap(partitionKey, partitionValue));
    }

    protected static PartitionSpec newPartitionSpec(
            List<String> partitionKeys, List<String> partitionValues) {
        checkArgument(partitionKeys.size() == partitionValues.size());
        Map<String, String> collectMap =
                IntStream.range(0, partitionKeys.size())
                        .boxed()
                        .collect(Collectors.toMap(partitionKeys::get, partitionValues::get));
        return new PartitionSpec(collectMap);
    }
}

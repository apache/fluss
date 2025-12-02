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

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.client.write.HashBucketAssigner;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** IT Case for {@link KvSnapshotBatchScanner}. */
class KvSnapshotBatchScannerITCase extends ClientToServerITCaseBase {

    private static final int DEFAULT_BUCKET_NUM = 3;

    private static final Schema DEFAULT_SCHEMA =
            Schema.newBuilder()
                    .primaryKey("id")
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .build();

    private static final TableDescriptor DEFAULT_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(DEFAULT_SCHEMA)
                    .distributedBy(DEFAULT_BUCKET_NUM, "id")
                    .build();

    private static final CompactedKeyEncoder DEFAULT_KEY_ENCODER =
            new CompactedKeyEncoder(
                    DEFAULT_SCHEMA.getRowType(), DEFAULT_SCHEMA.getPrimaryKeyIndexes());

    private static final HashBucketAssigner DEFAULT_BUCKET_ASSIGNER =
            new HashBucketAssigner(DEFAULT_BUCKET_NUM);

    private static final String DEFAULT_DB = "test-snapshot-scan-db";

    private RemoteFileDownloader remoteFileDownloader;

    @BeforeEach
    protected void setup() throws Exception {
        super.setup();
        remoteFileDownloader = new RemoteFileDownloader(1);
    }

    @AfterEach
    protected void teardown() throws Exception {
        if (remoteFileDownloader != null) {
            remoteFileDownloader.close();
            remoteFileDownloader = null;
        }
        super.teardown();
    }

    @Test
    void testScanSnapshot() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test-table-snapshot");
        long tableId = createTable(tablePath, DEFAULT_TABLE_DESCRIPTOR, true);

        // scan the snapshot
        Map<TableBucket, List<InternalRow>> expectedRowByBuckets = putRows(tableId, tablePath, 10);

        // wait snapshot finish
        waitUntilAllSnapshotFinished(expectedRowByBuckets.keySet(), 0);

        // test read snapshot
        testSnapshotRead(tablePath, expectedRowByBuckets);

        // test again;
        expectedRowByBuckets = putRows(tableId, tablePath, 20);

        // wait snapshot finish
        waitUntilAllSnapshotFinished(expectedRowByBuckets.keySet(), 1);

        // test read snapshot
        testSnapshotRead(tablePath, expectedRowByBuckets);
    }

    @Test
    void testScanSnapshotDuringSchemaChange() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test-table-snapshot-schema-change");
        long tableId = createTable(tablePath, DEFAULT_TABLE_DESCRIPTOR, true);

        // put into values with old schema.
        Map<TableBucket, List<InternalRow>> oldSchemaRowByBuckets = putRows(tableId, tablePath, 10);
        waitUntilAllSnapshotFinished(oldSchemaRowByBuckets.keySet(), 0);

        // add a new column and rename an existing column
        admin.alterTable(
                        tablePath,
                        Collections.singletonList(
                                TableChange.addColumn(
                                        "new_column",
                                        DataTypes.BIGINT().copy(true),
                                        null,
                                        TableChange.ColumnPosition.last())),
                        false)
                .get();
        FLUSS_CLUSTER_EXTENSION.waitAllSchemaSync(tablePath, 2);

        Schema newSchema =
                Schema.newBuilder()
                        .primaryKey("id")
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("new_column", DataTypes.BIGINT())
                        .build();
        // put into values with new schema.
        List<InternalRow> rows = new ArrayList<>();
        for (int i = 10; i < 20; i++) {
            InternalRow row =
                    compactedRow(newSchema.getRowType(), new Object[] {i, "v" + i, (long) i});
            rows.add(row);
        }
        Map<TableBucket, List<InternalRow>> newSchemaByBuckets = putRows(tableId, tablePath, rows);

        Map<TableBucket, List<InternalRow>> expectedRowByBuckets = new HashMap<>();
        for (TableBucket tableBucket : oldSchemaRowByBuckets.keySet()) {
            List<InternalRow> expectedRows =
                    expectedRowByBuckets.computeIfAbsent(tableBucket, k -> new ArrayList<>());
            oldSchemaRowByBuckets
                    .get(tableBucket)
                    .forEach(
                            row ->
                                    expectedRows.add(
                                            ProjectedRow.from(DEFAULT_SCHEMA, newSchema)
                                                    .replaceRow(row)));
        }
        for (TableBucket tableBucket : newSchemaByBuckets.keySet()) {
            expectedRowByBuckets
                    .computeIfAbsent(tableBucket, k -> new ArrayList<>())
                    .addAll(newSchemaByBuckets.get(tableBucket));
        }

        // wait snapshot finish
        waitUntilAllSnapshotFinished(expectedRowByBuckets.keySet(), 1);

        // test read snapshot with new Schema
        testSnapshotRead(tablePath, expectedRowByBuckets);
    }

    private Map<TableBucket, List<InternalRow>> putRows(
            long tableId, TablePath tablePath, int rowNumber) throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        for (int i = 0; i < rowNumber; i++) {
            InternalRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {i, "v" + i});
            rows.add(row);
        }
        return putRows(tableId, tablePath, rows);
    }

    private Map<TableBucket, List<InternalRow>> putRows(
            long tableId, TablePath tablePath, List<InternalRow> rows) throws Exception {
        Map<TableBucket, List<InternalRow>> rowsByBuckets = new HashMap<>();
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            for (InternalRow row : rows) {
                upsertWriter.upsert(row);
                TableBucket tableBucket = new TableBucket(tableId, getBucketId(row));
                rowsByBuckets.computeIfAbsent(tableBucket, k -> new ArrayList<>()).add(row);
            }
            upsertWriter.flush();
        }
        return rowsByBuckets;
    }

    private void testSnapshotRead(
            TablePath tablePath, Map<TableBucket, List<InternalRow>> bucketRows) throws Exception {
        Table table = conn.getTable(tablePath);
        KvSnapshots kvSnapshots = admin.getLatestKvSnapshots(tablePath).get();
        for (int bucketId : kvSnapshots.getBucketIds()) {
            TableBucket tableBucket =
                    new TableBucket(
                            kvSnapshots.getTableId(), kvSnapshots.getPartitionId(), bucketId);
            assertThat(kvSnapshots.getSnapshotId(bucketId).isPresent()).isTrue();
            BatchScanner scanner =
                    table.newScan()
                            .createBatchScanner(
                                    tableBucket, kvSnapshots.getSnapshotId(bucketId).getAsLong());
            List<InternalRow> actualRows = BatchScanUtils.collectRows(scanner);
            List<InternalRow> expectedRows = bucketRows.get(tableBucket);
            assertThat(actualRows).containsExactlyInAnyOrderElementsOf(expectedRows);
            scanner.close();
        }
        table.close();
    }

    // -------- Utils method

    private static int getBucketId(InternalRow row) {
        KeyEncoder keyEncoder =
                KeyEncoder.of(
                        DEFAULT_SCHEMA.getRowType(),
                        DEFAULT_SCHEMA.getPrimaryKeyColumnNames(),
                        DataLakeFormat.PAIMON);
        BucketingFunction function = BucketingFunction.of(DataLakeFormat.PAIMON);
        byte[] key = keyEncoder.encodeKey(row);
        return function.bucketing(key, DEFAULT_BUCKET_NUM);
    }

    private void waitUntilAllSnapshotFinished(Set<TableBucket> tableBuckets, long snapshotId) {
        for (TableBucket tableBucket : tableBuckets) {
            FLUSS_CLUSTER_EXTENSION.waitUntilSnapshotFinished(tableBucket, snapshotId);
        }
    }
}

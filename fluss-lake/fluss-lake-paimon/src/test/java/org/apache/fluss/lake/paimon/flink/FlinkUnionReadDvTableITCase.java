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
import org.apache.fluss.config.AutoPartitionTimeUnit;
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
 * The IT case for Flink union data in lake and fluss for paimon deletion-vectors enabled tables.
 */
class FlinkUnionReadDvTableITCase extends FlinkUnionReadTestBase {

    @TempDir private File tempDir;

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
    }

    @Test
    void testUnionReadDvTable() throws Exception {
        // first of all, start tiering
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName = "testUnionReadDvTable";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            int bucketNum = 3;
            long tableId = createDvEnabledTable(tablePath, bucketNum, false);

            List<Row> writtenRows = writeRows(tablePath, 0, 10);
            Map<TableBucket, Long> bucketLogEndOffset =
                    getBucketLogEndOffset(tableId, bucketNum, null);
            waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, false);
            assertReplicaStatus(bucketLogEndOffset);

            assertThatThrownBy(() -> admin.getReadableLakeSnapshot(tablePath).get())
                    .rootCause()
                    .isInstanceOf(LakeTableSnapshotNotExistException.class);

            // although the readable lake snapshot doesn't exist, we can still
            // so streaming reading
            CloseableIterator<Row> rowIter =
                    streamTEnv.executeSql("select * from " + tableName).collect();

            assertResultsIgnoreOrder(rowIter, toString(writtenRows), true);

            // Step 2: Compact bucket 0 and bucket 1
            FileStoreTable fileStoreTable = getPaimonTable(tablePath);
            CompactHelper compactHelper = new CompactHelper(fileStoreTable, tempDir);
            CompactCommitter compactBucket0 = compactHelper.compactBucket(0);
            CompactCommitter compactBucket1 = compactHelper.compactBucket(1);
            compactBucket0.commit();
            compactBucket1.commit();

            // Step 3: write more records
            writtenRows.addAll(writeRows(tablePath, 10, 20));

            // Step 4: Wait until bucket synced and verify result
            bucketLogEndOffset = getBucketLogEndOffset(tableId, bucketNum, null);
            waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, false);
            assertReplicaStatus(bucketLogEndOffset);

            // still no readable snapshot
            assertThatThrownBy(() -> admin.getReadableLakeSnapshot(tablePath).get())
                    .rootCause()
                    .isInstanceOf(LakeTableSnapshotNotExistException.class);

            CloseableIterator<Row> rowIter2 =
                    streamTEnv.executeSql("select * from " + tableName).collect();
            assertResultsIgnoreOrder(rowIter2, toString(writtenRows), true);

            // Step 5: Compact bucket 2
            CompactCommitter compactBucket2 = compactHelper.compactBucket(2);
            compactBucket2.commit();
            long expectedReadableSnapshot =
                    checkNotNull(fileStoreTable.snapshotManager().latestSnapshotId());

            // Step 6: Write more data
            writtenRows.addAll(writeRows(tablePath, 20, 25));

            // Step 7: Query to verify result
            bucketLogEndOffset = getBucketLogEndOffset(tableId, bucketNum, null);
            waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, false);
            assertReplicaStatus(bucketLogEndOffset);

            LakeSnapshot lakeSnapshot = admin.getReadableLakeSnapshot(tablePath).get();
            assertThat(lakeSnapshot.getSnapshotId()).isEqualTo(expectedReadableSnapshot);
            Map<TableBucket, Long> expectedEndOffset = new HashMap<>();
            expectedEndOffset.put(new TableBucket(tableId, 0), 4L);
            expectedEndOffset.put(new TableBucket(tableId, 1), 3L);
            expectedEndOffset.put(new TableBucket(tableId, 2), 7L);
            assertThat(lakeSnapshot.getTableBucketsOffset()).isEqualTo(expectedEndOffset);

            CloseableIterator<Row> rowIter3 =
                    streamTEnv.executeSql("select * from " + tableName).collect();
            assertResultsIgnoreOrder(rowIter3, toString(writtenRows), true);
        } finally {
            jobClient.cancel().get();
        }
    }

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

    private long createDvEnabledTable(TablePath tablePath, int bucketNum, boolean isPartitioned)
            throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .column("c3", DataTypes.STRING());

        TableDescriptor.Builder tableBuilder = TableDescriptor.builder().distributedBy(bucketNum);
        tableBuilder
                .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500));

        // enabled dv
        tableBuilder.customProperty("paimon.deletion-vectors.enabled", "true");

        if (isPartitioned) {
            tableBuilder.property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true);
            tableBuilder.partitionedBy("c3");
            schemaBuilder.primaryKey("c1", "c3");
            tableBuilder.property(
                    ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, AutoPartitionTimeUnit.YEAR);
        } else {
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

    private List<String> toString(List<Row> rows) {
        return rows.stream().map(Row::toString).collect(Collectors.toList());
    }
}

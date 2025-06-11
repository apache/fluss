/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.paimon.flink;

import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.server.replica.Replica;
import com.alibaba.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** The IT case for Flink union data in lake and fluss for primary key table. */
class FlinkUnionReadPrimaryKeyTableITCase extends FlinkUnionReadTestBase {

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testPrimaryKeyTable(boolean isPartitioned) throws Exception {
        // first of all, start tiering job
        JobClient jobClient = buildTieringJob(execEnv);

        String tableName = "pkTable_" + (isPartitioned ? "partitioned" : "non_partitioned");

        TablePath t1 = TablePath.of(DEFAULT_DB, tableName);
        Map<TableBucket, Long> bucketLogEndOffset = new HashMap<>();
        long tableId = preparePkTable(t1, DEFAULT_BUCKET_NUM, isPartitioned, bucketLogEndOffset);

        // wait unit records has has been synced
        waitUtilBucketSynced(t1, tableId, DEFAULT_BUCKET_NUM, isPartitioned);

        // write records again
        writeRowsToPkTable(t1, tableId, DEFAULT_BUCKET_NUM, isPartitioned, bucketLogEndOffset);

        // check the status of replica after synced
        assertReplicaStatus(t1, tableId, DEFAULT_BUCKET_NUM, isPartitioned, bucketLogEndOffset);

        // will read paimon snapshot, won't merge log
        List<String> rows =
                toSortedRows(batchTEnv.executeSql("select * from " + tableName), isPartitioned);
        List<Row> expectedRows =
                Arrays.asList(
                        Row.of("f0", 0, "v0"),
                        Row.of("f1", 1, "v1"),
                        Row.of("f2", 2, "v2"),
                        Row.of("f2222", 2222, "v2222"),
                        Row.of("f3", 3, "v3"));

        if (isPartitioned) {
            expectedRows = paddingPartition(t1, expectedRows);
        }

        assertThat(rows.toString()).isEqualTo(sortedRows(expectedRows).toString());

        // read paimon directly using $lake
        List<String> paimonSnapshotRows =
                toSortedRows(
                        batchTEnv.executeSql(String.format("select * from %s$lake", tableName)),
                        isPartitioned);
        // paimon's source will emit +U[0, v0, xx] instead of +I[0, v0, xx], so
        // replace +U with +I to make it equal
        assertThat(paimonSnapshotRows.toString().replace("+U", "+I"))
                .isEqualTo(sortedRows(expectedRows).toString());

        // test point query with fluss
        String queryFilterStr = "a = 2222";
        String partitionName =
                isPartitioned ? waitUntilPartitions(t1).values().iterator().next() : null;
        if (partitionName != null) {
            queryFilterStr = queryFilterStr + " and c = '" + partitionName + "'";
        }

        // test point query
        List<String> paimonPointQueryRows =
                toSortedRows(
                        batchTEnv.executeSql(
                                String.format(
                                        "select * from %s$lake where %s",
                                        tableName, queryFilterStr)),
                        isPartitioned);
        List<String> expectedPointQueryRows =
                expectedRows.stream()
                        .filter(
                                row -> {
                                    boolean isMatch = row.getField(1).equals(2222);
                                    if (partitionName != null) {
                                        isMatch = isMatch && row.getField(3).equals(partitionName);
                                    }
                                    return isMatch;
                                })
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        assertThat(paimonPointQueryRows).isEqualTo(expectedPointQueryRows);

        List<String> flussPointQueryRows =
                toSortedRows(
                        batchTEnv.executeSql(
                                String.format(
                                        "select * from %s where %s", tableName, queryFilterStr)),
                        isPartitioned);
        assertThat(flussPointQueryRows).isEqualTo(expectedPointQueryRows);

        // read paimon system table
        List<String> paimonOptionsRows =
                toSortedRows(
                        batchTEnv.executeSql(
                                String.format("select * from %s$lake$options", tableName)));
        assertThat(paimonOptionsRows.toString()).contains("+I[bucket, 3], +I[bucket-key, a]");

        // stop sync database job
        jobClient.cancel().get();

        // write records
        writeRowsToPkTable(
                t1,
                isPartitioned,
                Arrays.asList(
                        new Object[] {"f00", 0, "v0"},
                        new Object[] {"f11", 1, "v111"},
                        new Object[] {"f22", 2, "v222"},
                        new Object[] {"f44", 4, "v4"},
                        new Object[] {"f5", 5, "v5"},
                        new Object[] {"f6", 6, "v6"},
                        new Object[] {"f7", 7, "v7"}));

        // query again and check the data
        // it must union snapshot and log
        rows = toSortedRows(batchTEnv.executeSql("select * from " + tableName), isPartitioned);
        expectedRows =
                Arrays.asList(
                        Row.of("f00", 0, "v0"),
                        Row.of("f11", 1, "v111"),
                        Row.of("f22", 2, "v222"),
                        Row.of("f2222", 2222, "v2222"),
                        Row.of("f3", 3, "v3"),
                        Row.of("f44", 4, "v4"),
                        Row.of("f5", 5, "v5"),
                        Row.of("f6", 6, "v6"),
                        Row.of("f7", 7, "v7"));
        if (isPartitioned) {
            expectedRows = paddingPartition(t1, expectedRows);
        }
        assertThat(rows.toString()).isEqualTo(sortedRows(expectedRows).toString());

        // query with project push down
        rows = toSortedRows(batchTEnv.executeSql("select b,a from " + tableName));
        expectedRows =
                expectedRows.stream()
                        .map(row -> Row.of(row.getField(2), row.getField(1)))
                        .collect(Collectors.toList());
        assertThat(rows.toString()).isEqualTo(sortedRows(expectedRows).toString());

        rows = toSortedRows(batchTEnv.executeSql("select b from " + tableName));
        expectedRows =
                expectedRows.stream()
                        .map(row -> Row.of(row.getField(0)))
                        .collect(Collectors.toList());
        assertThat(rows.toString()).isEqualTo(sortedRows(expectedRows).toString());
    }

    private List<Row> paddingPartition(TablePath tablePath, List<Row> rows) {
        List<Row> paddingPartitionRows = new ArrayList<>();
        for (String partition : waitUntilPartitions(tablePath).values()) {
            for (Row row : rows) {
                paddingPartitionRows.add(
                        Row.of(row.getField(0), row.getField(1), row.getField(2), partition));
            }
        }
        return paddingPartitionRows;
    }

    private List<Row> sortedRows(List<Row> rows) {
        rows.sort(Comparator.comparing(Row::toString));
        return rows;
    }

    private List<String> toSortedRows(TableResult tableResult) {
        return CollectionUtil.iteratorToList(tableResult.collect()).stream()
                .map(Row::toString)
                .sorted()
                .collect(Collectors.toList());
    }

    private List<String> toSortedRows(TableResult tableResult, boolean isPartitioned) {
        return CollectionUtil.iteratorToList(tableResult.collect()).stream()
                .map(
                        row ->
                                isPartitioned
                                        ? Row.of(
                                                row.getField(0),
                                                row.getField(1),
                                                row.getField(2),
                                                row.getField(3))
                                        : Row.of(row.getField(0), row.getField(1), row.getField(2)))
                .map(Row::toString)
                .sorted()
                .collect(Collectors.toList());
    }

    private long preparePkTable(
            TablePath tablePath,
            int bucketNum,
            boolean isPartitioned,
            Map<TableBucket, Long> bucketLogEndOffset)
            throws Exception {
        long tableId = createPkTable(tablePath, bucketNum, isPartitioned);
        writeRowsToPkTable(tablePath, tableId, bucketNum, isPartitioned, bucketLogEndOffset);
        return tableId;
    }

    private void writeRowsToPkTable(
            TablePath tablePath,
            long tableId,
            int bucketNum,
            boolean isPartitioned,
            Map<TableBucket, Long> bucketLogEndOffset)
            throws Exception {
        if (isPartitioned) {
            Map<Long, String> partitionNameById = waitUntilPartitions(tablePath);
            for (String partition : partitionNameById.values()) {
                for (int i = 0; i < 2; i++) {
                    List<InternalRow> rows = generateKvRows(partition);
                    // write records
                    writeRows(tablePath, rows, false);
                }
            }
            for (Long partitionId : partitionNameById.keySet()) {
                bucketLogEndOffset.putAll(getBucketLogEndOffset(tableId, bucketNum, partitionId));
            }
        } else {
            for (int i = 0; i < 2; i++) {
                List<InternalRow> rows = generateKvRows(null);
                // write records
                writeRows(tablePath, rows, false);
            }
            bucketLogEndOffset.putAll(getBucketLogEndOffset(tableId, bucketNum, null));
        }
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

    private void writeRowsToPkTable(
            TablePath tablePath, boolean isPartitioned, List<Object[]> rowsValue) throws Exception {
        if (isPartitioned) {
            List<InternalRow> rows = new ArrayList<>();
            Map<Long, String> partitionNameById = waitUntilPartitions(tablePath);
            for (String partition : partitionNameById.values()) {
                for (Object[] values : rowsValue) {
                    rows.add(row(rowValues(values, partition)));
                }
                writeRows(tablePath, rows, false);
            }
        } else {
            List<InternalRow> rows = new ArrayList<>();
            for (Object[] values : rowsValue) {
                rows.add(row(values));
            }
            writeRows(tablePath, rows, false);
        }
    }

    protected long createPkTable(TablePath tablePath, int bucketNum, boolean isPartitioned)
            throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING());

        TableDescriptor.Builder tableBuilder =
                TableDescriptor.builder()
                        .distributedBy(bucketNum)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500));

        if (isPartitioned) {
            schemaBuilder.column("c", DataTypes.STRING());
            tableBuilder.property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true);
            tableBuilder.partitionedBy("c");
            schemaBuilder.primaryKey("a", "c");
            tableBuilder.property(
                    ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, AutoPartitionTimeUnit.YEAR);
        } else {
            schemaBuilder.primaryKey("a");
        }
        tableBuilder.schema(schemaBuilder.build());
        return createTable(tablePath, tableBuilder.build());
    }

    private List<InternalRow> generateKvRows(@Nullable String partition) {
        if (partition == null) {
            return Arrays.asList(
                    row("f0", 0, "v0"),
                    row("f1", 1, "v1"),
                    row("f2", 2, "v2"),
                    row("f3", 3, "v3"),
                    row("f2222", 2222, "v2222"));
        } else {
            return Arrays.asList(
                    row("f0", 0, "v0", partition),
                    row("f1", 1, "v1", partition),
                    row("f2", 2, "v2", partition),
                    row("f3", 3, "v3", partition),
                    row("f2222", 2222, "v2222", partition));
        }
    }
}

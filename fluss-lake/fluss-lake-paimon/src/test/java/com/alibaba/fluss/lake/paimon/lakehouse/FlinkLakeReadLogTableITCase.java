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

package com.alibaba.fluss.lake.paimon.lakehouse;

import com.alibaba.fluss.client.metadata.LakeSnapshot;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lake.paimon.PaimonLakeStorage;
import com.alibaba.fluss.lake.paimon.flink.FlinkUnionReadTestBase;
import com.alibaba.fluss.lake.source.LakeSource;
import com.alibaba.fluss.lake.source.RecordReader;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.utils.CloseableIterator;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test case for flink read fluss lakehouse paimon log table. */
public class FlinkLakeReadLogTableITCase extends FlinkUnionReadTestBase {
    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testReadLogTable(boolean isPartitioned) throws Exception {
        // first of all, start tiering
        buildTieringJob(execEnv);

        String tableName = "logTable_" + (isPartitioned ? "partitioned" : "non_partitioned");

        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        List<Row> writtenRows = new ArrayList<>();
        long tableId = prepareLogTable(tablePath, DEFAULT_BUCKET_NUM, isPartitioned, writtenRows);
        // wait until records has been synced
        waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, isPartitioned);

        // now, start to read the log table, which will read paimon
        // may read fluss or not, depends on the log offset of paimon snapshot

        Configuration configuration = new Configuration();
        configuration.setString("type", "paimon");
        configuration.setString("warehouse", warehousePath);
        PaimonLakeStorage lakeStorage = new PaimonLakeStorage(configuration);
        LakeSource<PaimonSplit> lakeSource = lakeStorage.createLakeSource(tablePath);
        LakeSnapshot lakeSnapshot = admin.getLatestLakeSnapshot(tablePath).get();
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        List<PaimonSplit> plan = lakeSource.createPlanner(lakeSnapshot::getSnapshotId).plan();

        List<Row> actual = new ArrayList<>();

        for (PaimonSplit paimonSplit : plan) {
            RecordReader recordReader = lakeSource.createRecordReader(() -> paimonSplit);
            CloseableIterator<LogRecord> iterator = recordReader.read();
            while (iterator.hasNext()) {
                InternalRow row = iterator.next().getRow();
                InternalRow.FieldGetter[] fieldGetters =
                        InternalRow.createFieldGetters(tableInfo.getRowType());
                Row flinkRow = new Row(fieldGetters.length);
                for (int i = 0; i < fieldGetters.length; i++) {
                    flinkRow.setField(i, fieldGetters[i].getFieldOrNull(row));
                }
                actual.add(flinkRow);
            }
        }

        assertThat(actual).containsExactlyInAnyOrderElementsOf(writtenRows);
    }

    private long prepareLogTable(
            TablePath tablePath, int bucketNum, boolean isPartitioned, List<Row> flinkRows)
            throws Exception {
        long t1Id = createLogTable(tablePath, bucketNum, isPartitioned);
        if (isPartitioned) {
            Map<Long, String> partitionNameById = waitUntilPartitions(tablePath);
            for (String partition : partitionNameById.values()) {
                for (int i = 0; i < 3; i++) {
                    flinkRows.addAll(writeRows(tablePath, 10, partition));
                }
            }
        } else {
            for (int i = 0; i < 3; i++) {
                flinkRows.addAll(writeRows(tablePath, 10, null));
            }
        }
        return t1Id;
    }

    private List<Row> writeRows(TablePath tablePath, int rowCount, @Nullable String partition)
            throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        List<Row> writeRows = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            if (partition == null) {
                rows.add(row(i, "v" + i));
                writeRows.add(Row.of(i, BinaryString.fromString("v" + i)));
            } else {
                rows.add(row(i, "v" + i, partition));
                writeRows.add(
                        Row.of(
                                i,
                                BinaryString.fromString("v" + i),
                                BinaryString.fromString(partition)));
            }
        }
        writeRows(tablePath, rows, true);
        return writeRows;
    }
}

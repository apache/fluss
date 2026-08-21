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

package org.apache.fluss.lake.iceberg.maintenance;

import org.apache.fluss.lake.iceberg.testutils.FlinkIcebergTieringTestBase;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.data.Record;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for Iceberg compaction. */
class IcebergRewriteITCase extends FlinkIcebergTieringTestBase {
    protected static final String DEFAULT_DB = "fluss";

    private static StreamExecutionEnvironment execEnv;

    private static final Schema pkSchema =
            Schema.newBuilder()
                    .column("f_int", DataTypes.INT())
                    .column("f_string", DataTypes.STRING())
                    .primaryKey("f_int")
                    .build();

    private static final Schema logSchema =
            Schema.newBuilder()
                    .column("f_int", DataTypes.INT())
                    .column("f_string", DataTypes.STRING())
                    .build();

    @BeforeAll
    protected static void beforeAll() {
        FlinkIcebergTieringTestBase.beforeAll();
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(2);
        execEnv.enableCheckpointing(1000);
    }

    @Test
    void testPkTableCompactionDisabledForCleanTable() throws Exception {
        // FIP-27: clean-schema tables have no __bucket column, so per-bucket compaction can no
        // longer scope an exclusive file set. Auto compaction is therefore disabled for clean
        // tables; verify the files written here are NOT merged. Legacy-table compaction remains
        // covered by IcebergRewriteTest.
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            TablePath t1 = TablePath.of(DEFAULT_DB, "pk_table_1");
            long t1Id = createPkTable(t1, 1, true, pkSchema);
            TableBucket t1Bucket = new TableBucket(t1Id, 0);
            List<InternalRow> flussRows = new ArrayList<>();

            List<InternalRow> rows = Collections.singletonList(row(1, "v1"));
            writeIcebergTableRecords(t1, t1Bucket, 1, false, rows);
            flussRows.addAll(rows);

            rows = Collections.singletonList(row(2, "v1"));
            writeIcebergTableRecords(t1, t1Bucket, 2, false, rows);
            flussRows.addAll(rows);

            rows = Collections.singletonList(row(3, "v1"));
            writeIcebergTableRecords(t1, t1Bucket, 3, false, rows);
            flussRows.addAll(rows);

            rows = Collections.singletonList(row(4, "v1"));
            writeIcebergTableRecords(t1, t1Bucket, 4, false, rows);
            flussRows.addAll(rows);

            // Would have triggered compaction on a legacy table; a clean table must not compact.
            checkNoCompactionInIcebergTable(t1, 4);
            // Data must still be complete and correct even without compaction.
            checkRecords(getIcebergRecords(t1), flussRows);
        } finally {
            jobClient.cancel().get();
        }
    }

    private void checkRecords(List<Record> actualRows, List<InternalRow> expectedRows) {
        // check records size
        assertThat(actualRows.size()).isEqualTo(expectedRows.size());

        // check records content
        Iterator<Record> actualIterator =
                actualRows.stream()
                        .sorted(Comparator.comparingInt((Record r) -> (int) r.get(0)))
                        .iterator();
        Iterator<InternalRow> expectedIterator =
                expectedRows.stream().sorted(Comparator.comparingInt(r -> r.getInt(0))).iterator();
        while (actualIterator.hasNext() && expectedIterator.hasNext()) {
            Record record = actualIterator.next();
            InternalRow row = expectedIterator.next();
            assertThat(record.get(0)).isEqualTo(row.getInt(0));
            assertThat(record.get(1)).isEqualTo(row.getString(1).toString());
        }
        assertThat(actualIterator.hasNext()).isFalse();
        assertThat(expectedIterator.hasNext()).isFalse();
    }

    @Test
    void testLogTableCompactionDisabledForCleanTable() throws Exception {
        // FIP-27: a clean bucket-aware log table still has no __bucket column; auto compaction is
        // disabled for clean tables, so the small files written here must not be merged.
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            TablePath t1 = TablePath.of(DEFAULT_DB, "log_table");
            long t1Id = createLogTable(t1, 1, true, logSchema);
            TableBucket t1Bucket = new TableBucket(t1Id, 0);

            int i = 0;
            List<InternalRow> flussRows = new ArrayList<>();
            flussRows.addAll(
                    writeIcebergTableRecords(
                            t1, t1Bucket, ++i, true, Collections.singletonList(row(1, "v1"))));

            flussRows.addAll(
                    writeIcebergTableRecords(
                            t1, t1Bucket, ++i, true, Collections.singletonList(row(1, "v1"))));

            flussRows.addAll(
                    writeIcebergTableRecords(
                            t1, t1Bucket, ++i, true, Collections.singletonList(row(1, "v1"))));

            flussRows.addAll(
                    writeIcebergTableRecords(
                            t1, t1Bucket, ++i, true, Collections.singletonList(row(1, "v1"))));

            // FIP-27: a clean table is not auto-compacted, so the files must not be merged.
            checkNoCompactionInIcebergTable(t1, 4);
            // check data in iceberg to make sure no data is lost or duplicated without compaction
            checkRecords(getIcebergRecords(t1), flussRows);
        } finally {
            jobClient.cancel().get();
        }
    }

    private List<InternalRow> writeIcebergTableRecords(
            TablePath tablePath,
            TableBucket tableBucket,
            long expectedLogEndOffset,
            boolean append,
            List<InternalRow> rows)
            throws Exception {
        writeRows(tablePath, rows, append);
        assertReplicaStatus(tableBucket, expectedLogEndOffset);
        return rows;
    }
}

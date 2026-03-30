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

package org.apache.fluss.flink.tiering;

import org.apache.fluss.lake.values.TestingValuesLake;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test tiering failover. */
class TieringFailoverITCase extends FlinkTieringTestBase {
    protected static final String DEFAULT_DB = "fluss";

    private static final Schema schema =
            Schema.newBuilder()
                    .column("f_int", DataTypes.INT())
                    .column("f_string", DataTypes.STRING())
                    .primaryKey("f_string")
                    .build();

    @BeforeAll
    protected static void beforeAll() {
        FlinkTieringTestBase.beforeAll();
    }

    @AfterAll
    protected static void afterAll() throws Exception {
        FlinkTieringTestBase.afterAll();
    }

    @Test
    void testTiering() throws Exception {
        // create a pk table, write some records and wait until snapshot finished
        TablePath t1 = TablePath.of(DEFAULT_DB, "pkTable");
        long t1Id = createTable(t1, schema);
        TableBucket t1Bucket = new TableBucket(t1Id, 0);
        // write records
        List<InternalRow> rows = Arrays.asList(row(1, "i1"), row(2, "i2"), row(3, "i3"));
        List<InternalRow> expectedRows = new ArrayList<>(rows);
        writeRows(t1, rows, false);
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(t1);

        // fail the first write to the pk table
        TestingValuesLake.failWhen(t1.toString()).failWriteOnce();

        // then start tiering job
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            // check the status of replica after synced
            assertReplicaStatus(t1Bucket, 3);

            checkDataInValuesTable(t1, rows);

            // then write data to the pk tables
            // write records
            rows = Arrays.asList(row(1, "i11"), row(2, "i22"), row(3, "i33"));
            expectedRows.addAll(rows);
            // write records
            writeRows(t1, rows, false);

            // check the status of replica of t1 after synced
            // not check start offset since we won't
            // update start log offset for primary key table
            assertReplicaStatus(t1Bucket, expectedRows.size());

            checkDataInValuesTable(t1, expectedRows);
        } finally {
            jobClient.cancel().get();
        }
    }

    private void checkDataInValuesTable(TablePath tablePath, List<InternalRow> expectedRows) {
        Iterator<InternalRow> actualIterator = getValuesRecords(tablePath).iterator();
        Iterator<InternalRow> iterator = expectedRows.iterator();
        while (iterator.hasNext() && actualIterator.hasNext()) {
            InternalRow row = iterator.next();
            InternalRow record = actualIterator.next();
            assertThat(record.getInt(0)).isEqualTo(row.getInt(0));
            assertThat(record.getString(1)).isEqualTo(row.getString(1));
        }
        assertThat(actualIterator.hasNext()).isFalse();
        assertThat(iterator.hasNext()).isFalse();
    }
}

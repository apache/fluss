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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.lake.paimon.testutils.FlinkPaimonTieringTestBase;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.testutils.DataTestUtils.row;

/** A Test case for dropping a pktable after tiering and creating one with the same tablePath. */
class ReCreateSameTableAfterTieringTest extends FlinkPaimonTieringTestBase {
    protected static final String DEFAULT_DB = "fluss";

    private static StreamExecutionEnvironment execEnv;

    @BeforeAll
    protected static void beforeAll() {
        FlinkPaimonTieringTestBase.beforeAll();
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(2);
        execEnv.enableCheckpointing(1000);
    }

    @Test
    void testReCreateSameTable() throws Exception {
        // create a pk table, write some records and wait until snapshot finished
        TablePath t1 = TablePath.of(DEFAULT_DB, "pkTable_drop");
        long t1Id = createPkTable(t1);
        TableBucket t1Bucket = new TableBucket(t1Id, 0);
        // write records
        List<InternalRow> rows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));
        writeRows(t1, rows, false);
        waitUntilSnapshot(t1Id, 1, 0);
        // then start tiering job
        JobClient jobClient = buildTieringJob(execEnv);

        // check the status of replica after synced
        assertReplicaStatus(t1Bucket, 3);
        // check data in paimon
        checkDataInPaimonPrimayKeyTable(t1, rows);

        // then drop the table
        dropTable(t1);
        // and create a new table with the same table path
        long t2Id = createPkTable(t1);
        TableBucket t2Bucket = new TableBucket(t2Id, 0);
        // write some new records
        List<InternalRow> newRows = Arrays.asList(row(4, "v4"), row(5, "v5"));
        writeRows(t1, newRows, false);
        // new table, so the snapshot id should be 0
        waitUntilSnapshot(t2Id, 1, 0);
        // check the status of replica after synced
        assertReplicaStatus(t2Bucket, 2);
        // check data in paimon
        checkDataInPaimonPrimayKeyTable(t1, newRows);

        // stop the tiering job
        jobClient.cancel().get();
    }
}

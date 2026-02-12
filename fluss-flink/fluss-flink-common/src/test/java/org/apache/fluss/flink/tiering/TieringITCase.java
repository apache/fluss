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

import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.ExceptionUtils;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/** The IT case for tiering. */
abstract class TieringITCase extends FlinkTieringTestBase {

    @BeforeAll
    protected static void beforeAll() {
        FlinkTieringTestBase.beforeAll();
    }

    @AfterAll
    protected static void afterAll() throws Exception {
        FlinkTieringTestBase.afterAll();
    }

    @BeforeEach
    @Override
    void beforeEach() {
        execEnv =
                StreamExecutionEnvironment.getExecutionEnvironment()
                        .setParallelism(1)
                        .setRuntimeMode(RuntimeExecutionMode.STREAMING);
    }

    @Test
    void testTieringReachMaxDuration() throws Exception {
        TablePath logTablePath = TablePath.of("fluss", "logtable");
        createTable(logTablePath, false);
        TablePath pkTablePath = TablePath.of("fluss", "pktable");
        createTable(pkTablePath, true);

        // write some records to log table
        List<InternalRow> rows = new ArrayList<>();
        int recordCount = 6;
        for (int i = 0; i < recordCount; i++) {
            rows.add(GenericRow.of(i, BinaryString.fromString("v" + i)));
        }
        writeRows(logTablePath, rows, true);

        rows = new ArrayList<>();
        //  write 6 records to primary key table, each bucket should only contain few record
        for (int i = 0; i < recordCount; i++) {
            rows.add(GenericRow.of(i, BinaryString.fromString("v" + i)));
        }
        writeRows(pkTablePath, rows, false);

        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(pkTablePath);

        // set tiering duration to a small value for testing purpose
        Configuration lakeTieringConfig = new Configuration();
        JobClient jobClient = buildTieringJob(execEnv, lakeTieringConfig);

        try {
            // verify the tiered records is less than the table total record to
            // make sure tiering is forced to complete when reach max duration
            LakeSnapshot logTableLakeSnapshot = waitLakeSnapshot(logTablePath);
            long tieredRecords = countTieredRecords(logTableLakeSnapshot);
            assertThat(tieredRecords).isLessThan(recordCount);

            // verify the tiered records is less than the table total record to
            // make sure tiering is forced to complete when reach max duration
            LakeSnapshot pkTableLakeSnapshot = waitLakeSnapshot(pkTablePath);
            tieredRecords = countTieredRecords(pkTableLakeSnapshot);
            assertThat(tieredRecords).isLessThan(recordCount);
        } finally {
            jobClient.cancel();
        }
    }

    private long countTieredRecords(LakeSnapshot lakeSnapshot) {
        return lakeSnapshot.getTableBucketsOffset().values().stream()
                .mapToLong(Long::longValue)
                .sum();
    }

    private LakeSnapshot waitLakeSnapshot(TablePath tablePath) {
        return waitValue(
                () -> {
                    try {
                        return Optional.of(admin.getLatestLakeSnapshot(tablePath).get());
                    } catch (Exception e) {
                        if (ExceptionUtils.stripExecutionException(e)
                                instanceof LakeTableSnapshotNotExistException) {
                            return Optional.empty();
                        }
                        throw e;
                    }
                },
                Duration.ofSeconds(30),
                "Fail to wait for one round of tiering finish for table " + tablePath);
    }

    private void createTable(TablePath tablePath, boolean isPrimaryKeyTable) throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder().column("a", DataTypes.INT()).column("b", DataTypes.STRING());
        if (isPrimaryKeyTable) {
            schemaBuilder.primaryKey("a");
        }

        // see TestingPaimonStoragePlugin#TestingPaimonWriter, we set write-pause
        // to 1s to make it easy to mock tiering reach max duration
        Map<String, String> customProperties = Collections.singletonMap("write-pause", "1s");
        createTable(
                tablePath,
                3,
                Collections.singletonList("a"),
                schemaBuilder.build(),
                customProperties);
    }
}

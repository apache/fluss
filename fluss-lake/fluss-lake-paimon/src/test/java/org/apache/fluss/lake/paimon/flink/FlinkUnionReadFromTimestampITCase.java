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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.lake.paimon.testutils.FlinkPaimonTieringTestBase;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.utils.clock.ManualClock;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.core.testutils.CommonTestUtils.waitUtil;
import static org.apache.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertRowResultsIgnoreOrder;
import static org.apache.fluss.testutils.DataTestUtils.row;

/** The ITCase for Flink union read from a timestamp. */
class FlinkUnionReadFromTimestampITCase extends FlinkPaimonTieringTestBase {

    private static final ManualClock CLOCK = new ManualClock();

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(initConfig())
                    .setNumOfTabletServers(3)
                    .setClock(CLOCK)
                    .build();

    private StreamTableEnvironment streamTEnv;

    protected static Configuration initConfig() {
        Configuration configuration = FlinkPaimonTieringTestBase.initConfig();
        // set file size to 10b to make log segment roll frequently
        configuration.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("10b"));
        configuration.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofMillis(100));
        return configuration;
    }

    @BeforeAll
    static void beforeAll() {
        FlinkPaimonTieringTestBase.beforeAll(FLUSS_CLUSTER_EXTENSION.getClientConfig());
    }

    @BeforeEach
    public void beforeEach() {
        super.beforeEach();
        buildStreamTEnv();
    }

    @Override
    protected FlussClusterExtension getFlussClusterExtension() {
        return FLUSS_CLUSTER_EXTENSION;
    }

    @Test
    void testUnionReadFromTimestamp() throws Exception {
        // first of all, start tiering
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName = "logTable_read_timestamp";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId = createLogTable(tablePath, 1);
            TableBucket t1Bucket = new TableBucket(tableId, 0);

            List<Row> rows = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                rows.addAll(writeRows(tablePath, 3));
                // each round advance 1s to make sure each round of writing has
                // different timestamp
                CLOCK.advanceTime(Duration.ofSeconds(1));
            }
            assertReplicaStatus(t1Bucket, rows.size());

            Replica t1Replica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(t1Bucket);

            // wait util only 2(default keep 2 segments in local) log segments in local
            waitUtil(
                    () -> t1Replica.getLogTablet().logSegments().size() == 2,
                    Duration.ofMinutes(1),
                    "Fail to wait util only 2 segments in local.");

            // advance 10 days to mock remote log ttl
            CLOCK.advanceTime(Duration.ofDays(10));
            // wait util remote log ttl, should can't fetch from remote log for offset 10
            waitUtil(
                    () -> !t1Replica.getLogTablet().canFetchFromRemoteLog(10),
                    Duration.ofMinutes(1),
                    "Fail to wait log offset 10 ttl from remote log.");

            // verify scan from timestamp 0, should read full data
            assertRowResultsIgnoreOrder(
                    streamTEnv
                            .executeSql(
                                    "select * from "
                                            + tableName
                                            + " /*+ OPTIONS('scan.startup.mode' = 'timestamp',\n"
                                            + "'scan.startup.timestamp' = '0') */")
                            .collect(),
                    rows,
                    true);

            // verify scan from timestamp 2000, shouldn't read the rows written in first two
            // rounds,
            CloseableIterator<Row> actualRows =
                    streamTEnv
                            .executeSql(
                                    "select * from "
                                            + tableName
                                            + " /*+ OPTIONS('scan.startup.mode' = 'timestamp',\n"
                                            + "'scan.startup.timestamp' = '2000') */")
                            .collect();
            List<Row> expectedRows = rows.stream().skip(2 * 3).collect(Collectors.toList());
            assertRowResultsIgnoreOrder(actualRows, expectedRows, true);

            // verify scan from earliest
            assertRowResultsIgnoreOrder(
                    streamTEnv
                            .executeSql(
                                    "select * from "
                                            + tableName
                                            + " /*+ OPTIONS('scan.startup.mode' = 'earliest') */")
                            .collect(),
                    rows,
                    true);

        } finally {
            jobClient.cancel();
        }
    }

    private List<Row> writeRows(TablePath tablePath, int rows) throws Exception {
        List<InternalRow> writtenRows = new ArrayList<>();
        List<Row> flinkRow = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            writtenRows.add(row(i, "v" + i));
            flinkRow.add(Row.of(i, "v" + i));
        }
        writeRows(tablePath, writtenRows, true);
        return flinkRow;
    }

    private void buildStreamTEnv() {
        String bootstrapServers = String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        // create table environment
        streamTEnv = StreamTableEnvironment.create(execEnv, EnvironmentSettings.inStreamingMode());
        // crate catalog using sql
        streamTEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        streamTEnv.executeSql("use catalog " + CATALOG_NAME);
        streamTEnv.executeSql("use " + DEFAULT_DB);
    }
}

/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.connector.flink.source;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.server.zk.ZooKeeperClient;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.time.Duration;
import java.time.Year;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.alibaba.fluss.connector.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static com.alibaba.fluss.connector.flink.source.testutils.FlinkTestBase.assertResultsIgnoreOrder;
import static com.alibaba.fluss.connector.flink.source.testutils.FlinkTestBase.createPartitions;
import static com.alibaba.fluss.connector.flink.source.testutils.FlinkTestBase.dropPartitions;
import static com.alibaba.fluss.connector.flink.source.testutils.FlinkTestBase.waitUntilPartitions;
import static com.alibaba.fluss.testutils.DataTestUtils.row;

/** IT case for flink table source fail over. */
class FlinkTableSourceFailOverITCase {

    private static final String CATALOG_NAME = "testcatalog";

    @TempDir public static File checkpointDir;
    @TempDir public static File savepointDir;

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(
                            new com.alibaba.fluss.config.Configuration()
                                    // set snapshot interval to 1s for testing purposes
                                    .set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1))
                                    // not to clean snapshots for test purpose
                                    .set(
                                            ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS,
                                            Integer.MAX_VALUE))
                    .setNumOfTabletServers(3)
                    .build();

    com.alibaba.fluss.config.Configuration clientConf;
    ZooKeeperClient zkClient;
    Connection conn;

    @BeforeEach
    protected void beforeEach() {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        conn = ConnectionFactory.createConnection(clientConf);
    }

    @AfterEach
    protected void afterEach() throws Exception {
        conn.close();
    }

    private StreamTableEnvironment initTableEnvironment(@Nullable String savepointPath) {
        Configuration conf = new Configuration();
        if (savepointPath != null) {
            conf.setString("execution.savepoint.path", savepointPath);
        }
        StreamExecutionEnvironment execEnv =
                StreamExecutionEnvironment.getExecutionEnvironment(conf);
        execEnv.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(execEnv, EnvironmentSettings.inStreamingMode());
        String bootstrapServers = String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        // crate catalog using sql
        tEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tEnv.executeSql("use catalog " + CATALOG_NAME);
        return tEnv;
    }

    @Test
    void testRestore() throws Exception {
        final int numTaskManagers = 2;
        final int numSlotsPerTaskManager = 2;

        // Start Flink
        MiniClusterWithClientResource cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(getFileBasedCheckpointsConfig(savepointDir))
                                .setNumberTaskManagers(numTaskManagers)
                                .setNumberSlotsPerTaskManager(numSlotsPerTaskManager)
                                .build());

        cluster.before();

        try {
            StreamTableEnvironment tEnv = initTableEnvironment(null);
            tEnv.executeSql(
                    "create table test_partitioned ("
                            + "a int, b varchar"
                            + ") partitioned by (b) "
                            + "with ("
                            + "'table.auto-partition.enabled' = 'true',"
                            + "'table.auto-partition.time-unit' = 'year',"
                            + "'scan.partition.discovery.interval' = '100ms',"
                            + "'table.auto-partition.num-precreate' = '1')");
            tEnv.executeSql("create table result_table (a int, b varchar)");

            TablePath tablePath = TablePath.of("fluss", "test_partitioned");

            // create a partition manually
            createPartitions(zkClient, tablePath, Collections.singletonList("4000"));
            waitUntilPartitions(zkClient, tablePath, 2);

            // append 3 records for each partition
            Table table = conn.getTable(tablePath);
            AppendWriter writer = table.newAppend().createWriter();
            String thisYear = String.valueOf(Year.now().getValue());
            List<String> expected = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                writer.append(row(i, thisYear));
                writer.append(row(i, "4000"));
                expected.add("+I[" + i + ", " + thisYear + "]");
                expected.add("+I[" + i + ", 4000]");
            }
            writer.flush();

            // execute the query to fetch logs from the table
            TableResult insertResult =
                    tEnv.executeSql("insert into result_table select * from test_partitioned");
            // we have to create a intermediate table to collect result,
            // because CollectSink can't be restored from savepoint
            CloseableIterator<Row> results =
                    tEnv.executeSql("select * from result_table").collect();
            assertResultsIgnoreOrder(results, expected, false);
            expected.clear();

            // drop the partition manually
            dropPartitions(zkClient, tablePath, Collections.singleton("4000"));
            waitUntilPartitions(zkClient, tablePath, 1);

            // create a new partition again and append records into it
            createPartitions(zkClient, tablePath, Collections.singletonList("5000"));
            waitUntilPartitions(zkClient, tablePath, 2);
            writer.append(row(4, "5000")).get();
            expected.add("+I[4, 5000]");
            // if the source subscribes the new partition successfully,
            // it should have removed the old partition successfully
            assertResultsIgnoreOrder(results, expected, false);
            expected.clear();

            // now, stop the job with save point
            String savepointPath =
                    insertResult
                            .getJobClient()
                            .get()
                            .stopWithSavepoint(
                                    false,
                                    savepointDir.getAbsolutePath(),
                                    SavepointFormatType.CANONICAL)
                            .get();

            tEnv = initTableEnvironment(savepointPath);
            insertResult =
                    tEnv.executeSql("insert into result_table select * from test_partitioned");
            // append a new row again to check if the source can restore the state correctly
            writer.append(row(5, "5000")).get();
            expected.add("+I[5, 5000]");
            assertResultsIgnoreOrder(results, expected, true);
            // cancel the insert job
            insertResult.getJobClient().get().cancel().get();
        } finally {
            // stop the cluster and thereby cancel the job
            cluster.after();
        }
    }

    private static Configuration getFileBasedCheckpointsConfig(File savepointDir) {
        return getFileBasedCheckpointsConfig(savepointDir.toURI().toString());
    }

    private static Configuration getFileBasedCheckpointsConfig(final String savepointDir) {
        final Configuration config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
        config.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, MemorySize.ZERO);
        config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
        return config;
    }
}

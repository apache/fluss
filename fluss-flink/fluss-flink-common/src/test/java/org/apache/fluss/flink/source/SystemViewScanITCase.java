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

package org.apache.fluss.flink.source;

import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.metadata.SystemTableConstants;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.collectRowsWithTimeout;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for reading system views via Flink SQL. */
abstract class SystemViewScanITCase extends FlinkTestBase {

    private static final String CATALOG_NAME = "testcatalog";
    private static final int NUM_TABLET_SERVERS = 3;

    private StreamTableEnvironment tEnv;

    @BeforeEach
    void before() {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(execEnv, EnvironmentSettings.inBatchMode());
        tEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tEnv.executeSql("use catalog " + CATALOG_NAME);
    }

    @Test
    void testScanServersView() throws Exception {
        tEnv.useDatabase(SystemTableConstants.SYSTEM_DATABASE);
        CloseableIterator<Row> collected =
                tEnv.executeSql("SELECT * FROM `tablet_servers`").collect();
        List<String> results = collectRowsWithTimeout(collected, NUM_TABLET_SERVERS);

        assertThat(results).hasSize(NUM_TABLET_SERVERS);
        // Each row should contain server_id, endpoints, rack, register_timestamp
        for (String row : results) {
            // Row format: +I[server_id, endpoints, rack, register_timestamp]
            assertThat(row).startsWith("+I[");
        }
    }

    @Test
    void testScanServersViewWithProjection() throws Exception {
        tEnv.useDatabase(SystemTableConstants.SYSTEM_DATABASE);
        CloseableIterator<Row> collected =
                tEnv.executeSql("SELECT server_id, rack FROM `tablet_servers`").collect();
        List<String> results = collectRowsWithTimeout(collected, NUM_TABLET_SERVERS);

        assertThat(results).hasSize(NUM_TABLET_SERVERS);

        List<String> expectedResults = new ArrayList<>();
        expectedResults.add("+I[0, rack-0]");
        expectedResults.add("+I[1, rack-1]");
        expectedResults.add("+I[2, rack-2]");

        assertThat(results).containsExactlyInAnyOrderElementsOf(expectedResults);
    }

    @Test
    void testScanServersViewWithFilter() throws Exception {
        tEnv.useDatabase(SystemTableConstants.SYSTEM_DATABASE);
        CloseableIterator<Row> collected =
                tEnv.executeSql("SELECT * FROM `tablet_servers` WHERE server_id = 0").collect();
        List<String> results = collectRowsWithTimeout(collected, 1);

        assertThat(results).hasSize(1);
        // The returned row should have server_id = 0
        assertThat(results.get(0)).startsWith("+I[0, ");
    }
}

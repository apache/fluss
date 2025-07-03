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

package com.alibaba.fluss.flink.metrics;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.metrics.MetricNames;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.types.DataTypes;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static com.alibaba.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static com.alibaba.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static org.assertj.core.api.Assertions.assertThat;

/** The IT case for fluss reporting metrics to Flink. */
abstract class FlinkMetricsITCase {

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

    private static final int DEFAULT_PARALLELISM = 4;
    private static final InMemoryReporter reporter = InMemoryReporter.createWithRetainedMetrics();

    public static final MiniClusterWithClientResource MINI_CLUSTER_EXTENSION =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .setConfiguration(reporter.addToConfiguration(new Configuration()))
                            .build());

    private static final String CATALOG_NAME = "testcatalog";
    private static final String DEFAULT_DB = "defaultdb";

    protected static Connection conn;
    protected static Admin admin;
    protected static com.alibaba.fluss.config.Configuration clientConf;

    private TableEnvironment tEnv;

    @BeforeAll
    protected static void beforeAll() {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();

        try {
            MINI_CLUSTER_EXTENSION.before();
        } catch (Exception e) {
            throw new FlussRuntimeException("Fail to init Flink mini cluster", e);
        }
    }

    @BeforeEach
    void beforeEach() throws Exception {
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());
        String bootstrapServers = String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        // crate catalog using sql
        tEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tEnv.executeSql("use catalog " + CATALOG_NAME);
        // create database
        tEnv.executeSql("create database " + DEFAULT_DB);
        tEnv.useDatabase(DEFAULT_DB);
    }

    @AfterEach
    void afterEach() {
        tEnv.useDatabase(BUILTIN_DATABASE);
        tEnv.executeSql(String.format("drop database %s cascade", DEFAULT_DB));
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (admin != null) {
            admin.close();
            admin = null;
        }

        if (conn != null) {
            conn.close();
            conn = null;
        }

        MINI_CLUSTER_EXTENSION.after();
    }

    protected long createTable(TablePath tablePath, TableDescriptor tableDescriptor)
            throws Exception {
        admin.createTable(tablePath, tableDescriptor, true).get();
        return admin.getTableInfo(tablePath).get().getTableId();
    }

    @Test
    void testMetricsReport() throws Exception {
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("name", DataTypes.STRING())
                                        .build())
                        .build();
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test");
        createTable(tablePath, tableDescriptor);

        // test write
        TableResult tableResult =
                tEnv.executeSql("insert into test values (1, 'name1'), (2, 'name2'), (3, 'name3')");
        JobClient client = tableResult.getJobClient().get();
        JobID jobID = client.getJobID();
        tableResult.await();

        // fluss client's writer metrics should be registered
        List<Tuple3<MetricGroup, String, Metric>> metricsList =
                reporter.findJobMetricGroups(jobID, MetricNames.WRITER_SEND_LATENCY_MS);
        assertThat(metricsList).hasSize(1);
        Metric sendLatencyMetrics = metricsList.get(0).f2;
        assertThat(sendLatencyMetrics).isInstanceOf(Gauge.class);
        // the default send latency is -1, so check it is >= 0, as the latency maybe very small 0ms
        assertThat((Long) ((Gauge<?>) sendLatencyMetrics).getValue()).isGreaterThanOrEqualTo(0);

        // test scan
        tableResult = tEnv.executeSql("select * from test");
        client = tableResult.getJobClient().get();
        jobID = client.getJobID();
        assertResultsIgnoreOrder(
                tableResult.collect(),
                Arrays.asList("+I[1, name1]", "+I[2, name2]", "+I[3, name3]"),
                true);

        // fluss client's scanner metrics should be registered
        metricsList = reporter.findJobMetricGroups(jobID, MetricNames.SCANNER_BYTES_PER_REQUEST);
        assertThat(metricsList).hasSize(1);
        Metric scannerBytesPerRequest = metricsList.get(0).f2;
        assertThat(scannerBytesPerRequest).isInstanceOf(Histogram.class);
        // just check scanner bytes per request is greater than 0
        assertThat(((Histogram) scannerBytesPerRequest).getCount()).isGreaterThan(0);
    }
}

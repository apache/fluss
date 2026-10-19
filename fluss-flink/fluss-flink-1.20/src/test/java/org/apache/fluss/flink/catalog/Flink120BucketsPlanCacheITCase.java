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

package org.apache.fluss.flink.catalog;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.api.endpoint.EndpointVersion;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.service.SqlGatewayServiceImpl;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.gateway.api.config.SqlGatewayServiceConfigOptions.SQL_GATEWAY_SESSION_PLAN_CACHE_ENABLED;
import static org.apache.flink.table.gateway.service.utils.SqlGatewayServiceTestUtil.awaitOperationTermination;
import static org.apache.flink.table.gateway.service.utils.SqlGatewayServiceTestUtil.fetchAllResults;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests that cached SQL Gateway plans load fresh {@code $buckets} metadata at runtime. */
public class Flink120BucketsPlanCacheITCase {

    private static final String CATALOG_NAME = "testcatalog";
    private static final String DEFAULT_DB = FlinkCatalogOptions.DEFAULT_DATABASE.defaultValue();

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(1).build();

    @RegisterExtension
    @Order(1)
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .build());

    @RegisterExtension
    @Order(2)
    static final SqlGatewayServiceExtension SQL_GATEWAY_SERVICE_EXTENSION =
            new SqlGatewayServiceExtension(MINI_CLUSTER::getClientConfiguration);

    private static SqlGatewayServiceImpl service;

    private SessionHandle sessionHandle;

    @BeforeAll
    static void beforeAll() {
        service = (SqlGatewayServiceImpl) SQL_GATEWAY_SERVICE_EXTENSION.getService();
    }

    @BeforeEach
    void before() {
        SessionEnvironment sessionEnvironment =
                SessionEnvironment.newBuilder()
                        .setSessionEndpointVersion(new EndpointVersion() {})
                        .addSessionConfig(
                                Collections.singletonMap(
                                        SQL_GATEWAY_SESSION_PLAN_CACHE_ENABLED.key(), "true"))
                        .build();
        sessionHandle = service.openSession(sessionEnvironment);

        Configuration flussConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        String bootstrapServers = String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        service.configureSession(
                sessionHandle,
                String.format(
                        "CREATE CATALOG %s WITH ('type' = 'fluss', 'bootstrap.servers' = '%s')",
                        CATALOG_NAME, bootstrapServers),
                -1);
        service.configureSession(sessionHandle, "USE CATALOG " + CATALOG_NAME, -1);
    }

    @AfterEach
    void after() {
        if (sessionHandle != null) {
            service.closeSession(sessionHandle);
        }
    }

    @Test
    void testCachedPlanLoadsFreshBucketMetadata() throws Exception {
        String tableName = "cached_plan_buckets";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        executeUpdate(
                "CREATE TABLE "
                        + tableName
                        + " (id INT, dt STRING) PARTITIONED BY (dt) "
                        + "WITH ('bucket.num' = '2')");

        String query =
                "SELECT partition_name, bucket_id FROM "
                        + tableName
                        + FlinkCatalog.BUCKETS_TABLE_SUFFIX;

        try (Connection connection =
                        ConnectionFactory.createConnection(
                                FLUSS_CLUSTER_EXTENSION.getClientConfig());
                Admin admin = connection.getAdmin()) {
            admin.createPartition(
                            tablePath,
                            new PartitionSpec(Collections.singletonMap("dt", "2026-10-18")),
                            false)
                    .get();
            waitForBucketCount(admin, tablePath, 2);

            List<RowData> initialRows = executeQuery(query);
            assertThat(initialRows).hasSize(2);
            assertThat(initialRows)
                    .extracting(row -> row.getString(0).toString())
                    .containsOnly("2026-10-18");

            admin.createPartition(
                            tablePath,
                            new PartitionSpec(Collections.singletonMap("dt", "2026-10-19")),
                            false)
                    .get();
            waitForBucketCount(admin, tablePath, 4);

            List<RowData> refreshedRows = executeQuery(query);
            assertThat(refreshedRows).hasSize(4);
            assertThat(refreshedRows)
                    .extracting(row -> row.getString(0).toString())
                    .containsExactlyInAnyOrder(
                            "2026-10-18", "2026-10-18", "2026-10-19", "2026-10-19");
        }
    }

    private void executeUpdate(String statement) throws Exception {
        OperationHandle operationHandle =
                service.executeStatement(
                        sessionHandle,
                        statement,
                        -1,
                        new org.apache.flink.configuration.Configuration());
        awaitOperationTermination(service, sessionHandle, operationHandle);
        service.closeOperation(sessionHandle, operationHandle);
    }

    private List<RowData> executeQuery(String statement) throws Exception {
        OperationHandle operationHandle =
                service.executeStatement(
                        sessionHandle,
                        statement,
                        -1,
                        new org.apache.flink.configuration.Configuration());
        awaitOperationTermination(service, sessionHandle, operationHandle);
        List<RowData> results = fetchAllResults(service, sessionHandle, operationHandle);
        service.closeOperation(sessionHandle, operationHandle);
        return results;
    }

    private void waitForBucketCount(Admin admin, TablePath tablePath, int expectedCount) {
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(admin.describeBuckets(tablePath).get()).hasSize(expectedCount));
    }
}

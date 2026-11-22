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

package org.apache.fluss.trino;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IOUtils;

import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedRow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** SQL bucket-pruning fallback tests on an Iceberg cluster. */
class FlussIcebergBucketPruningITCase {

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(clusterConfig())
                    .build();

    private final FlussQueryRunner.TestingHooks gate = new FlussQueryRunner.TestingHooks();

    private DistributedQueryRunner runner;
    private Connection connection;
    private Admin admin;

    @BeforeEach
    void setUp() throws Exception {
        connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
        admin = connection.getAdmin();
        runner = FlussQueryRunner.create(CLUSTER.getBootstrapServers(), gate);
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            waitUntil(
                    () -> gate.activeSources() == 0,
                    Duration.ofSeconds(30),
                    "Trino did not release all page sources");
        } finally {
            IOUtils.closeAll(runner, admin, connection);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testIcebergCompositeBucketKeyFallsBackThroughSql(boolean primaryKey) throws Exception {

        String name = "iceberg_composite_bucket_" + (primaryKey ? "pk" : "log");
        TablePath path = TablePath.of("fluss", name);

        Schema.Builder schema =
                Schema.newBuilder().column("a", DataTypes.INT()).column("b", DataTypes.INT());

        if (primaryKey) {
            // Use a primary key distinct from the composite bucket key.
            // The table is only scanned because Iceberg composite
            // bucket-key writes are not supported.
            schema.column("id", DataTypes.INT()).primaryKey("a", "b", "id");
        }

        admin.createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(schema.build())
                                .distributedBy(4, "a", "b")
                                .property(
                                        ConfigOptions.TABLE_DATALAKE_FORMAT, DataLakeFormat.ICEBERG)
                                .property(ConfigOptions.TABLE_DATALAKE_ENABLED, false)
                                .build(),
                        false)
                .get(30, TimeUnit.SECONDS);

        TableInfo tableInfo = admin.getTableInfo(path).get(30, TimeUnit.SECONDS);

        assertThat(tableInfo.getTableConfig().isDataLakeEnabled()).isFalse();
        assertThat(tableInfo.getTableConfig().getDataLakeFormat()).contains(DataLakeFormat.ICEBERG);
        assertThat(tableInfo.getBucketKeys()).containsExactly("a", "b");
        assertThat(tableInfo.hasPrimaryKey()).isEqualTo(primaryKey);

        for (int bucketId = 0; bucketId < tableInfo.getNumBuckets(); bucketId++) {
            CLUSTER.waitUntilAllReplicaReady(new TableBucket(tableInfo.getTableId(), bucketId));
        }

        assertThat(admin.describeBuckets(path).get(30, TimeUnit.SECONDS)).hasSize(4);

        for (String predicate : Arrays.asList("", " WHERE a = 1", " WHERE a = 1 AND b = 2")) {

            int before = gate.createdSources();

            assertThat(rows("SELECT a, b FROM " + name + predicate)).isEmpty();

            if (primaryKey) {
                // A complete composite bucket-key predicate must
                // not prune this unsupported Iceberg layout.
                assertThat(gate.createdSources() - before).isEqualTo(4);
            } else {
                // Empty log ranges produce no splits.
                // The pruning fallback is tested directly
                // in FlussBucketPrunerTest.
                assertThat(gate.createdSources() - before).isZero();
            }
        }
    }

    private static Configuration clusterConfig() {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.ICEBERG);
        configuration.set(ConfigOptions.DATALAKE_ENABLED, false);
        return configuration;
    }

    private List<List<Object>> rows(String sql) {
        return runner.execute(sql).getMaterializedRows().stream()
                .map(MaterializedRow::getFields)
                .collect(Collectors.toList());
    }
}

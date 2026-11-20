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
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IOUtils;

import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedRow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** SQL bucket-pruning fallback tests on a Hudi cluster. */
class FlussHudiBucketPruningITCase {

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

    @Test
    void testHudiRejectedPredicateFallsBackThroughSql() throws Exception {
        String name = "hudi_bucket_predicate";
        TablePath path = TablePath.of("fluss", name);
        admin.createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("k1", DataTypes.STRING())
                                                .column("k2", DataTypes.STRING())
                                                .column("payload", DataTypes.INT())
                                                .primaryKey("k1", "k2")
                                                .build())
                                .property(ConfigOptions.TABLE_DATALAKE_FORMAT, DataLakeFormat.HUDI)
                                .property(ConfigOptions.TABLE_DATALAKE_ENABLED, false)
                                .distributedBy(4)
                                .build(),
                        false)
                .get(30, TimeUnit.SECONDS);

        TableInfo tableInfo = admin.getTableInfo(path).get(30, TimeUnit.SECONDS);
        for (int bucketId = 0; bucketId < tableInfo.getNumBuckets(); bucketId++) {
            CLUSTER.waitUntilAllReplicaReady(new TableBucket(tableInfo.getTableId(), bucketId));
        }

        try (Table table = connection.getTable(path)) {
            table.newUpsert()
                    .createWriter()
                    .upsert(
                            GenericRow.of(
                                    BinaryString.fromString("valid"),
                                    BinaryString.fromString("x"),
                                    1))
                    .get(30, TimeUnit.SECONDS);
        }

        // Hudi must scan all buckets even with a complete primary-key equality predicate.
        int before = gate.createdSources();
        assertThat(rows("SELECT payload FROM " + name + " WHERE k1 = 'valid' AND k2 = 'x'"))
                .containsExactly(Collections.singletonList(1));
        assertThat(gate.createdSources() - before).isEqualTo(4);
        for (String value : Arrays.asList("a,b", "__null__", "__empty__")) {
            before = gate.createdSources();
            assertThat(rows("SELECT * FROM " + name + " WHERE k1 = '" + value + "' AND k2 = 'x'"))
                    .isEmpty();
            assertThat(gate.createdSources() - before).isEqualTo(4);
        }
    }

    private static Configuration clusterConfig() {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.HUDI);
        configuration.set(ConfigOptions.DATALAKE_ENABLED, false);
        return configuration;
    }

    private List<List<Object>> rows(String sql) {
        return runner.execute(sql).getMaterializedRows().stream()
                .map(MaterializedRow::getFields)
                .collect(Collectors.toList());
    }
}

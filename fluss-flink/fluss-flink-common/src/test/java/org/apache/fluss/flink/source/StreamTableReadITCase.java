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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.catalog.FlinkCatalog;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.collectRowsUntilEndWithTimeout;
import static org.apache.fluss.server.kv.KvTabletTestUtils.flushAndWait;
import static org.apache.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests Fluss-only SQL reads without a lake reader plugin. */
class StreamTableReadITCase extends AbstractTestBase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(
                            new Configuration()
                                    .set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.LANCE))
                    .build();

    private TableEnvironment tEnv;
    private FlinkCatalog catalog;
    private Connection connection;
    private Admin admin;
    private boolean databaseCreated;

    @BeforeEach
    void before() {
        databaseCreated = false;
        Configuration config = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        connection = ConnectionFactory.createConnection(config);
        admin = connection.getAdmin();
        tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.getConfig().set("parallelism.default", "1");
        catalog =
                new FlinkCatalog(
                        "fluss",
                        BUILTIN_DATABASE,
                        String.join(",", config.get(ConfigOptions.BOOTSTRAP_SERVERS)),
                        getClass().getClassLoader(),
                        Collections.emptyMap(),
                        () ->
                                Collections.singletonMap(
                                        "lance.warehouse", "file:///tmp/fluss-test-warehouse"));
        tEnv.registerCatalog("fluss", catalog);
        tEnv.useCatalog("fluss");
        tEnv.executeSql("CREATE DATABASE stream_test");
        databaseCreated = true;
        tEnv.useDatabase("stream_test");
    }

    @AfterEach
    void after() throws Exception {
        try {
            if (databaseCreated) {
                tEnv.useDatabase(BUILTIN_DATABASE);
                tEnv.executeSql("DROP DATABASE stream_test CASCADE");
            }
        } finally {
            catalog.close();
            admin.close();
            connection.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPrimaryKeyBatchRead(boolean lakeEnabled) throws Exception {
        tEnv.executeSql(
                "CREATE TABLE orders (id INT, name STRING, PRIMARY KEY (id) NOT ENFORCED) "
                        + "WITH ('bucket.num' = '1', 'table.datalake.enabled' = '"
                        + lakeEnabled
                        + "')");
        TablePath path = TablePath.of("stream_test", "orders");
        try (Table table = connection.getTable(path)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, "old"));
            writer.upsert(row(1, "new"));
            writer.upsert(row(2, "deleted"));
            writer.delete(row(2, "deleted"));
            writer.upsert(row(3, "three"));
            writer.flush();
        }
        TableBucket bucket = new TableBucket(admin.getTableInfo(path).get().getTableId(), 0);
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshots(Collections.singletonList(bucket));
        assertThat(query("SELECT * FROM `orders$stream`"))
                .containsExactlyInAnyOrder("+I[1, new]", "+I[3, three]");
        assertThat(query("SELECT name FROM `orders$stream` WHERE id = 1"))
                .containsExactly("+I[new]");
        assertThat(query("SELECT * FROM `orders$stream` LIMIT 1"))
                .hasSize(1)
                .allMatch(value -> value.equals("+I[1, new]") || value.equals("+I[3, three]"));
        assertThat(query("SELECT COUNT(*) FROM `orders$stream`")).containsExactly("+I[2]");
        assertThat(
                        query(
                                "SELECT * FROM `orders$stream` "
                                        + "/*+ OPTIONS('client.scanner.kv.batch-strategy' = 'snapshot-merge') */"))
                .containsExactlyInAnyOrder("+I[1, new]", "+I[3, three]");
        assertThat(
                        query(
                                "SELECT * FROM `orders$stream` "
                                        + "/*+ OPTIONS('client.scanner.kv.batch-strategy' = 'server-scan') */"))
                .containsExactlyInAnyOrder("+I[1, new]", "+I[3, three]");
    }

    @Test
    void testBatchReadStrategiesAfterTtlCompaction() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE ttl_orders (id INT, event_time BIGINT, PRIMARY KEY (id) NOT ENFORCED) "
                        + "WITH ('bucket.num' = '1', 'table.datalake.enabled' = 'true', "
                        + "'table.kv.ttl' = '1 h', 'table.kv.ttl.time-column' = 'event_time')");
        TablePath path = TablePath.of("stream_test", "ttl_orders");
        try (Table table = connection.getTable(path)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            // An expired event-time row stays visible until compaction; null never expires.
            writer.upsert(row(1, 0L));
            writer.upsert(row(2, null));
            writer.flush();
        }
        TableBucket bucket = new TableBucket(admin.getTableInfo(path).get().getTableId(), 0);
        // Retain a pre-cleanup snapshot: TTL compaction does not emit changelog deletes.
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshots(Collections.singletonList(bucket));
        assertThat(query("SELECT id FROM `ttl_orders$stream`"))
                .containsExactlyInAnyOrder("+I[1]", "+I[2]");

        KvTablet tablet = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(bucket).getKvTablet();
        flushAndWait(tablet, Long.MAX_VALUE);
        tablet.getRocksDBKv().getDb().compactRange();

        assertThat(query("SELECT id FROM `ttl_orders$stream`"))
                .containsExactlyInAnyOrder("+I[1]", "+I[2]");
        assertThat(
                        query(
                                "SELECT id FROM `ttl_orders$stream` "
                                        + "/*+ OPTIONS('client.scanner.kv.batch-strategy' = 'snapshot-merge', "
                                        + "'table.datalake.enabled' = 'true') */"))
                .containsExactlyInAnyOrder("+I[1]", "+I[2]");
        assertThat(query("SELECT id FROM `ttl_orders$stream` LIMIT 10")).containsExactly("+I[2]");
        assertThat(query("SELECT COUNT(*) FROM `ttl_orders$stream`")).containsExactly("+I[2]");
        assertThat(
                        query(
                                "SELECT id FROM `ttl_orders$stream` "
                                        + "/*+ OPTIONS('client.scanner.kv.batch-strategy' = 'server-scan') */"))
                .containsExactly("+I[2]");
        assertThat(
                        query(
                                "SELECT id FROM `ttl_orders$stream` "
                                        + "/*+ OPTIONS('client.scanner.kv.batch-strategy' = 'server-scan') */ LIMIT 10"))
                .containsExactly("+I[2]");
        assertThat(
                        query(
                                "SELECT COUNT(*) FROM `ttl_orders$stream` "
                                        + "/*+ OPTIONS('client.scanner.kv.batch-strategy' = 'server-scan') */"))
                .containsExactly("+I[1]");
        // Full-primary-key equality uses live lookup regardless of the batch scan strategy.
        assertThat(query("SELECT id FROM `ttl_orders$stream` WHERE id = 1")).isEmpty();
    }

    @Test
    void testPrimaryKeyChangelogInStreamingMode() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE changes (id INT, name STRING, PRIMARY KEY (id) NOT ENFORCED) "
                        + "WITH ('bucket.num' = '1', 'table.datalake.enabled' = 'true')");
        try (Table table = connection.getTable(TablePath.of("stream_test", "changes"))) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, "one"));
            writer.upsert(row(1, "updated"));
            writer.flush();
        }
        useStreamingMode();
        assertThat(
                        query(
                                "SELECT * FROM `changes$stream` "
                                        + "/*+ OPTIONS('scan.startup.mode' = 'earliest', 'scan.bounded.mode' = 'latest-offset') */"))
                .containsExactly("+I[1, one]", "-U[1, one]", "+U[1, updated]");
    }

    @Test
    void testPartitionedBatchRead() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE partitioned_orders (id INT, region STRING, name STRING, "
                        + "PRIMARY KEY (id, region) NOT ENFORCED) PARTITIONED BY (region) "
                        + "WITH ('bucket.num' = '1', 'table.datalake.enabled' = 'true')");
        tEnv.executeSql("ALTER TABLE partitioned_orders ADD PARTITION (region = 'cn')");
        tEnv.executeSql("ALTER TABLE partitioned_orders ADD PARTITION (region = 'us')");
        TablePath path = TablePath.of("stream_test", "partitioned_orders");
        try (Table table = connection.getTable(path)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, "cn", "one"));
            writer.upsert(row(2, "us", "two"));
            writer.flush();
        }
        long tableId = admin.getTableInfo(path).get().getTableId();
        List<TableBucket> buckets = new ArrayList<>();
        for (PartitionInfo partitionInfo : admin.listPartitionInfos(path).get()) {
            buckets.add(new TableBucket(tableId, partitionInfo.getPartitionId(), 0));
        }
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshots(buckets);
        assertThat(query("SELECT name FROM `partitioned_orders$stream` WHERE region = 'cn'"))
                .containsExactly("+I[one]");
        assertThat(
                        query(
                                "SELECT name FROM `partitioned_orders$stream` WHERE region = 'us' LIMIT 1"))
                .containsExactly("+I[two]");
        assertThat(
                        query(
                                "SELECT name FROM `partitioned_orders$stream` WHERE region = 'cn' AND id = 1"))
                .containsExactly("+I[one]");
        assertThat(
                        query(
                                "SELECT * FROM `partitioned_orders$stream` WHERE region = 'missing' AND id = 1"))
                .isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testLogReadInBatchAndStreamingModes(boolean streaming) throws Exception {
        tEnv.executeSql(
                "CREATE TABLE events (id INT, name STRING) "
                        + "WITH ('bucket.num' = '1', 'table.datalake.enabled' = 'true')");
        try (Table table = connection.getTable(TablePath.of("stream_test", "events"))) {
            AppendWriter writer = table.newAppend().createWriter();
            writer.append(row(1, "one"));
            writer.append(row(2, "two"));
            writer.flush();
        }
        if (streaming) {
            useStreamingMode();
        }
        assertThat(
                        query(
                                "SELECT name FROM `events$stream` "
                                        + (streaming
                                                ? "/*+ OPTIONS('scan.bounded.mode' = 'latest-offset') */"
                                                : "")))
                .containsExactlyInAnyOrder("+I[one]", "+I[two]");
        if (!streaming) {
            assertThat(
                            query(
                                    "SELECT * FROM `events$stream` "
                                            + "/*+ OPTIONS('scan.startup.mode' = 'latest') */"))
                    .isEmpty();
        }
    }

    private void useStreamingMode() {
        catalog.close();
        tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.getConfig().set("parallelism.default", "1");
        tEnv.registerCatalog("fluss", catalog);
        tEnv.useCatalog("fluss");
        tEnv.useDatabase("stream_test");
    }

    private List<String> query(String sql) {
        return collectRowsUntilEndWithTimeout(tEnv.executeSql(sql).collect());
    }
}

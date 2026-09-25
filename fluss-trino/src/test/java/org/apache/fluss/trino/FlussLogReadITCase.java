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
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.clock.ManualClock;

import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedRow;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** End-to-end reads through a coordinator and a separate Trino worker. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FlussLogReadITCase {
    private static final ManualClock CLOCK = new ManualClock(System.currentTimeMillis());

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClock(CLOCK)
                    .setClusterConf(clusterConfig())
                    .build();

    private final FlussQueryRunner.TestingHooks gate = new FlussQueryRunner.TestingHooks();
    private DistributedQueryRunner runner;
    private Connection connection;
    private Admin admin;

    @BeforeAll
    void setUp() throws Exception {
        runner = FlussQueryRunner.create(CLUSTER.getBootstrapServers(), gate);
    }

    @BeforeEach
    void openFixtureClient() {
        connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
        admin = connection.getAdmin();
    }

    @AfterAll
    void tearDown() throws Exception {
        IOUtils.closeAll(runner);
    }

    @AfterEach
    void readersAreReleased() throws Exception {
        try {
            waitUntil(
                    () -> gate.activeSources() == 0,
                    Duration.ofSeconds(30),
                    "Trino did not release all page sources");
        } finally {
            IOUtils.closeAll(admin, connection);
        }
    }

    @Test
    void testUserCancellationClosesActiveReader() throws Exception {
        createTable("cancel_events", Schema.newBuilder().column("id", DataTypes.INT()).build(), 1);
        try (Table table = connection.getTable(TablePath.of("fluss", "cancel_events"))) {
            AppendWriter writer = table.newAppend().createWriter();
            List<CompletableFuture<?>> writes = new ArrayList<>();
            for (int i = 0; i < 3000; i++) {
                writes.add(writer.append(GenericRow.of(i)));
            }
            CompletableFuture.allOf(writes.toArray(new CompletableFuture[0]))
                    .get(30, TimeUnit.SECONDS);
        }
        ExecutorService executor = Executors.newSingleThreadExecutor();
        String sql = "SELECT id FROM cancel_events";
        try (FlussQueryRunner.Barrier barrier = gate.pauseAfterRead("cancel_events")) {
            Future<?> query = executor.submit(() -> runner.execute(sql));
            barrier.awaitReached();
            assertThat(gate.activeSources()).isGreaterThan(0);
            io.trino.spi.QueryId queryId =
                    runner.getCoordinator().getQueryManager().getQueries().stream()
                            .filter(
                                    info ->
                                            info.getQuery().equals(sql)
                                                    && !info.getState().isDone())
                            .findFirst()
                            .get()
                            .getQueryId();
            runner.getCoordinator().getQueryManager().cancelQuery(queryId);
            assertThatThrownBy(() -> query.get(30, TimeUnit.SECONDS))
                    .hasStackTraceContaining("canceled");
            waitUntil(
                    () -> gate.activeSources() == 0,
                    Duration.ofSeconds(30),
                    "Canceled reader was not closed");
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void testDistributedLogReads() throws Exception {
        assertThat(runner.getNodeCount()).isEqualTo(2);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();
        createTable("events", schema, 3);
        append(
                "events",
                GenericRow.of(1, BinaryString.fromString("one")),
                GenericRow.of(2, BinaryString.fromString("二")),
                GenericRow.of(3, null));
        assertThat(rows("SELECT * FROM events"))
                .containsExactlyInAnyOrder(
                        Arrays.asList(1, "one"), Arrays.asList(2, "二"), Arrays.asList(3, null));
        assertThat(rows("SELECT name, id FROM events WHERE id >= 2 ORDER BY id DESC"))
                .containsExactly(Arrays.asList(null, 3), Arrays.asList("二", 2));
        assertThat(runner.execute("SELECT count(*) FROM events").getOnlyValue()).isEqualTo(3L);
        assertThat(rows("SELECT id FROM events ORDER BY id LIMIT 1"))
                .containsExactly(Arrays.asList(1));
        assertThat(rows("SELECT * FROM events LIMIT 1")).hasSize(1);
        assertThat(rows("DESCRIBE events")).hasSize(2);
        assertThat(runner.execute("SHOW CREATE TABLE events").getOnlyValue().toString())
                .contains("CREATE TABLE", "events");
        assertThat(rows("SELECT * FROM \"events$columns\"")).hasSize(2);
    }

    @Test
    void testEmptyTable() throws Exception {
        createTable("empty_events", Schema.newBuilder().column("id", DataTypes.INT()).build(), 3);
        assertThat(rows("SELECT * FROM empty_events")).isEmpty();
        assertThat(runner.execute("SELECT count(*) FROM empty_events").getOnlyValue())
                .isEqualTo(0L);
    }

    @Test
    void testAppendsAfterPlanningAreExcluded() throws Exception {
        createTable("bounded_events", Schema.newBuilder().column("id", DataTypes.INT()).build(), 3);
        append("bounded_events", GenericRow.of(1), GenericRow.of(2));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (FlussQueryRunner.Barrier barrier = gate.pause("bounded_events")) {
            Future<List<List<Object>>> query =
                    executor.submit(() -> rows("SELECT id FROM bounded_events"));
            barrier.awaitPlanned();
            append("bounded_events", GenericRow.of(3), GenericRow.of(4));
            barrier.close();
            assertThat(query.get(30, TimeUnit.SECONDS))
                    .containsExactlyInAnyOrder(Arrays.asList(1), Arrays.asList(2));
            assertThat(runner.execute("SELECT count(*) FROM bounded_events").getOnlyValue())
                    .isEqualTo(4L);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void testAllMappedTypesThroughSql() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("c", DataTypes.CHAR(4))
                        .column("short_decimal", DataTypes.DECIMAL(18, 2))
                        .column("long_decimal", DataTypes.DECIMAL(38, 9))
                        .column("d", DataTypes.DATE())
                        .column("t", DataTypes.TIME(3))
                        .column("ts", DataTypes.TIMESTAMP(6))
                        .column("ts_nanos", DataTypes.TIMESTAMP(9))
                        .column("ltz", DataTypes.TIMESTAMP_LTZ(3))
                        .column("ltz_nanos", DataTypes.TIMESTAMP_LTZ(9))
                        .column("items", DataTypes.ARRAY(DataTypes.INT()))
                        .column("mapping", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .column(
                                "nested",
                                DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.INT()),
                                        DataTypes.FIELD("name", DataTypes.STRING())))
                        .column("binary_value", DataTypes.BINARY(2))
                        .column("bytes_value", DataTypes.BYTES())
                        .build();
        admin.createTable(
                        TablePath.of("fluss", "all_types"),
                        TableDescriptor.builder()
                                .schema(schema)
                                .distributedBy(1)
                                .logFormat(LogFormat.INDEXED)
                                .build(),
                        false)
                .get(30, TimeUnit.SECONDS);
        append(
                "all_types",
                GenericRow.of(
                        BinaryString.fromString("中 "),
                        Decimal.fromBigDecimal(new BigDecimal("-9999999999999999.99"), 18, 2),
                        Decimal.fromBigDecimal(
                                new BigDecimal("12345678901234567890123456789.123456789"), 38, 9),
                        -1,
                        86399999,
                        TimestampNtz.fromMillis(-1, 999000),
                        TimestampNtz.fromMillis(-1, 999999),
                        TimestampLtz.fromEpochMillis(-1),
                        TimestampLtz.fromEpochMillis(-1, 999999),
                        GenericArray.of(1, null, 3),
                        new GenericMap(
                                Collections.singletonMap(BinaryString.fromString("k"), null)),
                        GenericRow.of(7, BinaryString.fromString("世界")),
                        new byte[] {0, (byte) 255},
                        new byte[] {1, 2}));
        String expected =
                "VALUES (CAST('中' AS CHAR(4)), DECIMAL '-9999999999999999.99', "
                        + "DECIMAL '12345678901234567890123456789.123456789', DATE '1969-12-31', TIME '23:59:59.999', "
                        + "TIMESTAMP '1969-12-31 23:59:59.999999', TIMESTAMP '1969-12-31 23:59:59.999999999', "
                        + "TIMESTAMP '1969-12-31 23:59:59.999 UTC', TIMESTAMP '1969-12-31 23:59:59.999999999 UTC', "
                        + "ARRAY[1, NULL, 3], MAP(ARRAY['k'], ARRAY[CAST(NULL AS INTEGER)]), ROW(7, '世界'), X'00FF', X'0102')";

        assertThat(runner.execute("SELECT * FROM all_types").getMaterializedRows())
                .containsExactlyElementsOf(runner.execute(expected).getMaterializedRows());
        append("all_types", new GenericRow(schema.getColumns().size()));
        assertThat(
                        runner.execute(
                                        "SELECT count(*) FROM all_types WHERE c IS NULL AND items IS NULL AND nested IS NULL")
                                .getOnlyValue())
                .isEqualTo(1L);
        assertThat(
                        runner.execute(
                                        io.trino.Session.builder(runner.getDefaultSession())
                                                .setTimeZoneKey(
                                                        io.trino.spi.type.TimeZoneKey
                                                                .getTimeZoneKey("Asia/Shanghai"))
                                                .build(),
                                        "SELECT to_unixtime(ltz), CAST(ts AS VARCHAR) FROM all_types")
                                .getMaterializedRows())
                .isEqualTo(
                        runner.execute(
                                        "SELECT to_unixtime(ltz), CAST(ts AS VARCHAR) FROM all_types")
                                .getMaterializedRows());
    }

    @Test
    void testUnsupportedTablesKeepMetadataAvailable() throws Exception {
        Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
        admin.createTable(
                        TablePath.of("fluss", "partitioned"),
                        TableDescriptor.builder()
                                .schema(schema)
                                .partitionedBy("id")
                                .distributedBy(1)
                                .build(),
                        false)
                .get(30, TimeUnit.SECONDS);
        admin.createTable(
                        TablePath.of("fluss", "primary_key"),
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT())
                                                .primaryKey("id")
                                                .build())
                                .distributedBy(1)
                                .build(),
                        false)
                .get(30, TimeUnit.SECONDS);
        admin.createTable(
                        TablePath.of("fluss", "lakehouse"),
                        TableDescriptor.builder()
                                .schema(schema)
                                .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                                .distributedBy(1)
                                .build(),
                        false)
                .get(30, TimeUnit.SECONDS);
        for (String name : Arrays.asList("partitioned", "primary_key", "lakehouse")) {
            assertThat(rows("DESCRIBE " + name)).hasSize(1);
            assertThat(rows("SHOW CREATE TABLE " + name)).hasSize(1);
            assertThat(rows("SELECT * FROM \"" + name + "$columns\"")).hasSize(1);
            assertThatThrownBy(() -> runner.execute("SELECT * FROM " + name))
                    .hasMessageContaining("not supported");
        }
    }

    @Test
    void testContinuousAppendsDoNotExtendQuery() throws Exception {
        createTable(
                "continuous_events", Schema.newBuilder().column("id", DataTypes.INT()).build(), 1);
        append("continuous_events", GenericRow.of(1));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        AtomicBoolean writing = new AtomicBoolean(true);
        CountDownLatch appended = new CountDownLatch(1);
        Future<?> writer = null;
        try (FlussQueryRunner.Barrier barrier = gate.pause("continuous_events")) {
            Future<List<List<Object>>> query =
                    executor.submit(() -> rows("SELECT id FROM continuous_events"));
            barrier.awaitPlanned();
            writer =
                    executor.submit(
                            () -> {
                                try (Table table =
                                        connection.getTable(
                                                TablePath.of("fluss", "continuous_events"))) {
                                    AppendWriter appender = table.newAppend().createWriter();
                                    while (writing.get()) {
                                        appender.append(GenericRow.of(2)).get(30, TimeUnit.SECONDS);
                                        appended.countDown();
                                    }
                                }
                                return null;
                            });
            assertThat(appended.await(30, TimeUnit.SECONDS)).isTrue();
            barrier.close();
            assertThat(query.get(30, TimeUnit.SECONDS)).containsExactly(Arrays.asList(1));
            assertThat(writer.isDone()).isFalse();
        } finally {
            writing.set(false);
            try {
                if (writer != null) {
                    writer.get(30, TimeUnit.SECONDS);
                }
            } finally {
                executor.shutdownNow();
                assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @Test
    void testExpiredPlannedStartFails() throws Exception {
        TablePath path = TablePath.of("fluss", "retained_events");
        admin.createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                                .distributedBy(1)
                                .property(ConfigOptions.TABLE_LOG_TTL, Duration.ofSeconds(1))
                                .build(),
                        false)
                .get(30, TimeUnit.SECONDS);
        append("retained_events", GenericRow.of(1));
        LogTablet log =
                CLUSTER.waitAndGetLeaderReplica(
                                new TableBucket(
                                        admin.getTableInfo(path)
                                                .get(30, TimeUnit.SECONDS)
                                                .getTableId(),
                                        0))
                        .getLogTablet();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (FlussQueryRunner.Barrier barrier = gate.pause("retained_events")) {
            Future<List<List<Object>>> query =
                    executor.submit(() -> rows("SELECT * FROM retained_events"));
            barrier.awaitPlanned();
            log.roll(Optional.empty());
            CLOCK.advanceTime(Duration.ofSeconds(2));
            log.deleteExpiredSegments();
            assertThat(log.logStartOffset()).isGreaterThan(0);
            barrier.close();
            assertThatThrownBy(() -> query.get(30, TimeUnit.SECONDS))
                    .hasStackTraceContaining("OffsetOutOfRange");
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void testReplacementAfterPlanningFails() throws Exception {
        Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
        createTable("replaced_events", schema, 1);
        append("replaced_events", GenericRow.of(1));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (FlussQueryRunner.Barrier barrier = gate.pause("replaced_events")) {
            Future<List<List<Object>>> query =
                    executor.submit(() -> rows("SELECT * FROM replaced_events"));
            barrier.awaitPlanned();
            admin.dropTable(TablePath.of("fluss", "replaced_events"), false)
                    .get(30, TimeUnit.SECONDS);
            createTable("replaced_events", schema, 1);
            append("replaced_events", GenericRow.of(2));
            barrier.close();
            assertThatThrownBy(() -> query.get(30, TimeUnit.SECONDS))
                    .hasStackTraceContaining("changed during query planning");
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void testSchemaChangeAfterPlanningFails() throws Exception {
        createTable(
                "evolving_events", Schema.newBuilder().column("id", DataTypes.INT()).build(), 1);
        append("evolving_events", GenericRow.of(1));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (FlussQueryRunner.Barrier barrier = gate.pause("evolving_events")) {
            Future<?> query =
                    executor.submit(() -> runner.execute("SELECT * FROM evolving_events"));
            barrier.awaitPlanned();
            admin.alterTable(
                            TablePath.of("fluss", "evolving_events"),
                            Collections.singletonList(
                                    TableChange.addColumn(
                                            "name",
                                            DataTypes.STRING(),
                                            null,
                                            TableChange.ColumnPosition.last())),
                            false)
                    .get(30, TimeUnit.SECONDS);
            barrier.close();
            assertThatThrownBy(() -> query.get(30, TimeUnit.SECONDS))
                    .hasStackTraceContaining("changed during query planning");
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void testTabletRestartAfterPlanning() throws Exception {
        createTable("restart_events", Schema.newBuilder().column("id", DataTypes.INT()).build(), 1);
        append("restart_events", GenericRow.of(1), GenericRow.of(2));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (FlussQueryRunner.Barrier barrier = gate.pause("restart_events")) {
            Future<List<List<Object>>> query =
                    executor.submit(() -> rows("SELECT * FROM restart_events"));
            barrier.awaitPlanned();
            CLUSTER.restartTabletServer(0, new Configuration());
            barrier.close();
            assertThat(query.get(30, TimeUnit.SECONDS))
                    .containsExactlyInAnyOrder(Arrays.asList(1), Arrays.asList(2));
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void testLimitReleasesPartiallyConsumedReader() throws Exception {
        createTable("limit_events", Schema.newBuilder().column("id", DataTypes.INT()).build(), 1);
        try (Table table = connection.getTable(TablePath.of("fluss", "limit_events"))) {
            AppendWriter writer = table.newAppend().createWriter();
            List<CompletableFuture<?>> writes = new ArrayList<>();
            for (int i = 0; i < 3000; i++) {
                writes.add(writer.append(GenericRow.of(i)));
            }
            CompletableFuture.allOf(writes.toArray(new CompletableFuture[0]))
                    .get(30, TimeUnit.SECONDS);
        }
        int created = gate.createdSources();
        assertThat(rows("SELECT id FROM limit_events LIMIT 1")).hasSize(1);
        assertThat(gate.createdSources()).isGreaterThan(created);
        waitUntil(
                () -> gate.activeSources() == 0,
                Duration.ofSeconds(30),
                "LIMIT did not release its reader");
    }

    @Test
    void testPrimitiveValuesThroughSql() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("b", DataTypes.BOOLEAN())
                        .column("tiny", DataTypes.TINYINT())
                        .column("small", DataTypes.SMALLINT())
                        .column("big", DataTypes.BIGINT())
                        .column("f", DataTypes.FLOAT())
                        .column("d", DataTypes.DOUBLE())
                        .build();
        createTable("primitives", schema, 1);
        append(
                "primitives",
                GenericRow.of(
                        true,
                        Byte.MIN_VALUE,
                        Short.MAX_VALUE,
                        Long.MAX_VALUE,
                        Float.NaN,
                        Double.NEGATIVE_INFINITY));
        assertThat(rows("SELECT * FROM primitives"))
                .containsExactlyElementsOf(
                        rows(
                                "VALUES (true, TINYINT '-128', SMALLINT '32767', BIGINT '9223372036854775807', CAST(nan() AS REAL), -infinity())"));
    }

    @Test
    void testMultipleNonemptyBucketsAndEmptyBuckets() throws Exception {
        TablePath path = TablePath.of("fluss", "bucket_events");
        admin.createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                                .distributedBy(3, "id")
                                .build(),
                        false)
                .get(30, TimeUnit.SECONDS);
        append("bucket_events", GenericRow.of(0));
        assertThat(rows("SELECT * FROM bucket_events")).containsExactly(Arrays.asList(0));
        try (Table table = connection.getTable(path)) {
            AppendWriter writer = table.newAppend().createWriter();
            List<CompletableFuture<?>> writes = new ArrayList<>();
            for (int i = 1; i < 300; i++) {
                writes.add(writer.append(GenericRow.of(i)));
            }
            CompletableFuture.allOf(writes.toArray(new CompletableFuture[0]))
                    .get(30, TimeUnit.SECONDS);
        }
        assertThat(
                        admin.listOffsets(
                                        path,
                                        Arrays.asList(0, 1, 2),
                                        new org.apache.fluss.client.admin.OffsetSpec.LatestSpec())
                                .all()
                                .get(30, TimeUnit.SECONDS)
                                .values())
                .allSatisfy(offset -> assertThat(offset).isGreaterThan(0));
        assertThat(rows("SELECT count(*), sum(id) FROM bucket_events"))
                .containsExactly(Arrays.asList(300L, 44850L));
        assertThat(rows("SELECT id FROM bucket_events")).hasSize(300);
    }

    @Test
    void testNonpartitionedTopologyCannotChangeAfterPlanning() throws Exception {
        createTable(
                "topology_events", Schema.newBuilder().column("id", DataTypes.INT()).build(), 1);
        append("topology_events", GenericRow.of(1));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (FlussQueryRunner.Barrier barrier = gate.pause("topology_events")) {
            Future<List<List<Object>>> query =
                    executor.submit(() -> rows("SELECT * FROM topology_events"));
            barrier.awaitPlanned();
            assertThatThrownBy(
                            () ->
                                    admin.alterTable(
                                                    TablePath.of("fluss", "topology_events"),
                                                    Collections.singletonList(
                                                            TableChange.modifyBucketCount(2)),
                                                    false)
                                            .get(30, TimeUnit.SECONDS))
                    .hasStackTraceContaining("Non-partitioned table rescale is not yet supported");
            barrier.close();
            assertThat(query.get(30, TimeUnit.SECONDS)).containsExactly(Arrays.asList(1));
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static Configuration clusterConfig() {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ZERO);
        configuration.set(
                ConfigOptions.DATALAKE_FORMAT, org.apache.fluss.metadata.DataLakeFormat.PAIMON);
        return configuration;
    }

    //    private void createTable(String name, Schema schema, int buckets) throws Exception {
    //        admin.createTable(
    //                        TablePath.of("fluss", name),
    //
    // TableDescriptor.builder().schema(schema).distributedBy(buckets).build(),
    //                        false)
    //                .get(30, TimeUnit.SECONDS);
    //    }

    private void createTable(String name, Schema schema, int buckets) throws Exception {
        TablePath tablePath = TablePath.of("fluss", name);

        admin.createTable(
                        tablePath,
                        TableDescriptor.builder().schema(schema).distributedBy(buckets).build(),
                        false)
                .get(30, TimeUnit.SECONDS);

        long tableId = admin.getTableInfo(tablePath).get(30, TimeUnit.SECONDS).getTableId();

        CLUSTER.waitUntilTableReady(tableId);
    }

    private void append(String name, InternalRow... rows) throws Exception {
        try (Table table = connection.getTable(TablePath.of("fluss", name))) {
            AppendWriter writer = table.newAppend().createWriter();
            for (InternalRow row : rows) {
                writer.append(row).get(30, TimeUnit.SECONDS);
            }
        }
    }

    private List<List<Object>> rows(String sql) {
        return runner.execute(sql).getMaterializedRows().stream()
                .map(MaterializedRow::getFields)
                .collect(Collectors.toList());
    }
}

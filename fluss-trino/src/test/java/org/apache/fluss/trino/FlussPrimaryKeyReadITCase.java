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
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
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
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IOUtils;

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

/** Current-state SQL reads using the production connector on a separate Trino worker. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FlussPrimaryKeyReadITCase {
    private static final int SNAPSHOT_ROWS = 3000;
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(clusterConfig())
                    .build();

    private final FlussQueryRunner.TestingHooks gate = new FlussQueryRunner.TestingHooks();
    private final List<TestingKvScanControl> scanners = new ArrayList<>();
    private DistributedQueryRunner runner;
    private Connection connection;
    private Admin admin;

    @BeforeAll
    void setUp() throws Exception {
        runner = FlussQueryRunner.create(CLUSTER.getBootstrapServers(), gate);
    }

    @BeforeEach
    void openClient() {
        connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
        admin = connection.getAdmin();
    }

    @AfterEach
    void readersAreReleased() throws Exception {
        try {
            waitUntil(
                    () -> gate.activeSources() == 0,
                    Duration.ofSeconds(30),
                    "Trino did not release its KV sources");
            for (TestingKvScanControl control : scanners) {
                waitUntil(
                        () -> control.activeScannerCount() == 0,
                        Duration.ofSeconds(30),
                        "Fluss did not release its KV sessions");
            }
        } finally {
            scanners.clear();
            IOUtils.closeAll(admin, connection);
        }
    }

    @AfterAll
    void tearDown() throws Exception {
        IOUtils.closeAll(runner);
    }

    @Test
    void testCurrentStateAndSqlOperations() throws Exception {
        assertThat(runner.getNodeCount()).isEqualTo(2);
        createTable("current_state", SCHEMA, 3);
        upsert("current_state", row(1, "one"), row(2, "two"), row(3, "three"));
        assertThat(rows("SELECT * FROM current_state"))
                .containsExactlyInAnyOrder(
                        Arrays.asList(1, "one"),
                        Arrays.asList(2, "two"),
                        Arrays.asList(3, "three"));
        upsert("current_state", row(1, "updated"), row(4, null));
        delete("current_state", row(2, null));
        assertThat(rows("SELECT * FROM current_state"))
                .containsExactlyInAnyOrder(
                        Arrays.asList(1, "updated"),
                        Arrays.asList(3, "three"),
                        Arrays.asList(4, null));
        assertThat(rows("SELECT name, id FROM current_state WHERE id >= 3 ORDER BY id DESC"))
                .containsExactly(Arrays.asList(null, 4), Arrays.asList("three", 3));
        assertThat(runner.execute("SELECT count(*) FROM current_state").getOnlyValue())
                .isEqualTo(3L);
        assertThat(rows("SELECT id FROM current_state ORDER BY id LIMIT 1"))
                .containsExactly(Arrays.asList(1));
        assertThat(rows("DESCRIBE current_state")).hasSize(2);
        assertThat(runner.execute("SHOW CREATE TABLE current_state").getOnlyValue().toString())
                .contains("primary_key =");
        assertThat(
                        runner.execute(
                                        "SELECT primary_key_position FROM \"current_state$columns\" WHERE column_name = 'id'")
                                .getOnlyValue())
                .isEqualTo(1L);
    }

    @Test
    void testCompositeKeysAndEmptyBuckets() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("tenant", DataTypes.INT())
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("tenant", "id")
                        .build();
        createTable("composite_state", schema, 3);
        assertThat(rows("SELECT * FROM composite_state")).isEmpty();
        assertThat(runner.execute("SELECT count(*) FROM composite_state").getOnlyValue())
                .isEqualTo(0L);
        upsert("composite_state", GenericRow.of(1, 1, BinaryString.fromString("first")));
        assertThat(rows("SELECT * FROM composite_state"))
                .containsExactly(Arrays.asList(1, 1, "first"));
        try (Table table = connection.getTable(path("composite_state"))) {
            UpsertWriter writer = table.newUpsert().createWriter();
            List<CompletableFuture<?>> writes = new ArrayList<>();
            for (int i = 0; i < 300; i++) {
                writes.add(
                        writer.upsert(GenericRow.of(i % 2, i, BinaryString.fromString("value"))));
            }
            CompletableFuture.allOf(writes.toArray(new CompletableFuture[0]))
                    .get(30, TimeUnit.SECONDS);
        }
        assertThat(
                        admin.listOffsets(
                                        path("composite_state"),
                                        Arrays.asList(0, 1, 2),
                                        new org.apache.fluss.client.admin.OffsetSpec.LatestSpec())
                                .all()
                                .get(30, TimeUnit.SECONDS)
                                .values())
                .allSatisfy(offset -> assertThat(offset).isGreaterThan(0));
        upsert("composite_state", GenericRow.of(0, 1, BinaryString.fromString("other tenant")));
        delete("composite_state", GenericRow.of(1, 1, null));
        assertThat(rows("SELECT tenant, id, name FROM composite_state WHERE id = 1"))
                .containsExactly(Arrays.asList(0, 1, "other tenant"));
        assertThat(rows("SELECT count(*), sum(id) FROM composite_state"))
                .containsExactly(Arrays.asList(300L, 44850L));
    }

    @Test
    void testSnapshotOpensAfterPlanning() throws Exception {
        createTable("open_boundary", SCHEMA, 1);
        upsert("open_boundary", row(1, "old"), row(2, "delete"));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (FlussQueryRunner.Barrier barrier = gate.pause("open_boundary")) {
            Future<List<List<Object>>> query =
                    executor.submit(() -> rows("SELECT * FROM open_boundary"));
            barrier.awaitPlanned();
            upsert("open_boundary", row(1, "new"), row(3, "insert"));
            delete("open_boundary", row(2, null));
            barrier.close();
            assertThat(query.get(30, TimeUnit.SECONDS))
                    .containsExactlyInAnyOrder(Arrays.asList(1, "new"), Arrays.asList(3, "insert"));
        } finally {
            shutdown(executor);
        }
    }

    @Test
    void testOpenedSnapshotSurvivesInsertsUpdatesAndDeletes() throws Exception {
        TestingKvScanControl control = createLargeTable("snapshot_state");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (FlussQueryRunner.Barrier barrier = gate.pauseAfterRead("snapshot_state")) {
            Future<List<List<Object>>> query =
                    executor.submit(() -> rows("SELECT * FROM snapshot_state"));
            barrier.awaitReached();
            assertThat(control.activeScannerCount()).isEqualTo(1);
            upsert("snapshot_state", row(0, "new"), row(SNAPSHOT_ROWS, "inserted"));
            delete("snapshot_state", row(SNAPSHOT_ROWS - 1, null));
            barrier.close();
            assertThat(query.get(30, TimeUnit.SECONDS))
                    .containsExactlyInAnyOrderElementsOf(expectedSnapshot());
            assertThat(rows("SELECT * FROM snapshot_state WHERE id IN (0, 2999, 3000)"))
                    .containsExactlyInAnyOrder(
                            Arrays.asList(0, "new"), Arrays.asList(SNAPSHOT_ROWS, "inserted"));
        } finally {
            shutdown(executor);
        }
    }

    @Test
    void testContinuingWritesDoNotExtendSnapshot() throws Exception {
        TestingKvScanControl control = createLargeTable("continuous_state");
        ExecutorService executor = Executors.newFixedThreadPool(2);
        AtomicBoolean writing = new AtomicBoolean(true);
        CountDownLatch written = new CountDownLatch(1);
        Future<?> writerTask = null;
        try (FlussQueryRunner.Barrier barrier = gate.pauseAfterRead("continuous_state")) {
            Future<List<List<Object>>> query =
                    executor.submit(() -> rows("SELECT * FROM continuous_state"));
            barrier.awaitReached();
            assertThat(control.activeScannerCount()).isEqualTo(1);
            writerTask =
                    executor.submit(
                            () -> {
                                try (Table table = connection.getTable(path("continuous_state"))) {
                                    UpsertWriter writer = table.newUpsert().createWriter();
                                    int id = SNAPSHOT_ROWS;
                                    while (writing.get()) {
                                        writer.upsert(row(id++, "new")).get(30, TimeUnit.SECONDS);
                                        written.countDown();
                                    }
                                }
                                return null;
                            });
            assertThat(written.await(30, TimeUnit.SECONDS)).isTrue();
            barrier.close();
            assertThat(query.get(30, TimeUnit.SECONDS))
                    .containsExactlyInAnyOrderElementsOf(expectedSnapshot());
            assertThat(writerTask.isDone()).isFalse();
        } finally {
            writing.set(false);
            try {
                if (writerTask != null) {
                    writerTask.get(30, TimeUnit.SECONDS);
                }
            } finally {
                shutdown(executor);
            }
        }
    }

    @Test
    void testLimitClosesRemoteSession() throws Exception {
        TestingKvScanControl control = createLargeTable("limit_state");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (FlussQueryRunner.Barrier barrier = gate.pauseAfterRead("limit_state")) {
            Future<List<List<Object>>> query =
                    executor.submit(() -> rows("SELECT * FROM limit_state LIMIT 1"));
            barrier.awaitReached();
            assertThat(control.activeScannerCount()).isEqualTo(1);
            assertThat(gate.activeSources()).isGreaterThan(0);
            barrier.close();
            assertThat(query.get(30, TimeUnit.SECONDS)).hasSize(1);
            awaitClosed(control);
        } finally {
            shutdown(executor);
        }
    }

    @Test
    void testCancellationClosesRemoteSession() throws Exception {
        TestingKvScanControl control = createLargeTable("cancel_state");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        String sql = "SELECT * FROM cancel_state";
        try (FlussQueryRunner.Barrier barrier = gate.pauseAfterRead("cancel_state")) {
            Future<?> query = executor.submit(() -> runner.execute(sql));
            barrier.awaitReached();
            assertThat(control.activeScannerCount()).isEqualTo(1);
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
            barrier.close();
            awaitClosed(control);
        } finally {
            shutdown(executor);
        }
    }

    @Test
    void testExpiredSessionFailsWithoutReopeningSnapshot() throws Exception {
        assertInvalidSessionFails("expired_state", true);
    }

    @Test
    void testUnknownSessionFailsWithoutReopeningSnapshot() throws Exception {
        assertInvalidSessionFails("unknown_state", false);
    }

    private void assertInvalidSessionFails(String name, boolean expire) throws Exception {
        TestingKvScanControl control = createLargeTable(name);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (FlussQueryRunner.Barrier barrier = gate.pauseAfterRead(name)) {
            Future<?> query = executor.submit(() -> runner.execute("SELECT * FROM " + name));
            barrier.awaitReached();
            assertThat(control.scannerIds()).hasSize(1);
            control.awaitCallSequence(1);
            if (expire) {
                control.expireAll();
            } else {
                control.removeAll();
            }
            assertThat(control.activeScannerCount()).isZero();
            barrier.close();
            assertThatThrownBy(() -> query.get(30, TimeUnit.SECONDS))
                    .hasStackTraceContaining("scanner");
            awaitClosed(control);
        } finally {
            shutdown(executor);
        }
    }

    @Test
    void testHistoricalSchemaValuesAreMapped() throws Exception {
        createTable("schema_history", SCHEMA, 1);
        upsert("schema_history", row(1, "old"));
        addColumn("schema_history");
        upsert("schema_history", GenericRow.of(2, BinaryString.fromString("new"), 20));
        assertThat(rows("SELECT * FROM schema_history"))
                .containsExactlyInAnyOrder(
                        Arrays.asList(1, "old", null), Arrays.asList(2, "new", 20));
    }

    @Test
    void testSchemaChangeAfterPlanningFails() throws Exception {
        createTable("schema_planned", SCHEMA, 1);
        upsert("schema_planned", row(1, "old"));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (FlussQueryRunner.Barrier barrier = gate.pause("schema_planned")) {
            Future<?> query = executor.submit(() -> runner.execute("SELECT * FROM schema_planned"));
            barrier.awaitPlanned();
            addColumn("schema_planned");
            barrier.close();
            assertThatThrownBy(() -> query.get(30, TimeUnit.SECONDS))
                    .hasStackTraceContaining("changed during query planning");
        } finally {
            shutdown(executor);
        }
    }

    @Test
    void testSchemaChangeAfterOpeningPreservesTargetSchema() throws Exception {
        TestingKvScanControl control = createLargeTable("schema_opened");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (FlussQueryRunner.Barrier barrier = gate.pauseAfterRead("schema_opened")) {
            Future<List<List<Object>>> query =
                    executor.submit(() -> rows("SELECT * FROM schema_opened"));
            barrier.awaitReached();
            assertThat(control.activeScannerCount()).isEqualTo(1);
            addColumn("schema_opened");
            upsert(
                    "schema_opened",
                    GenericRow.of(SNAPSHOT_ROWS, BinaryString.fromString("new"), 20));
            barrier.close();
            assertThat(query.get(30, TimeUnit.SECONDS))
                    .containsExactlyInAnyOrderElementsOf(expectedSnapshot());
            assertThat(rows("SELECT * FROM schema_opened WHERE id = 3000"))
                    .containsExactly(Arrays.asList(SNAPSHOT_ROWS, "new", 20));
        } finally {
            shutdown(executor);
        }
    }

    @Test
    void testReplacementAfterPlanningFails() throws Exception {
        createTable("replacement_planned", SCHEMA, 1);
        upsert("replacement_planned", row(1, "old"));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (FlussQueryRunner.Barrier barrier = gate.pause("replacement_planned")) {
            Future<?> query =
                    executor.submit(() -> runner.execute("SELECT * FROM replacement_planned"));
            barrier.awaitPlanned();
            admin.dropTable(path("replacement_planned"), false).get(30, TimeUnit.SECONDS);
            createTable("replacement_planned", SCHEMA, 1);
            upsert("replacement_planned", row(2, "replacement"));
            barrier.close();
            assertThatThrownBy(() -> query.get(30, TimeUnit.SECONDS))
                    .hasStackTraceContaining("changed during query planning");
        } finally {
            shutdown(executor);
        }
    }

    @Test
    void testReplacementAfterOpeningFailsWithoutReadingReplacement() throws Exception {
        TestingKvScanControl control = createLargeTable("replacement_opened");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (FlussQueryRunner.Barrier barrier = gate.pauseAfterRead("replacement_opened")) {
            Future<?> query =
                    executor.submit(() -> runner.execute("SELECT * FROM replacement_opened"));
            barrier.awaitReached();
            assertThat(control.activeScannerCount()).isEqualTo(1);
            admin.dropTable(path("replacement_opened"), false).get(30, TimeUnit.SECONDS);
            createTable("replacement_opened", SCHEMA, 1);
            upsert("replacement_opened", row(SNAPSHOT_ROWS, "replacement"));
            barrier.close();
            assertThatThrownBy(() -> query.get(30, TimeUnit.SECONDS))
                    .hasStackTraceContaining("Failed reading Fluss KV snapshot");
            assertThat(rows("SELECT * FROM replacement_opened"))
                    .containsExactly(Arrays.asList(SNAPSHOT_ROWS, "replacement"));
        } finally {
            shutdown(executor);
        }
    }

    @Test
    void testNonpartitionedRescaleRemainsUnsupported() throws Exception {
        createTable("rescale_state", SCHEMA, 1);
        upsert("rescale_state", row(1, "value"));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (FlussQueryRunner.Barrier barrier = gate.pause("rescale_state")) {
            Future<List<List<Object>>> query =
                    executor.submit(() -> rows("SELECT * FROM rescale_state"));
            barrier.awaitPlanned();
            assertThatThrownBy(
                            () ->
                                    admin.alterTable(
                                                    path("rescale_state"),
                                                    Collections.singletonList(
                                                            TableChange.modifyBucketCount(2)),
                                                    false)
                                            .get(30, TimeUnit.SECONDS))
                    .hasStackTraceContaining("Non-partitioned table rescale is not yet supported");
            barrier.close();
            assertThat(query.get(30, TimeUnit.SECONDS)).containsExactly(Arrays.asList(1, "value"));
        } finally {
            shutdown(executor);
        }
    }

    @Test
    void testAllMappedTypesThroughSql() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
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
                        .primaryKey("id")
                        .build();
        createTable("all_types", schema, 1);
        upsert(
                "all_types",
                GenericRow.of(
                        1,
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
                "VALUES (1, CAST('中' AS CHAR(4)), DECIMAL '-9999999999999999.99', "
                        + "DECIMAL '12345678901234567890123456789.123456789', DATE '1969-12-31', TIME '23:59:59.999', "
                        + "TIMESTAMP '1969-12-31 23:59:59.999999', TIMESTAMP '1969-12-31 23:59:59.999999999', "
                        + "TIMESTAMP '1969-12-31 23:59:59.999 UTC', TIMESTAMP '1969-12-31 23:59:59.999999999 UTC', "
                        + "ARRAY[1, NULL, 3], MAP(ARRAY['k'], ARRAY[CAST(NULL AS INTEGER)]), ROW(7, '世界'), X'00FF', X'0102')";

        assertThat(runner.execute("SELECT * FROM all_types ORDER BY id").getMaterializedRows())
                .containsExactlyElementsOf(runner.execute(expected).getMaterializedRows());
        GenericRow nulls = new GenericRow(schema.getColumns().size());
        nulls.setField(0, 2);
        upsert("all_types", nulls);
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
                                        "SELECT to_unixtime(ltz), CAST(ts AS VARCHAR) FROM all_types ORDER BY id")
                                .getMaterializedRows())
                .isEqualTo(
                        runner.execute(
                                        "SELECT to_unixtime(ltz), CAST(ts AS VARCHAR) FROM all_types ORDER BY id")
                                .getMaterializedRows());
    }

    @Test
    void testPrimitiveValuesThroughSql() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("b", DataTypes.BOOLEAN())
                        .column("tiny", DataTypes.TINYINT())
                        .column("small", DataTypes.SMALLINT())
                        .column("big", DataTypes.BIGINT())
                        .column("f", DataTypes.FLOAT())
                        .column("d", DataTypes.DOUBLE())
                        .primaryKey("id")
                        .build();
        createTable("primitives", schema, 1);
        upsert(
                "primitives",
                GenericRow.of(
                        1,
                        true,
                        Byte.MIN_VALUE,
                        Short.MAX_VALUE,
                        Long.MAX_VALUE,
                        Float.NaN,
                        Double.NEGATIVE_INFINITY));
        assertThat(rows("SELECT * FROM primitives"))
                .containsExactlyElementsOf(
                        rows(
                                "VALUES (1, true, TINYINT '-128', SMALLINT '32767', BIGINT '9223372036854775807', CAST(nan() AS REAL), -infinity())"));
    }

    private TestingKvScanControl createLargeTable(String name) throws Exception {
        createTable(name, SCHEMA, 1);
        try (Table table = connection.getTable(path(name))) {
            UpsertWriter writer = table.newUpsert().createWriter();
            List<CompletableFuture<?>> writes = new ArrayList<>();
            for (int i = 0; i < SNAPSHOT_ROWS; i++) {
                writes.add(writer.upsert(row(i, payload(i))));
            }
            CompletableFuture.allOf(writes.toArray(new CompletableFuture[0]))
                    .get(30, TimeUnit.SECONDS);
        }
        return scanners.get(scanners.size() - 1);
    }

    private void createTable(String name, Schema schema, int buckets) throws Exception {
        admin.createTable(
                        path(name),
                        TableDescriptor.builder().schema(schema).distributedBy(buckets).build(),
                        false)
                .get(30, TimeUnit.SECONDS);
        long tableId = admin.getTableInfo(path(name)).get(30, TimeUnit.SECONDS).getTableId();
        CLUSTER.waitUntilTableReady(tableId);
        for (int bucket = 0; bucket < buckets; bucket++) {
            scanners.add(TestingKvScanControl.forBucket(CLUSTER, new TableBucket(tableId, bucket)));
        }
    }

    private void upsert(String name, InternalRow... rows) throws Exception {
        try (Table table = connection.getTable(path(name))) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (InternalRow row : rows) {
                writer.upsert(row).get(30, TimeUnit.SECONDS);
            }
        }
    }

    private void delete(String name, InternalRow row) throws Exception {
        try (Table table = connection.getTable(path(name))) {
            table.newUpsert().createWriter().delete(row).get(30, TimeUnit.SECONDS);
        }
    }

    private void addColumn(String name) throws Exception {
        admin.alterTable(
                        path(name),
                        Collections.singletonList(
                                TableChange.addColumn(
                                        "extra",
                                        DataTypes.INT(),
                                        null,
                                        TableChange.ColumnPosition.last())),
                        false)
                .get(30, TimeUnit.SECONDS);
    }

    private void awaitClosed(TestingKvScanControl control) throws Exception {
        waitUntil(
                () -> gate.activeSources() == 0,
                Duration.ofSeconds(30),
                "Local source remained active");
        waitUntil(
                () -> control.activeScannerCount() == 0,
                Duration.ofSeconds(30),
                "Remote session remained active");
    }

    private List<List<Object>> rows(String sql) {
        return runner.execute(sql).getMaterializedRows().stream()
                .map(MaterializedRow::getFields)
                .collect(Collectors.toList());
    }

    private static List<List<Object>> expectedSnapshot() {
        List<List<Object>> expected = new ArrayList<>();
        for (int i = 0; i < SNAPSHOT_ROWS; i++) {
            expected.add(Arrays.asList(i, payload(i)));
        }
        return expected;
    }

    private static GenericRow row(int id, String value) {
        return GenericRow.of(id, value == null ? null : BinaryString.fromString(value));
    }

    private static String payload(int id) {
        return "snapshot-value-padding-to-force-many-server-batches-" + id;
    }

    private static TablePath path(String name) {
        return TablePath.of("fluss", name);
    }

    private static void shutdown(ExecutorService executor) throws InterruptedException {
        executor.shutdownNow();
        assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }

    private static Configuration clusterConfig() {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.KV_SCANNER_MAX_BATCH_SIZE, new MemorySize(4096));
        return configuration;
    }
}

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

package org.apache.fluss.flink.sink;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.admin.ProducerOffsetsResult;
import org.apache.fluss.client.admin.RegisterResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;
import org.apache.fluss.flink.sink.testutils.CountingSource;
import org.apache.fluss.flink.sink.testutils.FailingCountingSource;
import org.apache.fluss.flink.sink.undo.RecoveryAction;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.test.util.TestUtils.waitUntilAllTasksAreRunning;
import static org.apache.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Undo Recovery functionality using Aggregation Merge Engine.
 *
 * <p>This test suite verifies aggregation failover recovery in two categories:
 *
 * <h3>Checkpoint Failover Tests (use FailingCountingSource)</h3>
 *
 * <p>These tests use explicit source gates to establish a committed prefix, dirty writes, an
 * injected failure, and complete post-restart emission before checking the stable result.
 *
 * <h3>Savepoint Rescale Tests (use CountingSource)</h3>
 *
 * <p>These tests verify undo recovery when parallelism changes during savepoint restore. Savepoints
 * are required for rescaling because checkpoints are tied to specific parallelism:
 *
 * <ul>
 *   <li><b>Rescale Up</b>: {@link #testRescaleUp()} - Parallelism 1 → 2
 *   <li><b>Rescale Down</b>: {@link #testRescaleDown()} - Parallelism 2 → 1
 * </ul>
 *
 * <p><b>Note:</b> Rescale tests continue to use savepoints because Flink checkpoints are bound to
 * specific parallelism and cannot be used for rescaling. Savepoints provide the portable state
 * format needed for parallelism changes.
 *
 * <p>Failover tests use deterministic source gates, while rescale tests use bounded sources. Both
 * paths verify stable materialized values with exact assertions.
 */
abstract class UndoRecoveryITCase {

    private static final Logger LOG = LoggerFactory.getLogger(UndoRecoveryITCase.class);

    protected static final String CATALOG_NAME = "testcatalog";
    protected static final String DEFAULT_DB = "fluss";
    protected static final int DEFAULT_BUCKET_NUM = 1;
    protected static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(60);
    protected static final long VALUE_PER_RECORD = 10L;

    @TempDir public static File checkpointDir;
    @TempDir public static File savepointDir;
    @TempDir public static File failureMarkerDir;

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(
                            new org.apache.fluss.config.Configuration()
                                    .set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1))
                                    .set(
                                            ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS,
                                            Integer.MAX_VALUE))
                    .setNumOfTabletServers(3)
                    .build();

    protected org.apache.fluss.config.Configuration clientConf;
    protected Connection conn;
    protected Admin admin;
    protected MiniClusterWithClientResource miniCluster;
    protected String bootstrapServers;

    @BeforeEach
    protected void beforeEach() throws Exception {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        bootstrapServers = String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();

        miniCluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(getFileBasedCheckpointsConfig())
                                .setNumberTaskManagers(2)
                                .setNumberSlotsPerTaskManager(2)
                                .build());
        miniCluster.before();
    }

    @AfterEach
    protected void afterEach() throws Exception {
        if (miniCluster != null) {
            miniCluster.after();
            miniCluster = null;
        }
        if (admin != null) {
            admin.close();
            admin = null;
        }
        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    // ==================== Test Methods ====================

    /** Tests DataStream-builder UNDO recovery after a completed checkpoint. */
    @Test
    void testCheckpointFailoverRecovery() throws Exception {
        String tableName = "undo_checkpoint_failover_" + System.currentTimeMillis();
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        String producerId = "undo-datastream-" + System.nanoTime();

        initTableEnvironment(null, false)
                .executeSql(createAggTableDDL(tableName, DEFAULT_BUCKET_NUM, "allow"));
        FailingCountingSource source =
                FailingCountingSource.coordinatedSingleKey(
                        failureMarkerDir, 1L, 10L, 1, 10L, 9, 10L, 3);
        JobClient job = startCoordinatedFailoverDataStreamJob(tablePath, producerId, source);

        retry(
                DEFAULT_TIMEOUT,
                () ->
                        assertThat(lookupSum(tablePath, 1L))
                                .as("Committed SUM prefix")
                                .isEqualTo(10L));
        long initialCheckpointId = triggerCompletedCheckpointAfter(job.getJobID(), -1L);
        assertRecoveryActionObservable(tablePath, producerId, RecoveryAction.UNDO);
        long checkpointLogEndOffset = getLatestLogEndOffset(tablePath, 0);

        source.releaseDirtyEmission();
        waitForLogEndOffsetAtLeast(tablePath, 0, checkpointLogEndOffset + 9);
        retry(
                DEFAULT_TIMEOUT,
                () ->
                        assertThat(lookupSum(tablePath, 1L))
                                .as("Dirty SUM before failure")
                                .isEqualTo(100L));

        triggerFailureAndWaitForRecovery(source);
        long postRecoveryCheckpointId =
                triggerCompletedCheckpointAfter(job.getJobID(), initialCheckpointId);
        assertThat(postRecoveryCheckpointId).isGreaterThan(initialCheckpointId);
        retry(
                DEFAULT_TIMEOUT,
                () ->
                        assertThat(lookupSum(tablePath, 1L))
                                .as("Stable SUM after undo, replay, and recovery emission")
                                .isEqualTo(130L));
        assertProducerOffsetsAbsent(producerId);

        job.cancel().get();
        waitForJobTermination(job, DEFAULT_TIMEOUT);
        assertThat(lookupSum(tablePath, 1L)).isEqualTo(130L);
    }

    /** Tests DataStream-builder NO_OP recovery and one-time stale offset cleanup. */
    @Test
    void testDataStreamMaxFailoverUsesNoOpRecoveryAndCleansStaleOffsets() throws Exception {
        String tableName = "no_op_datastream_max_" + System.currentTimeMillis();
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        String producerId = "no-op-datastream-" + System.nanoTime();

        initTableEnvironment(null, false)
                .executeSql(createMaxAggTableDDL(tableName, producerId, "ignore"));
        long tableId = admin.getTableInfo(tablePath).get().getTableId();
        Map<TableBucket, Long> staleOffsets =
                Collections.singletonMap(new TableBucket(tableId, 0), 0L);
        assertThat(admin.registerProducerOffsets(producerId, staleOffsets).get())
                .isEqualTo(RegisterResult.CREATED);

        FailingCountingSource source =
                FailingCountingSource.coordinatedSingleKey(
                        failureMarkerDir, 1L, 1L, 1, 10L, 1, 20L, 1);
        JobClient job = startCoordinatedFailoverDataStreamJob(tablePath, producerId, source);

        retry(
                DEFAULT_TIMEOUT,
                () ->
                        assertThat(lookupSum(tablePath, 1L))
                                .as("Committed MAX prefix")
                                .isEqualTo(1L));
        assertProducerOffsetsAbsent(producerId);
        long initialCheckpointId = triggerCompletedCheckpointAfter(job.getJobID(), -1L);
        assertProducerOffsetsAbsent(producerId);
        long checkpointLogEndOffset = getLatestLogEndOffset(tablePath, 0);

        source.releaseDirtyEmission();
        waitForLogEndOffsetAtLeast(tablePath, 0, checkpointLogEndOffset + 1);
        retry(
                DEFAULT_TIMEOUT,
                () ->
                        assertThat(lookupSum(tablePath, 1L))
                                .as("Dirty MAX before failure")
                                .isEqualTo(10L));
        assertProducerOffsetsAbsent(producerId);

        triggerFailureAndWaitForRecovery(source);
        assertProducerOffsetsAbsent(producerId);
        long postRecoveryCheckpointId =
                triggerCompletedCheckpointAfter(job.getJobID(), initialCheckpointId);
        assertThat(postRecoveryCheckpointId).isGreaterThan(initialCheckpointId);
        retry(
                DEFAULT_TIMEOUT,
                () ->
                        assertThat(lookupSum(tablePath, 1L))
                                .as("Recovery MAX value must not be dropped")
                                .isEqualTo(20L));
        assertProducerOffsetsAbsent(producerId);

        job.cancel().get();
        waitForJobTermination(job, DEFAULT_TIMEOUT);
        assertThat(lookupSum(tablePath, 1L)).isEqualTo(20L);
        assertProducerOffsetsAbsent(producerId);
        assertThat(admin.registerProducerOffsets(producerId, staleOffsets).get())
                .as("NO_OP must not leave an offset registration behind")
                .isEqualTo(RegisterResult.CREATED);
        admin.deleteProducerOffsets(producerId).get();
    }

    @Test
    void testMixedAggregationSafePartialUpdateUsesNoOpRecovery() throws Exception {
        String tableName = "no_op_mixed_partial_" + System.currentTimeMillis();
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        String producerId = "no-op-mixed-" + System.nanoTime();
        FailingCountingSource source =
                FailingCountingSource.coordinatedSingleKey(
                        failureMarkerDir, 1L, 1L, 1, 10L, 1, 20L, 1);
        TableResult result =
                startFailoverSqlJob(
                        tablePath,
                        createMixedAggTableDDL(tableName, producerId, "ignore"),
                        1,
                        Arrays.asList("id", "max_value"),
                        source,
                        TimeUnit.DAYS.toMillis(1));
        JobClient job =
                result.getJobClient()
                        .orElseThrow(() -> new RuntimeException("JobClient not available"));

        retry(
                DEFAULT_TIMEOUT,
                () ->
                        assertThat(lookupLong(tablePath, 1L, 1))
                                .as("Committed MAX prefix")
                                .isEqualTo(1L));
        long initialCheckpointId = triggerCompletedCheckpointAfter(job.getJobID(), -1L);
        long checkpointLogEndOffset = getLatestLogEndOffset(tablePath, 0);
        assertRecoveryActionObservable(tablePath, producerId, RecoveryAction.NO_OP);

        source.releaseDirtyEmission();
        waitForLogEndOffsetAtLeast(tablePath, 0, checkpointLogEndOffset + 1);
        retry(
                DEFAULT_TIMEOUT,
                () ->
                        assertThat(lookupLong(tablePath, 1L, 1))
                                .as("Dirty MAX before failure")
                                .isEqualTo(10L));

        triggerFailureAndWaitForRecovery(source);
        assertThat(triggerCompletedCheckpointAfter(job.getJobID(), initialCheckpointId))
                .isGreaterThan(initialCheckpointId);
        retry(
                DEFAULT_TIMEOUT,
                () -> assertThat(lookupLong(tablePath, 1L, 1)).as("Recovered MAX").isEqualTo(20L));
        assertProducerOffsetsAbsent(producerId);

        job.cancel().get();
        waitForJobTermination(job, DEFAULT_TIMEOUT);
        assertThat(lookupLong(tablePath, 1L, 1)).isEqualTo(20L);
    }

    /** Tests producer-offset recovery before the first checkpoint. */
    @Test
    void testProducerOffsetRecoveryWithFailover() throws Exception {
        String tableName = "undo_producer_offset_" + System.currentTimeMillis();
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        String producerId = "undo-before-checkpoint-" + System.nanoTime();

        initTableEnvironment(null, false)
                .executeSql(createAggTableDDL(tableName, DEFAULT_BUCKET_NUM, "allow"));
        FailingCountingSource source =
                FailingCountingSource.coordinatedSingleKey(
                        failureMarkerDir, 1L, 10L, 0, 10L, 2, 10L, 5);
        JobClient job = startCoordinatedFailoverDataStreamJob(tablePath, producerId, source);

        long initialLogEndOffset = getLatestLogEndOffset(tablePath, 0);
        source.releaseDirtyEmission();
        waitForLogEndOffsetAtLeast(tablePath, 0, initialLogEndOffset + 2);
        retry(
                DEFAULT_TIMEOUT,
                () ->
                        assertThat(lookupSum(tablePath, 1L))
                                .as("Dirty writes before the first checkpoint")
                                .isEqualTo(20L));
        assertThat(findLatestCompletedCheckpoint(job.getJobID().toHexString()))
                .as("The injected failure must precede every completed checkpoint")
                .isEqualTo(-1L);
        assertRecoveryActionObservable(tablePath, producerId, RecoveryAction.UNDO);

        triggerFailureAndWaitForRecovery(source);
        long firstCheckpointId = triggerCompletedCheckpointAfter(job.getJobID(), -1L);
        assertThat(firstCheckpointId).isGreaterThanOrEqualTo(0L);
        retry(
                DEFAULT_TIMEOUT,
                () ->
                        assertThat(lookupSum(tablePath, 1L))
                                .as("Stable SUM after producer-offset recovery")
                                .isEqualTo(70L));

        job.cancel().get();
        waitForJobTermination(job, DEFAULT_TIMEOUT);
        assertThat(lookupSum(tablePath, 1L)).isEqualTo(70L);
        admin.deleteProducerOffsets(producerId).get();
    }

    /** Tests Rescale Up - undo recovery when parallelism increases (1 -> 2). */
    @Test
    void testRescaleUp() throws Exception {
        runThreePhaseMultiBucketUndoTest(
                "undo_rescale_up",
                2, // buckets
                1, // phase1 & phase2 parallelism
                2, // phase3 parallelism (scale up)
                new int[] {10, 5, 3});
    }

    /** Tests Rescale Down - undo recovery when parallelism decreases (2 -> 1). */
    @Test
    void testRescaleDown() throws Exception {
        runThreePhaseMultiBucketUndoTest(
                "undo_rescale_down",
                2, // buckets
                2, // phase1 & phase2 parallelism
                1, // phase3 parallelism (scale down)
                new int[] {10, 5, 3});
    }

    /**
     * Tests that undo recovery works through the SQL/Table API sink path.
     *
     * <p>The SQL path creates the sink via {@code FlinkTableSink.getSinkRuntimeProvider()} → {@code
     * getFlinkSink()}, which is a different code path from the DataStream API's {@code
     * FlussSinkBuilder}. This test verifies that the SQL path correctly configures and executes
     * undo recovery.
     *
     * <p>Pattern:
     *
     * <ol>
     *   <li>Phase 1 (DataStream API): Write dirty data using CountingSource with a specific
     *       producerId, then cancel without checkpoint — leaving uncommitted writes
     *   <li>Phase 2 (SQL INSERT): Execute SQL INSERT with the same producerId via {@code
     *       sink.producer-id} table option — the SQL sink's UndoRecoveryOperator should undo the
     *       dirty data from Phase 1
     *   <li>Verify: Final result contains only the SQL INSERT data (dirty data undone)
     * </ol>
     */
    @Test
    void testSqlInsertWithUndoRecovery() throws Exception {
        String tableName = "undo_sql_test_" + System.currentTimeMillis();
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        String producerId = "test-producer-sql-" + System.currentTimeMillis();

        // Create table with sink.producer-id option (needed for SQL INSERT in Phase 2)
        initTableEnvironment(null, false)
                .executeSql(
                        String.format(
                                "CREATE TABLE `%s`.`%s` ("
                                        + "  id BIGINT NOT NULL PRIMARY KEY NOT ENFORCED,"
                                        + "  sum_val BIGINT"
                                        + ") WITH ("
                                        + "  'bucket.num' = '1',"
                                        + "  'table.merge-engine' = 'aggregation',"
                                        + "  'fields.sum_val.agg' = 'sum',"
                                        + "  'sink.producer-id' = '%s'"
                                        + ")",
                                DEFAULT_DB, tableName, producerId));

        // Phase 1: Write dirty data via DataStream API with the same producerId, cancel without
        // checkpoint
        JobClient dirtyJob = startBoundedJob(tablePath, producerId, null, 5, 1, false, false);

        // Wait for dirty data to be written
        long dirtySum = 5 * VALUE_PER_RECORD; // 50
        retry(
                DEFAULT_TIMEOUT,
                () -> {
                    Long sum = lookupSum(tablePath, 1L);
                    assertThat(sum).as("Dirty data sum").isEqualTo(dirtySum);
                });

        // Cancel without checkpoint — leaves uncommitted writes
        dirtyJob.cancel().get();
        waitForJobTermination(dirtyJob, DEFAULT_TIMEOUT);

        // Verify dirty data is still visible
        Long sumAfterCancel = lookupSum(tablePath, 1L);
        LOG.info("Sum after dirty job cancel: {} (dirty, uncommitted)", sumAfterCancel);
        assertThat(sumAfterCancel).as("Dirty data should still be visible").isEqualTo(dirtySum);

        // Phase 2: SQL INSERT with the same producerId — triggers undo recovery via SQL path
        StreamTableEnvironment tEnv = initTableEnvironment(null, true);

        // SQL INSERT writes new data — undo recovery should undo the dirty Phase 1 data
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO `%s`.`%s` VALUES (1, 100), (1, 200)",
                                DEFAULT_DB, tableName))
                .await();

        // Verify: final sum = only SQL INSERT data (dirty data undone)
        // If undo recovery didn't work, sum would be 50 + 300 = 350
        // With undo recovery: 300 (dirty 50 undone, then 100 + 200 written)
        long expectedSum = 300L;
        retry(
                DEFAULT_TIMEOUT,
                () -> {
                    Long sum = lookupSum(tablePath, 1L);
                    assertThat(sum)
                            .as(
                                    "Final sum should be SQL INSERT data only (dirty data undone by SQL sink's undo recovery)")
                            .isEqualTo(expectedSum);
                });

        // Verify producer offsets cleaned up
        assertProducerOffsetsAbsent(producerId);
    }

    /**
     * Tests that producer offsets are cleaned up via endInput() for bounded jobs without
     * checkpointing.
     *
     * <p>When a bounded source reaches end of input without a completed checkpoint, {@code
     * endInput()} still removes its producer offsets.
     */
    @Test
    void testBoundedJobCleansUpProducerOffsetsWithoutCheckpoint() throws Exception {
        String tableName = "undo_bounded_cleanup_" + System.currentTimeMillis();
        String producerId = "test-producer-bounded-" + System.currentTimeMillis();

        StreamTableEnvironment tEnv = initTableEnvironment(null, false);
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE `%s`.`%s` ("
                                + "  id BIGINT NOT NULL PRIMARY KEY NOT ENFORCED,"
                                + "  sum_val BIGINT"
                                + ") WITH ("
                                + "  'bucket.num' = '1',"
                                + "  'table.merge-engine' = 'aggregation',"
                                + "  'fields.sum_val.agg' = 'sum',"
                                + "  'sink.producer-id' = '%s'"
                                + ")",
                        DEFAULT_DB, tableName, producerId));

        // Execute bounded SQL INSERT (no checkpointing enabled in initTableEnvironment)
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO `%s`.`%s` VALUES (1, 10), (1, 20), (1, 30)",
                                DEFAULT_DB, tableName))
                .await();

        // Verify data written
        TablePath actualPath = TablePath.of(DEFAULT_DB, tableName);
        retry(
                DEFAULT_TIMEOUT,
                () -> {
                    Long sum = lookupSum(actualPath, 1L);
                    assertThat(sum).as("Sum after bounded write").isEqualTo(60L);
                });

        // Verify producer offsets cleaned up via endInput() path
        assertProducerOffsetsAbsent(producerId);
    }

    // ==================== Reusable Test Patterns ====================

    private void triggerFailureAndWaitForRecovery(FailingCountingSource source) throws Exception {
        source.triggerFailure();
        waitUntil(
                source::hasFailed,
                DEFAULT_TIMEOUT,
                "Timeout waiting for the source failure marker");
        waitUntil(
                source::hasRestarted,
                DEFAULT_TIMEOUT,
                "Timeout waiting for the source restart marker");
        waitUntil(
                source::hasRecoveryEmissionCompleted,
                DEFAULT_TIMEOUT,
                "Timeout waiting for complete recovery emission");
    }

    private void waitForLogEndOffsetAtLeast(
            TablePath tablePath, int bucketId, long minimumLogEndOffset) {
        retry(
                DEFAULT_TIMEOUT,
                () ->
                        assertThat(getLatestLogEndOffset(tablePath, bucketId))
                                .as("Records acknowledged in the Fluss log")
                                .isGreaterThanOrEqualTo(minimumLogEndOffset));
    }

    private void assertRecoveryActionObservable(
            TablePath tablePath, String producerId, RecoveryAction expectedRecoveryAction)
            throws Exception {
        if (expectedRecoveryAction == RecoveryAction.NO_OP) {
            assertThat(admin.getProducerOffsets(producerId).get())
                    .as("NO_OP must not register producer offsets")
                    .isNull();
            return;
        }

        long tableId = admin.getTableInfo(tablePath).get().getTableId();
        TableBucket expectedBucket = new TableBucket(tableId, 0);
        retry(
                DEFAULT_TIMEOUT,
                () -> {
                    ProducerOffsetsResult result = admin.getProducerOffsets(producerId).get();
                    assertThat(result).as("UNDO must register producer offsets").isNotNull();
                    assertThat(result.getProducerId()).isEqualTo(producerId);
                    assertThat(result.getTableOffsets()).containsOnlyKeys(tableId);
                    assertThat(result.getTableOffsets().get(tableId))
                            .containsOnlyKeys(expectedBucket);
                    assertThat(result.getTableOffsets().get(tableId).get(expectedBucket))
                            .isGreaterThanOrEqualTo(0L);
                });
    }

    private long getLatestLogEndOffset(TablePath tablePath, int bucketId) throws Exception {
        try {
            Map<Integer, Long> latestOffsets =
                    admin.listOffsets(
                                    tablePath,
                                    Collections.singleton(bucketId),
                                    new OffsetSpec.LatestSpec())
                            .all()
                            .get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(latestOffsets).containsOnlyKeys(bucketId);
            assertThat(latestOffsets.get(bucketId)).isNotNull();
            return latestOffsets.get(bucketId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    /**
     * Runs a three-phase undo recovery test with multiple buckets and multiple keys.
     *
     * <p>This test verifies that undo recovery works correctly across different buckets when
     * parallelism changes. It uses multiple keys (1, 2, 3) to ensure data is distributed across
     * buckets and verifies each key's sum independently.
     *
     * <p>Pattern: Phase1 (write + savepoint) -> Phase2 (write + cancel) -> Phase3 (restore + undo +
     * write)
     */
    private void runThreePhaseMultiBucketUndoTest(
            String tablePrefix,
            int buckets,
            int phase12Parallelism,
            int phase3Parallelism,
            int[] recordsPerPhase)
            throws Exception {

        String tableName = tablePrefix + "_" + System.currentTimeMillis();
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        String producerId = "test-producer-" + tablePrefix + "-" + System.currentTimeMillis();

        LOG.info(
                "Test: {} with producerId: {}, buckets: {}, parallelism: {} -> {}",
                tablePrefix,
                producerId,
                buckets,
                phase12Parallelism,
                phase3Parallelism);

        initTableEnvironment(null, false).executeSql(createAggTableDDL(tableName, buckets));

        // Phase 1: Write records for multiple keys and take savepoint
        JobClient phase1Job =
                startBoundedJob(
                        tablePath,
                        producerId,
                        null,
                        recordsPerPhase[0],
                        phase12Parallelism,
                        true,
                        true);

        // Wait for data to be written before checkpoint (verify all 3 keys)
        long expectedSumPerKey = recordsPerPhase[0] * VALUE_PER_RECORD;
        retry(
                DEFAULT_TIMEOUT,
                () -> {
                    Long[] sums = lookupSumsForKeys(tablePath, 1L, 2L, 3L);
                    for (int i = 0; i < 3; i++) {
                        assertThat(sums[i])
                                .as("Key " + (i + 1) + " sum before savepoint")
                                .isEqualTo(expectedSumPerKey);
                    }
                });

        waitForCheckpoint(phase1Job.getJobID());

        Long[] sumsAtSavepoint = lookupSumsForKeys(tablePath, 1L, 2L, 3L);
        LOG.info(
                "Sums at savepoint: key1={}, key2={}, key3={}",
                sumsAtSavepoint[0],
                sumsAtSavepoint[1],
                sumsAtSavepoint[2]);

        String savepointPath =
                phase1Job
                        .stopWithSavepoint(
                                false,
                                savepointDir.getAbsolutePath(),
                                SavepointFormatType.CANONICAL)
                        .get(60, TimeUnit.SECONDS);
        waitForJobTermination(phase1Job, DEFAULT_TIMEOUT);
        Long[] sumsAfterPhase1 = lookupSumsForKeys(tablePath, 1L, 2L, 3L);

        // Phase 2: Write more records (will be undone), then cancel
        JobClient phase2Job =
                startBoundedJob(
                        tablePath,
                        producerId,
                        savepointPath,
                        recordsPerPhase[1],
                        phase12Parallelism,
                        true,
                        true);

        // Wait for Phase 2 data to be written (verify all 3 keys)
        retry(
                DEFAULT_TIMEOUT,
                () -> {
                    Long[] sums = lookupSumsForKeys(tablePath, 1L, 2L, 3L);
                    for (int i = 0; i < 3; i++) {
                        long expected =
                                sumsAfterPhase1[i] + (recordsPerPhase[1] * VALUE_PER_RECORD);
                        assertThat(sums[i])
                                .as("Key " + (i + 1) + " sum after Phase 2 write")
                                .isEqualTo(expected);
                    }
                });

        phase2Job.cancel().get();
        waitForJobTermination(phase2Job, DEFAULT_TIMEOUT);

        Long[] sumsAfterPhase2 = lookupSumsForKeys(tablePath, 1L, 2L, 3L);
        LOG.info(
                "Sums after Phase 2: key1={}, key2={}, key3={}",
                sumsAfterPhase2[0],
                sumsAfterPhase2[1],
                sumsAfterPhase2[2]);

        // Phase 3: Restore from savepoint with different parallelism - triggers undo recovery
        JobClient phase3Job =
                startBoundedJob(
                        tablePath,
                        producerId,
                        savepointPath,
                        recordsPerPhase[2],
                        phase3Parallelism,
                        true,
                        true);

        // Wait for Phase 3 data to be written (after undo, verify all 3 keys)
        retry(
                DEFAULT_TIMEOUT,
                () -> {
                    Long[] sums = lookupSumsForKeys(tablePath, 1L, 2L, 3L);
                    for (int i = 0; i < 3; i++) {
                        long expected =
                                sumsAfterPhase1[i] + (recordsPerPhase[2] * VALUE_PER_RECORD);
                        assertThat(sums[i])
                                .as("Key " + (i + 1) + " final sum after undo")
                                .isEqualTo(expected);
                    }
                });

        // Verify producer offsets have been cleaned up after checkpoint.
        waitForCheckpoint(phase3Job.getJobID());
        assertProducerOffsetsAbsent(producerId);

        phase3Job.cancel().get();
        waitForJobTermination(phase3Job, DEFAULT_TIMEOUT);

        // Verify: Phase 2 data undone for all keys, Phase 3 data present
        Long[] finalSums = lookupSumsForKeys(tablePath, 1L, 2L, 3L);
        LOG.info(
                "Final sums: key1={}, key2={}, key3={} (parallelism changed from {} to {})",
                finalSums[0],
                finalSums[1],
                finalSums[2],
                phase12Parallelism,
                phase3Parallelism);

        for (int i = 0; i < 3; i++) {
            long expected = sumsAfterPhase1[i] + (recordsPerPhase[2] * VALUE_PER_RECORD);
            assertThat(finalSums[i])
                    .as(
                            "Key "
                                    + (i + 1)
                                    + " final sum (phase2 undone, parallelism: "
                                    + phase12Parallelism
                                    + " -> "
                                    + phase3Parallelism
                                    + ")")
                    .isEqualTo(expected);
        }
    }

    // ==================== Helper Methods ====================

    private String createAggTableDDL(String tableName, int bucketNum) {
        return createAggTableDDL(tableName, bucketNum, null);
    }

    private String createAggTableDDL(
            String tableName, int bucketNum, @Nullable String deleteBehavior) {
        String deleteBehaviorOption =
                deleteBehavior == null
                        ? ""
                        : String.format(",  'table.delete.behavior' = '%s'", deleteBehavior);
        return String.format(
                "CREATE TABLE `%s`.`%s` ("
                        + "  id BIGINT NOT NULL PRIMARY KEY NOT ENFORCED,"
                        + "  sum_val BIGINT"
                        + ") WITH ("
                        + "  'bucket.num' = '%d',"
                        + "  'table.merge-engine' = 'aggregation',"
                        + "  'fields.sum_val.agg' = 'sum'"
                        + "%s"
                        + ")",
                DEFAULT_DB, tableName, bucketNum, deleteBehaviorOption);
    }

    private String createMaxAggTableDDL(
            String tableName, String producerId, String deleteBehavior) {
        return String.format(
                "CREATE TABLE `%s`.`%s` ("
                        + "  id BIGINT NOT NULL PRIMARY KEY NOT ENFORCED,"
                        + "  max_value BIGINT"
                        + ") WITH ("
                        + "  'bucket.num' = '%d',"
                        + "  'table.merge-engine' = 'aggregation',"
                        + "  'fields.max_value.agg' = 'max',"
                        + "  'sink.producer-id' = '%s',"
                        + "  'table.delete.behavior' = '%s'"
                        + ")",
                DEFAULT_DB, tableName, DEFAULT_BUCKET_NUM, producerId, deleteBehavior);
    }

    private String createMixedAggTableDDL(
            String tableName, String producerId, String deleteBehavior) {
        return String.format(
                "CREATE TABLE `%s`.`%s` ("
                        + "  id BIGINT NOT NULL PRIMARY KEY NOT ENFORCED,"
                        + "  max_value BIGINT,"
                        + "  sum_value BIGINT"
                        + ") WITH ("
                        + "  'bucket.num' = '%d',"
                        + "  'table.merge-engine' = 'aggregation',"
                        + "  'fields.max_value.agg' = 'max',"
                        + "  'fields.sum_value.agg' = 'sum',"
                        + "  'sink.producer-id' = '%s',"
                        + "  'table.delete.behavior' = '%s'"
                        + ")",
                DEFAULT_DB, tableName, DEFAULT_BUCKET_NUM, producerId, deleteBehavior);
    }

    /**
     * Unified method to start a bounded streaming job.
     *
     * @param tablePath target table
     * @param producerId producer ID for undo recovery, or null to use default (Flink Job ID)
     * @param savepointPath savepoint to restore from, or null
     * @param maxRecords max records to emit (per key if multiKey)
     * @param parallelism job parallelism
     * @param multiKey if true, emit to keys 1,2,3 in round-robin
     * @param enableCheckpointing whether to enable periodic checkpoints
     */
    private JobClient startBoundedJob(
            TablePath tablePath,
            @Nullable String producerId,
            @Nullable String savepointPath,
            int maxRecords,
            int parallelism,
            boolean multiKey,
            boolean enableCheckpointing)
            throws Exception {

        Configuration conf = new Configuration();
        if (savepointPath != null) {
            conf.setString("execution.savepoint.path", savepointPath);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);
        if (enableCheckpointing) {
            env.enableCheckpointing(1000);
        }

        CountingSource source =
                multiKey
                        ? CountingSource.multiKey(VALUE_PER_RECORD, maxRecords)
                        : CountingSource.singleKey(1L, VALUE_PER_RECORD, maxRecords);

        DataStreamSource<RowData> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "counting-source");

        FlussSinkBuilder<RowData> sinkBuilder =
                FlussSink.<RowData>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(tablePath.getDatabaseName())
                        .setTable(tablePath.getTableName())
                        .setSerializationSchema(new RowDataSerializationSchema(false, true));
        if (producerId != null) {
            sinkBuilder.setProducerId(producerId);
        }

        stream.sinkTo(sinkBuilder.build()).name("Fluss Sink");

        String jobName = multiKey ? "Multi-Key Undo Test" : "Undo Test (p=" + parallelism + ")";
        return env.executeAsync(jobName);
    }

    private JobClient startCoordinatedFailoverDataStreamJob(
            TablePath tablePath, String producerId, FailingCountingSource source) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(TimeUnit.DAYS.toMillis(1));

        DataStreamSource<RowData> stream =
                env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "failing-counting-source",
                        source.getProducedType());
        FlussSink<RowData> sink =
                FlussSink.<RowData>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(tablePath.getDatabaseName())
                        .setTable(tablePath.getTableName())
                        .setProducerId(producerId)
                        .setSerializationSchema(new RowDataSerializationSchema(false, true))
                        .build();
        stream.sinkTo(sink).name("Fluss Sink");
        return env.executeAsync("Coordinated aggregation failover");
    }

    private TableResult startFailoverSqlJob(
            TablePath tablePath,
            String ddl,
            int parallelism,
            @Nullable List<String> targetColumns,
            FailingCountingSource source,
            long checkpointInterval)
            throws Exception {

        Configuration conf = new Configuration();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);
        env.enableCheckpointing(checkpointInterval);

        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG %s WITH ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tEnv.executeSql("USE CATALOG " + CATALOG_NAME);

        DataStreamSource<RowData> stream =
                env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "failing-counting-source",
                        source.getProducedType());

        tEnv.createTemporaryView(
                "source_table",
                stream,
                Schema.newBuilder()
                        .column("key", DataTypes.BIGINT())
                        .column("value", DataTypes.BIGINT())
                        .build());

        tEnv.executeSql(ddl).await();

        String targetClause =
                targetColumns == null
                        ? ""
                        : targetColumns.stream()
                                .map(column -> "`" + column + "`")
                                .collect(Collectors.joining(", ", " (", ")"));
        String sourceQuery =
                targetColumns == null
                        ? "SELECT * FROM source_table"
                        : "SELECT `key`, `value` FROM source_table";
        return tEnv.executeSql(
                "INSERT INTO `"
                        + tablePath.getDatabaseName()
                        + "`.`"
                        + tablePath.getTableName()
                        + "`"
                        + targetClause
                        + " "
                        + sourceQuery);
    }

    /** Verifies that no producer-offset registration remains for the given producer. */
    private void assertProducerOffsetsAbsent(String producerId) {
        retry(
                DEFAULT_TIMEOUT,
                () -> {
                    ProducerOffsetsResult result = admin.getProducerOffsets(producerId).get();
                    assertThat(result)
                            .as(
                                    "Producer offsets for '"
                                            + producerId
                                            + "' should be cleaned up from ZK")
                            .isNull();
                });
        LOG.info("Verified producer offsets cleaned up for {}", producerId);
    }

    @Nullable
    private Long lookupSum(TablePath tablePath, Long key) throws Exception {
        return lookupLong(tablePath, key, 1);
    }

    @Nullable
    private Long lookupLong(TablePath tablePath, Long key, int fieldIndex) throws Exception {
        try (Table table = conn.getTable(tablePath)) {
            Lookuper lookuper = table.newLookup().createLookuper();
            InternalRow result = lookuper.lookup(row(key)).get().getSingletonRow();
            return result == null ? null : result.getLong(fieldIndex);
        }
    }

    private Long[] lookupSumsForKeys(TablePath tablePath, Long... keys) throws Exception {
        Long[] sums = new Long[keys.length];
        for (int i = 0; i < keys.length; i++) {
            sums[i] = lookupSum(tablePath, keys[i]);
        }
        return sums;
    }

    protected StreamTableEnvironment initTableEnvironment(
            @Nullable String savepointPath, boolean enableCheckpointing) {
        Configuration conf = new Configuration();
        if (savepointPath != null) {
            conf.setString("execution.savepoint.path", savepointPath);
        }

        StreamExecutionEnvironment execEnv =
                StreamExecutionEnvironment.getExecutionEnvironment(conf);
        execEnv.setParallelism(1);
        if (enableCheckpointing) {
            execEnv.enableCheckpointing(1000);
        }

        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(execEnv, EnvironmentSettings.inStreamingMode());

        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG %s WITH ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tEnv.executeSql("USE CATALOG " + CATALOG_NAME);

        return tEnv;
    }

    protected static Configuration getFileBasedCheckpointsConfig() {
        Configuration config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
        config.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, MemorySize.ZERO);
        config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir.toURI().toString());

        // Restart strategy for automatic recovery from checkpoints
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        config.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(1));

        return config;
    }

    protected void waitForJobTermination(JobClient jobClient, Duration timeout) {
        waitUntil(
                () -> jobClient.getJobStatus().get().isTerminalState(),
                timeout,
                "Timeout waiting for job termination");
    }

    protected void waitForCheckpoint(JobID jobId) {
        waitForCompletedCheckpointAfter(jobId, -1L);
    }

    private long triggerCompletedCheckpointAfter(JobID jobId, long minimumCheckpointId)
            throws Exception {
        long deadlineNanos = System.nanoTime() + DEFAULT_TIMEOUT.toNanos();
        while (true) {
            waitUntilAllTasksAreRunning(miniCluster.getRestClusterClient(), jobId);
            long remainingMillis =
                    Math.max(1L, TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()));
            try {
                miniCluster
                        .getMiniCluster()
                        .triggerCheckpoint(jobId)
                        .get(remainingMillis, TimeUnit.MILLISECONDS);
                return waitForCompletedCheckpointAfter(jobId, minimumCheckpointId);
            } catch (ExecutionException e) {
                if (!isTaskReadinessCheckpointFailure(e) || System.nanoTime() >= deadlineNanos) {
                    throw e;
                }
                LOG.info("Checkpoint raced with a task restart; retrying when all tasks run");
                Thread.sleep(50L);
            }
        }
    }

    private boolean isTaskReadinessCheckpointFailure(ExecutionException exception) {
        Throwable cause = exception.getCause();
        return cause instanceof CheckpointException
                && ((CheckpointException) cause).getCheckpointFailureReason()
                        == CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING;
    }

    private long waitForCompletedCheckpointAfter(JobID jobId, long minimumCheckpointId) {
        String jobIdStr = jobId.toHexString();
        waitUntil(
                () -> findLatestCompletedCheckpoint(jobIdStr) > minimumCheckpointId,
                DEFAULT_TIMEOUT,
                "Timeout waiting for a completed checkpoint after "
                        + minimumCheckpointId
                        + " for job "
                        + jobIdStr);
        return findLatestCompletedCheckpoint(jobIdStr);
    }

    private long findLatestCompletedCheckpoint(String jobIdStr) {
        File jobCheckpointDir = new File(checkpointDir, jobIdStr);
        File[] checkpoints =
                jobCheckpointDir.listFiles(
                        file ->
                                file.isDirectory()
                                        && file.getName().startsWith("chk-")
                                        && new File(file, "_metadata").isFile());
        long latestCheckpointId = -1L;
        if (checkpoints != null) {
            for (File checkpoint : checkpoints) {
                try {
                    latestCheckpointId =
                            Math.max(
                                    latestCheckpointId,
                                    Long.parseLong(
                                            checkpoint.getName().substring("chk-".length())));
                } catch (NumberFormatException ignored) {
                    // Ignore unrelated directories with a chk- prefix.
                }
            }
        }
        return latestCheckpointId;
    }
}
